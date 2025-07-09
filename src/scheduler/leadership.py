import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from loguru import logger
from redis.exceptions import LockError

from src.shared.redis_client import RedisClient


class LeadershipManager:
    """
    Manages leader election and lease management for distributed schedulers.

    This class handles:
    - Leader election with automatic failover
    - Leadership lease management with TTL
    - Leadership state tracking and transitions
    - Statistics and monitoring
    """

    def __init__(
        self,
        redis_client: RedisClient,
        scheduler_id: str,
        check_interval: float = 12.0,
        lease_duration: int = 30,
    ):
        """
        Initialize the LeadershipManager.

        Args:
            redis_client: Redis client for leader election
            scheduler_id: Unique identifier for this scheduler
            check_interval: How often to check/renew leadership (seconds)
            lease_duration: Leadership lease duration (seconds)
        """
        self.LEADER_LOCK_KEY = "scheduler_leader_lock"
        self.LEADER_KEY = "scheduler_leader"

        self.redis_client = redis_client
        self.scheduler_id = scheduler_id
        self._check_interval = check_interval
        self._lease_duration = lease_duration
        self.leader_lock_key = self.LEADER_LOCK_KEY

        # Leadership state
        self._is_leader = False
        self._shutdown_event = asyncio.Event()
        self._monitor_task: Optional[asyncio.Task] = None
        self._lock = self.redis_client.client.lock(
            self.leader_lock_key,
            timeout=self._lease_duration,
            blocking=False,
            thread_local=False,
        )

        # Leadership tracking
        self._start_time = datetime.now(timezone.utc)
        self._leadership_transitions = 0
        self._last_transition_time: Optional[datetime] = None
        self._became_leader_at: Optional[datetime] = None

        # Callbacks
        self._on_leadership_acquired: Optional[Callable[[], None]] = None
        self._on_leadership_lost: Optional[Callable[[], None]] = None

    async def start(self) -> None:
        """Start the leadership monitoring task."""
        if self._monitor_task is not None:
            logger.warning("Leadership monitor already started")
            return

        logger.info(f"Starting leadership manager for {self.scheduler_id}")
        self._shutdown_event.clear()
        self._monitor_task = asyncio.create_task(
            self._leadership_monitor(), name=f"leadership_monitor_{self.scheduler_id}"
        )

    async def stop(self) -> None:
        """Stop the leadership monitoring and release leadership."""
        logger.info(f"Stopping leadership manager for {self.scheduler_id}")

        # Stop monitoring
        self._shutdown_event.set()
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

        # Release leadership if we're the leader
        await self._release_leadership()

    def is_leader(self) -> bool:
        """Check if this scheduler is currently the leader."""
        return self._is_leader

    async def get_stats(self) -> Dict[str, Any]:
        """Get leadership statistics."""
        current_time = datetime.now(timezone.utc)
        uptime_seconds = (current_time - self._start_time).total_seconds()

        leadership_duration = (
            (current_time - self._became_leader_at).total_seconds()
            if self._is_leader and self._became_leader_at
            else None
        )

        return {
            "scheduler_id": self.scheduler_id,
            "is_leader": self._is_leader,
            "current_leader": await self.get_current_leader(),
            "uptime_seconds": uptime_seconds,
            "leadership": {
                "transitions": self._leadership_transitions,
                "last_transition_at": self._last_transition_time,
                "became_leader_at": self._became_leader_at,
                "duration_seconds": leadership_duration,
            },
        }

    def on_leadership_acquired(self, callback: Callable[[], None]) -> None:
        """Set callback to be called when leadership is acquired."""
        self._on_leadership_acquired = callback

    def on_leadership_lost(self, callback: Callable[[], None]) -> None:
        """Set callback to be called when leadership is lost."""
        self._on_leadership_lost = callback

    async def get_current_leader(self) -> Optional[str]:
        """Get the current leader scheduler ID."""
        try:
            return await self.redis_client.client.get(self.LEADER_KEY)
        except Exception as e:
            logger.error(f"Failed to get current leader: {e}")
            return None

    async def _release_leadership(self) -> None:
        """Release leadership."""
        if self._is_leader:
            try:
                await self._lock.release()
            except LockError:
                logger.warning(
                    f"Tried to release lock {self.leader_lock_key} but it was not held."
                )
            except Exception as e:
                logger.error(f"Failed to release leadership: {e}")

            self._is_leader = False
            self._log_transition(is_leader=False)
            logger.info(f"Scheduler {self.scheduler_id} released leadership")
            if self._on_leadership_lost:
                try:
                    self._on_leadership_lost()
                except Exception as e:
                    logger.error(f"Error in leadership lost callback: {e}")

    async def _leadership_monitor(self) -> None:
        """Monitor leadership status and handle transitions."""
        logger.info(f"Started leadership monitor for scheduler {self.scheduler_id}")

        while not self._shutdown_event.is_set():
            try:
                if self._is_leader:
                    # We are the leader, renew the lock
                    if not await self._lock.reacquire():
                        # We lost the lock
                        self._is_leader = False
                        self._log_transition(is_leader=False)
                        logger.warning(f"Scheduler {self.scheduler_id} lost leadership")
                        if self._on_leadership_lost:
                            try:
                                self._on_leadership_lost()
                            except Exception as e:
                                logger.error(f"Error in leadership lost callback: {e}")
                    else:
                        # Renew the leader scheduler_id key
                        await self.redis_client.client.set(
                            self.LEADER_KEY, self.scheduler_id, ex=self._lease_duration
                        )

                else:
                    # We are not the leader, try to acquire the lock
                    if await self._lock.acquire():
                        # We got the lock
                        self._is_leader = True
                        self._log_transition(is_leader=True)

                        # Set the current scheduler_id
                        await self.redis_client.client.set(
                            self.LEADER_KEY, self.scheduler_id, ex=self._lease_duration
                        )

                        logger.info(
                            f"Scheduler {self.scheduler_id} acquired leadership"
                        )
                        if self._on_leadership_acquired:
                            try:
                                self._on_leadership_acquired()
                            except Exception as e:
                                logger.error(
                                    f"Error in leadership acquired callback: {e}"
                                )

                await asyncio.sleep(self._check_interval)

            except Exception as e:
                logger.error(f"Error in leadership monitor: {e}")
                self._is_leader = False
                await asyncio.sleep(5.0)

    def _log_transition(self, is_leader: bool) -> None:
        """Log leadership transitions."""
        self._leadership_transitions += 1
        now = datetime.now(timezone.utc)
        self._last_transition_time = now

        if is_leader:
            self._became_leader_at = now
        else:
            self._became_leader_at = None
