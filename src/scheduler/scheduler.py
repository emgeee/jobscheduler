import asyncio
from datetime import datetime, timedelta
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from loguru import logger

from src.shared.utils import get_current_timestamp
from src.shared.models import (
    Job,
    JobDefinition,
    JobStatus,
    ResourceRequirements,
    AvailableResources,
)
from src.shared.redis_client import (
    RedisClient,
    EXECUTOR_HEALTH_KEY,
    EXECUTOR_AVAILABLE_RESOURCES_KEY,
)
from src.shared.job_manager import JobAssignmentError, JobManager

from .leadership import LeadershipManager


class Scheduler:
    def __init__(
        self,
        redis_client: RedisClient,
        job_manager: JobManager,
        scheduler_id: str,
        leadership_check_interval: float = 12.0,
        leadership_lease_duration: int = 30,
    ):
        self.redis_client = redis_client
        self.job_manager = job_manager
        self.scheduler_id = scheduler_id
        self._shutdown_event = asyncio.Event()
        self._background_tasks: List[asyncio.Task] = []
        self._start_time = datetime.now(timezone.utc)

        # Initialize leadership manager
        self._leadership_manager = LeadershipManager(
            redis_client=redis_client,
            scheduler_id=self.scheduler_id,
            check_interval=leadership_check_interval,
            lease_duration=leadership_lease_duration,
        )

        self.ALLOCATION_TIMEOUT_S = 15

    def is_leader(self) -> bool:
        """Check if this scheduler is the current leader."""
        return self._leadership_manager.is_leader()

    async def start_background_tasks(self) -> None:
        """Start all background processing tasks on all schedulers."""
        logger.info("Starting JobManager background tasks")

        # Start leadership manager
        await self._leadership_manager.start()

        self._background_tasks = [
            asyncio.create_task(
                self._process_pending_jobs(), name="process_pending_jobs"
            ),
            # Leader-only tasks are also started but will be dormant on non-leaders
            asyncio.create_task(
                self._process_assigned_jobs(), name="process_assigned_jobs"
            ),
            asyncio.create_task(
                self._process_running_jobs(), name="process_running_jobs"
            ),
        ]

        logger.info(f"Started {len(self._background_tasks)} background tasks.")

    async def stop_background_tasks(self) -> None:
        """Stop background processing tasks."""
        logger.info("Stopping JobManager background tasks")

        # Stop leadership manager
        await self._leadership_manager.stop()

        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        logger.info("Stopped all background tasks")

    async def fetch_healthy_executors_resources(
        self,
    ) -> list[AvailableResources]:
        keys = [
            key
            async for key in self.redis_client.client.scan_iter(
                match=EXECUTOR_HEALTH_KEY.format(executor_id="*")
            )
        ]

        if not keys:
            return []

        executor_ids = []
        keys_to_fetch = []

        for key in keys:
            executor_id_str = key.split(":")[-1]
            executor_ids.append(executor_id_str)
            keys_to_fetch.append(
                EXECUTOR_AVAILABLE_RESOURCES_KEY.format(executor_id=executor_id_str)
            )

        resources = await self.redis_client.get_models(
            keys_to_fetch, AvailableResources
        )
        return resources

    async def _process_pending_jobs(self) -> None:
        """Background task to process pending jobs (runs only on leader)."""
        logger.info(f"Started pending jobs processor for scheduler {self.scheduler_id}")

        while not self._shutdown_event.is_set():
            try:
                if not self.is_leader():
                    await asyncio.sleep(1.0)
                    continue

                logger.opt(colors=True).info(
                    f"<green>Leader {self.scheduler_id} processing pending jobs</green>"
                )

                await self._assign_jobs()
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in pending jobs processor: {e}")
                await asyncio.sleep(5.0)

    async def _assign_jobs(self):
        available_resources = await self.fetch_healthy_executors_resources()
        pending_jobs = await self.job_manager.fetch_pending_jobs()

        # logger.info(f"Available resources: {available_resources}")
        logger.info(f"pending jobs: {len(pending_jobs)}")

        available_map = {r.id: r for r in available_resources}

        for job in pending_jobs:
            # check if Job has been pending for too long and should be failed
            delta = abs(get_current_timestamp() - job.created_at)
            if delta > timedelta(seconds=self.ALLOCATION_TIMEOUT_S):
                logger.warning(
                    f"Job {job.id} failed to be allocated after {self.ALLOCATION_TIMEOUT_S} seconds"
                )
                await self.job_manager.fail_job(job.id)
                continue

            resource = AvailableResources.best_fit(
                available_map.values(), job.definition.resources
            )
            if not resource:
                logger.warning(f"No candidates for job {job.id}")
                continue

            logger.info(f"assigning job: {job.id} to executor: {resource.id}")

            try:
                async with self.redis_client.lock_executor(resource.id):
                    # query the resource again to make sure it hasn't changed
                    resource_key = EXECUTOR_AVAILABLE_RESOURCES_KEY.format(
                        executor_id=resource.id
                    )
                    _resource = await self.redis_client.get_model(
                        resource_key,
                        AvailableResources,
                    )

                    if not _resource or _resource != resource:
                        # available resource was updated during assignment so we
                        # should throw out assignment and try again next loop
                        logger.warning(
                            f"Resource allocation changed for {resource.id} when assigning {job.id}"
                        )
                        continue

                    # Update in memory view of available resources so subsequent iterations don't over-allocate
                    available_map[resource.id] = resource - job.definition.resources

                    # Update available resources back to redis. This allows the scheduler
                    # to have an optimistic view of available resources in-case the
                    # executor takes too long to start the job and update this entry itself
                    await self.redis_client.set_model(
                        resource_key, available_map[resource.id]
                    )

                    # If we fail to assign but have already updated the available resources
                    # they will be reverted next tick by the executor
                    await self.job_manager.assign_job(job.id, resource.id)

            except Exception as e:
                logger.error(f"Error in job assignment: {e}")

    async def _process_assigned_jobs(self) -> None:
        """Background task to process orphaned jobs in the assigned state (runs only on leader)."""
        logger.info(
            f"Started assigned jobs processor for scheduler {self.scheduler_id}"
        )

        while not self._shutdown_event.is_set():
            try:
                if not self.is_leader():
                    await asyncio.sleep(1.0)
                    continue

                # TODO

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in assigned jobs processor: {e}")
                await asyncio.sleep(5.0)

    async def _process_running_jobs(self) -> None:
        """Background task to garbage collect orphaned jobs in running state (runs only on leader)"""
        logger.info(f"Started running jobs processor for scheduler {self.scheduler_id}")

        while not self._shutdown_event.is_set():
            try:
                if not self.is_leader():
                    await asyncio.sleep(1.0)
                    continue

                # TODO

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in running jobs processor: {e}")
                await asyncio.sleep(5.0)

    async def get_stats(self) -> Dict[str, Any]:
        """Get job manager statistics."""
        current_time = datetime.now(timezone.utc)
        uptime_seconds = (current_time - self._start_time).total_seconds()

        # Get leadership stats from leadership manager
        leadership_stats = await self._leadership_manager.get_stats()

        return {
            "scheduler_id": self.scheduler_id,
            "is_leader": self.is_leader(),
            "current_leader": self._leadership_manager.get_current_leader(),
            "background_tasks": len(self._background_tasks),
            "uptime_seconds": uptime_seconds,
            "timestamp": current_time,
            "leadership": leadership_stats["leadership"],
        }
