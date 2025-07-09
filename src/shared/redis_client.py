from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Type, TypeVar
from uuid import UUID

import redis.asyncio as redis
from loguru import logger
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, RedisError

from .models import FlatRedisModel, TF

T = TypeVar("T", bound=BaseModel)

EXECUTOR_INFO_KEY = "executor:info:{executor_id}"  # Stores json of executor info
EXECUTOR_AVAILABLE_RESOURCES_KEY = (
    "executor:available_resources:{executor_id}"  # stores json of available resources
)
EXECUTOR_HEALTH_KEY = "executor:healthy:{executor_id}"
EXECUTOR_ASSIGNED_JOBS_KEY = (
    "executor:assigned_jobs:{executor_id}"  # set of assigned jobs to executor
)

JOB_INFO_KEY = "jobs:info:{job_id}"

# JOB_CURRENT_STATUS_KEY = "jobs:current_status:{job_id}"  # current status of job_id
JOB_PENDING_KEY = "jobs:pending"  # list of all pending jobs
JOB_EXECUTOR_KEY = "jobs:executor:{job_id}"  # executor ID job is assigned to

JOB_PENDING_AT = "jobs:pending_at:{job_id}"
JOB_HEALTHY_KEY = "jobs:healthy:{job_id}"


def _get_env_file() -> str:
    """Get the appropriate .env file based on the environment"""
    import os
    import sys

    # Check if we're running tests
    if "pytest" in sys.modules or "PYTEST_CURRENT_TEST" in os.environ:
        return ".env.test"
    else:
        return ".env"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_file=_get_env_file(),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Redis settings
    host: str = Field(default="localhost", description="Redis server host")
    port: int = Field(default=6379, ge=1, le=65535, description="Redis server port")
    db: int = Field(default=0, ge=0, description="Redis database number")
    password: Optional[str] = Field(
        default=None, description="Redis password (if required)"
    )
    max_connections: int = Field(
        default=10, ge=1, description="Maximum Redis connections in pool"
    )
    socket_timeout: float = Field(
        default=5.0, gt=0, description="Redis socket timeout in seconds"
    )
    socket_connect_timeout: float = Field(
        default=5.0, gt=0, description="Redis socket connect timeout in seconds"
    )
    retry_on_timeout: bool = Field(
        default=True, description="Whether to retry Redis operations on timeout"
    )


class RedisClient:
    """
    Async Redis client for the job scheduling system.
    """

    def __init__(self, settings: RedisSettings):
        self.LOCK_PREFIX = "lock"
        self.client = redis.Redis(
            host=settings.host,
            port=settings.port,
            db=settings.db,
            password=settings.password,
            max_connections=settings.max_connections,
            socket_timeout=settings.socket_timeout,
            socket_connect_timeout=settings.socket_connect_timeout,
            decode_responses=True,
        )

    async def ping(self) -> bool:
        try:
            return await self.client.ping()
        except RedisError:
            return False

    async def close(self) -> None:
        await self.client.aclose()

    async def set_model(self, key: str, model: FlatRedisModel) -> int:
        """Serialize a flattened pydantic model to a key"""
        redis_data = model.to_redis_dict()
        return await self.client.hset(key, mapping=redis_data)  # pyright: ignore reportGeneralTypeIssues

    async def get_model(self, key: str, model_cls: Type[TF]) -> Optional[TF]:
        """Retrieve a flattened pydantic model from a key"""
        try:
            data = await self.client.hgetall(key)  # pyright: ignore reportGeneralTypeIssues
            if data:
                return model_cls.from_redis_dict(data)
            return None
        except (RedisError, ValueError) as e:
            logger.error(f"Failed to get model for key {key}: {e}")
            return None

    async def get_models(self, keys: list[str], model_cls: Type[TF]) -> list[TF]:
        """Retrieve multiple flattened pydantic models"""
        async with self.client.pipeline() as pipe:
            for key in keys:
                pipe.hgetall(key)
            raw_values = await pipe.execute()

            results = []
            for value in raw_values:
                if value is not None:
                    try:
                        results.append(model_cls.from_redis_dict(value))
                    except (ValueError, TypeError) as e:
                        logger.error(f"Failed to parse model for a key: {value} {e}")
            return results

    @asynccontextmanager
    async def lock(
        self,
        name: str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout=10.0,
    ) -> AsyncGenerator[Lock, None]:
        lock_key = f"{self.LOCK_PREFIX}:{name}"
        lock = self.client.lock(
            lock_key,
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        )
        try:
            if not await lock.acquire(blocking=blocking):
                raise LockError(f"Could not acquire lock {name}")
            yield lock
        finally:
            try:
                await lock.release()
            except LockError:
                pass

    @asynccontextmanager
    async def lock_job(
        self,
        job_id: UUID | str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout: float = 10.0,
    ) -> AsyncGenerator[Lock, None]:
        lock_name = f"jobs:{job_id}"
        async with self.client.lock(
            name=lock_name,
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        ) as lock:
            yield lock

    @asynccontextmanager
    async def lock_executor(
        self,
        job_id: UUID | str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout: float = 10.0,
    ) -> AsyncGenerator[Lock, None]:
        lock_name = f"executors:{job_id}"
        async with self.client.lock(
            name=lock_name,
            timeout=timeout,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
        ) as lock:
            yield lock

    # --- Utility ---
    async def clear_all_data(self) -> bool:
        try:
            await self.client.flushdb()
            return True
        except RedisError as e:
            logger.error(f"Failed to clear all data: {e}")
            return False


def create_client() -> RedisClient:
    """Create a new redis client for env configuration"""
    redis_settings = RedisSettings()
    logger.info(f"Redis connection: {redis_settings.host}:{redis_settings.port}")

    return RedisClient(redis_settings)
