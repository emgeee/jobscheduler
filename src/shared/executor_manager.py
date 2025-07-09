from typing import Dict, List, Optional
from uuid import UUID

from loguru import logger

from .models import ExecutorInfo, AvailableResources
from .redis_client import (
    EXECUTOR_INFO_KEY,
    EXECUTOR_AVAILABLE_RESOURCES_KEY,
    EXECUTOR_HEALTH_KEY,
    EXECUTOR_ASSIGNED_JOBS_KEY,
    RedisClient,
)


class ExecutorNotFound(Exception):
    def __init__(
        self,
        executor_id: str,
        message: str = "Failed to find executor",
    ):
        self.executor_id = executor_id
        super().__init__(f"{message}: executor_id: {executor_id}")


class ExecutorManager:
    """Executor manager responsible for fetching and managing executor information."""

    def __init__(self, redis_client: RedisClient):
        self.redis_client = redis_client

    async def get_healthy_executors(self) -> List[str]:
        """
        Get list of healthy executor IDs by scanning for health keys.

        Returns:
            List of executor IDs that have active health keys
        """
        try:
            # Use SCAN to find all health keys
            health_keys = [
                key
                async for key in self.redis_client.client.scan_iter(
                    match=EXECUTOR_HEALTH_KEY.format(executor_id="*")
                )
            ]

            if not health_keys:
                return []

            # Extract executor IDs from health keys
            healthy_executor_ids = []
            for key in health_keys:
                # Key format: "executor:healthy:{executor_id}"
                # Extract the executor_id part
                if isinstance(key, bytes):
                    key = key.decode("utf-8")

                parts = key.split(":")
                if len(parts) >= 3:
                    executor_id = parts[2]
                    healthy_executor_ids.append(executor_id)

            logger.info(f"Found {len(healthy_executor_ids)} healthy executors")
            return healthy_executor_ids

        except Exception as e:
            logger.error(f"Error fetching healthy executors: {e}")
            return []

    async def get_executor_info(self, executor_id: str) -> Optional[ExecutorInfo]:
        """Get executor info by ID."""
        executor_info_key = EXECUTOR_INFO_KEY.format(executor_id=executor_id)
        return await self.redis_client.get_model(executor_info_key, ExecutorInfo)

    async def get_executor_available_resources(
        self, executor_id: str
    ) -> Optional[AvailableResources]:
        """Get executor available resources by ID."""
        resources_key = EXECUTOR_AVAILABLE_RESOURCES_KEY.format(executor_id=executor_id)
        return await self.redis_client.get_model(resources_key, AvailableResources)

    async def get_executor_assigned_jobs_count(self, executor_id: str) -> int:
        """Get the number of assigned jobs for an executor."""
        try:
            assigned_jobs_key = EXECUTOR_ASSIGNED_JOBS_KEY.format(
                executor_id=executor_id
            )
            count = await self.redis_client.client.scard(assigned_jobs_key)  # type: ignore
            return count or 0
        except Exception as e:
            logger.error(f"Error fetching assigned jobs count for {executor_id}: {e}")
            return 0

    async def get_executor_info_with_resources(
        self, executor_id: str
    ) -> Optional[Dict]:
        """
        Get executor info combined with available resources.

        Args:
            executor_id: The executor ID to fetch info for

        Returns:
            Dictionary containing executor info and available resources, or None if not found
        """
        try:
            # Fetch info, resources, and assigned jobs count concurrently
            info = await self.get_executor_info(executor_id)
            resources = await self.get_executor_available_resources(executor_id)
            assigned_jobs_count = await self.get_executor_assigned_jobs_count(
                executor_id
            )

            if info is None:
                logger.warning(f"Executor info not found for {executor_id}")
                return None

            if resources is None:
                logger.warning(f"Executor resources not found for {executor_id}")
                return None

            return {
                "executor_info": info,
                "available_resources": resources,
                "assigned_jobs_count": assigned_jobs_count,
            }

        except Exception as e:
            logger.error(
                f"Error fetching executor info with resources for {executor_id}: {e}"
            )
            return None

    async def get_all_healthy_executors_with_resources(self) -> List[Dict]:
        """
        Get all healthy executors with their info and available resources.

        Returns:
            List of dictionaries containing executor info and available resources
        """
        healthy_executor_ids = await self.get_healthy_executors()

        if not healthy_executor_ids:
            return []

        executors_with_resources = []

        for executor_id in healthy_executor_ids:
            executor_data = await self.get_executor_info_with_resources(executor_id)
            if executor_data:
                executors_with_resources.append(executor_data)

        logger.info(
            f"Retrieved {len(executors_with_resources)} healthy executors with resources"
        )
        return executors_with_resources
