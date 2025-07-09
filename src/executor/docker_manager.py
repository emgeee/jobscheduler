import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import docker
from docker.client import DockerClient
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from docker.types import DeviceRequest, containers
from loguru import logger

from src.shared.models import Job, JobDefinition
from src.shared.utils import get_current_timestamp, parse_timestamp

DOCKER_LABEL = "job_scheduler"
GPU_LABEL = "assigned_gpus"


class DockerManager:
    """Query and interact with the local Docker daemon."""

    def __init__(self, docker_client: DockerClient):
        self.client = docker_client
        self.executor_id = None
        try:
            # Ping the daemon to ensure the connection is alive
            if not self.client.ping():
                # The ping() method returns True on success.
                # In some older versions or scenarios, it might not,
                # so an exception is more likely. This is for robustness.
                raise DockerException("The Docker daemon is not responding.")
            logger.info("Successfully connected to the Docker daemon.")
        except DockerException as e:
            logger.error(f"Failed to connect to Docker daemon: {e}")
            # Re-raise the exception to enforce the "fail-fast" principle.
            # The calling code should handle this.
            raise

    def set_executor_id(self, id: str):
        self.executor_id = id

    def log_docker_info(self):
        # 2. Get server info
        info = self.client.info()

        # 3. Log key details
        logger.info("--- Successfully connected to Docker daemon ---")
        logger.info(f"Host OS: {info.get('OperatingSystem')}")
        logger.info(f"Total CPUs: {info.get('NCPU')}")
        # Convert memory from bytes to GB for readability
        total_mem_gb = info.get("MemTotal", 0) / (1024**3)
        logger.info(f"Total Memory: {total_mem_gb:.2f} GB")
        logger.info(f"Docker Server Version: {info.get('ServerVersion')}")
        logger.info("---------------------------------------------")

    @classmethod
    def from_environment(cls):
        """A factory to create a DockerManager from environment variables."""
        client = docker.from_env()
        return cls(docker_client=client)

    def start_job_container(
        self, job: Job, gpus: Optional[list[str]] = None
    ) -> Container:
        """
        Starts a new Docker container for the given job.

        Args:
            job_id: The unique identifier for the job, used as the container name.
            job_definition: The definition of the job, including the Docker image and command.

        Returns:
            The Docker container object if successful, otherwise None.
        """
        job_definition: JobDefinition = job.definition
        labels = self._add_labels(
            {
                "job_id": str(job.id),
            }
        )
        logger.info(f"Starting Job {job.id} container. Labels {labels}")

        device_request = None
        if gpus:
            logger.info(f"Requesting GPUs {",".join(gpus)}")
            labels[GPU_LABEL] = ",".join(gpus)
            device_request = DeviceRequest(device_ids=gpus, capabilities=[["gpus"]])

        try:
            container = self.client.containers.run(
                image=job_definition.image,
                command=job_definition.command,
                environment=job_definition.environment,
                labels=labels,
                detach=True,  # Run the container in the background
                #############
                ##### Because I'm developing this on a mac, there aren't ACTUALLY GPUs to allocate
                #############
                # device_requests=[device_request] if device_request else None,
            )
            logger.info(f"Job {job.id} started. container {container.name}")
            return container
        except ImageNotFound as e:
            # Handle the case where the image is not found
            print(f"Error: Docker image not found for job {job.id}: {e}")
            raise
        except DockerException as e:
            # Handle other Docker-related errors
            print(f"Error starting container for job {job.id}: {e}")
            raise

    def stop_container(self, container: Container) -> bool:
        """
        Issues a stop command to a running Docker container (fire-and-forget).
        Does not wait for the container to actually stop.

        Args:
            container: The container to stop.

        Returns:
            True if the stop command was issued successfully, otherwise False.
        """

        try:
            job_id = container.labels.get("job_id", "unknown")
            logger.info(f"Issuing stop command for job {job_id}: {container.id}")

            # Issue stop command in background without waiting for completion
            async def _stop_container():
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, lambda: container.stop(timeout=10)
                    )
                    logger.info(f"Container stopped for job {job_id}: {container.id}")
                except Exception as e:
                    logger.error(f"Error stopping container for job {job_id}: {e}")

            # Create the task but don't await it (fire-and-forget)
            asyncio.create_task(_stop_container())

            logger.info(f"Stop command issued for job {job_id}: {container.id}")
            return True

        except Exception as e:
            job_id = container.labels.get("job_id", "unknown")
            logger.error(f"Unexpected error issuing stop command for job {job_id}: {e}")
            return False

    def get_container(self, job_id: str) -> Optional[Container]:
        try:
            # Filter by the job_id label to find the specific container

            filters = self._get_filters({"job_id": job_id})

            containers = self.client.containers.list(all=True, filters=filters)
            if not containers:
                return None
            # Return the 'State' attribute of the first found container
            return containers[0]
        except APIError as e:
            if "invalid filter" in str(e):
                return None
            else:
                raise
        except DockerException as e:
            print(f"Error getting container with job_id {job_id}: {e}")
            raise

    def get_gpu_containers(self) -> list[Container]:
        try:
            filters = self._get_filters({GPU_LABEL: None})

            containers = self.client.containers.list(all=True, filters=filters)
            return containers
        except APIError as e:
            if "invalid filter" in str(e):
                return []
            else:
                raise
        except DockerException as e:
            print(f"Error getting gpu containers: {e}")
            raise

    def get_containers(self) -> list[Container]:
        try:
            filters = self._get_filters({})
            containers = self.client.containers.list(all=True, filters=filters)
            return containers
        except APIError as e:
            if "invalid filter" in str(e):
                return []
            else:
                raise
        except DockerException as e:
            print(f"Error getting gpu containers: {e}")
            raise

    def cleanup_old_containers(
        self, tracked_job_ids: Set[str], max_age_seconds: int = 120
    ) -> int:
        """
        Remove containers that have been stopped for more than max_age_seconds
        and are no longer tracked in the given job IDs.

        Args:
            tracked_job_ids: Set of job IDs that are currently being tracked
            max_age_seconds: Maximum age in seconds for a stopped container before removal

        Returns:
            Number of containers removed
        """
        try:
            # Get all containers with the job scheduler label (both running and stopped)
            all_containers = self.client.containers.list(
                all=True, filters=self._get_filters({})
            )

            current_time = get_current_timestamp()
            containers_to_remove = []

            for container in all_containers:
                # Skip running containers
                if self.is_container_running(container):
                    continue

                # Get the job_id from container labels
                job_id = container.labels.get("job_id")
                if not job_id:
                    continue

                # Skip if job is still tracked
                if job_id in tracked_job_ids:
                    continue

                # Check if container finished more than max_age_seconds ago
                try:
                    finished_at = self.get_container_finished_at(container)
                    if (
                        finished_at
                        and (current_time - finished_at).total_seconds()
                        > max_age_seconds
                    ):
                        containers_to_remove.append(container)
                except Exception as e:
                    logger.warning(
                        f"Could not get finished time for container {container.id}: {e}"
                    )
                    continue

            # Remove old containers
            removed_count = 0
            for container in containers_to_remove:
                try:
                    job_id = container.labels.get("job_id", "unknown")
                    logger.info(
                        f"Removing old container for job {job_id}: {container.id}"
                    )
                    container.remove()
                    removed_count += 1
                except Exception as e:
                    logger.error(f"Failed to remove container {container.id}: {e}")

            return removed_count

        except APIError as e:
            if "invalid filter" in str(e):
                return 0
            else:
                raise
        except Exception as e:
            logger.error(f"Error cleaning old containers: {e}")
            return 0

    def _add_labels(self, labels: dict[str, str | None]):
        """Annotate dict with relevant docker labels"""
        labels[DOCKER_LABEL] = "true"

        if self.executor_id:
            labels["executor_id"] = self.executor_id

        return labels

    def _get_filters(self, filters: dict[str, str | None]):
        """Annotate dict with relevant docker labels"""
        filters = self._add_labels(filters)
        return {
            "label": [f"{k}={v}" if v is not None else k for k, v in filters.items()]
        }

    @staticmethod
    def is_container_running(container: Container) -> bool:
        return container.status in {"running", "paused", "restarting"}

    @staticmethod
    def get_container_started_at(container: Container) -> datetime:
        raw = container.attrs["State"]["StartedAt"]
        return parse_timestamp(raw)

    @staticmethod
    def get_container_finished_at(container: Container) -> datetime:
        raw = container.attrs["State"]["FinishedAt"]
        return parse_timestamp(raw)

    @staticmethod
    def get_gpus(container: Container) -> list[str] | None:
        raw = container.labels.get(GPU_LABEL, None)

        if raw is not None:
            return raw.split(",")
