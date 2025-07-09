import asyncio
import traceback
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from docker.models.containers import Container
from docker.types import containers
from loguru import logger

from src.executor.docker_manager import DockerManager
from src.shared.job_manager import JobManager
from src.shared.models import (AvailableResources, ExecutorInfo, Job,
                               JobStatus, ResourceRequirements)
from src.shared.redis_client import (EXECUTOR_AVAILABLE_RESOURCES_KEY,
                                     EXECUTOR_HEALTH_KEY, EXECUTOR_INFO_KEY,
                                     RedisClient)


class GpuAssignmentError(Exception):
    def __init__(
        self,
        job_id: UUID | str,
        gpus_requested: str | int,
        gpus_available: str | int,
    ):
        self.job_id = job_id
        super().__init__(
            f"Not enough GPUs to assign! job: {job_id} requested GPUs: {gpus_requested} available gpus: {gpus_available}"
        )


class Executor:
    """Reads jobs from redis and coordinates them via local resources"""

    def __init__(
        self,
        redis_client: RedisClient,
        job_manager: JobManager,
        docker_manager: DockerManager,
        executor_id: str,
        ip_address: str,
        resources: ResourceRequirements,
        heartbeat_interval: int = 5,
    ):
        self.redis_client = redis_client
        self.job_manager = job_manager
        self.docker_manager = docker_manager
        docker_manager.set_executor_id(executor_id)
        self.heartbeat_interval = heartbeat_interval
        self._shutdown_event = asyncio.Event()

        self.info = ExecutorInfo(
            id=executor_id,
            ip_address=ip_address,
            resources=resources,
        )

        self.jobs = []
        self.gpus: dict[str, str | None] = {}

        self.INFO_KEY = EXECUTOR_INFO_KEY.format(executor_id=executor_id)
        self.HEALTH_KEY = EXECUTOR_HEALTH_KEY.format(executor_id=executor_id)
        self.AVAILABLE_RESOURCES_KEY = EXECUTOR_AVAILABLE_RESOURCES_KEY.format(
            executor_id=executor_id
        )
        self.PURGE_OLD_CONTAINERS_AFTER_S = 120

        logger.info(f"Executor initialized with ID: {self.info.id}")

    async def run(self):
        """Main loop for the executor."""

        # logger.info(f"Executor {self.info.id} registered and starting main loop.")

        while not self._shutdown_event.is_set():
            try:
                await self.register()
                await self._heartbeat()
                await self._fetch_assigned_jobs()
                await self._sync_gpu_state()
                await self._set_available_resources()
                await self._sync_jobs()
                await self._clean_old_containers()
                logger.opt(colors=True).info(
                    f"<light-green>Executor {self.info.id} checking jobs</light-green>"
                )
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(f"Error in executor main loop: {e} {trace}")
                await asyncio.sleep(self.heartbeat_interval)

        logger.info(f"Executor {self.info.id} shutting down.")

    async def stop(self):
        """Stops the executor's main loop."""
        self._shutdown_event.set()
        # Remove health key
        await self.redis_client.client.delete(self.HEALTH_KEY)

    async def register(self):
        """Updates redis store with system info"""
        await self.redis_client.set_model(self.INFO_KEY, self.info)

    async def _heartbeat(self):
        """Refreshes the executor's health key in Redis."""
        if not self.info:
            logger.warning("Executor info not set, cannot send heartbeat.")
            return
        await self.redis_client.client.set(
            self.HEALTH_KEY, "true", ex=self.heartbeat_interval * 2
        )
        logger.debug(f"Executor {self.info.id} heartbeat sent.")

    async def _fetch_assigned_jobs(self):
        assigned_jobs = await self.job_manager.fetch_executor_jobs(self.info.id)

        # Cache current assigned jobs. This avoids having to re-fetch later in the loop
        self.jobs = assigned_jobs
        logger.info(f"Num assigned Jobs: {len(self.jobs)}")

    async def _sync_gpu_state(self):
        logger.debug(f"Syncing GPU state {self.gpus}")
        containers = self.docker_manager.get_gpu_containers()

        # clear all GPUs before refreshing
        for i in range(self.info.resources.gpu_count):
            self.gpus[str(i)] = None

        for c in containers:
            if self.docker_manager.is_container_running(c):
                name = c.name
                gpus = self.docker_manager.get_gpus(c)
                if not gpus:
                    continue

                for i in gpus:
                    self.gpus[i] = name
        logger.debug(f"--> New GPU state {self.gpus}")

    async def _set_available_resources(self):
        async with self.redis_client.lock_executor(self.info.id):
            logger.info("Setting available resources")
            _resource = self.info.to_available_resources()

            logger.info(f"Fetching jobs {self.info.id}")

            # calculate and update available resources based on assigned jobs
            # This will keep available resources in sync based off assigned jobs
            for job in self.jobs:
                try:
                    _resource -= job.definition.resources
                except ValueError:
                    logger.error(f"Executor over-assigned {job.definition}")
                    # Future work: this means the scheduler messed up. Kill/fail jobs to bring resources back to correct levels
                    continue

            logger.info(f"Available resources: {_resource}")

            resource_key = EXECUTOR_AVAILABLE_RESOURCES_KEY.format(
                executor_id=self.info.id
            )
            await self.redis_client.set_model(resource_key, _resource)

    async def _sync_jobs(self):
        """Synchronizes job states between Redis and the local Docker daemon"""

        for job in self.jobs:
            logger.info(f"job id: {job.id} status: {job.status}")

            container = self.docker_manager.get_container(str(job.id))
            if container:
                logger.info(f"----- Found container {container.name} Container state {container.status}")

            if job.status == JobStatus.ASSIGNED:
                if container:
                    logger.warning(f"Job {job.id} in assigned state but already exists")
                    # Start the job so it can proceed through the lifecycle
                    await self.job_manager.start_job(job.id)
                    continue

                logger.info(f"======{container}")

                try:
                    gpus_to_assign = self._calculate_gpu_assignment(job)

                    if gpus_to_assign:
                        logger.opt(colors=True).info(
                            f"<light-blue>Assigning GPUs {gpus_to_assign}</light-blue>"
                        )

                    container = self.docker_manager.start_job_container(
                        job,
                        gpus=gpus_to_assign,
                    )

                    # Update current state of gpus
                    # self._allocate_gpus(container.name, gpus_to_assign)
                    await self._sync_gpu_state()

                except GpuAssignmentError as e:
                    logger.error(f"{e}")
                    continue
                except Exception as e:
                    trace = traceback.format_exc()
                    logger.warning(
                        f"Failed to start docker container for job {job.id}: {e} {trace}"
                    )
                    continue

                await self.job_manager.start_job(job.id)

            elif job.status == JobStatus.RUNNING:
                if not container:
                    logger.error(
                        f"Job {job.id} should be running but container does not exist"
                    )
                    # Fail the job
                    await self.job_manager.fail_job(job.id, executor_id=self.info.id)

                    continue

                # Confirm job is actually running and update health check
                if container.status in {"running", "restarting"}:
                    logger.info(f"Job {job.id} running! {container.status}")
                    await self.job_manager.update_job_heath_check(job.id)
                elif container.status in {"created"}:
                    logger.info(f"Job {job.id} created! {container.status} - starting")
                    try:
                        container.start()
                        await self.job_manager.update_job_heath_check(job.id)
                    except:
                        # nvm stop the job
                        logger.error(f"Job {job.id} stuck - {container.status} - stopping and failing")
                        self.docker_manager.stop_container(container)
                        await self.job_manager.fail_job(
                            job.id, executor_id=self.info.id
                        )

                elif container.status in {"paused"}:
                    logger.info(f"Job {job.id} paused! {container.status} - unpausing")
                    container.unpause()
                    await self.job_manager.update_job_heath_check(job.id)
                else:
                    exit_code = container.attrs["State"]["ExitCode"]
                    logger.warning(
                        f"Job {job.id} not running: {container.status} -- exit code: {exit_code}"
                    )

                    if exit_code > 0:
                        # fail job
                        logger.info(f"Job failed with exit code {exit_code}")
                        await self.job_manager.fail_job(
                            job.id, executor_id=self.info.id
                        )
                    else:
                        logger.info(f"Job succeeded!")
                        await self.job_manager.finish_job(job.id, self.info.id)

                    await self._sync_gpu_state()

            elif job.status == JobStatus.FAILED:
                # Edge case: Could occur if scheduler failed job because it wasn't allocated in time
                # Simply remove the job from our ownership
                await self.job_manager.unassign_job(job.id, self.info.id)
            elif job.status == JobStatus.ABORTED:
                if not container:
                    # Container doesn't exist so we're free to un-assign
                    await self.job_manager.unassign_job(job.id, self.info.id)
                    continue

                # Check if contianer is running, if it is, stop it
                # we will release ownership at later time when it has actually stopped
                if self.docker_manager.is_container_running(container):
                    logger.opt(colors=True).info(
                        f"<yellow>Aborting {job.id} -- container name: {container.name}, status {container.status}</yellow>"
                    )
                    self.docker_manager.stop_container(container)
                else:
                    # Container is stopped so we can release ownership
                    await self.job_manager.unassign_job(job.id, self.info.id)
                    await self._sync_gpu_state()
                    # Future work: update job with container run information

    async def _clean_old_containers(self):
        """Remove old containers that have been stopped and are no longer tracked in self.jobs."""
        tracked_job_ids = set(str(job.id) for job in self.jobs)
        removed_count = self.docker_manager.cleanup_old_containers(
            tracked_job_ids, self.PURGE_OLD_CONTAINERS_AFTER_S
        )
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old containers")

    def _calculate_gpu_assignment(self, job: Job) -> list[str] | None:
        gpus_to_assign: list[str] = []
        gpu_reqs = job.definition.resources.gpu_count
        logger.info(f"GPU state {self.gpus}")
        if gpu_reqs > 0:
            for gpu, name in self.gpus.items():
                logger.info(f"{gpu} - {name}")
                if name:
                    # GPU already assigned
                    continue
                else:
                    gpus_to_assign.append(gpu)
            logger.info(f"{gpus_to_assign}")
            if len(gpus_to_assign) < gpu_reqs:
                raise GpuAssignmentError(
                    job_id=job.id,
                    gpus_requested=gpu_reqs,
                    gpus_available=len(gpus_to_assign),
                )

            gpus_to_assign = gpus_to_assign[:gpu_reqs]
            return gpus_to_assign
