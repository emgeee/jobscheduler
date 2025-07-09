from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional
from uuid import UUID

from loguru import logger
from redis.asyncio.lock import Lock

from .utils import get_current_timestamp
from .models import Job, JobDefinition, JobStatus, ExecutorInfo
from .redis_client import (
    JOB_INFO_KEY,
    JOB_PENDING_KEY,
    EXECUTOR_ASSIGNED_JOBS_KEY,
    EXECUTOR_INFO_KEY,
    JOB_HEALTHY_KEY,
    RedisClient,
)


class JobNotFound(Exception):
    def __init__(
        self,
        job_id: UUID | str,
        message: str = "Failed to find job",
    ):
        self.job_id = job_id
        super().__init__(f"{message}: job_id: {job_id}")


class JobAssignmentError(Exception):
    def __init__(
        self,
        job_id: UUID | str,
        executor_id: UUID | str,
        message: str = "Failed to assign job",
    ):
        self.job_id = job_id
        super().__init__(f"{message}: job_id: {job_id} executor_id: {executor_id}")


class JobManager:
    """Job manager responsible for updating and interacting with jobs."""

    def __init__(
        self,
        redis_client: RedisClient,
    ):
        self.redis_client = redis_client

        self.JOB_HEALTHY_TIMEOUT = 30

    async def create_job(self, definition: JobDefinition) -> Job:
        """Create a new job and add it to the processing queue."""
        job = Job(definition=definition)  # type: ignore

        job_key = JOB_INFO_KEY.format(job_id=job.id)

        await self.redis_client.set_model(job_key, job)
        await self.redis_client.client.sadd(JOB_PENDING_KEY, str(job.id))  # pyright: ignore reportGeneralTypeIssues

        return job

    async def get_job(self, job_id: UUID | str) -> Job | None:
        job_key = JOB_INFO_KEY.format(job_id=job_id)

        return await self.redis_client.get_model(job_key, Job)

    async def list_jobs(self) -> List[Job]:
        """List all jobs and their corresponding statuses, ordered by creation time (newest first)."""
        # Use SCAN to non-blockingly iterate over all job info keys.
        job_info_keys = [
            key
            async for key in self.redis_client.client.scan_iter(
                match=JOB_INFO_KEY.format(job_id="*")
            )
        ]

        if not job_info_keys:
            return []

        jobs = await self.redis_client.get_models(job_info_keys, Job)
        # Sort by created_at in descending order (newest first)
        return sorted(jobs, key=lambda job: job.created_at, reverse=True)

    async def fetch_pending_jobs(self) -> list[Job]:
        """Fetch pending jobs"""
        job_ids = await self.redis_client.client.smembers(JOB_PENDING_KEY)  # pyright: ignore[reportGeneralTypeIssues]
        info_keys = [JOB_INFO_KEY.format(job_id=job_id) for job_id in job_ids]

        jobs = await self.redis_client.get_models(info_keys, Job)

        return jobs

    async def fetch_executor_jobs(self, executor_id: str) -> list[Job]:
        """Fetch jobs assigned to executor"""
        key = EXECUTOR_ASSIGNED_JOBS_KEY.format(executor_id=executor_id)
        job_ids = await self.redis_client.client.smembers(key)  # pyright: ignore[reportGeneralTypeIssues]
        info_keys = [JOB_INFO_KEY.format(job_id=job_id) for job_id in job_ids]

        jobs = await self.redis_client.get_models(info_keys, Job)

        return jobs

    async def abort_job(self, job_id: UUID) -> Job:
        """Abort a job."""
        async with self.redis_client.lock_job(str(job_id)):
            job = await self.get_job(job_id)
            if job is None:
                raise JobNotFound(job_id)
            # Can't abort a job in a terminal state
            if job.status.is_terminal():
                raise Exception(f"Terminal status job can't be aborted: {job.status}")

            now = get_current_timestamp()
            job.status = JobStatus.ABORTED
            job.updated_at = now
            job.aborted_at = now

            to_update = job.select_fields(["status", "updated_at", "aborted_at"])

            job_key = JOB_INFO_KEY.format(job_id=job.id)
            async with self.redis_client.client.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=to_update)
                # Always try to remove from PENDING
                pipe.srem(JOB_PENDING_KEY, str(job_id))
                await pipe.execute()

            return job

    async def fail_job(
        self,
        job_id: UUID,
        finished_at: Optional[datetime] = None,
        executor_id: Optional[str] = None,
    ) -> Job:
        job_key = JOB_INFO_KEY.format(job_id=job_id)
        executor_assigned_jobs_key = EXECUTOR_ASSIGNED_JOBS_KEY.format(
            executor_id=executor_id
        )
        job_healthy_key = JOB_HEALTHY_KEY.format(job_id=job_id)

        async with self.redis_client.lock_job(str(job_id)):
            job = await self.get_job(job_id)
            if job is None:
                raise JobNotFound(job_id)
            # Can't abort a job in a terminal state
            if job.status.is_terminal():
                raise Exception(f"Terminal status job can't be failed: {job.status}")

            now = get_current_timestamp()
            job.status = JobStatus.FAILED
            job.updated_at = now
            job.finished_at = finished_at if finished_at else now

            to_update = job.select_fields(["status", "updated_at", "finished_at"])

            async with self.redis_client.client.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=to_update)
                # Always try to remove from PENDING
                pipe.srem(JOB_PENDING_KEY, str(job_id))

                if executor_id:
                    pipe.srem(executor_assigned_jobs_key, str(job_id))

                # Remove health
                pipe.delete(job_healthy_key)
                await pipe.execute()

            return job

    async def finish_job(
        self,
        job_id: UUID,
        executor_id: str,
    ) -> Job:
        job_key = JOB_INFO_KEY.format(job_id=job_id)
        executor_assigned_jobs_key = EXECUTOR_ASSIGNED_JOBS_KEY.format(
            executor_id=executor_id
        )
        job_healthy_key = JOB_HEALTHY_KEY.format(job_id=job_id)

        async with self.redis_client.lock_job(str(job_id)):
            job = await self.get_job(job_id)
            if job is None:
                raise JobNotFound(job_id)
            # Can't abort a job in a terminal state
            if job.status.is_terminal():
                raise Exception(f"Terminal status job can't be failed: {job.status}")

            now = get_current_timestamp()
            job.status = JobStatus.SUCCEEDED
            job.updated_at = now
            job.finished_at = now

            to_update = job.select_fields(["status", "updated_at", "finished_at"])

            async with self.redis_client.client.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=to_update)
                pipe.srem(executor_assigned_jobs_key, str(job_id))

                # Remove health key
                pipe.delete(job_healthy_key)
                await pipe.execute()

            return job

    async def assign_job(self, job_id: UUID, executor_id: str):
        job_key = JOB_INFO_KEY.format(job_id=job_id)
        executor_info_key = EXECUTOR_INFO_KEY.format(executor_id=executor_id)
        executor_assigned_jobs_key = EXECUTOR_ASSIGNED_JOBS_KEY.format(
            executor_id=executor_id
        )

        async with self.redis_client.lock_job(str(job_id)):
            job = await self.get_job(job_id)
            if job is None:
                raise JobNotFound(job_id)

            if job.status != JobStatus.PENDING:
                raise JobAssignmentError(
                    job_id=job_id,
                    executor_id=executor_id,
                    message=f"Can't assign job with status={job.status}",
                )

            executor = await self.redis_client.get_model(
                executor_info_key, ExecutorInfo
            )
            now = get_current_timestamp()
            job.status = JobStatus.ASSIGNED
            job.updated_at = now
            job.assigned_at = now
            job.assigned_executor_id = executor_id

            if executor:
                job.ip_address = executor.ip_address

            to_update = job.select_fields(
                [
                    "status",
                    "updated_at",
                    "assigned_at",
                    "assigned_executor_id",
                    "ip_address",
                ]
            )

            async with self.redis_client.client.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=to_update)
                pipe.sadd(executor_assigned_jobs_key, str(job_id))
                pipe.srem(JOB_PENDING_KEY, str(job_id))
                await pipe.execute()

            return job

    async def start_job(self, job_id: UUID):
        job_key = JOB_INFO_KEY.format(job_id=job_id)
        job_healthy_key = JOB_HEALTHY_KEY.format(job_id=job_id)

        async with self.redis_client.lock_job(str(job_id)):
            job = await self.get_job(job_id)
            if job is None:
                raise JobNotFound(job_id)
            # Can't abort a job in a terminal state
            if job.status.is_terminal():
                raise Exception(f"Terminal status job can't be failed: {job.status}")

            now = get_current_timestamp()
            job.status = JobStatus.RUNNING
            job.updated_at = now
            job.started_at = now

            to_update = job.select_fields(["status", "updated_at", "started_at"])

            async with self.redis_client.client.pipeline(transaction=True) as pipe:
                pipe.hset(job_key, mapping=to_update)

                # Set the health key
                pipe.set(job_healthy_key, str(now), ex=self.JOB_HEALTHY_TIMEOUT)
                await pipe.execute()

            return job

    async def update_job_heath_check(self, job_id: UUID):
        job_healthy_key = JOB_HEALTHY_KEY.format(job_id=job_id)
        now = get_current_timestamp()

        await self.redis_client.client.set(
            job_healthy_key, str(now), ex=self.JOB_HEALTHY_TIMEOUT
        )

    async def unassign_job(self, job_id: UUID, executor_id: str):
        key = EXECUTOR_ASSIGNED_JOBS_KEY.format(executor_id=executor_id)
        await self.redis_client.client.srem(key, str(job_id))  # pyright: ignore reportGeneralTypeIssues
