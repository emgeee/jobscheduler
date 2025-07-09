from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel, Field
from starlette.status import HTTP_201_CREATED

from src.scheduler.scheduler import Scheduler
from src.shared.job_manager import JobManager
from src.shared.executor_manager import ExecutorManager
from src.shared.models import (
    Job,
    JobDefinition,
    JobStatus,
    ExecutorInfo,
    AvailableResources,
)


# Request/Response Models
class JobSubmissionRequest(BaseModel):
    definition: JobDefinition = Field(..., description="Job definition")


class JobListResponse(BaseModel):
    jobs: List[Job]


class ErrorResponse(BaseModel):
    detail: str
    error_code: Optional[str] = None


class JobStatsResponse(BaseModel):
    pending: int
    running: int
    succeeded: int
    failed: int
    aborted: int


class SchedulerStatusResponse(BaseModel):
    scheduler_id: str
    is_leader: bool
    current_leader: Optional[str]
    background_tasks: int
    uptime_seconds: float
    redis_connected: bool
    timestamp: datetime


class ExecutorWithResources(BaseModel):
    executor_info: ExecutorInfo
    available_resources: AvailableResources
    assigned_jobs_count: int


class HealthyExecutorsResponse(BaseModel):
    executors: List[ExecutorWithResources]


def get_scheduler(request: Request) -> Scheduler:
    """Get Scheduler instance from app state"""
    return request.app.state.scheduler


def get_job_manager(request: Request) -> JobManager:
    """Get JobManager instance from app state"""
    return request.app.state.job_manager


def get_executor_manager(request: Request) -> ExecutorManager:
    """Get ExecutorManager instance from app state"""
    return request.app.state.executor_manager


def create_app(lifespan: Optional[Callable] = None) -> FastAPI:
    """Create FastAPI application with optional lifespan manager"""
    app = FastAPI(
        title="Job Scheduler API",
        description="Distributed job scheduling system API",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/jobs", response_model=Job, status_code=status.HTTP_201_CREATED)
    async def submit_job(
        request: JobSubmissionRequest,
        job_manager: JobManager = Depends(get_job_manager),
    ) -> Job:
        """Submit a new job for execution"""

        job = await job_manager.create_job(request.definition)
        return job

    @app.get("/jobs/{job_id}", response_model=Job)
    async def get_job(
        job_id: UUID,
        job_manager: JobManager = Depends(get_job_manager),
    ) -> Job:
        """Get job details by ID"""
        job = await job_manager.get_job(job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found",
            )
        return job

    @app.post("/jobs/{job_id}/abort", status_code=status.HTTP_202_ACCEPTED)
    async def abort_job(
        job_id: UUID,
        job_manager: JobManager = Depends(get_job_manager),
    ) -> Job:
        """Abort a running job"""
        try:
            return await job_manager.abort_job(job_id)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e),
            )

    @app.get("/jobs", response_model=JobListResponse)
    async def list_jobs(
        status_filter: Optional[JobStatus] = None,
        job_manager: JobManager = Depends(get_job_manager),
    ) -> JobListResponse:
        """List all jobs"""
        all_jobs = await job_manager.list_jobs()

        # Filter jobs by status if specified
        if status_filter:
            all_jobs = [job for job in all_jobs if job.status == status_filter]

        return JobListResponse(jobs=all_jobs)

    @app.get("/executors/healthy", response_model=HealthyExecutorsResponse)
    async def list_healthy_executors(
        executor_manager: ExecutorManager = Depends(get_executor_manager),
    ) -> HealthyExecutorsResponse:
        """List all healthy executors with their current available resources"""
        try:
            executors_data = (
                await executor_manager.get_all_healthy_executors_with_resources()
            )

            # Convert to response format
            executors = []
            for executor_data in executors_data:
                executor_with_resources = ExecutorWithResources(
                    executor_info=executor_data["executor_info"],
                    available_resources=executor_data["available_resources"],
                    assigned_jobs_count=executor_data["assigned_jobs_count"],
                )
                executors.append(executor_with_resources)

            return HealthyExecutorsResponse(executors=executors)

        except Exception as e:
            logger.error(f"Error fetching healthy executors: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch healthy executors: {str(e)}",
            )

    @app.get("/health")
    async def health_check(
        scheduler: Scheduler = Depends(get_scheduler),
        job_manager: JobManager = Depends(get_job_manager),
    ):
        """Health check endpoint"""
        return {"status": "healthy", "timestamp": datetime.now()}

    @app.get("/status", response_model=SchedulerStatusResponse)
    async def get_job_scheduler_status(
        scheduler: Scheduler = Depends(get_scheduler),
    ) -> SchedulerStatusResponse:
        """Get Scheduler status and statistics

        Returns comprehensive information about the current Scheduler instance including:
        - Scheduler ID and leadership status
        - Background task and queue metrics
        - Job statistics by status
        - System uptime and connectivity status

        This endpoint is useful for monitoring multi-scheduler deployments and debugging
        job processing issues.
        """
        # Get stats from job manager
        stats = await scheduler.get_stats()

        # Check Redis connection
        redis_connected = await scheduler.redis_client.ping()

        # Build response
        return SchedulerStatusResponse(
            scheduler_id=stats["scheduler_id"],
            is_leader=stats["is_leader"],
            current_leader=stats["current_leader"],
            background_tasks=stats["background_tasks"],
            uptime_seconds=stats["uptime_seconds"],
            redis_connected=redis_connected,
            timestamp=stats["timestamp"],
        )

    return app


app = create_app()
