import os
from contextlib import asynccontextmanager

from loguru import logger

from src.scheduler.api import create_app
from src.shared.job_manager import JobManager
from src.shared.executor_manager import ExecutorManager
from src.scheduler.scheduler import Scheduler
from src.scheduler.settings import SchedulerSettings
from src.shared.logging_config import setup_logging
from src.shared.redis_client import create_client


@asynccontextmanager
async def lifespan(app):
    """Lifespan context manager for FastAPI application"""
    setup_logging()

    # Load settings from environment
    settings = SchedulerSettings()

    logger.info(f"Starting scheduler server on {settings.host}:{settings.port}")

    scheduler_id: str = settings.id  # type: ignore

    # Configure logging
    logger.info(f"Starting scheduler with ID: {scheduler_id}")

    # Initialize Redis client and managers with scheduler ID
    redis_client = create_client()
    job_manager = JobManager(
        redis_client=redis_client,
    )
    executor_manager = ExecutorManager(
        redis_client=redis_client,
    )
    scheduler = Scheduler(
        redis_client=redis_client,
        job_manager=job_manager,
        scheduler_id=scheduler_id,
        leadership_check_interval=settings.leadership_check_interval,
        leadership_lease_duration=settings.leadership_lease_duration,
    )

    # Store references in app state
    app.state.redis_client = redis_client
    app.state.scheduler = scheduler
    app.state.scheduler_id = scheduler_id
    app.state.job_manager = job_manager
    app.state.executor_manager = executor_manager

    # Startup
    logger.info("Starting JobManager background tasks")
    await scheduler.start_background_tasks()

    yield

    # Shutdown
    logger.info("Stopping JobManager background tasks")
    await scheduler.stop_background_tasks()
    await redis_client.close()


# Create app instance for uvicorn import
app = create_app(lifespan)
