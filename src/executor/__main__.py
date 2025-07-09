import asyncio
import signal
import traceback

from loguru import logger

from src.executor.docker_manager import DockerManager
from src.executor.executor import Executor
from src.executor.settings import ExecutorSettings
from src.shared.job_manager import JobManager
from src.shared.logging_config import setup_logging
from src.shared.redis_client import create_client


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
) -> None:
    def shutdown() -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)


async def main() -> None:
    setup_logging()

    settings = ExecutorSettings()  # pyright: ignore[reportCallIssue]
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    install_signal_handlers(loop, stop_event)

    redis_client = create_client()
    job_manager = JobManager(redis_client)
    docker_manager = DockerManager.from_environment()

    docker_manager.log_docker_info()

    executor = Executor(
        redis_client=redis_client,
        job_manager=job_manager,
        docker_manager=docker_manager,
        executor_id=settings.id,
        ip_address=str(settings.ip_address),
        resources=settings.get_resource_requirements(),
    )

    logger.info("Starting executor. Press Ctrl+C to stop.")

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(executor.run())
            # Wait for shutdown request
            await stop_event.wait()
            await executor.stop()
            logger.info("Executor shutting down...")
    except* Exception as eg:
        trace = traceback.format_exc()
        logger.exception(f"Executor failed with unhandled exception(s): {eg} {trace}")
    finally:
        await redis_client.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
