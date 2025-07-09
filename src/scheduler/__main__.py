import uvicorn
from loguru import logger

from src.scheduler.settings import SchedulerSettings
from src.shared.logging_config import LogSettings


def main():
    """Start the scheduler FastAPI server"""
    # Load settings from environment
    settings = SchedulerSettings()
    log_settings = LogSettings()

    try:
        # Run the server
        uvicorn.run(
            "src.scheduler.app:app",
            host=settings.host,
            port=settings.port,
            log_level=log_settings.log_level.lower(),
            reload=settings.reload,
        )
    except Exception as e:
        logger.error(f"Failed to start scheduler server: {e}")
        raise


if __name__ == "__main__":
    main()
