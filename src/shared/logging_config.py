import sys

from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings


class LogSettings(BaseSettings):
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )


_CONFIGURED = False


def setup_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    settings = LogSettings()
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level.upper(),
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>",
        backtrace=True,
        diagnose=True,
    )
    _CONFIGURED = True
