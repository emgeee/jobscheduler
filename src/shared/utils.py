import asyncio
import socket
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger


def get_current_timestamp() -> datetime:
    """Get current UTC timestamp"""
    return datetime.now(timezone.utc)


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO string"""
    return dt.isoformat()


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse ISO timestamp string to datetime"""
    return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))


def calculate_resource_fit(
    available: Dict[str, Any], required: Dict[str, Any]
) -> float:
    """Calculate how well resources fit (0.0 to 1.0, higher is better)"""
    if not available or not required:
        return 0.0

    cpu_fit = min(1.0, available.get("cpu_cores", 0) / required.get("cpu_cores", 1))
    memory_fit = min(1.0, available.get("memory_mb", 0) / required.get("memory_mb", 1))

    # GPU fit is binary - either it fits or it doesn't
    gpu_fit = 1.0
    if required.get("gpu_count", 0) > 0:
        if available.get("gpu_type") != required.get("gpu_type") or available.get(
            "gpu_count", 0
        ) < required.get("gpu_count", 0):
            gpu_fit = 0.0

    # Weighted average - CPU and memory are most important
    return cpu_fit * 0.4 + memory_fit * 0.4 + gpu_fit * 0.2


def safe_json_serialize(obj: Any) -> Any:
    """Safely serialize object to JSON-compatible format"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, "__dict__"):
        return obj.__dict__
    elif hasattr(obj, "dict"):
        return obj.dict()
    else:
        return str(obj)


def validate_docker_image(
    image: str, allowed_images: Optional[List[str]] = None
) -> bool:
    """Validate Docker image name"""
    if not image or not isinstance(image, str):
        return False

    # Basic format validation
    if any(char in image for char in ["<", ">", "|", "&", ";", "`", "$"]):
        return False

    # Check against allowed images if specified
    if allowed_images is not None:
        return image in allowed_images

    return True


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


def format_bytes(bytes_value: int) -> str:
    """Format bytes to human-readable string"""
    value = float(bytes_value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024.0:
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}PB"


class AsyncTimer:
    """Async context manager for timing operations"""

    def __init__(self, name: str = "operation"):
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.logger = logger

    async def __aenter__(self):
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        if self.start_time is not None:
            duration = self.end_time - self.start_time
            self.logger.info(f"{self.name} took {format_duration(duration)}")

    @property
    def duration(self) -> Optional[float]:
        """Get duration in seconds"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


async def run_with_timeout(
    coro, timeout: float, timeout_error_msg: str = "Operation timed out"
):
    """Run coroutine with timeout"""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(timeout_error_msg)


def retry_with_exponential_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
):
    """Decorator for retrying functions with exponential backoff"""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception: Optional[Exception] = None
            delay = base_delay

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt == max_retries:
                        break

                    await asyncio.sleep(min(delay, max_delay))
                    delay *= backoff_factor

            if last_exception is not None:
                raise last_exception

        return wrapper

    return decorator
