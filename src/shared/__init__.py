"""
Shared components for the job scheduler system.

This module contains common models, utilities, and configuration
used by both the scheduler and executor components.
"""

from .config import Settings, get_settings, init_settings
from .logging_config import LogSettings, setup_logging
from .models import (
    ContainerStatus,
    ExecutorInfo,
    GPUType,
    Job,
    JobDefinition,
    JobStatus,
    ResourceRequirements,
)
from .utils import (
    AsyncTimer,
    calculate_resource_fit,
    format_bytes,
    format_duration,
    format_timestamp,
    get_current_timestamp,
    parse_timestamp,
    retry_with_exponential_backoff,
    run_with_timeout,
    safe_json_serialize,
    validate_docker_image,
)

__all__ = [
    # Models
    "JobStatus",
    "GPUType",
    "ResourceRequirements",
    "JobDefinition",
    "Job",
    "ExecutorInfo",
    "ContainerStatus",
    # Configuration
    "Settings",
    "get_settings",
    "init_settings",
    # Utils
    "get_current_timestamp",
    "format_timestamp",
    "parse_timestamp",
    "calculate_resource_fit",
    "safe_json_serialize",
    "validate_docker_image",
    "format_duration",
    "format_bytes",
    "AsyncTimer",
    "run_with_timeout",
    "retry_with_exponential_backoff",
    # Logging
    "LogSettings",
    "setup_logging",
]
