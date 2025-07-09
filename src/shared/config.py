import os
from typing import Optional, Dict, Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_prefix="JOBSCHEDULER_",
        extra="ignore",
    )

    # Redis Configuration
    redis_url: str = Field(
        default="redis://localhost:6379", description="Redis connection URL"
    )
    redis_max_connections: int = Field(
        default=10, description="Maximum Redis connections in pool"
    )
    redis_socket_keepalive: bool = Field(
        default=True, description="Enable Redis socket keepalive"
    )
    redis_retry_on_timeout: bool = Field(
        default=True, description="Retry Redis operations on timeout"
    )

    # Scheduler Configuration
    scheduler_host: str = Field(default="0.0.0.0", description="Scheduler API host")
    scheduler_port: int = Field(default=8080, description="Scheduler API port")
    scheduler_workers: int = Field(default=1, description="Number of API workers")
    scheduler_heartbeat_interval: int = Field(
        default=30, description="Heartbeat check interval in seconds"
    )
    scheduler_heartbeat_timeout: int = Field(
        default=90, description="Heartbeat timeout in seconds"
    )
    scheduler_job_allocation_timeout: int = Field(
        default=300, description="Job allocation timeout in seconds"
    )

    # Executor Configuration
    executor_id: Optional[str] = Field(
        default=None, description="Unique executor identifier"
    )
    executor_region: Optional[str] = Field(default=None, description="Executor region")
    executor_datacenter: Optional[str] = Field(
        default=None, description="Executor datacenter"
    )
    executor_labels: Dict[str, str] = Field(
        default_factory=dict, description="Custom executor labels"
    )
    executor_heartbeat_interval: int = Field(
        default=30, description="Executor heartbeat interval in seconds"
    )
    executor_resource_overhead: float = Field(
        default=0.1, description="Resource overhead percentage (0.1 = 10%)"
    )

    # Docker Configuration
    docker_socket: str = Field(
        default="unix:///var/run/docker.sock", description="Docker socket path"
    )
    docker_network: str = Field(
        default="bridge", description="Docker network for containers"
    )
    docker_log_driver: str = Field(
        default="json-file", description="Docker logging driver"
    )
    docker_log_options: Dict[str, str] = Field(
        default_factory=lambda: {"max-size": "10m", "max-file": "3"},
        description="Docker log driver options",
    )
    docker_pull_timeout: int = Field(
        default=600, description="Docker image pull timeout in seconds"
    )
    docker_container_stop_timeout: int = Field(
        default=30, description="Container stop timeout in seconds"
    )

    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format string",
    )
    log_file: Optional[str] = Field(default=None, description="Log file path")
    structured_logging: bool = Field(
        default=True, description="Enable structured JSON logging"
    )

    # Security Configuration
    api_key: Optional[str] = Field(default=None, description="API authentication key")
    allow_privileged_containers: bool = Field(
        default=False, description="Allow privileged container execution"
    )
    allowed_images: Optional[list] = Field(
        default=None,
        description="List of allowed Docker images (if None, all are allowed)",
    )

    # Performance Configuration
    max_concurrent_jobs: int = Field(
        default=10, description="Maximum concurrent jobs per executor"
    )
    job_cleanup_interval: int = Field(
        default=3600, description="Job cleanup interval in seconds"
    )
    job_retention_days: int = Field(
        default=7, description="Job retention period in days"
    )

    # Development Configuration
    debug: bool = Field(default=False, description="Enable debug mode")
    auto_reload: bool = Field(
        default=False, description="Enable auto-reload for development"
    )

    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.debug or os.getenv("ENVIRONMENT", "").lower() in (
            "dev",
            "development",
        )

    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return not self.is_development

    def get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration dict"""
        return {
            "redis_url": self.redis_url,
            "max_connections": self.redis_max_connections,
            "socket_keepalive": self.redis_socket_keepalive,
            "retry_on_timeout": self.redis_retry_on_timeout,
        }

    def get_docker_config(self) -> Dict[str, Any]:
        """Get Docker configuration dict"""
        return {
            "base_url": self.docker_socket,
            "timeout": self.docker_pull_timeout,
        }

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration dict"""
        config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": self.log_format,
                },
                "json": {
                    "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "level": self.log_level,
                    "formatter": "json" if self.structured_logging else "standard",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["default"],
                    "level": self.log_level,
                    "propagate": False,
                },
            },
        }

        if self.log_file:
            config["handlers"]["file"] = {
                "level": self.log_level,
                "formatter": "json" if self.structured_logging else "standard",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": self.log_file,
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
            }
            config["loggers"][""]["handlers"].append("file")

        return config


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get global settings instance"""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def init_settings(**kwargs) -> Settings:
    """Initialize settings with optional overrides"""
    global _settings
    _settings = Settings(**kwargs)
    return _settings
