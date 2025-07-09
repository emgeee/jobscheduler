import uuid
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SchedulerSettings(BaseSettings):
    """Configuration settings for the job scheduler."""

    model_config = SettingsConfigDict(
        env_prefix="SCHEDULER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Server settings
    host: str = Field(
        default="0.0.0.0", description="Host to bind the scheduler server to"
    )
    port: int = Field(
        default=8000, ge=1, le=65535, description="Port to bind the scheduler server to"
    )
    reload: bool = Field(
        default=False, description="Enable auto-reload for development"
    )

    id: Optional[str] = Field(default=None, description="name of the scheduler")

    leadership_check_interval: float = Field(
        default=12.0, gt=0, description="Interval in seconds between leadership checks"
    )
    leadership_lease_duration: int = Field(
        default=30, gt=0, description="Leadership lease duration in seconds"
    )

    @model_validator(mode="after")
    def set_default_id(self) -> "SchedulerSettings":
        if self.id is None:
            self.id = f"scheduler-{uuid.uuid4().hex[:8]}"
        return self
