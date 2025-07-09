import uuid
from typing import Optional

from pydantic import Field, IPvAnyAddress, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.shared.models import GPUType, ResourceRequirements


class ExecutorSettings(BaseSettings):
    """Configuration settings."""

    model_config = SettingsConfigDict(
        env_prefix="EXECUTOR_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    id: str = Field(description="executor id")

    # Because our setup is designed to be run in docker-compose, we must manually configure cpus/memory available to the executor. In a real system running natively we could add more logic to automatically introspect these values
    cpus: float = Field(description="Number of CPUs available to the executor")
    memory: int = Field(
        description="Amount of memory (in MB) available to the executor"
    )

    gpus: int = Field(default=0, description="number of GPUs on the machine")
    gpu_type: Optional[GPUType] = Field(default=None, description="type of GPU")
    # Needs to be passed in as env var because executor script running in docker will only be able to retrieve IP address of itself running inside docker container
    ip_address: IPvAnyAddress = Field(description="IP address of the executor")

    @model_validator(mode="after")
    def validate_gpu_type(self):
        if self.gpus > 0 and self.gpu_type is None:
            raise ValueError("gpu_type must be specified when gpus > 0")
        return self

    def get_resource_requirements(self) -> ResourceRequirements:
        return ResourceRequirements(
            cpu_cores=self.cpus,
            memory_mb=self.memory,
            gpu_count=self.gpus,
            gpu_type=self.gpu_type,
        )
