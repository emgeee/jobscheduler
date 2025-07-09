from datetime import datetime, timezone
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Iterable
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from typing import Any, Dict, Type, TypeVar
from pydantic import BaseModel

TF = TypeVar("TF", bound="FlatRedisModel")


class FlatRedisModel(BaseModel):
    """Mixin that adds Redis-safe flatten/unflatten behavior."""

    def to_flattened_dict(self, sep: str = ".") -> dict[str, Any]:
        return self._flatten(self.model_dump(), sep=sep)

    def to_redis_dict(self, sep: str = ".") -> dict[str, str]:
        """Convert model to Redis-safe dictionary with proper serialization."""

        def custom_json_serializer(obj):
            if isinstance(obj, UUID):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, Enum):
                return obj.value
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not JSON serializable"
            )

        flattened = self.to_flattened_dict(sep=sep)
        # Filter out None values and convert non-string types to strings
        filtered = {}
        for k, v in flattened.items():
            if v is not None:
                # Handle different types appropriately
                if isinstance(v, (UUID, datetime)):
                    # Convert UUID and datetime to strings directly
                    filtered[k] = str(v)
                elif isinstance(v, Enum):
                    # Convert enums to their values
                    filtered[k] = v.value
                elif isinstance(v, (str, int, float, bool)):
                    # Keep basic types as-is
                    filtered[k] = v
                else:
                    # Use JSON serialization for complex types like lists/dicts
                    filtered[k] = json.dumps(v, default=custom_json_serializer)
        return filtered

    def select_fields(self, fields: list[str]) -> dict[str, str]:
        flat = self.to_flattened_dict()
        selected: dict[str, str] = {}
        for field in fields:
            if field not in flat:
                raise KeyError(f"Field '{field}' does not exist")
            value = flat[field]
            # Handle enum serialization properly
            if isinstance(value, Enum):
                selected[field] = value.value
            else:
                selected[field] = str(value)
        return selected

    @classmethod
    def from_flattened_dict(cls: Type[TF], flat: dict[str, Any], sep: str = ".") -> TF:
        nested = cls._unflatten(flat, sep=sep)
        return cls.model_validate(nested)

    @classmethod
    def from_redis_dict(
        cls: Type[TF], redis_data: dict[str, Any], sep: str = "."
    ) -> TF:
        """Create model from Redis data with proper deserialization."""
        # Attempt to deserialize JSON-encoded fields
        processed_data = {}
        for k, v in redis_data.items():
            if isinstance(v, str) and v.startswith(("[", "{")):
                # Try to parse as JSON
                try:
                    processed_data[k] = json.loads(v)
                except json.JSONDecodeError:
                    processed_data[k] = v
            else:
                processed_data[k] = v

        return cls.from_flattened_dict(processed_data, sep=sep)

    @staticmethod
    def _flatten(d: Dict[str, Any], parent: str = "", sep: str = ".") -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in d.items():
            key = f"{parent}{sep}{k}" if parent else k
            if isinstance(v, dict):
                out.update(FlatRedisModel._flatten(v, key, sep=sep))
            else:
                out[key] = v
        return out

    @staticmethod
    def _unflatten(flat: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
        tree: dict[str, Any] = {}
        for compound_key, value in flat.items():
            parts = compound_key.split(sep)
            current = tree
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = value
        return tree


class JobStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"

    def is_terminal(self) -> bool:
        return self in {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.ABORTED,
        }


class GPUType(str, Enum):
    NVIDIA_T4 = "nvidia-t4"
    NVIDIA_A100 = "nvidia-a100"
    NVIDIA_V100 = "nvidia-v100"
    NVIDIA_K80 = "nvidia-k80"
    NVIDIA_P100 = "nvidia-p100"
    NVIDIA_RTX_3080 = "nvidia-rtx-3080"
    NVIDIA_RTX_3090 = "nvidia-rtx-3090"
    NVIDIA_RTX_4090 = "nvidia-rtx-4090"


class ResourceRequirements(BaseModel):
    cpu_cores: float = Field(..., ge=0, description="Number of CPU cores required")
    memory_mb: int = Field(..., ge=0, description="Memory in MB required")
    gpu_type: Optional[GPUType] = Field(None, description="GPU type required")
    gpu_count: int = Field(0, ge=0, description="Number of GPUs required")

    @field_validator("gpu_count")
    @classmethod
    def validate_gpu_count(cls, v, info):
        if v > 0 and not info.data.get("gpu_type"):
            raise ValueError("gpu_type must be specified when gpu_count > 0")
        return v


class AvailableResources(FlatRedisModel):
    id: str = Field(..., description="executor_id")
    resources: ResourceRequirements = Field(..., description="Available resources")

    def can_fulfill(self, req: ResourceRequirements) -> bool:
        if req.gpu_count:
            if self.resources.gpu_type != req.gpu_type:
                return False
            if req.gpu_count > self.resources.gpu_count:
                return False
        return (
            req.cpu_cores <= self.resources.cpu_cores
            and req.memory_mb <= self.resources.memory_mb
            and req.gpu_count <= self.resources.gpu_count
        )

    def __sub__(self, req: ResourceRequirements) -> "AvailableResources":
        if not self.can_fulfill(req):
            raise ValueError("Insufficient or incompatible resources")
        updated = self.resources.model_copy(
            update={
                "cpu_cores": self.resources.cpu_cores - req.cpu_cores,
                "memory_mb": self.resources.memory_mb - req.memory_mb,
                "gpu_count": self.resources.gpu_count - req.gpu_count,
            }
        )
        return self.model_copy(update={"resources": updated})

    def __eq__(self, other: object) -> bool:
        """
        Checks for equality between two AvailableResources instances.
        """
        if not isinstance(other, AvailableResources):
            return NotImplemented
        return self.id == other.id and self.resources == other.resources

    def __ne__(self, other: object) -> bool:
        """
        Checks for inequality between two AvailableResources instances.
        """
        equal = self.__eq__(other)
        if equal is NotImplemented:
            return NotImplemented
        return not equal

    @staticmethod
    def best_fit(
        candidates: Iterable["AvailableResources"],
        req: ResourceRequirements,
    ) -> Optional["AvailableResources"]:
        viable = [c for c in candidates if c.can_fulfill(req)]
        if not viable:
            return None

        def score(ar: "AvailableResources") -> tuple[float, int, int]:
            remaining = (ar - req).resources
            return (
                remaining.cpu_cores,  # minimize wasted CPU
                remaining.memory_mb,  # … then memory
                remaining.gpu_count,  # … then GPUs
            )

        return min(viable, key=score)


class JobDefinition(BaseModel):
    name: str = Field(..., description="Job name")
    image: str = Field(..., description="Docker image to run")
    command: List[str] = Field(..., description="Command to execute")
    resources: ResourceRequirements = Field(..., description="Resource requirements")
    environment: Dict[str, str] = Field(
        default_factory=dict, description="Environment variables"
    )


class ContainerStatus(BaseModel):
    container_id: Optional[str] = Field(None, description="Docker container ID")
    status: str = Field(..., description="Container status")
    exit_code: Optional[int] = Field(None, description="Container exit code")
    started_at: Optional[datetime] = Field(None, description="Container start time")
    finished_at: Optional[datetime] = Field(None, description="Container finish time")
    logs: Optional[str] = Field(None, description="Container logs")


class Job(FlatRedisModel):
    id: UUID = Field(default_factory=uuid4, description="Unique job identifier")
    status: JobStatus = Field(
        default=JobStatus.PENDING, description="Current Status of the job"
    )
    definition: JobDefinition = Field(..., description="Job definition")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Job creation time",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Last update time",
    )
    assigned_executor_id: Optional[str] = Field(None, description="Assigned executor")
    ip_address: Optional[str] = Field(
        None, description="IP address of the assigned executor"
    )
    assigned_at: Optional[datetime] = Field(None, description="Time job was assigned")
    started_at: Optional[datetime] = Field(
        None, description="Time executor started the job"
    )
    finished_at: Optional[datetime] = Field(
        None, description="Time executor marked the job as finished"
    )
    aborted_at: Optional[datetime] = Field(
        None, description="Time the job was marked aborted"
    )
    container_status: Optional[ContainerStatus] = Field(
        None, description="Container status"
    )
    error_message: Optional[str] = Field(None, description="Error message if failed")
    model_config = ConfigDict()


class ExecutorInfo(FlatRedisModel):
    id: str = Field(..., description="Unique executor identifier")
    ip_address: Optional[str] = Field(None, description="IP address of the executor")
    resources: ResourceRequirements = Field(
        ..., description="Total available resources"
    )

    model_config = ConfigDict()

    def to_available_resources(self) -> AvailableResources:
        return AvailableResources(id=self.id, resources=self.resources)
