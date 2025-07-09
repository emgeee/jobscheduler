from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from src.shared.models import (
    AvailableResources,
    ContainerStatus,
    ExecutorInfo,
    FlatRedisModel,
    GPUType,
    Job,
    JobDefinition,
    JobStatus,
    ResourceRequirements,
)


class TestFlatRedisModel:
    def test_to_flattened_dict_simple(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int

        model = SimpleModel(name="test", value=42)
        flattened = model.to_flattened_dict()
        assert flattened == {"name": "test", "value": 42}

    def test_to_flattened_dict_nested(self):
        class NestedModel(FlatRedisModel):
            user: dict
            config: dict

        model = NestedModel(
            user={"name": "alice", "age": 30},
            config={"settings": {"theme": "dark", "debug": True}},
        )
        flattened = model.to_flattened_dict()
        expected = {
            "user.name": "alice",
            "user.age": 30,
            "config.settings.theme": "dark",
            "config.settings.debug": True,
        }
        assert flattened == expected

    def test_to_flattened_dict_custom_separator(self):
        class SimpleModel(FlatRedisModel):
            data: dict

        model = SimpleModel(data={"nested": {"key": "value"}})
        flattened = model.to_flattened_dict(sep=":")
        assert flattened == {"data:nested:key": "value"}

    def test_from_flattened_dict_simple(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int

        flat_data = {"name": "test", "value": 42}
        model = SimpleModel.from_flattened_dict(flat_data)
        assert model.name == "test"
        assert model.value == 42

    def test_from_flattened_dict_nested(self):
        class NestedModel(FlatRedisModel):
            user: dict
            config: dict

        flat_data = {
            "user.name": "alice",
            "user.age": 30,
            "config.settings.theme": "dark",
            "config.settings.debug": True,
        }
        model = NestedModel.from_flattened_dict(flat_data)
        assert model.user == {"name": "alice", "age": 30}
        assert model.config == {"settings": {"theme": "dark", "debug": True}}

    def test_from_flattened_dict_custom_separator(self):
        class SimpleModel(FlatRedisModel):
            data: dict

        flat_data = {"data:nested:key": "value"}
        model = SimpleModel.from_flattened_dict(flat_data, sep=":")
        assert model.data == {"nested": {"key": "value"}}

    def test_flatten_unflatten_roundtrip(self):
        class ComplexModel(FlatRedisModel):
            simple: str
            nested: dict
            deep: dict

        original_data = {
            "simple": "value",
            "nested": {"key1": "val1", "key2": "val2"},
            "deep": {"level1": {"level2": {"key": "deep_value"}}},
        }
        model = ComplexModel(**original_data)
        flattened = model.to_flattened_dict()
        restored = ComplexModel.from_flattened_dict(flattened)
        assert model.model_dump() == restored.model_dump()

    def test_flatten_static_method(self):
        data = {"a": {"b": {"c": "value"}}}
        result = FlatRedisModel._flatten(data)
        assert result == {"a.b.c": "value"}

    def test_unflatten_static_method(self):
        flat_data = {"a.b.c": "value"}
        result = FlatRedisModel._unflatten(flat_data)
        assert result == {"a": {"b": {"c": "value"}}}

    def test_select_fields_simple(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int

        model = SimpleModel(name="test", value=42)
        selected = model.select_fields(["name", "value"])
        assert selected == {"name": "test", "value": "42"}

    def test_select_fields_nested(self):
        class NestedModel(FlatRedisModel):
            user: dict
            config: dict

        model = NestedModel(
            user={"name": "alice", "age": 30},
            config={"settings": {"theme": "dark", "debug": True}},
        )
        selected = model.select_fields(["user.name", "config.settings.theme"])
        assert selected == {"user.name": "alice", "config.settings.theme": "dark"}

    def test_select_fields_subset(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int
            extra: str

        model = SimpleModel(name="test", value=42, extra="unused")
        selected = model.select_fields(["name", "value"])
        assert selected == {"name": "test", "value": "42"}

    def test_select_fields_nonexistent_field_raises_error(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int

        model = SimpleModel(name="test", value=42)
        with pytest.raises(KeyError, match="Field 'nonexistent' does not exist"):
            model.select_fields(["name", "nonexistent"])

    def test_select_fields_empty_list(self):
        class SimpleModel(FlatRedisModel):
            name: str
            value: int

        model = SimpleModel(name="test", value=42)
        selected = model.select_fields([])
        assert selected == {}

    def test_select_fields_converts_to_string(self):
        class ComplexModel(FlatRedisModel):
            name: str
            value: int
            flag: bool
            data: dict

        model = ComplexModel(
            name="test", value=42, flag=True, data={"nested": {"count": 10}}
        )
        selected = model.select_fields(["name", "value", "flag", "data.nested.count"])
        assert selected == {
            "name": "test",
            "value": "42",
            "flag": "True",
            "data.nested.count": "10",
        }

    def test_select_fields_with_none_values(self):
        class ModelWithOptional(FlatRedisModel):
            name: str
            optional_field: Optional[str] = None

        model = ModelWithOptional(name="test", optional_field=None)
        selected = model.select_fields(["name", "optional_field"])
        assert selected == {"name": "test", "optional_field": "None"}


class TestJobStatus:
    def test_all_statuses_exist(self):
        assert JobStatus.PENDING == "pending"
        assert JobStatus.ASSIGNED == "assigned"
        assert JobStatus.RUNNING == "running"
        assert JobStatus.SUCCEEDED == "succeeded"
        assert JobStatus.FAILED == "failed"
        assert JobStatus.ABORTED == "aborted"

    def test_is_terminal_for_terminal_statuses(self):
        assert JobStatus.SUCCEEDED.is_terminal() is True
        assert JobStatus.FAILED.is_terminal() is True
        assert JobStatus.ABORTED.is_terminal() is True

    def test_is_terminal_for_non_terminal_statuses(self):
        assert JobStatus.PENDING.is_terminal() is False
        assert JobStatus.ASSIGNED.is_terminal() is False
        assert JobStatus.RUNNING.is_terminal() is False


class TestGPUType:
    def test_all_gpu_types_exist(self):
        assert GPUType.NVIDIA_T4 == "nvidia-t4"
        assert GPUType.NVIDIA_A100 == "nvidia-a100"
        assert GPUType.NVIDIA_V100 == "nvidia-v100"
        assert GPUType.NVIDIA_K80 == "nvidia-k80"
        assert GPUType.NVIDIA_P100 == "nvidia-p100"
        assert GPUType.NVIDIA_RTX_3080 == "nvidia-rtx-3080"
        assert GPUType.NVIDIA_RTX_3090 == "nvidia-rtx-3090"
        assert GPUType.NVIDIA_RTX_4090 == "nvidia-rtx-4090"


class TestResourceRequirements:
    def test_valid_resource_requirements(self):
        req = ResourceRequirements(
            cpu_cores=2.0,
            memory_mb=1024,
            gpu_type=GPUType.NVIDIA_T4,
            gpu_count=1,
        )
        assert req.cpu_cores == 2.0
        assert req.memory_mb == 1024
        assert req.gpu_type == GPUType.NVIDIA_T4
        assert req.gpu_count == 1

    def test_gpu_count_without_gpu_type_raises_error(self):
        with pytest.raises(ValidationError) as exc_info:
            ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=1)
        assert "gpu_type must be specified when gpu_count > 0" in str(exc_info.value)

    def test_gpu_count_zero_without_gpu_type_is_valid(self):
        req = ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0)
        assert req.gpu_count == 0
        assert req.gpu_type is None

    def test_negative_cpu_cores_raises_error(self):
        with pytest.raises(ValidationError):
            ResourceRequirements(cpu_cores=-1.0, memory_mb=512)

    def test_negative_memory_raises_error(self):
        with pytest.raises(ValidationError):
            ResourceRequirements(cpu_cores=1.0, memory_mb=-512)

    def test_negative_gpu_count_raises_error(self):
        with pytest.raises(ValidationError):
            ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=-1)


class TestAvailableResources:
    def test_can_fulfill_sufficient_resources(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        assert available.can_fulfill(required) is True

    def test_can_fulfill_insufficient_cpu(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=2048, gpu_count=0),
        )
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        assert available.can_fulfill(required) is False

    def test_can_fulfill_insufficient_memory(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=512, gpu_count=0),
        )
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        assert available.can_fulfill(required) is False

    def test_can_fulfill_gpu_type_mismatch(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(
                cpu_cores=4.0, memory_mb=2048, gpu_type=GPUType.NVIDIA_T4, gpu_count=1
            ),
        )
        required = ResourceRequirements(
            cpu_cores=2.0, memory_mb=1024, gpu_type=GPUType.NVIDIA_A100, gpu_count=1
        )
        assert available.can_fulfill(required) is False

    def test_can_fulfill_insufficient_gpu_count(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(
                cpu_cores=4.0, memory_mb=2048, gpu_type=GPUType.NVIDIA_T4, gpu_count=1
            ),
        )
        required = ResourceRequirements(
            cpu_cores=2.0, memory_mb=1024, gpu_type=GPUType.NVIDIA_T4, gpu_count=2
        )
        assert available.can_fulfill(required) is False

    def test_can_fulfill_matching_gpu_requirements(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(
                cpu_cores=4.0, memory_mb=2048, gpu_type=GPUType.NVIDIA_T4, gpu_count=2
            ),
        )
        required = ResourceRequirements(
            cpu_cores=2.0, memory_mb=1024, gpu_type=GPUType.NVIDIA_T4, gpu_count=1
        )
        assert available.can_fulfill(required) is True

    def test_subtraction_success(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        result = available - required
        assert result.id == "executor1"
        assert result.resources.cpu_cores == 2.0
        assert result.resources.memory_mb == 1024
        assert result.resources.gpu_count == 0

    def test_subtraction_with_gpu(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(
                cpu_cores=4.0, memory_mb=2048, gpu_type=GPUType.NVIDIA_T4, gpu_count=2
            ),
        )
        required = ResourceRequirements(
            cpu_cores=2.0, memory_mb=1024, gpu_type=GPUType.NVIDIA_T4, gpu_count=1
        )
        result = available - required
        assert result.resources.cpu_cores == 2.0
        assert result.resources.memory_mb == 1024
        assert result.resources.gpu_count == 1
        assert result.resources.gpu_type == GPUType.NVIDIA_T4

    def test_subtraction_insufficient_resources_raises_error(self):
        available = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        with pytest.raises(ValueError, match="Insufficient or incompatible resources"):
            available - required

    def test_equality(self):
        resources1 = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        resources2 = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        assert resources1 == resources2

    def test_inequality_different_id(self):
        resources1 = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        resources2 = AvailableResources(
            id="executor2",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        assert resources1 != resources2

    def test_inequality_different_resources(self):
        resources1 = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        resources2 = AvailableResources(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=2.0, memory_mb=2048, gpu_count=0),
        )
        assert resources1 != resources2

    def test_best_fit_single_candidate(self):
        candidates = [
            AvailableResources(
                id="executor1",
                resources=ResourceRequirements(
                    cpu_cores=4.0, memory_mb=2048, gpu_count=0
                ),
            )
        ]
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        result = AvailableResources.best_fit(candidates, required)
        assert result is not None
        assert result.id == "executor1"

    def test_best_fit_no_viable_candidates(self):
        candidates = [
            AvailableResources(
                id="executor1",
                resources=ResourceRequirements(
                    cpu_cores=1.0, memory_mb=512, gpu_count=0
                ),
            )
        ]
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        result = AvailableResources.best_fit(candidates, required)
        assert result is None

    def test_best_fit_chooses_minimal_waste(self):
        candidates = [
            AvailableResources(
                id="executor1",
                resources=ResourceRequirements(
                    cpu_cores=8.0, memory_mb=4096, gpu_count=0
                ),
            ),
            AvailableResources(
                id="executor2",
                resources=ResourceRequirements(
                    cpu_cores=4.0, memory_mb=2048, gpu_count=0
                ),
            ),
            AvailableResources(
                id="executor3",
                resources=ResourceRequirements(
                    cpu_cores=2.5, memory_mb=1536, gpu_count=0
                ),
            ),
        ]
        required = ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0)
        result = AvailableResources.best_fit(candidates, required)
        assert result is not None
        assert result.id == "executor3"  # Has least waste


class TestJobDefinition:
    def test_job_definition_creation(self):
        job_def = JobDefinition(
            name="test-job",
            image="python:3.12",
            command=["python", "script.py"],
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        assert job_def.name == "test-job"
        assert job_def.image == "python:3.12"
        assert job_def.command == ["python", "script.py"]
        assert job_def.environment == {}

    def test_job_definition_with_environment(self):
        job_def = JobDefinition(
            name="test-job",
            image="python:3.12",
            command=["python", "script.py"],
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
            environment={"VAR1": "value1", "VAR2": "value2"},
        )
        assert job_def.environment == {"VAR1": "value1", "VAR2": "value2"}


class TestContainerStatus:
    def test_container_status_minimal(self):
        status = ContainerStatus(status="running")
        assert status.status == "running"
        assert status.container_id is None
        assert status.exit_code is None
        assert status.started_at is None
        assert status.finished_at is None
        assert status.logs is None

    def test_container_status_complete(self):
        started = datetime.now(timezone.utc)
        finished = datetime.now(timezone.utc)
        status = ContainerStatus(
            container_id="abc123",
            status="exited",
            exit_code=0,
            started_at=started,
            finished_at=finished,
            logs="Hello world\n",
        )
        assert status.container_id == "abc123"
        assert status.status == "exited"
        assert status.exit_code == 0
        assert status.started_at == started
        assert status.finished_at == finished
        assert status.logs == "Hello world\n"


class TestJob:
    def test_job_creation_auto_fields(self):
        job_def = JobDefinition(
            name="test-job",
            image="python:3.12",
            command=["python", "script.py"],
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        job = Job(definition=job_def)

        assert isinstance(job.id, UUID)
        assert isinstance(job.created_at, datetime)
        assert isinstance(job.updated_at, datetime)
        assert job.definition == job_def
        assert job.assigned_executor_id is None
        assert job.ip_address is None
        assert job.assigned_at is None
        assert job.started_at is None
        assert job.finished_at is None
        assert job.aborted_at is None
        assert job.container_status is None
        assert job.error_message is None

    def test_job_with_explicit_id(self):
        job_id = uuid4()
        job_def = JobDefinition(
            name="test-job",
            image="python:3.12",
            command=["python", "script.py"],
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        job = Job(id=job_id, definition=job_def)
        assert job.id == job_id

    def test_job_flat_redis_inheritance(self):
        job_def = JobDefinition(
            name="test-job",
            image="python:3.12",
            command=["python", "script.py"],
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        job = Job(definition=job_def)

        # Test that it can be flattened and unflattened
        flattened = job.to_flattened_dict()
        restored = Job.from_flattened_dict(flattened)

        assert job.id == restored.id
        assert job.definition.name == restored.definition.name
        assert job.definition.image == restored.definition.image
        assert job.definition.command == restored.definition.command


class TestExecutorInfo:
    def test_executor_info_creation(self):
        executor = ExecutorInfo(
            id="executor1",
            ip_address="192.168.1.100",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        assert executor.id == "executor1"
        assert executor.ip_address == "192.168.1.100"
        assert executor.resources.cpu_cores == 4.0

    def test_executor_info_optional_ip(self):
        executor = ExecutorInfo(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        assert executor.ip_address is None

    def test_to_available_resources(self):
        executor = ExecutorInfo(
            id="executor1",
            ip_address="192.168.1.100",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        available = executor.to_available_resources()
        assert isinstance(available, AvailableResources)
        assert available.id == "executor1"
        assert available.resources.cpu_cores == 4.0
        assert available.resources.memory_mb == 2048

    def test_executor_info_flat_redis_inheritance(self):
        executor = ExecutorInfo(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )

        # Test that it can be flattened and unflattened
        flattened = executor.to_flattened_dict()
        restored = ExecutorInfo.from_flattened_dict(flattened)

        assert executor.id == restored.id
        assert executor.resources.cpu_cores == restored.resources.cpu_cores
        assert executor.resources.memory_mb == restored.resources.memory_mb


class TestEnumSerialization:
    """Test enum serialization in FlatRedisModel methods"""

    def test_to_redis_dict_with_job_status_enum(self):
        """Test that JobStatus enums are serialized to their values in to_redis_dict"""
        job = Job(
            definition=JobDefinition(
                name="test-job",
                image="ubuntu:latest",
                command=["echo", "hello"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.ASSIGNED,
        )

        redis_data = job.to_redis_dict()

        # JobStatus.ASSIGNED should be serialized as "assigned"
        assert redis_data["status"] == "assigned"
        assert redis_data["status"] != "JobStatus.ASSIGNED"

    def test_to_redis_dict_with_gpu_type_enum(self):
        """Test that GPUType enums are serialized to their values in to_redis_dict"""
        job = Job(
            definition=JobDefinition(
                name="gpu-test",
                image="ubuntu:latest",
                command=["nvidia-smi"],
                resources=ResourceRequirements(
                    cpu_cores=2.0,
                    memory_mb=1024,
                    gpu_type=GPUType.NVIDIA_A100,
                    gpu_count=1,
                ),
            ),
            status=JobStatus.PENDING,
        )

        redis_data = job.to_redis_dict()

        # GPUType.NVIDIA_A100 should be serialized as "nvidia-a100"
        assert redis_data["definition.resources.gpu_type"] == "nvidia-a100"
        assert redis_data["definition.resources.gpu_type"] != "GPUType.NVIDIA_A100"

    def test_select_fields_with_job_status_enum(self):
        """Test that JobStatus enums are serialized to their values in select_fields"""
        job = Job(
            definition=JobDefinition(
                name="test-job",
                image="ubuntu:latest",
                command=["echo", "hello"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.RUNNING,
        )

        selected = job.select_fields(["status"])

        # JobStatus.RUNNING should be serialized as "running"
        assert selected["status"] == "running"
        assert selected["status"] != "JobStatus.RUNNING"
        assert isinstance(selected["status"], str)

    def test_select_fields_with_gpu_type_enum(self):
        """Test that GPUType enums are serialized to their values in select_fields"""
        job = Job(
            definition=JobDefinition(
                name="gpu-test",
                image="ubuntu:latest",
                command=["nvidia-smi"],
                resources=ResourceRequirements(
                    cpu_cores=2.0,
                    memory_mb=1024,
                    gpu_type=GPUType.NVIDIA_T4,
                    gpu_count=1,
                ),
            ),
            status=JobStatus.PENDING,
        )

        selected = job.select_fields(["definition.resources.gpu_type"])

        # GPUType.NVIDIA_T4 should be serialized as "nvidia-t4"
        assert selected["definition.resources.gpu_type"] == "nvidia-t4"
        assert selected["definition.resources.gpu_type"] != "GPUType.NVIDIA_T4"
        assert isinstance(selected["definition.resources.gpu_type"], str)

    def test_enum_serialization_roundtrip(self):
        """Test that enum serialization works correctly in full roundtrip"""
        job = Job(
            definition=JobDefinition(
                name="test-job",
                image="ubuntu:latest",
                command=["echo", "hello"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.SUCCEEDED,
        )

        # Serialize using to_redis_dict
        redis_data = job.to_redis_dict()
        assert redis_data["status"] == "succeeded"

        # Deserialize using from_redis_dict
        restored_job = Job.from_redis_dict(redis_data)
        assert restored_job.status == JobStatus.SUCCEEDED
        assert restored_job.status.value == "succeeded"

    def test_all_job_statuses_serialize_correctly(self):
        """Test that all JobStatus values serialize to their string values"""
        job = Job(
            definition=JobDefinition(
                name="test-job",
                image="ubuntu:latest",
                command=["echo", "hello"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.PENDING,
        )

        status_mappings = {
            JobStatus.PENDING: "pending",
            JobStatus.ASSIGNED: "assigned",
            JobStatus.RUNNING: "running",
            JobStatus.SUCCEEDED: "succeeded",
            JobStatus.FAILED: "failed",
            JobStatus.ABORTED: "aborted",
        }

        for status_enum, expected_value in status_mappings.items():
            job.status = status_enum

            # Test to_redis_dict
            redis_data = job.to_redis_dict()
            assert redis_data["status"] == expected_value

            # Test select_fields
            selected = job.select_fields(["status"])
            assert selected["status"] == expected_value

    def test_all_gpu_types_serialize_correctly(self):
        """Test that all GPUType values serialize to their string values"""
        job = Job(
            definition=JobDefinition(
                name="gpu-test",
                image="ubuntu:latest",
                command=["nvidia-smi"],
                resources=ResourceRequirements(
                    cpu_cores=2.0,
                    memory_mb=1024,
                    gpu_type=GPUType.NVIDIA_T4,
                    gpu_count=1,
                ),
            ),
            status=JobStatus.PENDING,
        )

        gpu_type_mappings = {
            GPUType.NVIDIA_T4: "nvidia-t4",
            GPUType.NVIDIA_A100: "nvidia-a100",
            GPUType.NVIDIA_V100: "nvidia-v100",
            GPUType.NVIDIA_K80: "nvidia-k80",
            GPUType.NVIDIA_P100: "nvidia-p100",
        }

        for gpu_type_enum, expected_value in gpu_type_mappings.items():
            job.definition.resources.gpu_type = gpu_type_enum

            # Test to_redis_dict
            redis_data = job.to_redis_dict()
            assert redis_data["definition.resources.gpu_type"] == expected_value

            # Test select_fields
            selected = job.select_fields(["definition.resources.gpu_type"])
            assert selected["definition.resources.gpu_type"] == expected_value
