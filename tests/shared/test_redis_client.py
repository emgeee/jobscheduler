import asyncio
import os
from typing import Optional
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel, ValidationError
from pydantic_settings import SettingsConfigDict
from redis.exceptions import LockError

from src.shared.models import (
    ExecutorInfo,
    Job,
    JobDefinition,
    ResourceRequirements,
    JobStatus,
    GPUType,
)
from src.shared.redis_client import (
    EXECUTOR_INFO_KEY,
    EXECUTOR_AVAILABLE_RESOURCES_KEY,
    EXECUTOR_HEALTH_KEY,
    EXECUTOR_ASSIGNED_JOBS_KEY,
    JOB_INFO_KEY,
    JOB_PENDING_KEY,
    JOB_EXECUTOR_KEY,
    JOB_PENDING_AT,
    JOB_HEALTHY_KEY,
    RedisClient,
    RedisSettings,
    create_client,
)


class SimpleModel(BaseModel):
    name: str
    value: int


@pytest_asyncio.fixture
async def redis_client():
    """Create a Redis client for testing using .env.test"""
    from pydantic_settings import BaseSettings

    class TestRedisSettings(BaseSettings):
        model_config = SettingsConfigDict(
            env_prefix="REDIS_",
            env_file=".env.test",
            env_file_encoding="utf-8",
            case_sensitive=False,
            extra="ignore",
        )

        host: str = "localhost"
        port: int = 6379
        db: int = 0
        password: Optional[str] = None
        max_connections: int = 10
        socket_timeout: float = 5.0
        socket_connect_timeout: float = 5.0
        retry_on_timeout: bool = True

    settings = TestRedisSettings()
    client = RedisClient(settings)

    # Ensure Redis is available
    if not await client.ping():
        pytest.skip("Redis is not available")

    yield client
    await client.close()


@pytest_asyncio.fixture
async def clean_redis(redis_client):
    """Clean Redis state before each test"""
    await redis_client.clear_all_data()
    yield
    await redis_client.clear_all_data()


class TestRedisSettings:
    def test_default_values(self):
        # Test with clean environment to verify defaults
        from pydantic_settings import BaseSettings

        class CleanRedisSettings(BaseSettings):
            model_config = SettingsConfigDict(
                env_prefix="REDIS_",
                env_file=None,  # Don't read from any file
                case_sensitive=False,
                extra="ignore",
            )

            host: str = "localhost"
            port: int = 6379
            db: int = 0
            password: Optional[str] = None
            max_connections: int = 10
            socket_timeout: float = 5.0
            socket_connect_timeout: float = 5.0
            retry_on_timeout: bool = True

        env_vars = [
            "REDIS_HOST",
            "REDIS_PORT",
            "REDIS_DB",
            "REDIS_PASSWORD",
            "REDIS_MAX_CONNECTIONS",
            "REDIS_SOCKET_TIMEOUT",
            "REDIS_SOCKET_CONNECT_TIMEOUT",
            "REDIS_RETRY_ON_TIMEOUT",
        ]
        original_values = {}

        for var in env_vars:
            if var in os.environ:
                original_values[var] = os.environ[var]
                del os.environ[var]

        try:
            settings = CleanRedisSettings()
            assert settings.host == "localhost"
            assert settings.port == 6379
            assert settings.db == 0
            assert settings.password is None
            assert settings.max_connections == 10
            assert settings.socket_timeout == 5.0
            assert settings.socket_connect_timeout == 5.0
            assert settings.retry_on_timeout is True
        finally:
            # Restore environment variables
            for var, value in original_values.items():
                os.environ[var] = value

    def test_valid_custom_values(self):
        settings = RedisSettings(
            host="redis.example.com",
            port=6380,
            db=1,
            password="secret",
            max_connections=20,
            socket_timeout=10.0,
            socket_connect_timeout=15.0,
            retry_on_timeout=False,
        )
        assert settings.host == "redis.example.com"
        assert settings.port == 6380
        assert settings.db == 1
        assert settings.password == "secret"
        assert settings.max_connections == 20
        assert settings.socket_timeout == 10.0
        assert settings.socket_connect_timeout == 15.0
        assert settings.retry_on_timeout is False

    def test_port_validation(self):
        with pytest.raises(ValidationError):
            RedisSettings(port=0)
        with pytest.raises(ValidationError):
            RedisSettings(port=65536)

    def test_db_validation(self):
        with pytest.raises(ValidationError):
            RedisSettings(db=-1)

    def test_max_connections_validation(self):
        with pytest.raises(ValidationError):
            RedisSettings(max_connections=0)

    def test_timeout_validation(self):
        with pytest.raises(ValidationError):
            RedisSettings(socket_timeout=0)
        with pytest.raises(ValidationError):
            RedisSettings(socket_connect_timeout=-1)

    def test_environment_variable_loading(self):
        # Store original values
        original_host = os.environ.get("REDIS_HOST")
        original_port = os.environ.get("REDIS_PORT")
        original_db = os.environ.get("REDIS_DB")

        os.environ["REDIS_HOST"] = "test-redis"
        os.environ["REDIS_PORT"] = "6380"
        os.environ["REDIS_DB"] = "2"

        try:
            settings = RedisSettings()
            assert settings.host == "test-redis"
            assert settings.port == 6380
            assert settings.db == 2
        finally:
            # Clean up environment variables
            for var, original in [
                ("REDIS_HOST", original_host),
                ("REDIS_PORT", original_port),
                ("REDIS_DB", original_db),
            ]:
                if original is not None:
                    os.environ[var] = original
                elif var in os.environ:
                    del os.environ[var]


class TestRedisClient:
    def test_initialization(self):
        settings = RedisSettings(host="localhost", port=6379, db=0)
        client = RedisClient(settings)

        assert client.LOCK_PREFIX == "lock"
        assert client.client is not None
        assert client.client.connection_pool.connection_kwargs["host"] == "localhost"
        assert client.client.connection_pool.connection_kwargs["port"] == 6379

    @pytest.mark.asyncio
    async def test_ping_success(self, redis_client):
        result = await redis_client.ping()
        assert result is True

    @pytest.mark.asyncio
    async def test_ping_failure(self):
        # Create client with invalid settings
        settings = RedisSettings(host="invalid-host", port=9999)
        client = RedisClient(settings)

        result = await client.ping()
        assert result is False

        await client.close()

    @pytest.mark.asyncio
    async def test_close(self, redis_client):
        # Test that close doesn't raise an exception
        await redis_client.close()
        # Note: Redis client may still respond to ping after close()
        # depending on connection pooling behavior

    @pytest.mark.asyncio
    async def test_clear_all_data(self, redis_client):
        # Add some data
        await redis_client.client.set("test_key", "test_value")

        # Clear all data
        result = await redis_client.clear_all_data()
        assert result is True

        # Verify data is cleared
        value = await redis_client.client.get("test_key")
        assert value is None


class TestFlatRedisModelOperations:
    @pytest.mark.asyncio
    async def test_set_model(self, redis_client, clean_redis):
        executor = ExecutorInfo(
            id="executor1",
            ip_address="192.168.1.100",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )

        result = await redis_client.set_model("test_executor", executor)
        assert result > 0  # Returns number of fields set

    @pytest.mark.asyncio
    async def test_get_model_success(self, redis_client, clean_redis):
        executor = ExecutorInfo(
            id="executor1",
            ip_address="192.168.1.100",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )

        await redis_client.set_model("test_executor", executor)
        retrieved = await redis_client.get_model("test_executor", ExecutorInfo)

        assert retrieved is not None
        assert retrieved.id == "executor1"
        assert retrieved.ip_address == "192.168.1.100"
        assert retrieved.resources.cpu_cores == 4.0

    @pytest.mark.asyncio
    async def test_get_model_not_found(self, redis_client, clean_redis):
        result = await redis_client.get_model("nonexistent", ExecutorInfo)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_models_multiple(self, redis_client, clean_redis):
        executor1 = ExecutorInfo(
            id="executor1",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )
        executor2 = ExecutorInfo(
            id="executor2",
            resources=ResourceRequirements(cpu_cores=2.0, memory_mb=1024, gpu_count=0),
        )

        await redis_client.set_model("exec1", executor1)
        await redis_client.set_model("exec2", executor2)

        results = await redis_client.get_models(
            ["exec1", "exec2", "nonexistent"], ExecutorInfo
        )

        assert len(results) == 2
        assert results[0].id == "executor1"
        assert results[1].id == "executor2"

    @pytest.mark.asyncio
    async def test_get_models_empty_list(self, redis_client, clean_redis):
        results = await redis_client.get_models([], ExecutorInfo)
        assert results == []

    @pytest.mark.asyncio
    async def test_flattened_model_roundtrip(self, redis_client, clean_redis):
        # Use ExecutorInfo as it's simpler for flattened serialization
        executor = ExecutorInfo(
            id="executor1",
            ip_address="192.168.1.100",
            resources=ResourceRequirements(cpu_cores=4.0, memory_mb=2048, gpu_count=0),
        )

        await redis_client.set_model("test_executor", executor)
        retrieved = await redis_client.get_model("test_executor", ExecutorInfo)

        assert retrieved is not None
        assert retrieved.id == "executor1"
        assert retrieved.ip_address == "192.168.1.100"
        assert retrieved.resources.cpu_cores == 4.0
        assert retrieved.resources.memory_mb == 2048


class TestLockOperations:
    @pytest.mark.asyncio
    async def test_lock_acquire_and_release(self, redis_client, clean_redis):
        async with redis_client.lock("test_lock") as lock:
            assert lock is not None
            # Lock should be acquired
            lock_exists = await redis_client.client.exists("lock:test_lock")
            assert lock_exists == 1

    @pytest.mark.asyncio
    async def test_lock_blocking_timeout(self, redis_client, clean_redis):
        # First acquire the lock
        async with redis_client.lock("test_lock", timeout=1.0):
            # Try to acquire the same lock with short timeout
            with pytest.raises(LockError):
                async with redis_client.lock("test_lock", blocking_timeout=0.1) as lock:
                    pass

    @pytest.mark.asyncio
    async def test_lock_job_with_uuid(self, redis_client, clean_redis):
        job_id = uuid4()

        async with redis_client.lock_job(job_id) as lock:
            assert lock is not None
            # Check that lock was created with correct name
            lock_exists = await redis_client.client.exists(f"jobs:{job_id}")
            assert lock_exists == 1

    @pytest.mark.asyncio
    async def test_lock_job_with_string(self, redis_client, clean_redis):
        job_id = "job_123"

        async with redis_client.lock_job(job_id) as lock:
            assert lock is not None
            lock_exists = await redis_client.client.exists(f"jobs:{job_id}")
            assert lock_exists == 1

    @pytest.mark.asyncio
    async def test_lock_executor_with_uuid(self, redis_client, clean_redis):
        executor_id = uuid4()

        async with redis_client.lock_executor(executor_id) as lock:
            assert lock is not None
            lock_exists = await redis_client.client.exists(f"executors:{executor_id}")
            assert lock_exists == 1

    @pytest.mark.asyncio
    async def test_lock_executor_with_string(self, redis_client, clean_redis):
        executor_id = "executor_123"

        async with redis_client.lock_executor(executor_id) as lock:
            assert lock is not None
            lock_exists = await redis_client.client.exists(f"executors:{executor_id}")
            assert lock_exists == 1

    @pytest.mark.asyncio
    async def test_lock_cleanup_on_exception(self, redis_client, clean_redis):
        try:
            async with redis_client.lock("test_lock") as lock:
                assert lock is not None
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Lock should be cleaned up
        lock_exists = await redis_client.client.exists("lock:test_lock")
        assert lock_exists == 0

    @pytest.mark.asyncio
    async def test_concurrent_locks_different_names(self, redis_client, clean_redis):
        async def acquire_lock(name):
            async with redis_client.lock(name) as lock:
                await asyncio.sleep(0.1)
                return lock is not None

        # Should be able to acquire different locks concurrently
        results = await asyncio.gather(
            acquire_lock("lock1"),
            acquire_lock("lock2"),
            acquire_lock("lock3"),
        )

        assert all(results)


class TestUtilityFunctions:
    def test_create_client(self):
        client = create_client()
        assert isinstance(client, RedisClient)
        assert client.LOCK_PREFIX == "lock"

    def test_create_client_with_env_vars(self):
        # Store original values
        original_host = os.environ.get("REDIS_HOST")
        original_port = os.environ.get("REDIS_PORT")

        os.environ["REDIS_HOST"] = "test-host"
        os.environ["REDIS_PORT"] = "6380"

        try:
            client = create_client()
            assert isinstance(client, RedisClient)
            # Note: We can't easily test the internal settings without accessing private members
            # The important thing is that it creates a client successfully
        finally:
            # Clean up environment variables
            for var, original in [
                ("REDIS_HOST", original_host),
                ("REDIS_PORT", original_port),
            ]:
                if original is not None:
                    os.environ[var] = original
                elif var in os.environ:
                    del os.environ[var]


class TestRedisConstants:
    def test_key_constants_exist(self):
        assert EXECUTOR_INFO_KEY == "executor:info:{executor_id}"
        assert (
            EXECUTOR_AVAILABLE_RESOURCES_KEY
            == "executor:available_resources:{executor_id}"
        )
        assert EXECUTOR_HEALTH_KEY == "executor:healthy:{executor_id}"
        assert EXECUTOR_ASSIGNED_JOBS_KEY == "executor:assigned_jobs:{executor_id}"
        assert JOB_INFO_KEY == "jobs:info:{job_id}"
        assert JOB_PENDING_KEY == "jobs:pending"
        assert JOB_EXECUTOR_KEY == "jobs:executor:{job_id}"
        assert JOB_PENDING_AT == "jobs:pending_at:{job_id}"
        assert JOB_HEALTHY_KEY == "jobs:healthy:{job_id}"

    def test_key_formatting(self):
        executor_id = "executor123"
        job_id = "job456"

        assert (
            EXECUTOR_INFO_KEY.format(executor_id=executor_id)
            == "executor:info:executor123"
        )
        assert JOB_INFO_KEY.format(job_id=job_id) == "jobs:info:job456"


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_get_model_invalid_data(self, redis_client, clean_redis):
        # Set invalid data directly
        await redis_client.client.hset(
            "invalid_executor", "invalid_field", "invalid_value"
        )

        result = await redis_client.get_model("invalid_executor", ExecutorInfo)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_models_with_invalid_data(self, redis_client, clean_redis):
        # Set one valid and one invalid model
        valid_executor = ExecutorInfo(
            id="valid",
            resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512, gpu_count=0),
        )
        await redis_client.set_model("valid", valid_executor)
        await redis_client.client.hset("invalid", "bad_field", "bad_value")

        results = await redis_client.get_models(["valid", "invalid"], ExecutorInfo)

        # Should only return the valid model
        assert len(results) == 1
        assert results[0].id == "valid"


class TestEnumSerializationInRedis:
    """Test enum serialization through Redis operations"""

    @pytest.mark.asyncio
    async def test_job_status_enum_through_redis(self):
        """Test that JobStatus enums are properly serialized and deserialized through Redis"""
        redis_client = create_client()

        job = Job(
            definition=JobDefinition(
                name="test-job",
                image="ubuntu:latest",
                command=["echo", "hello"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.ASSIGNED,
        )

        # Store the job
        await redis_client.set_model("test_job", job)

        # Verify the raw Redis data contains the enum value, not the string representation
        raw_data = await redis_client.client.hgetall("test_job")
        status_value = raw_data.get("status") or raw_data.get(b"status")
        if isinstance(status_value, bytes):
            status_value = status_value.decode()
        assert status_value == "assigned"
        assert status_value != "JobStatus.ASSIGNED"

        # Retrieve the job
        retrieved_job = await redis_client.get_model("test_job", Job)
        assert retrieved_job is not None
        assert retrieved_job.status == JobStatus.ASSIGNED
        assert retrieved_job.status.value == "assigned"

        await redis_client.close()

    @pytest.mark.asyncio
    async def test_gpu_type_enum_through_redis(self):
        """Test that GPUType enums are properly serialized and deserialized through Redis"""
        redis_client = create_client()

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

        # Store the job
        await redis_client.set_model("test_gpu_job", job)

        # Verify the raw Redis data contains the enum value, not the string representation
        raw_data = await redis_client.client.hgetall("test_gpu_job")
        gpu_type_value = raw_data.get("definition.resources.gpu_type") or raw_data.get(
            b"definition.resources.gpu_type"
        )
        if isinstance(gpu_type_value, bytes):
            gpu_type_value = gpu_type_value.decode()
        assert gpu_type_value == "nvidia-a100"
        assert gpu_type_value != "GPUType.NVIDIA_A100"

        # Retrieve the job
        retrieved_job = await redis_client.get_model("test_gpu_job", Job)
        assert retrieved_job is not None
        assert retrieved_job.definition.resources.gpu_type == GPUType.NVIDIA_A100
        assert retrieved_job.definition.resources.gpu_type.value == "nvidia-a100"

        await redis_client.close()

    @pytest.mark.asyncio
    async def test_multiple_jobs_with_different_statuses(self):
        """Test multiple jobs with different statuses are properly serialized"""
        redis_client = create_client()

        jobs = []
        statuses = [
            JobStatus.PENDING,
            JobStatus.ASSIGNED,
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
        ]

        # Create and store jobs with different statuses
        for i, status in enumerate(statuses):
            job = Job(
                definition=JobDefinition(
                    name=f"test-job-{i}",
                    image="ubuntu:latest",
                    command=["echo", f"hello-{i}"],
                    resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
                ),
                status=status,
            )
            jobs.append(job)
            await redis_client.set_model(f"test_job_{i}", job)

        # Retrieve all jobs
        job_keys = [f"test_job_{i}" for i in range(len(statuses))]
        retrieved_jobs = await redis_client.get_models(job_keys, Job)

        # Verify all jobs have correct statuses
        assert len(retrieved_jobs) == len(statuses)
        for retrieved_job, expected_status in zip(retrieved_jobs, statuses):
            assert retrieved_job.status == expected_status
            assert retrieved_job.status.value == expected_status.value

        await redis_client.close()

    @pytest.mark.asyncio
    async def test_enum_serialization_prevents_validation_errors(self):
        """Test that proper enum serialization prevents validation errors"""
        redis_client = create_client()

        job = Job(
            definition=JobDefinition(
                name="validation-test",
                image="ubuntu:latest",
                command=["echo", "validation"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.FAILED,
        )

        # Store the job
        await redis_client.set_model("validation_test", job)

        # Verify the raw Redis data has the correct enum value
        raw_data = await redis_client.client.hgetall("validation_test")
        status_value = raw_data.get("status") or raw_data.get(b"status")
        if isinstance(status_value, bytes):
            status_value = status_value.decode()

        # This should be the enum value, not the string representation
        assert status_value == "failed"
        # This would cause validation errors
        assert status_value != "JobStatus.FAILED"

        # Retrieve the job - this should not raise validation errors
        retrieved_job = await redis_client.get_model("validation_test", Job)
        assert retrieved_job is not None
        assert retrieved_job.status == JobStatus.FAILED

        await redis_client.close()

    @pytest.mark.asyncio
    async def test_enum_serialization_consistency(self):
        """Test that enum serialization is consistent across all Redis operations"""
        redis_client = create_client()

        job = Job(
            definition=JobDefinition(
                name="consistency-test",
                image="ubuntu:latest",
                command=["echo", "consistency"],
                resources=ResourceRequirements(cpu_cores=1.0, memory_mb=512),
            ),
            status=JobStatus.RUNNING,
        )

        # Test set_model
        await redis_client.set_model("consistency_test", job)

        # Test get_model
        retrieved_job = await redis_client.get_model("consistency_test", Job)
        assert retrieved_job is not None
        assert retrieved_job.status == JobStatus.RUNNING

        # Test get_models
        retrieved_jobs = await redis_client.get_models(["consistency_test"], Job)
        assert len(retrieved_jobs) == 1
        assert retrieved_jobs[0].status == JobStatus.RUNNING

        # Verify all operations use the same enum value format
        raw_data = await redis_client.client.hgetall("consistency_test")
        status_value = raw_data.get("status") or raw_data.get(b"status")
        if isinstance(status_value, bytes):
            status_value = status_value.decode()
        assert status_value == "running"

        await redis_client.close()
