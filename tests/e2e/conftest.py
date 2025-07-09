"""
E2E test configuration and fixtures for job scheduler system.

This module provides fixtures and utilities for testing the job scheduler
system running in docker-compose with multiple schedulers and executors.
"""

import asyncio
import time
from typing import Dict, Any, Optional, List
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from src.shared.models import JobDefinition, JobStatus, ResourceRequirements


@pytest.fixture(scope="session")
def api_base_url() -> str:
    """Base URL for the scheduler API."""
    return "http://localhost:8000"


@pytest_asyncio.fixture
async def api_client(api_base_url: str):
    """HTTP client for making API requests to the scheduler."""
    async with httpx.AsyncClient(base_url=api_base_url, timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture(scope="session")
async def wait_for_system_ready():
    """Wait for the scheduler system to be ready before running tests."""
    max_wait = 60  # seconds
    start_time = time.time()
    
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=30.0) as client:
        while time.time() - start_time < max_wait:
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    # Also check that we have healthy executors
                    executors_response = await client.get("/executors/healthy")
                    if executors_response.status_code == 200:
                        executors_data = executors_response.json()
                        if executors_data.get("executors"):
                            return
            except Exception:
                pass
            
            await asyncio.sleep(1)
    
    pytest.fail("System failed to become ready within timeout")


@pytest_asyncio.fixture
async def cleanup_jobs(api_client):
    """Fixture to cleanup jobs created during tests."""
    created_jobs: List[UUID] = []
    
    def track_job(job_id: UUID):
        created_jobs.append(job_id)
    
    # Yield the tracking function
    yield track_job
    
    # Cleanup after test
    for job_id in created_jobs:
        try:
            await api_client.post(f"/jobs/{job_id}/abort")
        except Exception:
            pass  # Job might already be finished or not exist


async def wait_for_job_status(
    api_client: httpx.AsyncClient,
    job_id: UUID,
    target_status: JobStatus,
    timeout: float = 60.0,
    intermediate_statuses: Optional[List[JobStatus]] = None
) -> Dict[str, Any]:
    """
    Wait for a job to reach the target status.
    
    Args:
        api_client: HTTP client for API requests
        job_id: Job ID to monitor
        target_status: Status to wait for
        timeout: Maximum time to wait in seconds
        intermediate_statuses: Optional list of expected intermediate statuses
    
    Returns:
        Job data when target status is reached
    
    Raises:
        TimeoutError: If target status is not reached within timeout
        AssertionError: If job transitions to unexpected status
    """
    start_time = time.time()
    last_status = None
    
    while time.time() - start_time < timeout:
        response = await api_client.get(f"/jobs/{job_id}")
        assert response.status_code == 200, f"Failed to get job {job_id}: {response.text}"
        
        job_data = response.json()
        current_status = JobStatus(job_data["status"])
        
        if current_status == target_status:
            return job_data
        
        # Check for unexpected terminal states
        if current_status in [JobStatus.FAILED, JobStatus.ABORTED] and target_status not in [JobStatus.FAILED, JobStatus.ABORTED]:
            raise AssertionError(f"Job {job_id} reached terminal status {current_status}, expected {target_status}")
        
        # Log status changes
        if current_status != last_status:
            last_status = current_status
        
        await asyncio.sleep(0.5)
    
    # Get final status for error message
    response = await api_client.get(f"/jobs/{job_id}")
    final_job_data = response.json() if response.status_code == 200 else {}
    final_status = final_job_data.get("status", "unknown")
    
    raise TimeoutError(f"Job {job_id} did not reach status {target_status} within {timeout}s. Final status: {final_status}")


async def submit_test_job(
    api_client: httpx.AsyncClient,
    image: str = "alpine:latest",
    command: List[str] = ["echo", "hello"],
    cpu: float = 0.1,
    memory: int = 128,
    gpu_count: int = 0,
    environment: Optional[Dict[str, str]] = None,
    name: str = "test-job"
) -> Dict[str, Any]:
    """
    Submit a test job with the given parameters.
    
    Args:
        api_client: HTTP client for API requests
        image: Docker image to use
        command: Command to execute
        cpu: CPU requirement in cores
        memory: Memory requirement in MB
        gpu_count: Number of GPUs required
        environment: Environment variables
        name: Job name
    
    Returns:
        Job data from the API response
    """
    resources = {
        "cpu_cores": cpu,
        "memory_mb": memory,
        "gpu_count": gpu_count
    }
    
    if gpu_count > 0:
        resources["gpu_type"] = "nvidia-t4"  # Default GPU type
    
    job_request = {
        "definition": {
            "name": name,
            "image": image,
            "command": command,
            "resources": resources
        }
    }
    
    if environment:
        job_request["definition"]["environment"] = environment
    
    response = await api_client.post("/jobs", json=job_request)
    assert response.status_code == 201, f"Failed to submit job: {response.text}"
    
    return response.json()


async def get_executor_resources(api_client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """
    Get current executor resources.
    
    Returns:
        List of executor data with available resources
    """
    response = await api_client.get("/executors/healthy")
    assert response.status_code == 200, f"Failed to get executors: {response.text}"
    
    return response.json()["executors"]


def create_job_definition(
    image: str = "alpine:latest",
    command: List[str] = ["echo", "hello"],
    cpu: float = 0.1,
    memory: int = 128,
    gpu_count: int = 0,
    environment: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Create a job definition dict for API requests."""
    definition = {
        "image": image,
        "command": command,
        "resources": {
            "cpu": cpu,
            "memory": memory,
            "gpu_count": gpu_count
        }
    }
    
    if environment:
        definition["environment"] = environment
    
    return definition


# Common test job definitions
TEST_JOBS = {
    "simple_success": {
        "name": "simple-success",
        "image": "alpine:latest",
        "command": ["echo", "hello world"],
        "resources": {"cpu_cores": 0.1, "memory_mb": 128, "gpu_count": 0}
    },
    "simple_failure": {
        "name": "simple-failure",
        "image": "alpine:latest",
        "command": ["sh", "-c", "exit 1"],
        "resources": {"cpu_cores": 0.1, "memory_mb": 128, "gpu_count": 0}
    },
    "long_running": {
        "name": "long-running",
        "image": "alpine:latest",
        "command": ["sleep", "30"],
        "resources": {"cpu_cores": 0.1, "memory_mb": 128, "gpu_count": 0}
    },
    "cpu_intensive": {
        "name": "cpu-intensive",
        "image": "alpine:latest",
        "command": ["sleep", "10"],
        "resources": {"cpu_cores": 3.0, "memory_mb": 128, "gpu_count": 0}
    },
    "memory_intensive": {
        "name": "memory-intensive",
        "image": "alpine:latest",
        "command": ["sleep", "10"],
        "resources": {"cpu_cores": 0.1, "memory_mb": 3000, "gpu_count": 0}
    },
    "gpu_job": {
        "name": "gpu-job",
        "image": "alpine:latest",
        "command": ["sleep", "10"],
        "resources": {"cpu_cores": 0.1, "memory_mb": 128, "gpu_count": 1, "gpu_type": "nvidia-t4"}
    }
}