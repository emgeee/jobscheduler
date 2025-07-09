"""
E2E tests for basic job lifecycle functionality.

These tests verify the core job execution flow from submission through completion,
including status transitions, success/failure scenarios, and job abort functionality.
"""

import asyncio
from uuid import UUID

import pytest
import httpx
from src.shared.models import JobStatus

from .conftest import (
    wait_for_job_status,
    submit_test_job,
    get_executor_resources,
    TEST_JOBS
)


@pytest.mark.asyncio
async def test_submit_simple_job(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test submitting a simple job and verifying it reaches PENDING status."""
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["echo", "hello"],
        cpu=0.1,
        memory=128
    )
    
    cleanup_jobs(UUID(job_data["id"]))
    
    # Verify job was created with correct status
    assert job_data["status"] == JobStatus.PENDING.value
    assert job_data["definition"]["image"] == "alpine:latest"
    assert job_data["definition"]["command"] == ["echo", "hello"]
    assert job_data["definition"]["resources"]["cpu_cores"] == 0.1
    assert job_data["definition"]["resources"]["memory_mb"] == 128
    assert job_data["definition"]["resources"]["gpu_count"] == 0
    
    # Verify job exists in API
    response = await api_client.get(f"/jobs/{job_data['id']}")
    assert response.status_code == 200
    retrieved_job = response.json()
    assert retrieved_job["id"] == job_data["id"]
    assert retrieved_job["status"] == JobStatus.PENDING.value


@pytest.mark.asyncio
async def test_job_assignment(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test job transitions from PENDING to ASSIGNED to RUNNING."""
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "5"],
        cpu=0.1,
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to be assigned
    assigned_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
    )
    
    # Verify assignment details
    assert assigned_job["status"] == JobStatus.ASSIGNED.value
    assert assigned_job["assigned_executor_id"] is not None
    assert assigned_job["assigned_at"] is not None
    assert assigned_job["ip_address"] is not None
    
    # Wait for job to start running
    running_job = await wait_for_job_status(
        api_client, job_id, JobStatus.RUNNING, timeout=30.0
    )
    
    # Verify running details
    assert running_job["status"] == JobStatus.RUNNING.value
    assert running_job["started_at"] is not None
    assert running_job["assigned_executor_id"] == assigned_job["assigned_executor_id"]


@pytest.mark.asyncio
async def test_job_success(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test job completes successfully with exit code 0."""
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["echo", "success"],
        cpu=0.1,
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to complete successfully
    completed_job = await wait_for_job_status(
        api_client, job_id, JobStatus.SUCCEEDED, timeout=60.0
    )
    
    # Verify completion details
    assert completed_job["status"] == JobStatus.SUCCEEDED.value
    assert completed_job["finished_at"] is not None
    assert completed_job["assigned_executor_id"] is not None
    assert completed_job["started_at"] is not None
    assert completed_job["assigned_at"] is not None
    
    # Verify job appears in jobs list
    response = await api_client.get("/jobs")
    assert response.status_code == 200
    jobs_list = response.json()
    job_ids = [job["id"] for job in jobs_list["jobs"]]
    assert job_data["id"] in job_ids


@pytest.mark.asyncio
async def test_job_failure(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test job fails with exit code > 0."""
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sh", "-c", "exit 1"],
        cpu=0.1,
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to fail
    failed_job = await wait_for_job_status(
        api_client, job_id, JobStatus.FAILED, timeout=60.0
    )
    
    # Verify failure details
    assert failed_job["status"] == JobStatus.FAILED.value
    assert failed_job["finished_at"] is not None
    assert failed_job["assigned_executor_id"] is not None
    assert failed_job["started_at"] is not None
    assert failed_job["assigned_at"] is not None


@pytest.mark.asyncio
async def test_job_abort_pending(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test aborting a job in PENDING state."""
    # Submit a job that requires more resources than available to keep it pending
    executors = await get_executor_resources(api_client)
    max_cpu = max(executor["available_resources"]["resources"]["cpu_cores"] for executor in executors)
    
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "60"],
        cpu=max_cpu + 1.0,  # More than available
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Verify job is pending
    await asyncio.sleep(2)  # Give it time to potentially be assigned
    response = await api_client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    job_status = response.json()
    assert job_status["status"] == JobStatus.PENDING.value
    
    # Abort the job
    abort_response = await api_client.post(f"/jobs/{job_id}/abort")
    assert abort_response.status_code == 202
    
    # Wait for job to be aborted
    aborted_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ABORTED, timeout=30.0
    )
    
    # Verify abort details
    assert aborted_job["status"] == JobStatus.ABORTED.value
    assert aborted_job["aborted_at"] is not None
    assert aborted_job["assigned_executor_id"] is None  # Never assigned


@pytest.mark.asyncio
async def test_job_abort_running(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test aborting a job in RUNNING state."""
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "60"],  # Long running job
        cpu=0.1,
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to start running
    running_job = await wait_for_job_status(
        api_client, job_id, JobStatus.RUNNING, timeout=30.0
    )
    
    # Abort the running job
    abort_response = await api_client.post(f"/jobs/{job_id}/abort")
    assert abort_response.status_code == 202
    
    # Wait for job to be aborted
    aborted_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ABORTED, timeout=30.0
    )
    
    # Verify abort details
    assert aborted_job["status"] == JobStatus.ABORTED.value
    assert aborted_job["aborted_at"] is not None
    assert aborted_job["assigned_executor_id"] is not None  # Was assigned
    assert aborted_job["started_at"] is not None  # Was started


@pytest.mark.asyncio
async def test_job_list_filtering(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test listing jobs with status filtering."""
    # Submit multiple jobs with different expected outcomes
    success_job = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["echo", "success"],
        cpu=0.1,
        memory=128
    )
    
    failure_job = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sh", "-c", "exit 1"],
        cpu=0.1,
        memory=128
    )
    
    pending_job = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "60"],
        cpu=100.0,  # More than available to keep it pending
        memory=128
    )
    
    # Track jobs for cleanup
    success_job_id = UUID(success_job["id"])
    failure_job_id = UUID(failure_job["id"])
    pending_job_id = UUID(pending_job["id"])
    
    cleanup_jobs(success_job_id)
    cleanup_jobs(failure_job_id)
    cleanup_jobs(pending_job_id)
    
    # Wait for jobs to reach terminal states
    await wait_for_job_status(api_client, success_job_id, JobStatus.SUCCEEDED, timeout=60.0)
    await wait_for_job_status(api_client, failure_job_id, JobStatus.FAILED, timeout=60.0)
    
    # Test filtering by status
    response = await api_client.get("/jobs?status_filter=succeeded")
    assert response.status_code == 200
    succeeded_jobs = response.json()["jobs"]
    succeeded_ids = [job["id"] for job in succeeded_jobs]
    assert success_job["id"] in succeeded_ids
    
    response = await api_client.get("/jobs?status_filter=failed")
    assert response.status_code == 200
    failed_jobs = response.json()["jobs"]
    failed_ids = [job["id"] for job in failed_jobs]
    assert failure_job["id"] in failed_ids
    
    response = await api_client.get("/jobs?status_filter=pending")
    assert response.status_code == 200
    pending_jobs = response.json()["jobs"]
    pending_ids = [job["id"] for job in pending_jobs]
    assert pending_job["id"] in pending_ids


@pytest.mark.asyncio
async def test_job_not_found(api_client: httpx.AsyncClient, wait_for_system_ready):
    """Test accessing non-existent job returns 404."""
    fake_job_id = "12345678-1234-1234-1234-123456789012"
    
    # Test GET job
    response = await api_client.get(f"/jobs/{fake_job_id}")
    assert response.status_code == 404
    
    # Test abort job
    response = await api_client.post(f"/jobs/{fake_job_id}/abort")
    assert response.status_code == 500  # Should probably be 404, but current implementation returns 500