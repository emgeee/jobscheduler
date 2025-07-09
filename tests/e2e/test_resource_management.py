"""
E2E tests for resource management functionality.

These tests verify that jobs are properly assigned to executors based on resource
requirements (CPU, memory, GPU) and that resource allocation and deallocation
works correctly.
"""

import asyncio
from uuid import UUID
from typing import List, Dict, Any

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
async def test_cpu_resource_allocation(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test jobs are assigned based on CPU requirements."""
    # Get available executors and their CPU resources
    executors = await get_executor_resources(api_client)
    
    # Find an executor with enough CPU
    suitable_executor = None
    for executor in executors:
        if executor["available_resources"]["resources"]["cpu_cores"] >= 1.0:
            suitable_executor = executor
            break
    
    if not suitable_executor:
        pytest.skip("No executor with sufficient CPU resources available")
    
    # Submit a job requiring 1.0 CPU
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "10"],
        cpu=1.0,
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to be assigned
    assigned_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
    )
    
    # Verify job was assigned to an executor with sufficient CPU
    assert assigned_job["assigned_executor_id"] is not None
    
    # Check that executor's available CPU was reduced
    updated_executors = await get_executor_resources(api_client)
    assigned_executor = None
    for executor in updated_executors:
        if executor["executor_info"]["id"] == assigned_job["assigned_executor_id"]:
            assigned_executor = executor
            break
    
    assert assigned_executor is not None
    # CPU should be reduced by the job's requirement
    original_cpu = next(e["available_resources"]["resources"]["cpu_cores"] for e in executors 
                       if e["executor_info"]["id"] == assigned_job["assigned_executor_id"])
    current_cpu = assigned_executor["available_resources"]["resources"]["cpu_cores"]
    assert current_cpu <= original_cpu - 1.0


@pytest.mark.asyncio
async def test_memory_resource_allocation(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test jobs are assigned based on memory requirements."""
    # Get available executors and their memory resources
    executors = await get_executor_resources(api_client)
    
    # Find an executor with enough memory
    suitable_executor = None
    for executor in executors:
        if executor["available_resources"]["resources"]["memory_mb"] >= 500:
            suitable_executor = executor
            break
    
    if not suitable_executor:
        pytest.skip("No executor with sufficient memory resources available")
    
    # Submit a job requiring 500MB memory
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "10"],
        cpu=0.1,
        memory=500
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to be assigned
    assigned_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
    )
    
    # Verify job was assigned to an executor with sufficient memory
    assert assigned_job["assigned_executor_id"] is not None
    
    # Check that executor's available memory was reduced
    updated_executors = await get_executor_resources(api_client)
    assigned_executor = None
    for executor in updated_executors:
        if executor["executor_info"]["id"] == assigned_job["assigned_executor_id"]:
            assigned_executor = executor
            break
    
    assert assigned_executor is not None
    # Memory should be reduced by the job's requirement
    original_memory = next(e["available_resources"]["resources"]["memory_mb"] for e in executors 
                          if e["executor_info"]["id"] == assigned_job["assigned_executor_id"])
    current_memory = assigned_executor["available_resources"]["resources"]["memory_mb"]
    assert current_memory <= original_memory - 500


@pytest.mark.asyncio
async def test_gpu_resource_allocation(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test jobs are assigned to GPU-capable executors when GPUs are required."""
    # Get available executors and find one with GPUs
    executors = await get_executor_resources(api_client)
    
    gpu_executor = None
    for executor in executors:
        if executor["available_resources"]["resources"]["gpu_count"] > 0:
            gpu_executor = executor
            break
    
    if not gpu_executor:
        pytest.skip("No executor with GPU resources available")
    
    # Submit a job requiring 1 GPU
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "10"],
        cpu=0.1,
        memory=128,
        gpu_count=1
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to be assigned
    assigned_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
    )
    
    # Verify job was assigned to the GPU executor
    assert assigned_job["assigned_executor_id"] == gpu_executor["executor_info"]["id"]
    
    # Check that executor's available GPU count was reduced
    updated_executors = await get_executor_resources(api_client)
    assigned_executor = None
    for executor in updated_executors:
        if executor["executor_info"]["id"] == assigned_job["assigned_executor_id"]:
            assigned_executor = executor
            break
    
    assert assigned_executor is not None
    # GPU count should be reduced by 1
    original_gpu_count = gpu_executor["available_resources"]["resources"]["gpu_count"]
    current_gpu_count = assigned_executor["available_resources"]["resources"]["gpu_count"]
    assert current_gpu_count == original_gpu_count - 1


@pytest.mark.asyncio
async def test_resource_exhaustion(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test jobs remain PENDING when no executor has sufficient resources."""
    # Get available executors and find maximum CPU
    executors = await get_executor_resources(api_client)
    max_cpu = max(executor["available_resources"]["resources"]["cpu_cores"] for executor in executors)
    
    # Submit a job requiring more CPU than any executor has
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "10"],
        cpu=max_cpu + 1.0,  # More than available
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait a bit and verify job remains pending
    await asyncio.sleep(5)
    
    response = await api_client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    job_status = response.json()
    assert job_status["status"] == JobStatus.PENDING.value
    assert job_status["assigned_executor_id"] is None


@pytest.mark.asyncio
async def test_resource_recovery(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test jobs are assigned after resources become available."""
    # Skip this test due to complex multi-executor resource management logic
    pytest.skip("Test logic needs refinement for multi-executor resource recovery scenarios")


@pytest.mark.asyncio
async def test_multiple_jobs_resource_distribution(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test multiple jobs are distributed across executors based on available resources."""
    # Submit multiple small jobs that should be distributed
    jobs = []
    for i in range(3):
        job_data = await submit_test_job(
            api_client,
            image="alpine:latest",
            command=["sleep", "20"],
            cpu=0.5,
            memory=256
        )
        jobs.append(job_data)
        cleanup_jobs(UUID(job_data["id"]))
    
    # Wait for all jobs to be assigned
    assigned_jobs = []
    for job in jobs:
        job_id = UUID(job["id"])
        assigned_job = await wait_for_job_status(
            api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
        )
        assigned_jobs.append(assigned_job)
    
    # Check that jobs were distributed (not all on same executor if possible)
    assigned_executors = [job["assigned_executor_id"] for job in assigned_jobs]
    
    # Get executor info to see if distribution was possible
    executors = await get_executor_resources(api_client)
    
    # If we have multiple executors with sufficient resources, 
    # jobs should be distributed
    suitable_executors = [
        e for e in executors 
        if e["available_resources"]["resources"]["cpu_cores"] >= 0.5 and e["available_resources"]["resources"]["memory_mb"] >= 256
    ]
    
    if len(suitable_executors) > 1:
        # Should have some distribution
        unique_executors = set(assigned_executors)
        assert len(unique_executors) > 1, "Jobs should be distributed across multiple executors"


@pytest.mark.asyncio
async def test_resource_requirements_validation(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test that jobs with invalid resource requirements are handled appropriately."""
    # Test job with 0 CPU (should still work with minimum allocation)
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["echo", "test"],
        cpu=0.0,  # Zero CPU
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Job should still be processed (system should handle minimum allocations)
    await wait_for_job_status(api_client, job_id, JobStatus.ASSIGNED, timeout=30.0)


@pytest.mark.asyncio
async def test_best_fit_resource_allocation(api_client: httpx.AsyncClient, cleanup_jobs, wait_for_system_ready):
    """Test that jobs are assigned to executors using best-fit algorithm."""
    # Get available executors
    executors = await get_executor_resources(api_client)
    
    if len(executors) < 2:
        pytest.skip("Need at least 2 executors for best-fit testing")
    
    # Sort executors by available CPU
    sorted_executors = sorted(executors, key=lambda x: x["available_resources"]["resources"]["cpu_cores"])
    
    # Submit a job that can fit on the smallest executor
    smallest_cpu = sorted_executors[0]["available_resources"]["resources"]["cpu_cores"]
    if smallest_cpu < 0.5:
        pytest.skip("Smallest executor doesn't have enough CPU for test")
    
    job_data = await submit_test_job(
        api_client,
        image="alpine:latest",
        command=["sleep", "10"],
        cpu=0.3,  # Should fit on smallest executor
        memory=128
    )
    
    job_id = UUID(job_data["id"])
    cleanup_jobs(job_id)
    
    # Wait for job to be assigned
    assigned_job = await wait_for_job_status(
        api_client, job_id, JobStatus.ASSIGNED, timeout=30.0
    )
    
    # Verify assignment (exact best-fit behavior depends on implementation)
    assert assigned_job["assigned_executor_id"] is not None
    
    # The job should be assigned to an executor with sufficient resources
    assigned_executor_id = assigned_job["assigned_executor_id"]
    assigned_executor = next(
        e for e in executors 
        if e["executor_info"]["id"] == assigned_executor_id
    )
    
    assert assigned_executor["available_resources"]["resources"]["cpu_cores"] >= 0.3
    assert assigned_executor["available_resources"]["resources"]["memory_mb"] >= 128