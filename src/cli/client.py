import os
import sys
from typing import Any, Dict, List, Optional
from uuid import UUID

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from pydantic import BaseModel

from src.shared.models import JobDefinition, JobStatus


class APIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url)

    def submit_job(self, definition: JobDefinition) -> Dict[str, Any]:
        """Submit a new job"""
        response = self.client.post(
            "/jobs", json={"definition": definition.model_dump()}
        )
        response.raise_for_status()
        return response.json()

    def get_job(self, job_id: UUID) -> Dict[str, Any]:
        """Get job details by ID"""
        response = self.client.get(f"/jobs/{job_id}")
        response.raise_for_status()
        return response.json()

    def list_jobs(
        self,
        status_filter: Optional[JobStatus] = None,
    ) -> Dict[str, Any]:
        """List jobs with optional filtering"""
        params: Dict[str, Any] = {}
        if status_filter:
            params["status_filter"] = status_filter.value

        response = self.client.get("/jobs", params=params)
        response.raise_for_status()
        return response.json()

    def abort_job(self, job_id: UUID) -> None:
        """Abort a job"""
        response = self.client.post(f"/jobs/{job_id}/abort")
        response.raise_for_status()

    def health_check(self) -> Dict[str, Any]:
        """Health check"""
        response = self.client.get("/health")
        response.raise_for_status()
        return response.json()

    def get_status(self) -> Dict[str, Any]:
        """Get JobManager status"""
        response = self.client.get("/status")
        response.raise_for_status()
        return response.json()

    def list_healthy_executors(self) -> Dict[str, Any]:
        """List healthy executors with their available resources"""
        response = self.client.get("/executors/healthy")
        response.raise_for_status()
        return response.json()

    def close(self):
        """Close the client"""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
