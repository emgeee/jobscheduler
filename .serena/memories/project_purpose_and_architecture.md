# Project Purpose and Architecture

## Purpose
This is a **distributed job scheduling system** built as a mini-Nomad clone for Docker workloads. The system implements a work trial project that mimics Hashicorp Nomad's functionality for scheduling Docker containers across a fleet of machines.

## Architecture
The system follows a **dual-executable architecture** with two main components:

### 1. Scheduler (`scheduler/main.py`)
- Central coordinator that exposes REST API for job management
- Handles job submission, status tracking, and resource allocation
- Implements leader election for high availability
- Manages job lifecycle through atomic state transitions

### 2. Executor (`executor/main.py`)
- Runs on each server to execute scheduled jobs
- Introspects system resources (CPU, memory, GPU)
- Manages Docker containers and reports job status
- Handles job abortion and cleanup

### 3. Shared Components (`shared/`)
- **RedisClient**: Core Redis operations for distributed coordination
- **Models**: Pydantic models for job definitions, statuses, and resources
- **Utils**: Common utilities for system operations and validation

### 4. CLI (`cli/`)
- Command-line interface for interacting with the scheduler API
- Rich formatting for job listings and status displays

## Key Features
- **Distributed Coordination**: Leader election and job claiming using Redis
- **Resource Management**: CPU, memory, and GPU allocation tracking
- **Job Lifecycle**: PENDING → RUNNING → SUCCEEDED/FAILED/ABORTED
- **High Availability**: Multi-scheduler support with automatic failover
- **Docker Integration**: Full container orchestration capabilities