# Codebase Structure

## Directory Organization

### `/scheduler/` - Scheduler Service
- **`main.py`**: Entry point and main application setup
- **`__main__.py`**: Module execution entry point
- **`app.py`**: FastAPI application setup with lifespan management
- **`api.py`**: REST API endpoints and request/response models
- **`job_manager.py`**: Core job lifecycle management and coordination
- **`settings.py`**: Scheduler-specific configuration settings

### `/executor/` - Executor Service
- **`main.py`**: Entry point for executor service (job execution)

### `/shared/` - Shared Components
- **`models.py`**: Pydantic models for Job, JobDefinition, ExecutorInfo, etc.
- **`redis_client.py`**: Core Redis operations and distributed coordination
- **`config.py`**: Global configuration and settings management
- **`utils.py`**: Common utilities (ID generation, timestamps, resources)
- **`logging_config.py`**: Logging setup using loguru

### `/cli/` - Command Line Interface
- **`main.py`**: Click-based CLI with job management commands
- **`client.py`**: HTTP client for API communication
- **`formatters.py`**: Rich formatting for terminal output

### `/tests/` - Test Suite
- **`conftest.py`**: Pytest fixtures and test configuration
- **`shared/`**: Tests for shared components
- **`scheduler/`**: Tests for scheduler functionality
- **`executor/`**: Tests for executor functionality

## Key Files
- **`pyproject.toml`**: Project configuration, dependencies, and tool settings
- **`justfile`**: Development task automation
- **`CLAUDE.md`**: Project instructions and development guidelines
- **`PROBLEM_DESCRIPTION.md`**: Original requirements (DO NOT MODIFY)
- **`README.md`**: Comprehensive project documentation

## Data Flow
1. **Job Submission**: Client → Scheduler API → JobManager → Redis
2. **Job Allocation**: JobManager → Redis (atomic operations) → Executor
3. **Job Execution**: Executor → Docker → Status Updates → Redis
4. **Status Retrieval**: Client → Scheduler API → Redis → Response

## Inter-Service Communication
- **Redis Pub/Sub**: Real-time job status updates
- **Redis Locks**: Atomic operations and resource allocation
- **Redis Queues**: Job distribution and processing
- **HTTP API**: Client-scheduler communication