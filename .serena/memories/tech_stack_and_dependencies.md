# Tech Stack and Dependencies

## Core Technologies
- **Python >= 3.12**: Primary programming language
- **Redis**: Storage, consensus, locking, and pub/sub communication
- **Docker**: Container orchestration and execution
- **FastAPI**: REST API framework for the scheduler
- **Pydantic**: Data validation and settings management

## Key Dependencies
- **docker>=7.1.0**: Docker Python SDK for container management
- **fastapi>=0.116.0**: Web framework for API endpoints
- **uvicorn>=0.27.0**: ASGI server for FastAPI
- **pydantic>=2.11.7**: Data validation and serialization
- **pydantic-settings>=2.10.1**: Settings management
- **httpx>=0.26.0**: HTTP client for API calls
- **psutil>=5.9.8**: System resource monitoring
- **GPUtil>=1.4.0**: GPU resource detection
- **redis>=6.2.0**: Redis client for distributed coordination
- **click>=8.2.1**: CLI framework
- **rich>=14.0.0**: Terminal formatting and tables
- **loguru>=0.7.3**: Structured logging (replaces standard logging)

## Development Tools
- **uv**: Package manager for dependency management
- **ruff**: Code formatting and linting
- **basedpyright**: Static type checking
- **pytest**: Testing framework with async support
- **pytest-cov**: Test coverage reporting

## System Requirements
- **Redis server**: Must be running for the system to function
- **Docker daemon**: Required for container execution
- **macOS/Linux**: Developed on Darwin (macOS) platform