# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.



## Project Overview

This is a Python job scheduling system with two main components:
- **Scheduler** (`src/scheduler`): Coordinates and manages job execution
    - Follows a leader/follower architecture
    - Leader is responsible for assigning jobs to available executors based off of available resources
- **Executor** (`src/executor`): Executes scheduled jobs
    - Uses redis for coordinating with schedulers and reporting status
    - Uses docker for running jobs
    - periodically polls running jobs and updates job state in redis


## Code style
- *IMPORTANT*: Do not worry about backwards compatibility when making changes -- there are NO existing deployments.
- *IMPORTANT*: Keep code as simple as simple and concise as possible!
- prefer writing async code using asyncio
- use loguru for logging `from loguru import logger`
- *IMPORTANT* always ask before adding or updating any tests!


### Python Environment
- Requires Python >= 3.12
- Uses standard Python project structure with `pyproject.toml`
- Use uv for dependency management. New dependencies should always be added with `uv add <dependency>`
- basedpyright for type checking: `basedpyright <path to file>`.
- Use ruff for formatting 
- *IMPORTANT* always run basedpyright and fix any errors before formatting code. If you cannot fix the error, add *line comments* instructing basedpyright to ignore the specific error
- *IMPORTANT* do not mock redis during unit tests

### Testing
- Run all tests: `uv run python -m pytest tests/ -v`
- Run specific test file: `uv run python -m pytest tests/shared/test_models.py -v`
- Run tests with coverage: `uv run python -m pytest tests/ --cov=. --cov-report=html`
- *IMPORTANT* redis must be running for unit tests to pass. If it is not running let me know and I will start it
