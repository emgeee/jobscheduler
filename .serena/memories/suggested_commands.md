# Suggested Commands for Development

## Package Management
```bash
# Install dependencies
uv install

# Add new dependency
uv add <package-name>
```

## Code Quality
```bash
# Type checking (REQUIRED before formatting)
uv run basedpyright .
# or for specific files:
uv run basedpyright <path-to-file>

# Format code (run AFTER type checking)
uv run ruff format .

# Linting
uv run ruff check .
```

## Testing
```bash
# Run all tests
uv run python -m pytest tests/ -v

# Run specific test file
uv run python -m pytest tests/shared/test_models.py -v

# Run tests with coverage
uv run python -m pytest tests/ --cov=. --cov-report=html

# Run specific test class
uv run python -m pytest tests/scheduler/test_api.py::TestJobSubmission -v
```

## Running Services
```bash
# Start scheduler with reload (development)
SCHEDULER_RELOAD=true uv run python -m scheduler

# Start scheduler (production)
uv run python -m scheduler.main

# Start executor
uv run python -m executor.main

# Use CLI tool
python cli/main.py --help
```

## Redis Management
```bash
# Flush all Redis keys (cleanup)
redis-cli FLUSHALL

# Check Redis connection
redis-cli ping
```

## Justfile Commands
```bash
# Available just commands:
just test              # Run tests
just lint              # Run linting
just format            # Format code
just typecheck         # Type checking
just clean_redis       # Flush Redis
just start_scheduler   # Start scheduler with reload
just cli <args>        # Run CLI with arguments
```

## Development Workflow
1. **Always run type checking first**: `uv run basedpyright .`
2. **Fix any type errors** (add ignore comments if needed)
3. **Format code**: `uv run ruff format .`
4. **Run relevant tests**: `uv run python -m pytest tests/ -v`
5. **Fix any test failures**