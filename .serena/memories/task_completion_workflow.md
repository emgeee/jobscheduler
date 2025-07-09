# Task Completion Workflow

## Required Steps After Code Changes

### 1. Type Checking (MANDATORY)
```bash
uv run basedpyright .
```
- **CRITICAL**: Always run type checking before formatting
- Fix any type errors or add line comments to ignore specific errors
- Type checking validates code correctness and catches issues early

### 2. Code Formatting
```bash
uv run ruff format .
```
- Run ONLY after type checking is clean
- Maintains consistent code style across the project

### 3. Testing
```bash
# Run all tests
uv run python -m pytest tests/ -v

# Run specific tests for changed components
uv run python -m pytest tests/scheduler/ -v  # for scheduler changes
uv run python -m pytest tests/shared/ -v     # for shared component changes
uv run python -m pytest tests/executor/ -v   # for executor changes
```

### 4. Linting (Optional but Recommended)
```bash
uv run ruff check .
```

## Important Notes
- **Redis Must Be Running**: Tests will fail if Redis is not available
- **No Mocking**: Don't mock Redis during unit tests (per project requirements)
- **Fix Before Proceeding**: All type errors and test failures must be resolved
- **Loguru Only**: Use `from loguru import logger` instead of standard logging

## Testing Strategy
- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **No Mocking**: Use real Redis for authentic testing
- **Coverage**: Maintain good test coverage for critical paths

## Common Issues
- **Redis Connection**: If tests fail due to Redis, notify user to start Redis
- **Type Errors**: Add `# pyright: ignore[specific-error]` comments if needed
- **Import Errors**: Ensure all imports use absolute paths from project root