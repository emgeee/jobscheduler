# Code Style and Conventions

## Core Principles
- **Simplicity**: Keep code as simple and concise as possible
- **No Backwards Compatibility**: Don't worry about backwards compatibility when making changes
- **Defensive Design**: Always use proper error handling and validation

## Python Style
- **Type Hints**: Use comprehensive type annotations
- **Async/Await**: Prefer async patterns for I/O operations
- **Pydantic Models**: Use Pydantic for data validation and serialization
- **Error Handling**: Comprehensive exception handling with proper logging

## Naming Conventions
- **Classes**: PascalCase (e.g., `JobManager`, `RedisClient`)
- **Functions/Methods**: snake_case (e.g., `get_job_status`, `atomic_update`)
- **Variables**: snake_case (e.g., `job_id`, `executor_info`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `DEFAULT_TIMEOUT`)

## Code Organization
- **Shared Components**: Common models, utilities, and Redis client in `shared/`
- **Service Separation**: Clear separation between scheduler and executor logic
- **Atomic Operations**: Use Redis locks and atomic operations for consistency
- **Resource Management**: Proper cleanup with context managers and try/finally blocks

## Import Style
- **Absolute Imports**: Use absolute imports from project root
- **Logical Grouping**: Group imports by standard library, third-party, and local modules
- **Specific Imports**: Import specific classes/functions rather than entire modules when possible

## Documentation
- **Docstrings**: Google-style docstrings for public APIs
- **Type Annotations**: Comprehensive type hints serve as documentation
- **Comments**: Minimal comments, prefer self-documenting code
- **README**: Comprehensive README with API documentation and examples