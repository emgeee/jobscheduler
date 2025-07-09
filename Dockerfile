# Multi-stage build for jobscheduler
FROM python:3.12-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Set work directory
WORKDIR /app

# Copy dependency files for better caching
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Production stage
FROM python:3.12-slim

# Install system dependencies needed at runtime
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r jobscheduler && useradd -r -g jobscheduler jobscheduler

# Set work directory
WORKDIR /app

# Copy installed dependencies from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/

# Change ownership to non-root user
RUN chown -R jobscheduler:jobscheduler /app

# Switch to non-root user
USER jobscheduler

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose port for scheduler API
EXPOSE 8000

# Set entrypoint to python for flexible service selection
ENTRYPOINT ["python"]
