# Run tests
test *args:
    uv run python -m pytest tests/ --ignore=tests/e2e {{args}}

# Run E2E tests (requires docker compose to be running)
test_e2e *args:
    uv run python -m pytest tests/e2e/ {{args}}

# Run E2E tests with verbose output
test_e2e_verbose *args:
    uv run python -m pytest tests/e2e/ -v {{args}}

# Run basic job lifecycle E2E tests only
test_e2e_lifecycle *args:
    uv run python -m pytest tests/e2e/test_basic_job_lifecycle.py {{args}}

# Run resource management E2E tests only
test_e2e_resources *args:
    uv run python -m pytest tests/e2e/test_resource_management.py {{args}}

# Run the CLI with arguments
cli *args:
    uv run python -m src.cli {{args}}

ipython:
  uv run ipython --ext autoreload --InteractiveShellApp.exec_lines="autoreload 2" \
    --InteractiveShellApp.exec_lines="from src.shared.redis_client import create_client; redis_client = create_client()" \
    --InteractiveShellApp.exec_lines="from src.executor.docker_manager import DockerManager; docker_manager = DockerManager.from_environment()" \
    --InteractiveShellApp.exec_lines="from src.shared.job_manager import JobManager; job_manager = JobManager(redis_client=redis_client)"

watch_jobs:
  uv run python -m src.cli jobs list --table --watch

watch_executors:
  uv run python -m src.cli executors list --table --watch

submit_job:
  uv run python -m src.cli jobs submit --name "short-success" --image "alpine:latest" --command '["sh", "-c", "exit 0"]' --cpu-cores 1.0 --memory-mb 512

submit_job_fail:
  uv run python -m src.cli jobs submit --name "short-fail" --image "alpine:latest" --command '["sh", "-c", "exit 1"]' --cpu-cores 1.0 --memory-mb 512

submit_job_long:
  uv run python -m src.cli jobs submit --name "long-success" --image "alpine:latest" --command '["sh", "-c", "sleep 30"]' --cpu-cores 1.0 --memory-mb 512

submit_job_long_fail:
  uv run python -m src.cli jobs submit --name "long-fail" --image "alpine:latest" --command '["sh", "-c", "sleep 30; exit 1"]' --cpu-cores 1.0 --memory-mb 512

submit_gpu_job_long:
  uv run python -m src.cli jobs submit --name "long-success" --image "alpine:latest" --command '["sh", "-c", "sleep 30"]' --cpu-cores 1.0 --memory-mb 512 --gpu-type "nvidia-t4" --gpu-count 1

# Flush Redis database (works with dockerized Redis on port 6380)
flush_redis:
    redis-cli -p 6380 FLUSHALL

start_scheduler:
  SCHEDULER_RELOAD=true uv run python -m src.scheduler

lint:
  ruff check .

format:
  ruff format .

typecheck:
  basedpyright .

docker_build:
  docker buildx build --load -t jobscheduler .

docker_start_single:
  docker compose up redis scheduler1 executor1 --watch

docker_restart_containers:
    @bash -c 'services=$(docker compose ps --services | grep -v "^redis$"); \
              echo "Restarting: $services"; \
              docker compose restart $services'

docker_compose_up:
  docker compose up --watch
