# Job Scheduler

A distributed job scheduling system with Docker workload execution, inspired by Nomad. Built with Python, Redis, and Docker.
This is a toy project.

## Architecture

### Components

**Scheduler (`src/scheduler/`)**: Central coordinator that manages job assignment
- Leader/follower architecture with Redis-based distributed locking
- Leader lock is greadily acquired by the first Leader
- Only the leader assigns jobs to available executors
- Exposes REST API for job management
- Handles resource allocation and job cleanup

**Executor (`src/executor/`)**: Worker nodes that execute jobs
- Introspects system resources (CPU, memory, GPU)
- Runs Docker containers for job execution
- Reports status and manages job lifecycle
- Handles GPU allocation and container cleanup

**Shared (`src/shared/`)**: Common libraries and models
- Redis client and job management
- Data models for jobs, executors, and resources
- Distributed locking and leadership coordination

## Job Lifecycle

When Jobs are created, they are added to a pending redis set. Periodically, the leader will processes the whole pending set and attempt to assign jobs to available executors based off their resources. When a job is assigned, it is removed from the pending set and added to the assigned jobs set for the respective executor in a single redis transaction. The executor is then responsible for synchronizing the Job state in redis with the locally running docker containers by launching and stopping containers as well as transitioning jobs to terminal states. When a job is transferred to a terminal state, the executor removes it from it's owned jobs.

### Job states

1. **Pending**: Job submitted to scheduler via API
2. **Assigned**: Leader scheduler assigns job to executor based on resource availability
3. **Running**: Executor starts Docker container and reports status
4. **Succeeded/Failed/Aborted**: Terminal states based on container exit code or user action

Jobs that can't be allocated within 15 seconds are automatically failed.

## Executor Lifecycle

1. **Registration**: Executor registers with Redis, reporting available resources
2. **Heartbeat**: Periodic health checks (5s interval) to maintain presence
3. **Job Polling**: Fetches assigned jobs from Redis
4. **Container Management**: Starts/stops Docker containers based on job status
5. **Resource Sync**: Updates available resources based on running jobs
6. **Cleanup**: Removes old containers after 120 seconds

## Scheduler Lifecycle

1. **Leadership Election**: Competes for leadership using Redis locks
2. **Job Assignment**: Leader processes pending jobs and assigns to best-fit executors
3. **Resource Monitoring**: Tracks executor health and available resources
4. **State Management**: Processes job state transitions and handles orphaned jobs
5. **Failover**: Non-leader schedulers become active if leader fails and lock expires

## Setup

### Prerequisites
- Python 3.12+
- Docker
- Redis
- [uv](https://github.com/astral-sh/uv) package manager
- [just](https://github.com/casey/just) command runner

### Installation

```bash
# Install dependencies
uv sync

# Build Docker image
just docker_build
```

### Running the System

**Development (single scheduler/executor):**
```bash
just docker_start_single
```

**Full cluster (3 schedulers, 3 executors):**
```bash
just docker_compose_up
```

**Restart containers after code changes:**
```bash
just docker_restart_containers
```

**Run e2e tests**
```bash
just test_e2e
```

Other helpful commands can be viewed in the [justfile]

## Resource Management

Jobs specify resource requirements (CPU cores, memory, GPU type/count). The scheduler uses a best-fit algorithm to assign jobs to executors with sufficient resources. GPU allocation is handled through Docker's GPU runtime.

## API Endpoints

- `POST /jobs` - Submit job
- `GET /jobs` - List jobs
- `GET /jobs/{id}` - Get job details
- `POST /jobs/{id}/abort` - Abort job
- `GET /executors` - List executors
- `GET /stats` - Scheduler statistics

## Configuration

Environment variables control scheduler and executor behavior:
- `REDIS_HOST/PORT` - Redis connection
- `SCHEDULER_ID` - Unique scheduler identifier
- `EXECUTOR_ID` - Unique executor identifier
- `EXECUTOR_CPUS/MEMORY/GPUS` - Resource limits
- `SCHEDULER_RELOAD` - Enable auto-reload in development

## Assorted Notes

- This project was developed on Mac so Nvidia GPU allocation is emulated but not actually assigned
- This project was developed to use one instance of Docker desktop - each executor also runs in docker and receives access to the docker daemon for job management. Because actual resources of the system cannot be easily introspected, an executor's available resources are instead passed in through env variables
- The actual docker container state management is fairly complicated. A best effort was made to manage these resources correctly, however much more robust logic would likely be needed for production use-cases
- For an actual production architecture, a different database other than Redis that offers better reliability guarantees via a consensus protocol (such as Raft) would probably be preferred. This would increase reliability and scalability at the expesnse of added complexity
- Because the leader processes pending jobs in batches, there is the opportunity to implement more optimized scheduling algorithms. The downside is that the single leader represents a bottleneck at scale
