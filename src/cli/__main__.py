#!/usr/bin/env python3

import json
import os
import sys
from typing import Dict, List, Optional
from uuid import UUID

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import click
import httpx
from pydantic import ValidationError

from cli.client import APIClient
from cli.formatters import (
    print_error,
    print_executors_table,
    print_health_status,
    print_info,
    print_job_details,
    print_jobs_table,
    print_json,
    print_status_summary,
    print_success,
    print_warning,
)
from src.shared.models import GPUType, JobDefinition, JobStatus, ResourceRequirements


@click.group()
@click.option(
    "--api-url",
    default="http://localhost:8000",
    help="API base URL",
    envvar="API_URL",
)
@click.pass_context
def cli(ctx, api_url):
    """Job Scheduler CLI - Interact with the scheduler API"""
    ctx.ensure_object(dict)
    ctx.obj["api_url"] = api_url


@cli.group()
def jobs():
    """Job management commands"""
    pass


@jobs.command()
@click.option("--name", required=True, help="Job name")
@click.option("--image", required=True, help="Docker image")
@click.option("--command", required=True, help="Command to run (JSON array)")
@click.option("--cpu-cores", type=float, required=True, help="CPU cores required")
@click.option("--memory-mb", type=int, required=True, help="Memory in MB required")
@click.option(
    "--gpu-type", type=click.Choice([t.value for t in GPUType]), help="GPU type"
)
@click.option("--gpu-count", type=int, default=0, help="Number of GPUs")
@click.option("--env", multiple=True, help="Environment variables (key=value)")
@click.pass_context
def submit(
    ctx,
    name,
    image,
    command,
    cpu_cores,
    memory_mb,
    gpu_type,
    gpu_count,
    env,
):
    """Submit a new job"""
    # Parse command as JSON array
    command_list = json.loads(command)
    if not isinstance(command_list, List):
        raise ValueError("Command must be a JSON array")

    # Parse environment variables
    env_dict = {}
    for env_var in env:
        if "=" not in env_var:
            raise ValueError(f"Invalid environment variable format: {env_var}")
        key, value = env_var.split("=", 1)
        env_dict[key] = value

    # Create resource requirements
    resources = ResourceRequirements(
        cpu_cores=cpu_cores,
        memory_mb=memory_mb,
        gpu_type=GPUType(gpu_type) if gpu_type else None,
        gpu_count=gpu_count,
    )

    # Create job definition
    job_def = JobDefinition(
        name=name,
        image=image,
        command=command_list,
        resources=resources,
        environment=env_dict,
    )

    print(job_def)

    # Submit job
    with APIClient(ctx.obj["api_url"]) as client:
        result = client.submit_job(job_def)
        print_success(f"Job submitted successfully: {result['id']}")
        print_job_details(result)


@jobs.command()
@click.argument("job_id", type=str)
@click.pass_context
def get(ctx, job_id):
    """Get job details by ID"""
    try:
        job_uuid = UUID(job_id)
        with APIClient(ctx.obj["api_url"]) as client:
            result = client.get_job(job_uuid)
            print_job_details(result)

    except ValueError:
        print_error("Invalid job ID format")
    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@jobs.command()
@click.option(
    "--status", type=click.Choice([s.value for s in JobStatus]), help="Filter by status"
)
@click.option("--table", is_flag=True, help="Display as table")
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - refresh every 2 seconds"
)
@click.option("--interval", "-i", type=int, default=2, help="Watch interval in seconds")
@click.pass_context
def list(ctx, status, table, watch, interval):
    """List jobs with optional filtering"""
    try:
        status_filter = JobStatus(status) if status else None

        if watch:
            import time

            from rich.console import Console

            console = Console()

            console.print("[bold cyan]Jobs List (Watch Mode)[/bold cyan]")
            console.print(
                f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
            )

            try:
                while True:
                    with APIClient(ctx.obj["api_url"]) as client:
                        result = client.list_jobs(status_filter)

                        # Clear screen
                        console.clear()
                        console.print("[bold cyan]Jobs List (Watch Mode)[/bold cyan]")
                        console.print(
                            f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
                        )

                        if table:
                            print_jobs_table(result)
                        else:
                            print_json(result, "Jobs List")

                    time.sleep(interval)

            except KeyboardInterrupt:
                console.print("\n[yellow]Watch mode stopped.[/yellow]")

        else:
            with APIClient(ctx.obj["api_url"]) as client:
                result = client.list_jobs(status_filter)

                if table:
                    print_jobs_table(result)
                else:
                    print_json(result, "Jobs List")

    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@jobs.command()
@click.argument("job_id", type=str)
@click.pass_context
def abort(ctx, job_id):
    """Abort a job"""
    try:
        job_uuid = UUID(job_id)
        with APIClient(ctx.obj["api_url"]) as client:
            client.abort_job(job_uuid)
            print_success(f"Job {job_id} aborted successfully")

    except ValueError:
        print_error("Invalid job ID format")
    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@cli.command()
@click.pass_context
def health(ctx):
    """Check scheduler health"""
    try:
        with APIClient(ctx.obj["api_url"]) as client:
            result = client.health_check()
            print_health_status(result)

    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@cli.command()
@click.option("--json", "output_json", is_flag=True, help="Output raw JSON")
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - refresh every 2 seconds"
)
@click.option("--interval", "-i", type=int, default=2, help="Watch interval in seconds")
@click.pass_context
def status(ctx, output_json, watch, interval):
    """Get JobManager status and statistics"""
    try:
        if watch:
            import time

            from rich.console import Console
            from rich.json import JSON
            from rich.live import Live

            console = Console()

            def get_status_display():
                try:
                    with APIClient(ctx.obj["api_url"]) as client:
                        result = client.get_status()
                        if output_json:
                            return JSON(json.dumps(result, indent=2, default=str))
                        else:
                            # Create a renderable for the status
                            import sys
                            from io import StringIO

                            from rich.columns import Columns
                            from rich.panel import Panel

                            # Capture the output of print_status_summary
                            old_stdout = sys.stdout
                            sys.stdout = mystdout = StringIO()

                            try:
                                print_status_summary(result)
                                output = mystdout.getvalue()
                            finally:
                                sys.stdout = old_stdout

                            return output
                except Exception as e:
                    return f"[red]Error: {e}[/red]"

            console.print("[bold cyan]JobManager Status (Watch Mode)[/bold cyan]")
            console.print(
                f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
            )

            try:
                while True:
                    with APIClient(ctx.obj["api_url"]) as client:
                        result = client.get_status()

                        # Clear screen
                        console.clear()
                        console.print(
                            "[bold cyan]JobManager Status (Watch Mode)[/bold cyan]"
                        )
                        console.print(
                            f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
                        )

                        if output_json:
                            print_json(result, "Status")
                        else:
                            print_status_summary(result)

                    time.sleep(interval)

            except KeyboardInterrupt:
                console.print("\n[yellow]Watch mode stopped.[/yellow]")

        else:
            with APIClient(ctx.obj["api_url"]) as client:
                result = client.get_status()

                if output_json:
                    print_json(result, "JobManager Status")
                else:
                    print_status_summary(result)

    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@cli.group()
def executors():
    """Executor management commands"""
    pass


@executors.command("list")
@click.option("--table", is_flag=True, help="Display as table")
@click.option(
    "--watch", "-w", is_flag=True, help="Watch mode - refresh every 2 seconds"
)
@click.option("--interval", "-i", type=int, default=2, help="Watch interval in seconds")
@click.pass_context
def list_executors(ctx, table, watch, interval):
    """List healthy executors with their available resources"""
    try:
        if watch:
            import time

            from rich.console import Console

            console = Console()

            console.print("[bold cyan]Healthy Executors (Watch Mode)[/bold cyan]")
            console.print(
                f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
            )

            try:
                while True:
                    with APIClient(ctx.obj["api_url"]) as client:
                        result = client.list_healthy_executors()

                        # Clear screen
                        console.clear()
                        console.print(
                            "[bold cyan]Healthy Executors (Watch Mode)[/bold cyan]"
                        )
                        console.print(
                            f"[dim]Refreshing every {interval} seconds. Press Ctrl+C to exit.[/dim]\n"
                        )

                        if table:
                            print_executors_table(result)
                        else:
                            print_json(result, "Healthy Executors")

                    time.sleep(interval)

            except KeyboardInterrupt:
                console.print("\n[yellow]Watch mode stopped.[/yellow]")

        else:
            with APIClient(ctx.obj["api_url"]) as client:
                result = client.list_healthy_executors()

                if table:
                    print_executors_table(result)
                else:
                    print_json(result, "Healthy Executors")

    except httpx.HTTPStatusError as e:
        print_error(f"API error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print_error(f"Unexpected error: {e}")


@cli.command()
def examples():
    """Show example commands"""
    examples_text = """
[bold cyan]Job Scheduler CLI Examples[/bold cyan]

[yellow]Submit a simple job:[/yellow]
scheduler jobs submit --name "hello-world" --image "alpine:latest" --command '["echo", "Hello World"]' --cpu-cores 1.0 --memory-mb 512

[yellow]Submit a job with GPU:[/yellow]
scheduler jobs submit --name "ml-training" --image "tensorflow/tensorflow:latest-gpu" --command '["python", "train.py"]' --cpu-cores 4.0 --memory-mb 8192 --gpu-type "nvidia-t4" --gpu-count 1

[yellow]Submit a job with environment variables:[/yellow]
scheduler jobs submit --name "env-test" --image "alpine:latest" --command '["env"]' --cpu-cores 0.5 --memory-mb 256 --env "DEBUG=true" --env "LOG_LEVEL=info"

[yellow]List all jobs:[/yellow]
scheduler jobs list

[yellow]List jobs in table format:[/yellow]
scheduler jobs list --table

[yellow]Filter jobs by status:[/yellow]
scheduler jobs list --status pending --table

[yellow]Get job details:[/yellow]
scheduler jobs get <job-id>

[yellow]Abort a job:[/yellow]
scheduler jobs abort <job-id>

[yellow]Set executor eligibility:[/yellow]
scheduler executors set-eligible executor-1 true
scheduler executors set-eligible executor-1 false

[yellow]Check health:[/yellow]
scheduler health

[yellow]Get JobManager status:[/yellow]
scheduler status

[yellow]Get status in JSON format:[/yellow]
scheduler status --json

[yellow]Watch status with live updates:[/yellow]
scheduler status --watch

[yellow]Watch status with custom interval:[/yellow]
scheduler status --watch --interval 5

[yellow]Use custom API URL:[/yellow]
scheduler --api-url http://localhost:8080 health
# or set environment variable:
export SCHEDULER_API_URL=http://localhost:8080
scheduler health
"""
    from rich.console import Console
    from rich.markdown import Markdown

    console = Console()
    console.print(Markdown(examples_text))


if __name__ == "__main__":
    cli()
