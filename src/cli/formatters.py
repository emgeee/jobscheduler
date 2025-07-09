import json
from typing import Any, Dict

from rich.console import Console
from rich.json import JSON
from rich.table import Table
from rich.text import Text


console = Console()


def print_json(data: Any, title: str = "") -> None:
    """Pretty print JSON data with syntax highlighting"""
    if title:
        console.print(f"\n[bold cyan]{title}[/bold cyan]")

    json_str = json.dumps(data, indent=2, default=str)
    renderable = JSON(json_str)
    console.print(renderable)


def print_error(message: str) -> None:
    """Print error message in red"""
    console.print(f"[bold red]Error:[/bold red] {message}")


def print_success(message: str) -> None:
    """Print success message in green"""
    console.print(f"[bold green]✓[/bold green] {message}")


def print_warning(message: str) -> None:
    """Print warning message in yellow"""
    console.print(f"[bold yellow]⚠[/bold yellow] {message}")


def print_info(message: str) -> None:
    """Print info message in blue"""
    console.print(f"[bold blue]ℹ[/bold blue] {message}")


def print_jobs_table(jobs_data: Dict[str, Any]) -> None:
    """Print jobs in a formatted table"""
    if not jobs_data.get("jobs"):
        print_info("No jobs found")
        return

    table = Table(title="Jobs", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Status", style="yellow")
    table.add_column("Image", style="green")
    table.add_column("Created", style="blue")

    for job in jobs_data["jobs"]:
        status_color = {
            "pending": "yellow",
            "running": "blue",
            "succeeded": "green",
            "failed": "red",
            "aborted": "magenta",
        }.get(job["status"], "white")

        table.add_row(
            str(job["id"])[:8] + "...",
            job["definition"]["name"],
            f"[{status_color}]{job['status']}[/{status_color}]",
            job["definition"]["image"],
            job["created_at"][:19],
        )

    console.print(table)
    console.print(f"Total: {len(jobs_data['jobs'])}")


def print_job_details(job_data: Dict[str, Any]) -> None:
    """Print detailed job information"""
    print_json(job_data, "Job Details")


def print_executors_table(executors_data: Dict[str, Any]) -> None:
    """Print executors in a formatted table"""
    if not executors_data.get("executors"):
        print_info("No healthy executors found")
        return

    table = Table(
        title="Healthy Executors", show_header=True, header_style="bold magenta"
    )
    table.add_column("ID", style="cyan")
    table.add_column("IP Address", style="white")
    table.add_column("CPU Cores", style="yellow")
    table.add_column("Memory (MB)", style="green")
    table.add_column("GPU Type", style="blue")
    table.add_column("Available GPUs", style="blue")
    table.add_column("Assigned Jobs", style="magenta")

    for executor in executors_data["executors"]:
        executor_info = executor["executor_info"]
        resources = executor["available_resources"]["resources"]
        assigned_jobs_count = executor.get("assigned_jobs_count", 0)

        # Format GPU information
        gpu_type = resources.get("gpu_type", "N/A")
        gpu_count = resources.get("gpu_count", 0)

        # Format IP address
        ip_address = executor_info.get("ip_address", "N/A")

        table.add_row(
            executor_info["id"],
            ip_address,
            str(resources["cpu_cores"]),
            str(resources["memory_mb"]),
            gpu_type if gpu_type != "N/A" else "None",
            str(gpu_count) if gpu_count > 0 else "0",
            str(assigned_jobs_count),
        )

    console.print(table)
    console.print(f"Total: {len(executors_data['executors'])}")


def print_health_status(health_data: Dict[str, Any]) -> None:
    """Print health check status"""
    status = health_data.get("status", "unknown")
    if status == "healthy":
        print_success(f"Scheduler is {status}")
    else:
        print_error(f"Scheduler status: {status}")

    print_json(health_data, "Health Details")


def print_status_summary(status_data: Dict[str, Any]) -> None:
    """Print JobManager status in a formatted table"""
    table = Table(
        title="JobManager Status", show_header=True, header_style="bold magenta"
    )
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="white")

    # Basic info
    table.add_row("Scheduler ID", str(status_data.get("scheduler_id", "Unknown")))
    table.add_row("Is Leader", "✓" if status_data.get("is_leader", False) else "✗")
    table.add_row("Current Leader", str(status_data.get("current_leader", "None")))
    table.add_row(
        "Redis Connected", "✓" if status_data.get("redis_connected", False) else "✗"
    )
    table.add_row("Background Tasks", str(status_data.get("background_tasks", 0)))
    table.add_row("Queue Size", str(status_data.get("queue_size", 0)))
    table.add_row("Claimed Jobs", str(status_data.get("claimed_jobs", 0)))

    # Uptime
    uptime = status_data.get("uptime_seconds", 0)
    uptime_str = f"{uptime:.2f}s"
    if uptime > 60:
        uptime_str = f"{uptime / 60:.1f}m"
    if uptime > 3600:
        uptime_str = f"{uptime / 3600:.1f}h"
    table.add_row("Uptime", uptime_str)

    console.print(table)

    # Job statistics
    job_stats = status_data.get("job_stats", {})
    if job_stats:
        console.print("\n[bold cyan]Job Statistics[/bold cyan]")
        stats_table = Table(show_header=True, header_style="bold magenta")
        stats_table.add_column("Status", style="cyan")
        stats_table.add_column("Count", style="white")

        for status_name, count in job_stats.items():
            color = {
                "pending": "yellow",
                "running": "blue",
                "succeeded": "green",
                "failed": "red",
                "aborted": "magenta",
            }.get(status_name, "white")
            stats_table.add_row(
                f"[{color}]{status_name.capitalize()}[/{color}]", str(count)
            )

        console.print(stats_table)

    # Timestamp
    timestamp = status_data.get("timestamp", "Unknown")
    console.print(f"\n[dim]Last updated: {timestamp}[/dim]")


def print_status_table(status_data: Dict[str, Any]) -> None:
    """Print JobManager status in a compact table format"""
    print_status_summary(status_data)
