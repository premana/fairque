"""CLI monitoring tool for FairQueue - similar to rq info."""

import time
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from rich import box
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table

try:
    from rich import box
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from fairque.core.config import FairQueueConfig
from fairque.queue.queue import TaskQueue


class FairqueInfo:
    """CLI monitoring tool for FairQueue queues and workers."""

    def __init__(self, config: FairQueueConfig) -> None:
        """Initialize FairqueInfo with configuration.

        Args:
            config: FairQueue configuration
        """
        self.config = config
        self.console = Console() if HAS_RICH else None

    def display_basic_info(self, specific_users: Optional[List[str]] = None) -> None:
        """Display basic queue information similar to 'rq info'.

        Args:
            specific_users: Optional list of specific users to display
        """
        try:
            with TaskQueue(self.config) as queue:
                # Get queue-specific metrics
                if specific_users:
                    users_to_check = specific_users
                else:
                    queue_metrics = queue.get_metrics("queue", "all")
                    users_to_check = list(queue_metrics.get("queues", {}).keys())

                if HAS_RICH and self.console:
                    self._display_rich_info(queue, users_to_check)
                else:
                    self._display_plain_info(queue, users_to_check)

        except Exception as e:
            print(f"Error retrieving queue information: {e}")

    def _display_rich_info(self, queue: TaskQueue, users: List[str]) -> None:
        """Display information using Rich formatting."""
        if not self.console:
            return

        # Create queue information table
        table = Table(title="FairQueue Status", box=box.ROUNDED)
        table.add_column("Queue", style="cyan", no_wrap=True)
        table.add_column("Size", justify="center")
        table.add_column("Progress", width=30)

        total_tasks = 0
        max_queue_size = max(1, max([self._get_user_total_size(queue, user) for user in users] + [1]))

        for user in users:
            sizes = queue.get_queue_sizes(user)
            critical_size = sizes.get("critical_size", 0)
            normal_size = sizes.get("normal_size", 0)
            total_size = critical_size + normal_size
            total_tasks += total_size

            # Create progress bar
            progress_ratio = total_size / max_queue_size if max_queue_size > 0 else 0
            bar_length = int(progress_ratio * 25)  # 25 characters max
            bar = "█" * bar_length + "░" * (25 - bar_length)

            # Format queue info
            queue_name = f"queue:user:{user}"
            if critical_size > 0:
                queue_name += ":critical"
                size_text = f"{critical_size}"
            else:
                queue_name += ":normal"
                size_text = f"{normal_size}"

            table.add_row(queue_name, size_text, f"[green]{bar}[/green] {total_size}")

        self.console.print(table)

        # Summary info
        summary_text = f"[bold]{len(users)} queues, {total_tasks} tasks total[/bold]"
        self.console.print(summary_text)

        # Worker info (placeholder for now)
        worker_info = f"[yellow]{self.config.worker.id}[/yellow] [green]status[/green]: active"
        self.console.print(worker_info)
        self.console.print(f"[bold]1 worker, {len(users)} queues[/bold]")

    def _display_plain_info(self, queue: TaskQueue, users: List[str]) -> None:
        """Display information using plain text formatting."""
        total_tasks = 0
        max_queue_size = max(1, max([self._get_user_total_size(queue, user) for user in users] + [1]))

        for user in users:
            sizes = queue.get_queue_sizes(user)
            critical_size = sizes.get("critical_size", 0)
            normal_size = sizes.get("normal_size", 0)
            total_size = critical_size + normal_size
            total_tasks += total_size

            # Create simple progress bar
            progress_ratio = total_size / max_queue_size if max_queue_size > 0 else 0
            bar_length = int(progress_ratio * 25)
            bar = "█" * bar_length + " " * (25 - bar_length)

            # Display queue info
            if critical_size > 0:
                print(f"queue:user:{user}:critical |{bar} {critical_size}")
            if normal_size > 0:
                print(f"queue:user:{user}:normal   |{bar} {normal_size}")

        print(f"{len(users)} queues, {total_tasks} tasks total")
        print(f"{self.config.worker.id} status: active")
        print(f"1 worker, {len(users)} queues")

    def _get_user_total_size(self, queue: TaskQueue, user: str) -> int:
        """Get total queue size for a user."""
        try:
            sizes = queue.get_queue_sizes(user)
            return sizes.get("critical_size", 0) + sizes.get("normal_size", 0)
        except Exception:
            return 0

    def monitor_realtime(self, interval: float = 1.0, specific_users: Optional[List[str]] = None) -> None:
        """Monitor queues in real-time with periodic updates.

        Args:
            interval: Update interval in seconds
            specific_users: Optional list of specific users to display
        """
        if not HAS_RICH or not self.console:
            print("Real-time monitoring requires 'rich' library. Install with: pip install rich")
            print("Falling back to periodic updates...")
            self._monitor_plain_realtime(interval, specific_users)
            return

        try:
            with Live(self._generate_live_table(specific_users), refresh_per_second=1/interval) as live:
                while True:
                    time.sleep(interval)
                    live.update(self._generate_live_table(specific_users))
        except KeyboardInterrupt:
            self.console.print("\n[yellow]Monitoring stopped.[/yellow]")

    def _generate_live_table(self, specific_users: Optional[List[str]] = None) -> "Table":
        """Generate a live-updating table for real-time monitoring."""
        try:
            with TaskQueue(self.config) as queue:
                if specific_users:
                    users_to_check = specific_users
                else:
                    queue_metrics = queue.get_metrics("queue", "all")
                    users_to_check = list(queue_metrics.get("queues", {}).keys())

                table = Table(title=f"FairQueue Status - {time.strftime('%H:%M:%S')}", box=box.ROUNDED)
                table.add_column("Queue", style="cyan", no_wrap=True)
                table.add_column("Critical", justify="center")
                table.add_column("Normal", justify="center")
                table.add_column("Total", justify="center")
                table.add_column("Progress", width=20)

                total_tasks = 0
                max_queue_size = max(1, max([self._get_user_total_size(queue, user) for user in users_to_check] + [1]))

                for user in users_to_check:
                    sizes = queue.get_queue_sizes(user)
                    critical_size = sizes.get("critical_size", 0)
                    normal_size = sizes.get("normal_size", 0)
                    total_size = critical_size + normal_size
                    total_tasks += total_size

                    # Create progress representation
                    progress_ratio = total_size / max_queue_size if max_queue_size > 0 else 0
                    bar_length = int(progress_ratio * 15)
                    bar = "█" * bar_length + "░" * (15 - bar_length)

                    table.add_row(
                        f"user:{user}",
                        str(critical_size),
                        str(normal_size),
                        str(total_size),
                        f"[green]{bar}[/green]"
                    )

                # Add summary row
                table.add_row(
                    "[bold]TOTAL[/bold]",
                    "",
                    "",
                    f"[bold]{total_tasks}[/bold]",
                    ""
                )

                return table

        except Exception as e:
            error_table = Table(title="Error")
            error_table.add_column("Message")
            error_table.add_row(f"[red]Error: {e}[/red]")
            return error_table

    def _monitor_plain_realtime(self, interval: float, specific_users: Optional[List[str]]) -> None:
        """Plain text real-time monitoring fallback."""
        try:
            while True:
                # Clear screen (basic approach)
                print("\033[2J\033[H")  # ANSI escape codes
                print("FairQueue Status -", time.strftime('%H:%M:%S'))
                print("=" * 50)
                self.display_basic_info(specific_users)
                print("\nPress Ctrl+C to stop monitoring...")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")


def create_fairque_info_from_config_file(config_path: str) -> FairqueInfo:
    """Create FairqueInfo instance from configuration file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        FairqueInfo instance
    """
    config = FairQueueConfig.from_yaml(config_path)
    return FairqueInfo(config)


def main() -> None:
    """Main CLI entry point for fairque-info command."""
    import argparse
    import sys
    from pathlib import Path

    parser = argparse.ArgumentParser(
        description="FairQueue monitoring tool - similar to 'rq info'"
    )
    parser.add_argument(
        "users",
        nargs="*",
        help="Specific user IDs to monitor (default: all users)"
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="fairque_config.yaml",
        help="Configuration file path (default: fairque_config.yaml)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Refresh interval in seconds for real-time monitoring"
    )
    parser.add_argument(
        "-R", "--by-queue",
        action="store_true",
        help="Organize output by queue (not implemented yet)"
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed metrics"
    )

    args = parser.parse_args()

    # Check if config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Configuration file '{config_path}' not found")
        print("Please provide a valid configuration file path with -c/--config")
        sys.exit(1)

    try:
        # Create FairqueInfo instance
        fairque_info = create_fairque_info_from_config_file(str(config_path))

        if args.interval is not None:
            # Real-time monitoring mode
            users = args.users if args.users else None
            fairque_info.monitor_realtime(args.interval, users)
        else:
            # Single display mode
            users = args.users if args.users else None

            if args.detailed:
                # Show detailed information
                with TaskQueue(fairque_info.config) as queue:
                    detailed_metrics = queue.get_metrics("detailed")
                    print("Detailed FairQueue Metrics:")
                    print("=" * 40)
                    for key, value in detailed_metrics.items():
                        print(f"{key}: {value}")
                    print()

            fairque_info.display_basic_info(users)

    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
