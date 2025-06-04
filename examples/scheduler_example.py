"""Example usage of TaskScheduler with FairQueue."""

import json
import logging
import time
from datetime import datetime

from fairque.core.config import FairQueueConfig, RedisConfig, WorkerConfig
from fairque.core.models import Priority, Task
from fairque.queue.queue import TaskQueue
from fairque.scheduler import TaskScheduler
from fairque.worker import TaskHandler, Worker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Example task handler
class ExampleTaskHandler(TaskHandler):
    """Example task handler that logs task information."""

    def _process_task(self, task: Task) -> bool:
        """Process a task."""
        logger.info(f"Processing task {task.task_id} for user {task.user_id}")
        logger.info(f"Payload: {json.dumps(task.payload, indent=2)}")

        # Check if this is a scheduled task
        if task.payload.get("__scheduled__"):
            logger.info(f"This is a scheduled task from schedule: {task.payload.get('__schedule_id__')}")

        # Simulate work
        time.sleep(1)

        # Return True for success
        return True


def main():
    """Main example function."""
    # Create configuration
    config = FairQueueConfig(
        redis=RedisConfig(host="localhost", port=6379, db=0),
        workers=[WorkerConfig()]
    )

    # Create FairQueue instance
    queue = TaskQueue(config)

    # Create TaskScheduler
    scheduler = TaskScheduler(
        queue=queue,
        scheduler_id="scheduler-001",
        check_interval=30,  # Check every 30 seconds
    )

    # Example 1: Add a daily task (every day at 9 AM)
    schedule_id_1 = scheduler.add_schedule(
        cron_expr="0 9 * * *",
        user_id="user1",
        priority=Priority.NORMAL,
        payload={
            "action": "daily_report",
            "email": "user1@example.com",
            "report_type": "sales",
        },
        timezone="America/New_York",
        metadata={"description": "Daily sales report"},
    )
    logger.info(f"Added daily schedule: {schedule_id_1}")

    # Example 2: Add an hourly task
    schedule_id_2 = scheduler.add_schedule(
        cron_expr="0 * * * *",  # Every hour
        user_id="user2",
        priority=Priority.HIGH,
        payload={
            "action": "sync_data",
            "source": "api",
            "destination": "database",
        },
        metadata={"description": "Hourly data sync"},
    )
    logger.info(f"Added hourly schedule: {schedule_id_2}")

    # Example 3: Add a task that runs every 5 minutes
    schedule_id_3 = scheduler.add_schedule(
        cron_expr="*/5 * * * *",  # Every 5 minutes
        user_id="user3",
        priority=Priority.LOW,
        payload={
            "action": "health_check",
            "services": ["api", "database", "cache"],
        },
        metadata={"description": "System health check"},
    )
    logger.info(f"Added 5-minute schedule: {schedule_id_3}")

    # Example 4: Add a weekly task (every Monday at 8 AM)
    schedule_id_4 = scheduler.add_schedule(
        cron_expr="0 8 * * 1",  # Monday at 8 AM
        user_id="user1",
        priority=Priority.VERY_HIGH,
        payload={
            "action": "weekly_summary",
            "include_metrics": True,
            "send_to_team": True,
        },
        timezone="Europe/London",
        metadata={"description": "Weekly team summary"},
    )
    logger.info(f"Added weekly schedule: {schedule_id_4}")

    # List all schedules
    logger.info("\nAll schedules:")
    all_schedules = scheduler.list_schedules()
    for schedule in all_schedules:
        logger.info(
            f"  - {schedule.schedule_id}: {schedule.cron_expression} "
            f"(user: {schedule.user_id}, priority: {schedule.priority.name})"
        )
        if schedule.next_run:
            next_run_dt = datetime.fromtimestamp(schedule.next_run)
            logger.info(f"    Next run: {next_run_dt}")

    # Update a schedule
    scheduler.update_schedule(
        schedule_id_2,
        cron_expr="*/30 * * * *",  # Change to every 30 minutes
        metadata={"description": "Updated: Half-hourly data sync"},
    )
    logger.info(f"\nUpdated schedule {schedule_id_2} to run every 30 minutes")

    # Deactivate a schedule
    scheduler.update_schedule(schedule_id_3, is_active=False)
    logger.info(f"Deactivated schedule {schedule_id_3}")

    # Get scheduler statistics
    stats = scheduler.get_statistics()
    logger.info(f"\nScheduler statistics: {json.dumps(stats, indent=2)}")

    # Start the scheduler
    logger.info("\nStarting scheduler...")
    scheduler.start()

    # Create and start a worker to process scheduled tasks
    handler = ExampleTaskHandler()
    worker = Worker(config, handler)

    logger.info("Starting worker...")
    worker.start()

    try:
        # Run for a while to see scheduled tasks being executed
        logger.info("Running for 5 minutes to demonstrate scheduling...")
        time.sleep(300)  # Run for 5 minutes

    except KeyboardInterrupt:
        logger.info("Interrupted by user")

    finally:
        # Stop scheduler and worker
        logger.info("Stopping scheduler and worker...")
        scheduler.stop()
        worker.stop()

        # Remove all schedules
        for schedule_id in [schedule_id_1, schedule_id_2, schedule_id_3, schedule_id_4]:
            scheduler.remove_schedule(schedule_id)
        logger.info("Cleaned up all schedules")


if __name__ == "__main__":
    main()
