"""Example demonstrating the refactored scheduler with Task-based scheduling."""

import time

from fairque.core.config import FairQueueConfig
from fairque.core.models import Priority, Task
from fairque.decorator import task
from fairque.scheduler.scheduler import TaskScheduler


# Define tasks using decorators
@task(task_id="daily_report", priority=Priority.HIGH)
def generate_daily_report():
    """Generate daily report."""
    print(f"Generating daily report at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    return {"report": "daily_summary.pdf", "status": "completed"}


@task(task_id="backup", priority=Priority.NORMAL)
def perform_backup():
    """Perform system backup."""
    print(f"Starting backup at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    return {"backup_file": "backup_20240101.tar.gz", "size_mb": 1024}


def main():
    """Demonstrate new scheduler API with Task objects."""

    # Create configuration
    config = FairQueueConfig.create_default(
        worker_id="scheduler-worker",
        assigned_users=["user:scheduler"],
    )

    # Create scheduler
    scheduler = TaskScheduler(config)

    print("=== FairQueue Scheduler Refactored Examples ===\n")

    # Example 1: Schedule task using task object
    print("1. Scheduling daily report task")

    report_task = generate_daily_report()
    schedule_id = scheduler.add_schedule(
        cron_expr="0 9 * * *",  # Daily at 9 AM
        task=report_task,
        timezone="UTC",
        metadata={"description": "Daily business report"}
    )
    print(f"Scheduled daily report with ID: {schedule_id}")

    # Example 2: Schedule backup task with custom task parameters
    print("\n2. Scheduling backup task with custom configuration")

    backup_task = Task.create(
        task_id="weekly_backup",
        user_id="user:scheduler",
        priority=Priority.HIGH,
        payload={"backup_type": "full", "retention_days": 30},
        func=perform_backup,
        max_retries=2
    )

    backup_schedule_id = scheduler.add_schedule(
        cron_expr="0 2 * * 0",  # Weekly on Sunday at 2 AM
        task=backup_task,
        timezone="UTC",
        metadata={"description": "Weekly full backup", "critical": True}
    )
    print(f"Scheduled weekly backup with ID: {backup_schedule_id}")

    # Example 3: Schedule task with dependencies and XCom
    print("\n3. Scheduling task with XCom configuration")

    xcom_task = Task.create(
        task_id="data_pipeline",
        user_id="user:scheduler",
        priority=Priority.NORMAL,
        payload={"source": "database", "destination": "warehouse"},
        enable_xcom=True,
        xcom_namespace="data_processing",
        auto_xcom=True
    )

    pipeline_schedule_id = scheduler.add_schedule(
        cron_expr="0 */6 * * *",  # Every 6 hours
        task=xcom_task,
        timezone="UTC",
        metadata={"description": "Data pipeline with XCom"}
    )
    print(f"Scheduled data pipeline with ID: {pipeline_schedule_id}")

    # List all schedules
    print("\n4. Listing all schedules")
    schedules = scheduler.list_schedules()
    for schedule in schedules:
        task_info = f"task_id={schedule.task.task_id}, user={schedule.task.user_id}"
        print(f"  Schedule {schedule.schedule_id}: {schedule.cron_expression} -> {task_info}")

    # Get specific schedule details
    print(f"\n5. Getting details for schedule {schedule_id}")
    schedule_details = scheduler.get_schedule(schedule_id)
    if schedule_details:
        print(f"  Cron: {schedule_details.cron_expression}")
        print(f"  Task ID: {schedule_details.task.task_id}")
        print(f"  User ID: {schedule_details.task.user_id}")
        print(f"  Priority: {schedule_details.task.priority}")
        print(f"  Next run: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(schedule_details.next_run))}")

    # Clean up
    print("\n6. Cleaning up schedules")
    for sid in [schedule_id, backup_schedule_id, pipeline_schedule_id]:
        removed = scheduler.remove_schedule(sid)
        print(f"  Removed schedule {sid}: {removed}")

    print("\n=== Scheduler examples completed successfully! ===")


if __name__ == "__main__":
    main()
