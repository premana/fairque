# Task Scheduling with FairQueue

FairQueue includes built-in support for cron-based task scheduling through the `TaskScheduler` class. This allows you to schedule recurring tasks that are automatically pushed to the queue at specified intervals.

## Features

- **Cron Expression Support**: Use standard cron expressions to define schedules
- **Timezone Awareness**: Schedule tasks in different timezones
- **Distributed Locking**: Only one scheduler instance processes tasks in a distributed setup
- **Integration with FairQueue**: Scheduled tasks are pushed to the existing queue system
- **Priority Support**: Scheduled tasks can have different priority levels
- **Management API**: Add, update, remove, and list schedules programmatically

## Installation

To use the scheduler, install with the scheduler extras:

```bash
pip install fairque[scheduler]
```

This installs the required dependencies: `croniter` and `pytz`.

## Basic Usage

```python
from fairque.scheduler import TaskScheduler
from fairque.core.models import Priority

# Create scheduler with your FairQueue instance
scheduler = TaskScheduler(
    queue=queue,
    scheduler_id="scheduler-001",
    check_interval=60,  # Check every minute
)

# Add a daily task
schedule_id = scheduler.add_schedule(
    cron_expr="0 9 * * *",  # Every day at 9 AM
    user_id="user1",
    priority=Priority.NORMAL,
    payload={
        "action": "daily_report",
        "email": "user@example.com",
    },
    timezone="America/New_York",
)

# Start the scheduler
scheduler.start()
```

## Cron Expression Examples

- `"* * * * *"` - Every minute
- `"0 * * * *"` - Every hour at minute 0
- `"0 9 * * *"` - Every day at 9:00 AM
- `"0 9 * * 1"` - Every Monday at 9:00 AM
- `"*/5 * * * *"` - Every 5 minutes
- `"0 0 1 * *"` - First day of every month at midnight

## Managing Schedules

```python
# List all schedules
schedules = scheduler.list_schedules()

# Get a specific schedule
schedule = scheduler.get_schedule(schedule_id)

# Update a schedule
scheduler.update_schedule(
    schedule_id,
    cron_expr="0 10 * * *",  # Change to 10 AM
    priority=Priority.HIGH,   # Increase priority
)

# Deactivate a schedule
scheduler.update_schedule(schedule_id, is_active=False)

# Remove a schedule
scheduler.remove_schedule(schedule_id)
```

## Timezone Support

Schedules can be created in different timezones:

```python
# Schedule in UTC
scheduler.add_schedule(
    cron_expr="0 0 * * *",
    user_id="user1",
    priority=Priority.NORMAL,
    payload={"task": "midnight_utc"},
    timezone="UTC",
)

# Schedule in Tokyo time
scheduler.add_schedule(
    cron_expr="0 9 * * *",
    user_id="user2",
    priority=Priority.NORMAL,
    payload={"task": "morning_tokyo"},
    timezone="Asia/Tokyo",
)
```

## Distributed Deployment

The scheduler uses Redis-based distributed locking to ensure only one scheduler instance processes tasks at a time:

```python
# Multiple scheduler instances can run
# Only one will acquire the lock and process tasks

# Instance 1
scheduler1 = TaskScheduler(queue, "scheduler-1")
scheduler1.start()

# Instance 2 (backup)
scheduler2 = TaskScheduler(queue, "scheduler-2")
scheduler2.start()
```

## Monitoring

Get scheduler statistics:

```python
stats = scheduler.get_statistics()
print(f"Active schedules: {stats['active_schedules']}")
print(f"Schedules by user: {stats['schedules_by_user']}")
print(f"Schedules by priority: {stats['schedules_by_priority']}")
```

## Complete Example

See `examples/scheduler_example.py` for a complete working example that demonstrates:
- Creating multiple schedules
- Different cron expressions and timezones
- Updating and removing schedules
- Integration with workers
- Monitoring scheduler activity

## Advanced Configuration

```python
scheduler = TaskScheduler(
    queue=queue,
    scheduler_id="main-scheduler",
    schedules_key="myapp:schedules",      # Custom Redis key
    lock_key="myapp:scheduler_lock",      # Custom lock key
    check_interval=30,                    # Check every 30 seconds
    lock_timeout=300,                     # 5-minute lock timeout
)
```
