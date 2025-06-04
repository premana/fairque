"""Unit tests for task scheduler functionality."""

import json
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from fairque.core.models import Priority, Task
from fairque.scheduler.models import ScheduledTask
from fairque.scheduler.scheduler import TaskScheduler


class TestScheduledTask(unittest.TestCase):
    """Test cases for ScheduledTask model."""

    def test_create_scheduled_task(self):
        """Test creating a scheduled task."""
        scheduled_task = ScheduledTask.create(
            cron_expr="0 9 * * *",
            user_id="user1",
            priority=Priority.NORMAL,
            payload={"action": "test"},
            timezone="UTC",
        )

        assert scheduled_task.schedule_id is not None
        assert scheduled_task.cron_expression == "0 9 * * *"
        assert scheduled_task.user_id == "user1"
        assert scheduled_task.priority == Priority.NORMAL
        assert scheduled_task.payload == {"action": "test"}
        assert scheduled_task.timezone == "UTC"
        assert scheduled_task.is_active is True
        assert scheduled_task.last_run is None
        assert scheduled_task.next_run is not None

    def test_calculate_next_run(self):
        """Test calculating next run time."""
        # Test with a simple cron expression
        scheduled_task = ScheduledTask.create(
            cron_expr="0 0 * * *",  # Daily at midnight
            user_id="user1",
            priority=Priority.NORMAL,
            payload={},
        )

        # Calculate next run
        base_time = datetime(2024, 1, 1, 10, 0, 0).timestamp()
        next_run = scheduled_task.calculate_next_run(from_time=base_time)

        # Should be next midnight
        next_midnight = datetime(2024, 1, 2, 0, 0, 0).timestamp()
        assert next_run == next_midnight

    def test_invalid_cron_expression(self):
        """Test handling of invalid cron expression."""
        with pytest.raises(ValueError, match="Invalid cron expression"):
            ScheduledTask.create(
                cron_expr="invalid cron",
                user_id="user1",
                priority=Priority.NORMAL,
                payload={},
            )

    def test_invalid_timezone(self):
        """Test handling of invalid timezone."""
        with pytest.raises(ValueError, match="Unknown timezone"):
            ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user1",
                priority=Priority.NORMAL,
                payload={},
                timezone="Invalid/Timezone",
            )

    def test_create_task(self):
        """Test converting scheduled task to regular task."""
        scheduled_task = ScheduledTask.create(
            cron_expr="0 0 * * *",
            user_id="user1",
            priority=Priority.HIGH,
            payload={"action": "test", "value": 123},
        )

        task = scheduled_task.create_task()

        assert isinstance(task, Task)
        assert task.user_id == "user1"
        assert task.priority == Priority.HIGH
        assert task.payload["action"] == "test"
        assert task.payload["value"] == 123
        assert task.payload["__scheduled__"] is True
        assert task.payload["__schedule_id__"] == scheduled_task.schedule_id

    def test_update_after_run(self):
        """Test updating schedule after execution."""
        scheduled_task = ScheduledTask.create(
            cron_expr="0 * * * *",  # Hourly
            user_id="user1",
            priority=Priority.NORMAL,
            payload={},
        )

        original_next_run = scheduled_task.next_run
        run_time = time.time()

        scheduled_task.update_after_run(run_time)

        assert scheduled_task.last_run == run_time
        assert scheduled_task.next_run is not None
        assert original_next_run is not None
        assert scheduled_task.next_run > original_next_run
        assert scheduled_task.updated_at >= run_time

    def test_serialization(self):
        """Test serialization to/from dict and JSON."""
        scheduled_task = ScheduledTask.create(
            cron_expr="*/5 * * * *",
            user_id="user1",
            priority=Priority.LOW,
            payload={"key": "value"},
            metadata={"description": "Test task"},
        )

        # Test to_dict
        data = scheduled_task.to_dict()
        assert data["schedule_id"] == scheduled_task.schedule_id
        assert data["cron_expression"] == "*/5 * * * *"
        assert data["priority"] == Priority.LOW.value

        # Test from_dict
        restored = ScheduledTask.from_dict(data)
        assert restored.schedule_id == scheduled_task.schedule_id
        assert restored.cron_expression == scheduled_task.cron_expression
        assert restored.priority == scheduled_task.priority

        # Test to_json/from_json
        json_str = scheduled_task.to_json()
        restored_from_json = ScheduledTask.from_json(json_str)
        assert restored_from_json.schedule_id == scheduled_task.schedule_id
        assert restored_from_json.payload == scheduled_task.payload


class TestTaskScheduler(unittest.TestCase):
    """Test cases for TaskScheduler."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock FairQueue
        self.mock_queue = MagicMock()
        self.mock_redis = MagicMock()
        self.mock_queue.redis = self.mock_redis

        # Create scheduler
        self.scheduler = TaskScheduler(
            queue=self.mock_queue,
            scheduler_id="test-scheduler",
            check_interval=1,  # 1 second for tests
            lock_timeout=60,
        )

    def test_add_schedule(self):
        """Test adding a new schedule."""
        # Mock Redis hset
        self.mock_redis.hset.return_value = True

        schedule_id = self.scheduler.add_schedule(
            cron_expr="0 0 * * *",
            user_id="user1",
            priority=Priority.NORMAL,
            payload={"action": "test"},
        )

        assert schedule_id is not None
        self.mock_redis.hset.assert_called_once()

        # Verify the call arguments
        call_args = self.mock_redis.hset.call_args[0]
        assert call_args[0] == self.scheduler.schedules_key
        assert call_args[1] == schedule_id

        # Verify the stored data
        stored_data = json.loads(call_args[2])
        assert stored_data["schedule_id"] == schedule_id
        assert stored_data["cron_expression"] == "0 0 * * *"
        assert stored_data["user_id"] == "user1"

    def test_get_schedule(self):
        """Test retrieving a schedule."""
        # Create test schedule data
        schedule_data = {
            "schedule_id": "test-id",
            "cron_expression": "0 0 * * *",
            "user_id": "user1",
            "priority": Priority.NORMAL.value,
            "payload": {"action": "test"},
            "timezone": "UTC",
            "is_active": True,
            "last_run": None,
            "next_run": time.time() + 3600,
            "created_at": time.time(),
            "updated_at": time.time(),
            "metadata": {},
        }

        self.mock_redis.hget.return_value = json.dumps(schedule_data)

        schedule = self.scheduler.get_schedule("test-id")

        assert schedule is not None
        assert schedule.schedule_id == "test-id"
        assert schedule.user_id == "user1"
        assert schedule.priority == Priority.NORMAL
        self.mock_redis.hget.assert_called_once_with(
            self.scheduler.schedules_key, "test-id"
        )

    def test_update_schedule(self):
        """Test updating a schedule."""
        # Mock get_schedule
        original_schedule = ScheduledTask.create(
            cron_expr="0 0 * * *",
            user_id="user1",
            priority=Priority.NORMAL,
            payload={"action": "test"},
        )

        self.mock_redis.hget.return_value = original_schedule.to_json()
        self.mock_redis.hset.return_value = True

        # Update schedule
        result = self.scheduler.update_schedule(
            original_schedule.schedule_id,
            cron_expr="0 */2 * * *",  # Every 2 hours
            priority=Priority.HIGH,
        )

        assert result is True

        # Verify update was saved
        self.mock_redis.hset.assert_called_once()
        call_args = self.mock_redis.hset.call_args[0]
        updated_data = json.loads(call_args[2])

        assert updated_data["cron_expression"] == "0 */2 * * *"
        assert updated_data["priority"] == Priority.HIGH.value

    def test_remove_schedule(self):
        """Test removing a schedule."""
        self.mock_redis.hdel.return_value = 1

        result = self.scheduler.remove_schedule("test-id")

        assert result is True
        self.mock_redis.hdel.assert_called_once_with(
            self.scheduler.schedules_key, "test-id"
        )

    def test_list_schedules(self):
        """Test listing schedules with filters."""
        # Create test schedules
        schedules_data = {
            "id1": ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user1",
                priority=Priority.NORMAL,
                payload={},
            ).to_json(),
            "id2": ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user2",
                priority=Priority.HIGH,
                payload={},
            ).to_json(),
            "id3": ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user1",
                priority=Priority.LOW,
                payload={},
            ).to_json(),
        }

        self.mock_redis.hgetall.return_value = schedules_data

        # Test listing all schedules
        all_schedules = self.scheduler.list_schedules()
        assert len(all_schedules) == 3

        # Test filtering by user
        user1_schedules = self.scheduler.list_schedules(user_id="user1")
        assert len(user1_schedules) == 2
        assert all(s.user_id == "user1" for s in user1_schedules)

    def test_distributed_locking(self):
        """Test distributed locking mechanism."""
        # Test acquiring lock
        self.mock_redis.set.return_value = True
        assert self.scheduler._try_acquire_lock() is True

        self.mock_redis.set.assert_called_once()
        call_args = self.mock_redis.set.call_args
        assert call_args[0][0] == self.scheduler.lock_key
        assert call_args[1]["nx"] is True
        assert call_args[1]["ex"] == self.scheduler.lock_timeout

        # Test failing to acquire lock
        self.mock_redis.set.return_value = None
        assert self.scheduler._try_acquire_lock() is False

    def test_process_scheduled_tasks(self):
        """Test processing scheduled tasks."""
        # Create a schedule that's ready to run
        current_time = time.time()
        schedule = ScheduledTask.create(
            cron_expr="* * * * *",  # Every minute
            user_id="user1",
            priority=Priority.NORMAL,
            payload={"action": "test"},
        )
        schedule.next_run = current_time - 10  # Past due

        # Mock methods
        self.scheduler.list_schedules = MagicMock(return_value=[schedule])
        self.mock_queue.push.return_value = {"task_id": "new-task-id"}
        self.mock_redis.hset.return_value = True

        # Process tasks
        self.scheduler._process_scheduled_tasks()

        # Verify task was pushed to queue
        self.mock_queue.push.assert_called_once()
        pushed_task = self.mock_queue.push.call_args[0][0]
        assert pushed_task.user_id == "user1"
        assert pushed_task.payload["__scheduled__"] is True

        # Verify schedule was updated
        self.mock_redis.hset.assert_called_once()

    def test_start_stop(self):
        """Test starting and stopping scheduler."""
        assert self.scheduler.is_running is False

        # Start scheduler
        self.scheduler.start()
        assert self.scheduler.is_running is True
        assert self.scheduler._scheduler_thread is not None
        assert self.scheduler._scheduler_thread.is_alive()

        # Stop scheduler
        self.scheduler.stop()
        assert self.scheduler.is_running is False

        # Wait a bit for thread to stop
        time.sleep(0.1)
        assert not self.scheduler._scheduler_thread.is_alive()

    def test_get_statistics(self):
        """Test getting scheduler statistics."""
        # Mock some schedules
        schedules = [
            ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user1",
                priority=Priority.NORMAL,
                payload={},
            ),
            ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user1",
                priority=Priority.HIGH,
                payload={},
            ),
            ScheduledTask.create(
                cron_expr="0 0 * * *",
                user_id="user2",
                priority=Priority.NORMAL,
                payload={},
            ),
        ]
        schedules[2].is_active = False  # One inactive

        self.scheduler.list_schedules = MagicMock(return_value=schedules)

        stats = self.scheduler.get_statistics()

        assert stats["scheduler_id"] == "test-scheduler"
        assert stats["total_schedules"] == 3
        assert stats["active_schedules"] == 2
        assert stats["inactive_schedules"] == 1
        assert stats["schedules_by_user"]["user1"] == 2
        assert stats["schedules_by_user"]["user2"] == 0  # Inactive not counted
        assert stats["schedules_by_priority"]["NORMAL"] == 1
        assert stats["schedules_by_priority"]["HIGH"] == 1
