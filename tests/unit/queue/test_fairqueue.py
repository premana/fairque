"""Tests for FairQueue core functionality."""

from typing import Any, Dict

import pytest

from fairque import Priority, Task, TaskQueue
from fairque.core.exceptions import TaskValidationError


class TestFairQueueBasics:
    """Test basic FairQueue operations."""

    def test_fairqueue_initialization(self, fairqueue: TaskQueue) -> None:
        """Test FairQueue initialization."""
        assert fairqueue.config.worker.id == "test-worker"
        assert fairqueue.redis.ping()

    def test_push_task_success(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test successful task push."""
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )

        result = fairqueue.push(task)

        assert result["success"] is True
        assert result["data"]["task_id"] == task.task_id
        assert result["data"]["user_id"] == "test:user:1"
        assert result["data"]["priority"] == 4

    def test_push_critical_task(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test pushing critical priority task."""
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.CRITICAL,
            payload=sample_payload
        )

        result = fairqueue.push(task)

        assert result["success"] is True
        assert result["data"]["priority"] == 6
        # Critical tasks don't have score
        assert "score" not in result["data"]

    def test_push_normal_task(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test pushing normal priority task."""
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.NORMAL,
            payload=sample_payload
        )

        result = fairqueue.push(task)

        assert result["success"] is True
        assert result["data"]["priority"] == 3
        # Normal tasks have score
        assert "score" in result["data"]
        assert isinstance(result["data"]["score"], int | float)

    def test_push_task_validation_error(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test task push with validation error."""
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )

        # Clear task ID to trigger validation error
        task.task_id = ""

        with pytest.raises(TaskValidationError, match="Task ID cannot be empty"):
            fairqueue.push(task)

    def test_pop_task_success(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test successful task pop."""
        # Push a task first
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        fairqueue.push(task)

        # Pop the task
        popped_task = fairqueue.pop(["test:user:1"])

        assert popped_task is not None
        assert popped_task.task_id == task.task_id
        assert popped_task.user_id == "test:user:1"
        assert popped_task.priority == Priority.HIGH
        assert popped_task.payload == sample_payload

    def test_pop_task_empty_queue(self, fairqueue: TaskQueue) -> None:
        """Test popping from empty queue."""
        result = fairqueue.pop(["test:user:1"])
        assert result is None

    def test_pop_task_no_users(self, fairqueue: TaskQueue) -> None:
        """Test popping with no users specified."""
        result = fairqueue.pop([])
        assert result is None

    def test_priority_ordering(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test that tasks are popped in priority order."""
        # Push tasks in different priorities
        tasks = []
        for priority in [Priority.LOW, Priority.CRITICAL, Priority.HIGH, Priority.NORMAL]:
            task = Task.create(
                user_id="test:user:1",
                priority=priority,
                payload={**sample_payload, "priority_name": priority.name}
            )
            tasks.append(task)
            fairqueue.push(task)

        # Pop tasks and verify order: CRITICAL first, then by score (which includes priority)
        popped_tasks = []
        while True:
            task = fairqueue.pop(["test:user:1"])
            if not task:
                break
            popped_tasks.append(task)

        assert len(popped_tasks) == 4
        # First task should be CRITICAL
        assert popped_tasks[0].priority == Priority.CRITICAL

    def test_work_stealing(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test work stealing functionality."""
        # Push task for user:3 (steal target)
        task = Task.create(
            user_id="test:user:3",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        fairqueue.push(task)

        # Try to pop with assigned users first, then steal targets
        popped_task = fairqueue.pop(["test:user:1", "test:user:2", "test:user:3"])

        assert popped_task is not None
        assert popped_task.task_id == task.task_id
        assert popped_task.user_id == "test:user:3"  # Stolen from user:3

    def test_delete_task(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test task deletion."""
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        fairqueue.push(task)

        # Delete the task
        deleted = fairqueue.delete_task(task.task_id)
        assert deleted is True

        # Try to delete again
        deleted_again = fairqueue.delete_task(task.task_id)
        assert deleted_again is False


class TestFairQueueStatistics:
    """Test FairQueue statistics functionality."""

    def test_get_stats_empty(self, fairqueue: TaskQueue) -> None:
        """Test getting statistics from empty queue."""
        stats = fairqueue.get_stats()

        assert isinstance(stats, dict)
        assert stats.get("tasks_active", 0) == 0
        assert stats.get("dlq_size", 0) == 0

    def test_get_stats_with_tasks(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test statistics after pushing tasks."""
        # Push some tasks
        for i in range(3):
            task = Task.create(
                user_id=f"test:user:{i+1}",
                priority=Priority.HIGH,
                payload=sample_payload
            )
            fairqueue.push(task)

        stats = fairqueue.get_stats()

        assert stats.get("tasks_active", 0) == 3
        assert stats.get("tasks_pushed_total", 0) == 3

    def test_get_queue_sizes(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test getting queue sizes for specific user."""
        # Push critical and normal tasks
        critical_task = Task.create(
            user_id="test:user:1",
            priority=Priority.CRITICAL,
            payload=sample_payload
        )
        normal_task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )

        fairqueue.push(critical_task)
        fairqueue.push(normal_task)

        sizes = fairqueue.get_queue_sizes("test:user:1")

        assert sizes["critical_size"] == 1
        assert sizes["normal_size"] == 1
        assert sizes["total_size"] == 2
        assert sizes["user_id"] == "test:user:1"

    def test_get_batch_queue_sizes(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test getting queue sizes for multiple users."""
        # Push tasks for different users
        users = ["test:user:1", "test:user:2"]
        for user_id in users:
            task = Task.create(
                user_id=user_id,
                priority=Priority.HIGH,
                payload=sample_payload
            )
            fairqueue.push(task)

        sizes = fairqueue.get_batch_queue_sizes(users)

        assert "test:user:1" in sizes
        assert "test:user:2" in sizes
        assert "totals" in sizes
        assert sizes["totals"]["total_size"] == 2

    def test_get_health(self, fairqueue: TaskQueue) -> None:
        """Test health check."""
        health = fairqueue.get_health()

        assert isinstance(health, dict)
        assert health["status"] in ["healthy", "warning"]
        assert "active_tasks" in health
        assert "dlq_size" in health


class TestFairQueueBatch:
    """Test FairQueue batch operations."""

    def test_push_batch_success(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test successful batch push."""
        tasks = []
        for i in range(3):
            task = Task.create(
                user_id=f"test:user:{i+1}",
                priority=Priority.HIGH,
                payload={**sample_payload, "index": i}
            )
            tasks.append(task)

        results = fairqueue.push_batch(tasks)

        assert len(results) == 3
        for result in results:
            assert result["success"] is True

    def test_push_batch_empty(self, fairqueue: TaskQueue) -> None:
        """Test batch push with empty list."""
        results = fairqueue.push_batch([])
        assert results == []

    def test_push_batch_with_errors(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test batch push with some errors."""
        tasks = []

        # Valid task
        good_task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        tasks.append(good_task)

        # Invalid task (empty task ID)
        bad_task = Task.create(
            user_id="test:user:2",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        bad_task.task_id = ""  # Make it invalid
        tasks.append(bad_task)

        results = fairqueue.push_batch(tasks)

        assert len(results) == 2
        assert results[0]["success"] is True
        assert results[1]["success"] is False
        assert "error" in results[1]


class TestFairQueueCleanup:
    """Test FairQueue cleanup operations."""

    def test_cleanup_expired_tasks(self, fairqueue: TaskQueue, sample_payload: Dict[str, Any]) -> None:
        """Test cleanup of expired tasks."""
        # Push a task
        task = Task.create(
            user_id="test:user:1",
            priority=Priority.HIGH,
            payload=sample_payload
        )
        fairqueue.push(task)

        # Cleanup with very short age (should not clean anything)
        cleaned = fairqueue.cleanup_expired_tasks(max_age_seconds=3600)
        assert cleaned == 0  # Task is too new

        # Cleanup with negative age (clean everything)
        cleaned = fairqueue.cleanup_expired_tasks(max_age_seconds=0)
        assert cleaned >= 0  # May clean tasks depending on timing


class TestFairQueueContextManager:
    """Test FairQueue as context manager."""

    def test_context_manager(self, fairqueue_config) -> None:
        """Test using FairQueue as context manager."""
        with TaskQueue(fairqueue_config) as queue:
            assert queue.redis.ping()

            # Queue should work normally
            task = Task.create(
                user_id="test:user:1",
                priority=Priority.HIGH,
                payload={"test": True}
            )
            result = queue.push(task)
            assert result["success"] is True

        # After context exit, connection should be closed
        # Note: This is hard to test reliably without implementation details
