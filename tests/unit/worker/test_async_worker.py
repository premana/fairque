"""Tests for async worker implementation."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from fairque import AsyncTaskHandler, AsyncTaskQueue, AsyncWorker, FairQueueConfig, Priority, Task


class MockAsyncTaskHandler(AsyncTaskHandler):
    """Mock async task handler for testing."""

    def __init__(self):
        self.processed_tasks = []
        self.failed_tasks = []
        self.timeout_tasks = []
        self.should_fail = False
        self.should_timeout = False
        self.process_delay = 0.1

    async def process_task(self, task: Task) -> bool:
        """Mock task processing."""
        if self.should_timeout:
            await asyncio.sleep(10)  # Simulate long processing

        await asyncio.sleep(self.process_delay)

        if self.should_fail:
            raise ValueError("Mock processing error")

        self.processed_tasks.append(task)
        return True

    async def on_task_success(self, task: Task, duration: float) -> None:
        """Handle successful task completion."""
        pass

    async def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        """Handle task failure."""
        self.failed_tasks.append((task, error))

    async def on_task_timeout(self, task: Task, duration: float) -> None:
        """Handle task timeout."""
        self.timeout_tasks.append(task)


@pytest.fixture
def async_worker_config():
    """Create test configuration for async worker."""
    return FairQueueConfig.from_dict({
        "redis": {
            "host": "localhost",
            "port": 6379,
            "db": 15  # Test database
        },
        "worker": {
            "id": "async-test-worker",
            "assigned_users": ["user1", "user2"],
            "steal_targets": ["user3"],
            "max_concurrent_tasks": 3,
            "poll_interval_seconds": 0.1,
            "task_timeout_seconds": 1.0,
            "graceful_shutdown_timeout": 2.0
        },
        "queue": {
            "enable_pipeline_optimization": True
        }
    })


@pytest.fixture
def mock_async_queue():
    """Create mock async queue."""
    queue = AsyncMock(spec=AsyncTaskQueue)
    queue.initialize = AsyncMock()
    queue.pop = AsyncMock()
    queue.delete_task = AsyncMock()
    queue.close = AsyncMock()
    queue.get_batch_queue_sizes = AsyncMock(return_value={"totals": {"total_size": 0}})
    return queue


@pytest.fixture
def sample_task():
    """Create a sample task for testing."""
    return Task(
        task_id="test-async-worker-task-001",
        user_id="user1",
        priority=Priority.NORMAL,
        payload={"type": "test", "data": "async_worker_test"}
    )


class TestAsyncWorker:
    """Test cases for AsyncWorker."""

    @pytest.mark.asyncio
    async def test_worker_initialization(self, async_worker_config):
        """Test async worker initialization."""
        task_handler = MockAsyncTaskHandler()
        worker = AsyncWorker(async_worker_config, task_handler)

        assert worker.config == async_worker_config
        assert worker.task_handler == task_handler
        assert not worker.is_running
        assert not worker.is_stopping
        assert worker.worker_task is None

    @pytest.mark.asyncio
    async def test_worker_start_stop(self, async_worker_config, mock_async_queue):
        """Test starting and stopping async worker."""
        task_handler = MockAsyncTaskHandler()
        worker = AsyncWorker(async_worker_config, task_handler, mock_async_queue)

        # Test start
        await worker.start()
        assert worker.is_running
        assert worker.worker_task is not None
        assert not worker.worker_task.done()

        # Test stop
        await worker.stop()
        assert not worker.is_running
        assert worker.worker_task.done()

        mock_async_queue.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_process_task_success(self, async_worker_config, mock_async_queue, sample_task):
        """Test successful task processing."""
        task_handler = MockAsyncTaskHandler()
        worker = AsyncWorker(async_worker_config, task_handler, mock_async_queue)

        # Mock queue to return a task once, then None
        mock_async_queue.pop.side_effect = [sample_task, None]

        await worker.start()

        # Wait for task processing
        await asyncio.sleep(0.5)

        await worker.stop()

        # Check that task was processed
        assert len(task_handler.processed_tasks) == 1
        assert task_handler.processed_tasks[0].task_id == sample_task.task_id

        # Check stats
        stats = worker.get_stats()
        assert stats["tasks_processed"] == 1
        assert stats["tasks_failed"] == 0
        assert stats["worker_type"] == "async"

    @pytest.mark.asyncio
    async def test_worker_context_manager(self, async_worker_config, mock_async_queue):
        """Test async worker context manager."""
        task_handler = MockAsyncTaskHandler()

        async with AsyncWorker(async_worker_config, task_handler, mock_async_queue) as worker:
            assert worker.is_running
            assert worker.worker_task is not None

        # After context exit, worker should be stopped
        assert not worker.is_running
