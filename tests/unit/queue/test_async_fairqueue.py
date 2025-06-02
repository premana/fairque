"""Tests for async queue implementation."""

import asyncio
from unittest.mock import patch

import pytest

from fairque import AsyncTaskQueue, FairQueueConfig, Priority, Task
from fairque.core.exceptions import LuaScriptError, TaskValidationError


@pytest.fixture
async def async_queue_config():
    """Create test configuration for async queue."""
    return FairQueueConfig.from_dict({
        "redis": {
            "host": "localhost",
            "port": 6379,
            "db": 15  # Test database
        },
        "worker": {
            "id": "async-test-worker",
            "assigned_users": ["user1", "user2"],
            "steal_targets": ["user3"]
        },
        "queue": {
            "enable_pipeline_optimization": True
        }
    })


@pytest.fixture
async def async_queue(async_queue_config):
    """Create test async queue instance."""
    queue = AsyncTaskQueue(async_queue_config)
    await queue.initialize()
    yield queue
    await queue.close()


@pytest.fixture
def sample_task():
    """Create a sample task for testing."""
    return Task(
        task_id="test-async-task-001",
        user_id="user1",
        priority=Priority.NORMAL,
        payload={"type": "test", "data": "async_test_data"}
    )


class TestAsyncTaskQueue:
    """Test cases for AsyncTaskQueue."""

    @pytest.mark.asyncio
    async def test_queue_initialization(self, async_queue_config):
        """Test async queue initialization."""
        queue = AsyncTaskQueue(async_queue_config)

        # Test that queue is not initialized yet
        assert not hasattr(queue, 'redis') or queue.redis is None

        # Initialize and test
        await queue.initialize()
        assert queue.redis is not None

        # Test connection
        result = await queue.redis.ping()
        assert result is True

        await queue.close()

    @pytest.mark.asyncio
    async def test_push_task(self, async_queue, sample_task):
        """Test pushing a task to async queue."""
        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = '{"success": true, "position": 1}'

            result = await async_queue.push(sample_task)

            assert result["success"] is True
            assert "position" in result
            mock_script.assert_called_once()

    @pytest.mark.asyncio
    async def test_push_invalid_task(self, async_queue):
        """Test pushing invalid task to async queue."""
        invalid_task = Task(
            task_id="",  # Invalid empty task_id
            user_id="user1",
            priority=Priority.NORMAL,
            payload={}
        )

        with pytest.raises(TaskValidationError, match="Task ID cannot be empty"):
            await async_queue.push(invalid_task)

    @pytest.mark.asyncio
    async def test_pop_task(self, async_queue):
        """Test popping a task from async queue."""
        mock_task_data = {
            "task_id": "test-async-task-001",
            "user_id": "user1",
            "priority": 3,
            "payload": '{"type": "test"}',
            "retry_count": 0,
            "max_retries": 3,
            "created_at": 1234567890.0,
            "execute_after": 1234567890.0,
            "selected_user": "user1"
        }

        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = f'{{"success": true, "data": {mock_task_data}}}'

            task = await async_queue.pop()

            assert task is not None
            assert task.task_id == "test-async-task-001"
            assert task.user_id == "user1"
            assert task.priority == Priority.NORMAL
            mock_script.assert_called_once()

    @pytest.mark.asyncio
    async def test_pop_no_tasks(self, async_queue):
        """Test popping when no tasks available."""
        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = '{"success": true, "data": null}'

            task = await async_queue.pop()

            assert task is None
            mock_script.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_stats(self, async_queue):
        """Test getting queue statistics."""
        mock_stats = {
            "total_tasks": 10,
            "pending_tasks": 5,
            "processing_tasks": 2
        }

        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = f'{{"success": true, "data": {mock_stats}}}'

            stats = await async_queue.get_stats()

            assert stats == mock_stats
            mock_script.assert_called_once_with("stats", args=["get_stats"])

    @pytest.mark.asyncio
    async def test_get_queue_sizes(self, async_queue):
        """Test getting queue sizes for specific user."""
        mock_sizes = {
            "critical_size": 2,
            "normal_size": 5,
            "total_size": 7
        }

        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = f'{{"success": true, "data": {mock_sizes}}}'

            sizes = await async_queue.get_queue_sizes("user1")

            assert sizes == mock_sizes
            mock_script.assert_called_once_with("stats", args=["get_queue_sizes", "user1"])

    @pytest.mark.asyncio
    async def test_push_batch_concurrent(self, async_queue):
        """Test concurrent batch push of tasks."""
        tasks = [
            Task(
                task_id=f"async-batch-task-{i}",
                user_id=f"user{i % 2 + 1}",
                priority=Priority.NORMAL,
                payload={"batch_id": i}
            ) for i in range(5)
        ]

        with patch.object(async_queue, 'push') as mock_push:
            mock_push.return_value = {"success": True, "position": 1}

            results = await async_queue.push_batch_concurrent(tasks)

            assert len(results) == 5
            assert all(r["success"] for r in results)
            assert mock_push.call_count == 5

    @pytest.mark.asyncio
    async def test_delete_task(self, async_queue):
        """Test deleting a task."""
        with patch.object(async_queue.redis, 'delete') as mock_delete:
            mock_delete.return_value = 1  # 1 key deleted

            result = await async_queue.delete_task("test-task-id")

            assert result is True
            mock_delete.assert_called_once_with("task:test-task-id")

    @pytest.mark.asyncio
    async def test_cleanup_expired_tasks(self, async_queue):
        """Test cleaning up expired tasks."""
        with patch.object(async_queue.redis, 'scan_iter') as mock_scan, \
             patch.object(async_queue.redis, 'hget') as mock_hget, \
             patch.object(async_queue.redis, 'delete') as mock_delete:

            # Mock scan results
            mock_scan.return_value = [
                "task:old-task-1",
                "task:old-task-2",
                "task:recent-task"
            ].__aiter__()

            # Mock hget results (old timestamp, old timestamp, recent timestamp)
            mock_hget.side_effect = ["1000000", "1000000", str(asyncio.get_event_loop().time())]
            mock_delete.return_value = 1

            cleanup_count = await async_queue.cleanup_expired_tasks(max_age_seconds=86400)

            assert cleanup_count == 2  # Two old tasks cleaned up
            assert mock_delete.call_count == 2

    @pytest.mark.asyncio
    async def test_lua_script_error_handling(self, async_queue, sample_task):
        """Test Lua script error handling."""
        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = '{"success": false, "error_code": "TEST_ERROR", "message": "Test error"}'

            with pytest.raises(LuaScriptError):
                await async_queue.push(sample_task)

    @pytest.mark.asyncio
    async def test_context_manager(self, async_queue_config):
        """Test async context manager functionality."""
        async with AsyncTaskQueue(async_queue_config) as queue:
            assert queue.redis is not None

            # Test that we can use the queue
            result = await queue.redis.ping()
            assert result is True

        # After context exit, connection should be closed
        # Note: We can't easily test this as aclose() might not change the state immediately

    @pytest.mark.asyncio
    async def test_batch_queue_sizes(self, async_queue):
        """Test getting batch queue sizes."""
        mock_batch_sizes = {
            "user1": {"critical_size": 1, "normal_size": 3, "total_size": 4},
            "user2": {"critical_size": 0, "normal_size": 2, "total_size": 2},
            "totals": {"critical_size": 1, "normal_size": 5, "total_size": 6}
        }

        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = f'{{"success": true, "data": {mock_batch_sizes}}}'

            sizes = await async_queue.get_batch_queue_sizes(["user1", "user2"])

            assert sizes == mock_batch_sizes
            mock_script.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check(self, async_queue):
        """Test health check functionality."""
        mock_health = {
            "redis_connected": True,
            "total_memory_usage": 1024,
            "scripts_loaded": 3
        }

        with patch.object(async_queue.lua_manager, 'execute_script') as mock_script:
            mock_script.return_value = f'{{"success": true, "data": {mock_health}}}'

            health = await async_queue.get_health()

            assert health == mock_health
            mock_script.assert_called_once_with("stats", args=["get_health"])
