"""Test configuration and fixtures for FairQueue tests."""

from typing import Generator

import pytest
import redis

from fairque import FairQueueConfig, TaskQueue
from fairque.core.config import QueueConfig, RedisConfig, WorkerConfig


@pytest.fixture
def redis_config() -> RedisConfig:
    """Redis configuration for tests."""
    return RedisConfig(
        host="localhost",
        port=6379,
        db=15,  # Use test database
        decode_responses=True
    )


@pytest.fixture
def worker_config() -> WorkerConfig:
    """Worker configuration for tests."""
    return WorkerConfig(
        id="test-worker",
        assigned_users=["test:user:1", "test:user:2"],
        steal_targets=["test:user:3", "test:user:4"],
        poll_interval_seconds=0.1,  # Fast polling for tests
        task_timeout_seconds=5.0,
        max_concurrent_tasks=2,
        graceful_shutdown_timeout=5.0
    )


@pytest.fixture
def queue_config() -> QueueConfig:
    """Queue configuration for tests."""
    return QueueConfig(
        stats_prefix="test_fq",
        lua_script_cache_size=10,
        max_retry_attempts=2,
        default_task_timeout=5.0,
        default_max_retries=2,
        enable_pipeline_optimization=True,
        pipeline_batch_size=10,
        pipeline_timeout=1.0,
        queue_cleanup_interval=60,
        stats_aggregation_interval=30
    )


@pytest.fixture
def fairqueue_config(
    redis_config: RedisConfig,
    worker_config: WorkerConfig,
    queue_config: QueueConfig
) -> FairQueueConfig:
    """Complete FairQueue configuration for tests."""
    return FairQueueConfig(
        redis=redis_config,
        worker=worker_config,
        queue=queue_config
    )


@pytest.fixture
def redis_client(redis_config: RedisConfig) -> Generator[redis.Redis, None, None]:
    """Redis client for tests with cleanup."""
    client = redis_config.create_redis_client()

    # Clean test database before test
    client.flushdb()

    yield client

    # Clean test database after test
    client.flushdb()
    client.close()


@pytest.fixture
def fairqueue(fairqueue_config: FairQueueConfig, redis_client: redis.Redis) -> Generator[TaskQueue, None, None]:
    """FairQueue instance for tests with cleanup."""
    queue = TaskQueue(fairqueue_config, redis_client)

    yield queue

    # Cleanup
    queue.close()


@pytest.fixture
def sample_payload() -> dict:
    """Sample task payload for tests."""
    return {
        "action": "test_action",
        "data": {"key": "value", "number": 42},
        "metadata": {"test": True}
    }
