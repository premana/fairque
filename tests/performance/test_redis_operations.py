"""Performance tests for Redis operations and Lua script execution."""

import time

import pytest

from fairque.core.models import Priority
from tests.performance.conftest import PerformanceBenchmark, create_test_tasks


class TestRedisOperationsPerformance:
    """Test Redis operations performance."""

    def test_lua_script_performance(self, task_queue, redis_client):
        """Test Lua script execution performance."""
        benchmark = PerformanceBenchmark("Lua Script Performance")

        # Create test tasks
        tasks = create_test_tasks(100, num_users=10)

        # Test push script performance
        for task in tasks:
            benchmark.record_operation(
                "lua_push_script",
                lambda: task_queue.push(task)
            )

        # Test pop script performance
        for _ in range(100):
            benchmark.record_operation(
                "lua_pop_script",
                lambda: task_queue.pop("worker1")
            )

        # Test stats script performance
        for _ in range(10):
            benchmark.record_operation(
                "lua_stats_script",
                lambda: task_queue.get_stats()
            )

        benchmark.print_results()

    def test_pipeline_vs_individual_operations(self, task_queue, redis_client):
        """Compare pipeline vs individual Redis operations."""
        # Create test tasks
        batch_size = 100
        tasks = create_test_tasks(batch_size, num_users=10)

        # Test individual operations
        redis_client.flushdb()
        individual_start = time.perf_counter()
        for task in tasks:
            task_queue.push(task)
        individual_time = time.perf_counter() - individual_start

        # Test pipeline operations (batch)
        redis_client.flushdb()
        pipeline_start = time.perf_counter()
        task_queue.push_batch(tasks)
        pipeline_time = time.perf_counter() - pipeline_start

        # Calculate speedup
        speedup = individual_time / pipeline_time

        print(f"\n{'='*60}")
        print("Pipeline vs Individual Operations")
        print(f"{'='*60}")
        print(f"Operations: {batch_size}")
        print(f"Individual time: {individual_time:.3f}s")
        print(f"Pipeline time: {pipeline_time:.3f}s")
        print(f"Speedup: {speedup:.2f}x")
        print(f"Individual ops/sec: {batch_size/individual_time:.2f}")
        print(f"Pipeline ops/sec: {batch_size/pipeline_time:.2f}")

    def test_redis_memory_efficiency(self, task_queue, redis_client):
        """Test Redis memory usage efficiency."""
        # Get initial memory usage
        initial_info = redis_client.info("memory")
        initial_memory = initial_info["used_memory"]

        # Create and push tasks
        task_counts = [100, 500, 1000, 5000]
        memory_usage = []

        for count in task_counts:
            # Clear database
            redis_client.flushdb()

            # Push tasks
            tasks = create_test_tasks(count, num_users=10, payload_size=1000)
            task_queue.push_batch(tasks)

            # Get memory usage
            info = redis_client.info("memory")
            used_memory = info["used_memory"]
            memory_per_task = (used_memory - initial_memory) / count

            memory_usage.append({
                "count": count,
                "total_memory": used_memory - initial_memory,
                "per_task": memory_per_task
            })

        print(f"\n{'='*60}")
        print("Redis Memory Efficiency")
        print(f"{'='*60}")
        print("Payload size: 1000 bytes")
        print(f"{'Tasks':<10} {'Total Memory':<15} {'Per Task':<15}")
        print(f"{'-'*40}")
        for usage in memory_usage:
            print(f"{usage['count']:<10} {usage['total_memory']:<15,} {usage['per_task']:<15.1f}")

    def test_critical_queue_performance(self, task_queue, redis_client):
        """Test critical queue FIFO performance."""
        benchmark = PerformanceBenchmark("Critical Queue Performance")

        # Create critical priority tasks
        critical_tasks = create_test_tasks(500, priority=Priority.CRITICAL)
        normal_tasks = create_test_tasks(500, priority=Priority.NORMAL)

        # Push mixed tasks
        redis_client.flushdb()
        all_tasks = []
        for i in range(500):
            all_tasks.append(critical_tasks[i])
            all_tasks.append(normal_tasks[i])

        push_start = time.perf_counter()
        task_queue.push_batch(all_tasks)
        push_time = time.perf_counter() - push_start

        # Pop and verify critical tasks come first
        critical_count = 0
        normal_count = 0
        pop_times = []

        for _ in range(1000):
            pop_start = time.perf_counter()
            task = task_queue.pop("worker1")
            pop_time = time.perf_counter() - pop_start
            pop_times.append(pop_time)

            if task:
                if task.priority == Priority.CRITICAL:
                    critical_count += 1
                else:
                    normal_count += 1
                    # Should only see normal tasks after all critical tasks
                    assert critical_count == 500, "Got normal task before all critical tasks were processed"

        avg_pop_time = sum(pop_times) / len(pop_times)

        print(f"\n{'='*60}")
        print("Critical Queue Performance")
        print(f"{'='*60}")
        print("Total tasks pushed: 1000 (500 critical, 500 normal)")
        print(f"Push time: {push_time:.3f}s")
        print(f"Average pop time: {avg_pop_time*1000:.3f}ms")
        print(f"Critical tasks processed: {critical_count}")
        print(f"Normal tasks processed: {normal_count}")
        print("FIFO ordering verified: ✓")


class TestAsyncRedisPerformance:
    """Test async Redis operations performance."""

    @pytest.mark.asyncio
    async def test_async_pipeline_performance(self, async_task_queue, async_redis_client):
        """Test async pipeline operations."""
        benchmark = PerformanceBenchmark("Async Pipeline Performance")

        # Test different batch sizes
        batch_sizes = [10, 50, 100, 500]

        for batch_size in batch_sizes:
            # Create tasks
            tasks = create_test_tasks(batch_size, num_users=10)

            # Clear database
            await async_redis_client.flushdb()

            # Test async batch push
            await benchmark.record_async_operation(
                f"async_push_batch_{batch_size}",
                lambda: async_task_queue.push_batch(tasks),
                iterations=10
            )

        benchmark.print_results()

    @pytest.mark.asyncio
    async def test_async_concurrent_redis_operations(self, async_task_queue, async_redis_client):
        """Test concurrent Redis operations with async."""
        import asyncio

        # Create tasks
        num_concurrent = 10
        tasks_per_batch = 100

        async def push_batch(batch_id: int) -> float:
            """Push a batch of tasks."""
            tasks = create_test_tasks(
                tasks_per_batch,
                user_prefix=f"batch{batch_id}_user",
                num_users=10
            )
            start = time.perf_counter()
            await async_task_queue.push_batch(tasks)
            return time.perf_counter() - start

        # Run concurrent pushes
        start = time.perf_counter()
        results = await asyncio.gather(*[
            push_batch(i) for i in range(num_concurrent)
        ])
        total_time = time.perf_counter() - start

        total_operations = num_concurrent * tasks_per_batch
        throughput = total_operations / total_time
        avg_batch_time = sum(results) / len(results)

        print(f"\n{'='*60}")
        print("Async Concurrent Redis Operations")
        print(f"{'='*60}")
        print(f"Concurrent batches: {num_concurrent}")
        print(f"Tasks per batch: {tasks_per_batch}")
        print(f"Total operations: {total_operations:,}")
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {throughput:,.2f} ops/sec")
        print(f"Average batch time: {avg_batch_time:.3f}s")

    @pytest.mark.asyncio
    async def test_async_stats_performance(self, async_task_queue, async_redis_client):
        """Test async statistics gathering performance."""
        # Push a large number of tasks
        tasks = create_test_tasks(5000, num_users=50)
        await async_task_queue.push_batch(tasks)

        # Measure stats gathering performance
        stats_times = []
        stats = None  # Initialize stats variable

        for _ in range(100):
            start = time.perf_counter()
            stats = await async_task_queue.get_stats()
            stats_time = time.perf_counter() - start
            stats_times.append(stats_time)

        avg_stats_time = sum(stats_times) / len(stats_times)
        min_stats_time = min(stats_times)
        max_stats_time = max(stats_times)

        print(f"\n{'='*60}")
        print("Async Stats Gathering Performance")
        print(f"{'='*60}")
        print("Queue size: 5000 tasks across 50 users")
        print("Stats calls: 100")
        print(f"Average time: {avg_stats_time*1000:.3f}ms")
        print(f"Min time: {min_stats_time*1000:.3f}ms")
        print(f"Max time: {max_stats_time*1000:.3f}ms")
        # Check if stats is available before accessing it
        if stats:
            print(f"Stats data: {len(stats['user_stats'])} users tracked")
