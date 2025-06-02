"""Performance tests for queue throughput operations."""

import asyncio
import concurrent.futures
import time

import pytest

from fairque.core.models import Priority
from tests.performance.conftest import PerformanceBenchmark, create_test_tasks


class TestQueueThroughput:
    """Test queue throughput performance."""

    def test_single_push_pop_performance(self, task_queue, redis_client):
        """Test single push/pop operation performance."""
        benchmark = PerformanceBenchmark("Single Push/Pop Operations")
        tasks = create_test_tasks(1000, num_users=10)

        # Test push operations
        for task in tasks:
            benchmark.record_operation(
                "push_single",
                lambda: task_queue.push(task)
            )

        # Test pop operations
        popped_count = 0
        for _ in range(1000):
            result = benchmark.record_operation(
                "pop_single",
                lambda: task_queue.pop("worker1")
            )
            if result:
                popped_count += 1

        benchmark.print_results()
        assert popped_count > 0

    def test_batch_push_performance(self, task_queue, redis_client):
        """Test batch push operation performance."""
        benchmark = PerformanceBenchmark("Batch Push Operations")

        # Test different batch sizes
        batch_sizes = [10, 50, 100, 500, 1000]

        for batch_size in batch_sizes:
            tasks = create_test_tasks(batch_size, num_users=10)

            # Clear queue
            redis_client.flushdb()

            # Test batch push
            benchmark.record_operation(
                f"push_batch_{batch_size}",
                lambda: task_queue.push_batch(tasks),
                iterations=10
            )

        benchmark.print_results()

    def test_concurrent_push_performance(self, task_queue, redis_client):
        """Test concurrent push operations."""
        benchmark = PerformanceBenchmark("Concurrent Push Operations")
        num_threads = 4
        tasks_per_thread = 250

        def push_tasks(thread_id: int) -> int:
            """Push tasks from a single thread."""
            tasks = create_test_tasks(
                tasks_per_thread,
                user_prefix=f"thread{thread_id}_user",
                num_users=10
            )
            start = time.perf_counter()
            for task in tasks:
                task_queue.push(task)
            return time.perf_counter() - start

        # Run concurrent pushes
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            start = time.perf_counter()
            futures = [
                executor.submit(push_tasks, i)
                for i in range(num_threads)
            ]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
            total_time = time.perf_counter() - start

        total_operations = num_threads * tasks_per_thread
        ops_per_second = total_operations / total_time

        print(f"\n{'='*60}")
        print("Concurrent Push Performance")
        print(f"{'='*60}")
        print(f"Threads: {num_threads}")
        print(f"Tasks per thread: {tasks_per_thread}")
        print(f"Total operations: {total_operations:,}")
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {ops_per_second:,.2f} ops/sec")
        print(f"Average thread time: {sum(results)/len(results):.3f}s")

    def test_priority_queue_performance(self, task_queue, redis_client):
        """Test performance with different priority levels."""
        benchmark = PerformanceBenchmark("Priority Queue Operations")

        # Create tasks with different priorities
        priorities = [
            Priority.VERY_LOW,
            Priority.LOW,
            Priority.NORMAL,
            Priority.HIGH,
            Priority.VERY_HIGH,
            Priority.CRITICAL
        ]

        for priority in priorities:
            tasks = create_test_tasks(100, priority=priority)

            # Clear queue
            redis_client.flushdb()

            # Push tasks
            push_start = time.perf_counter()
            task_queue.push_batch(tasks)
            push_time = time.perf_counter() - push_start

            # Pop tasks
            pop_times = []
            for _ in range(100):
                pop_start = time.perf_counter()
                task = task_queue.pop("worker1")
                if task:
                    pop_times.append(time.perf_counter() - pop_start)

            avg_pop_time = sum(pop_times) / len(pop_times) if pop_times else 0

            print(f"\nPriority {priority.name}:")
            print(f"  Push time (100 tasks): {push_time*1000:.3f}ms")
            print(f"  Avg pop time: {avg_pop_time*1000:.3f}ms")

    def test_work_stealing_performance(self, task_queue, redis_client):
        """Test work stealing performance."""
        benchmark = PerformanceBenchmark("Work Stealing Operations")

        # Push tasks for users that worker2 can steal from
        steal_tasks = create_test_tasks(
            500,
            user_prefix="user",
            num_users=5  # These are worker1's users
        )
        task_queue.push_batch(steal_tasks)

        # Measure work stealing performance
        stolen_count = 0
        steal_times = []

        for _ in range(100):
            start = time.perf_counter()
            task = task_queue.pop("worker2")  # worker2 will steal from worker1
            steal_time = time.perf_counter() - start

            if task:
                stolen_count += 1
                steal_times.append(steal_time)

        if steal_times:
            avg_steal_time = sum(steal_times) / len(steal_times)
            print(f"\n{'='*60}")
            print("Work Stealing Performance")
            print(f"{'='*60}")
            print(f"Stolen tasks: {stolen_count}")
            print(f"Average steal time: {avg_steal_time*1000:.3f}ms")
            print(f"Min steal time: {min(steal_times)*1000:.3f}ms")
            print(f"Max steal time: {max(steal_times)*1000:.3f}ms")


class TestAsyncQueueThroughput:
    """Test async queue throughput performance."""

    @pytest.mark.asyncio
    async def test_async_single_operations(self, async_task_queue, async_redis_client):
        """Test async single push/pop performance."""
        benchmark = PerformanceBenchmark("Async Single Operations")
        tasks = create_test_tasks(1000, num_users=10)

        # Test async push operations
        for task in tasks:
            await benchmark.record_async_operation(
                "async_push_single",
                lambda: async_task_queue.push(task)
            )

        # Test async pop operations
        popped_count = 0
        for _ in range(1000):
            result = await benchmark.record_async_operation(
                "async_pop_single",
                lambda: async_task_queue.pop("worker1")
            )
            if result:
                popped_count += 1

        benchmark.print_results()
        assert popped_count > 0

    @pytest.mark.asyncio
    async def test_async_concurrent_operations(self, async_task_queue, async_redis_client):
        """Test async concurrent operations."""
        benchmark = PerformanceBenchmark("Async Concurrent Operations")
        num_coroutines = 10
        tasks_per_coroutine = 100

        async def push_tasks_async(coro_id: int) -> float:
            """Push tasks from a single coroutine."""
            tasks = create_test_tasks(
                tasks_per_coroutine,
                user_prefix=f"coro{coro_id}_user",
                num_users=10
            )
            start = time.perf_counter()
            for task in tasks:
                await async_task_queue.push(task)
            return time.perf_counter() - start

        # Run concurrent async pushes
        start = time.perf_counter()
        results = await asyncio.gather(*[
            push_tasks_async(i) for i in range(num_coroutines)
        ])
        total_time = time.perf_counter() - start

        total_operations = num_coroutines * tasks_per_coroutine
        ops_per_second = total_operations / total_time

        print(f"\n{'='*60}")
        print("Async Concurrent Push Performance")
        print(f"{'='*60}")
        print(f"Coroutines: {num_coroutines}")
        print(f"Tasks per coroutine: {tasks_per_coroutine}")
        print(f"Total operations: {total_operations:,}")
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {ops_per_second:,.2f} ops/sec")
        print(f"Average coroutine time: {sum(results)/len(results):.3f}s")

    @pytest.mark.asyncio
    async def test_async_batch_concurrent(self, async_task_queue, async_redis_client):
        """Test async batch operations with concurrency."""
        benchmark = PerformanceBenchmark("Async Batch Concurrent")

        # Create batches of tasks
        batches = []
        for i in range(10):
            batch = create_test_tasks(
                100,
                user_prefix=f"batch{i}_user",
                num_users=10
            )
            batches.append(batch)

        # Test push_batch_concurrent
        start = time.perf_counter()
        await async_task_queue.push_batch_concurrent(batches)
        total_time = time.perf_counter() - start

        total_operations = sum(len(batch) for batch in batches)
        ops_per_second = total_operations / total_time

        print(f"\n{'='*60}")
        print("Async Batch Concurrent Performance")
        print(f"{'='*60}")
        print(f"Batches: {len(batches)}")
        print("Tasks per batch: 100")
        print(f"Total operations: {total_operations:,}")
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {ops_per_second:,.2f} ops/sec")
