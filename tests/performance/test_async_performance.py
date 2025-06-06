"""Performance tests comparing sync vs async implementations."""

import asyncio
import concurrent.futures
import time

import pytest

from tests.performance.conftest import create_test_tasks


class TestAsyncVsSyncPerformance:
    """Compare performance between sync and async implementations."""

    @pytest.mark.asyncio
    async def test_throughput_comparison(self, task_queue, async_task_queue, redis_client, async_redis_client):
        """Compare throughput between sync and async implementations."""
        # Test parameters
        num_tasks = 1000
        num_users = 10

        # Create tasks
        tasks = create_test_tasks(num_tasks, num_users=num_users)

        # Test sync implementation
        redis_client.flushdb()
        sync_start = time.perf_counter()
        task_queue.push_batch(tasks)
        sync_push_time = time.perf_counter() - sync_start

        # Pop all tasks
        sync_pop_start = time.perf_counter()
        popped_sync = 0
        while task_queue.pop("worker1"):
            popped_sync += 1
        sync_pop_time = time.perf_counter() - sync_pop_start

        # Test async implementation
        await async_redis_client.flushdb()
        async_start = time.perf_counter()
        await async_task_queue.push_batch(tasks)
        async_push_time = time.perf_counter() - async_start

        # Pop all tasks async
        async_pop_start = time.perf_counter()
        popped_async = 0
        while await async_task_queue.pop("worker1"):
            popped_async += 1
        async_pop_time = time.perf_counter() - async_pop_start

        # Calculate metrics
        sync_total = sync_push_time + sync_pop_time
        async_total = async_push_time + async_pop_time

        print(f"\n{'='*60}")
        print("Sync vs Async Throughput Comparison")
        print(f"{'='*60}")
        print(f"Tasks: {num_tasks}")
        print("\nSync Implementation:")
        print(f"  Push time: {sync_push_time:.3f}s ({num_tasks/sync_push_time:.0f} ops/sec)")
        print(f"  Pop time: {sync_pop_time:.3f}s ({popped_sync/sync_pop_time:.0f} ops/sec)")
        print(f"  Total time: {sync_total:.3f}s")
        print("\nAsync Implementation:")
        print(f"  Push time: {async_push_time:.3f}s ({num_tasks/async_push_time:.0f} ops/sec)")
        print(f"  Pop time: {async_pop_time:.3f}s ({popped_async/async_pop_time:.0f} ops/sec)")
        print(f"  Total time: {async_total:.3f}s")
        print(f"\nSpeedup: {sync_total/async_total:.2f}x")

    @pytest.mark.asyncio
    async def test_concurrent_operations_comparison(self, task_queue, async_task_queue, redis_client, async_redis_client):
        """Compare concurrent operations between sync and async."""
        num_concurrent = 10
        tasks_per_operation = 100

        # Sync implementation with threads
        def sync_push_operation(op_id: int) -> float:
            tasks = create_test_tasks(
                tasks_per_operation,
                user_prefix=f"sync_op{op_id}_user",
                num_users=10
            )
            start = time.perf_counter()
            for task in tasks:
                task_queue.push(task)
            return time.perf_counter() - start

        redis_client.flushdb()
        sync_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            sync_futures = [
                executor.submit(sync_push_operation, i)
                for i in range(num_concurrent)
            ]
            sync_results = [f.result() for f in concurrent.futures.as_completed(sync_futures)]
        sync_total_time = time.perf_counter() - sync_start

        # Async implementation with coroutines
        async def async_push_operation(op_id: int) -> float:
            tasks = create_test_tasks(
                tasks_per_operation,
                user_prefix=f"async_op{op_id}_user",
                num_users=10
            )
            start = time.perf_counter()
            for task in tasks:
                await async_task_queue.push(task)
            return time.perf_counter() - start

        await async_redis_client.flushdb()
        async_start = time.perf_counter()
        async_results = await asyncio.gather(*[
            async_push_operation(i) for i in range(num_concurrent)
        ])
        async_total_time = time.perf_counter() - async_start

        # Calculate metrics
        total_operations = num_concurrent * tasks_per_operation
        sync_throughput = total_operations / sync_total_time
        async_throughput = total_operations / async_total_time

        print(f"\n{'='*60}")
        print("Concurrent Operations: Sync (Threads) vs Async (Coroutines)")
        print(f"{'='*60}")
        print(f"Concurrent operations: {num_concurrent}")
        print(f"Tasks per operation: {tasks_per_operation}")
        print(f"Total tasks: {total_operations}")
        print("\nSync (ThreadPoolExecutor):")
        print(f"  Total time: {sync_total_time:.3f}s")
        print(f"  Throughput: {sync_throughput:,.0f} ops/sec")
        print(f"  Avg operation time: {sum(sync_results)/len(sync_results):.3f}s")
        print("\nAsync (asyncio.gather):")
        print(f"  Total time: {async_total_time:.3f}s")
        print(f"  Throughput: {async_throughput:,.0f} ops/sec")
        print(f"  Avg operation time: {sum(async_results)/len(async_results):.3f}s")
        print(f"\nAsync advantage: {async_throughput/sync_throughput:.2f}x")

    @pytest.mark.asyncio
    async def test_high_concurrency_comparison(self, task_queue, async_task_queue, redis_client, async_redis_client):
        """Test performance under high concurrency load."""
        # Test with increasing concurrency levels
        concurrency_levels = [10, 50, 100, 200]
        tasks_per_operation = 10

        print(f"\n{'='*60}")
        print("High Concurrency Performance Comparison")
        print(f"{'='*60}")
        print(f"{'Concurrency':<15} {'Sync Time':<15} {'Async Time':<15} {'Speedup':<10}")
        print(f"{'-'*55}")

        for concurrency in concurrency_levels:
            # Sync test
            redis_client.flushdb()

            def sync_operation(op_id: int) -> None:
                tasks = create_test_tasks(
                    tasks_per_operation,
                    user_prefix=f"sync{op_id}",
                    num_users=5
                )
                for task in tasks:
                    task_queue.push(task)

            sync_start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(sync_operation, i) for i in range(concurrency)]
                concurrent.futures.wait(futures)
            sync_time = time.perf_counter() - sync_start

            # Async test
            await async_redis_client.flushdb()

            async def async_operation(op_id: int) -> None:
                tasks = create_test_tasks(
                    tasks_per_operation,
                    user_prefix=f"async{op_id}",
                    num_users=5
                )
                for task in tasks:
                    await async_task_queue.push(task)

            async_start = time.perf_counter()
            await asyncio.gather(*[async_operation(i) for i in range(concurrency)])
            async_time = time.perf_counter() - async_start

            speedup = sync_time / async_time
            print(f"{concurrency:<15} {sync_time:<15.3f} {async_time:<15.3f} {speedup:<10.2f}x")

    @pytest.mark.asyncio
    async def test_resource_usage_comparison(self, task_queue, async_task_queue, redis_client, async_redis_client):
        """Compare resource usage between sync and async implementations."""
        import os

        import psutil

        # Get current process
        process = psutil.Process(os.getpid())

        # Test parameters
        num_operations = 1000
        batch_size = 10

        # Measure sync implementation
        redis_client.flushdb()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        initial_cpu = process.cpu_percent()

        sync_start = time.perf_counter()
        for i in range(num_operations):
            tasks = create_test_tasks(batch_size, user_prefix=f"sync{i}", num_users=5)
            task_queue.push_batch(tasks)
        sync_time = time.perf_counter() - sync_start

        sync_memory = process.memory_info().rss / 1024 / 1024  # MB
        sync_cpu = process.cpu_percent()
        sync_memory_delta = sync_memory - initial_memory

        # Let system settle
        await asyncio.sleep(1)

        # Measure async implementation
        await async_redis_client.flushdb()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        async_start = time.perf_counter()
        for i in range(num_operations):
            tasks = create_test_tasks(batch_size, user_prefix=f"async{i}", num_users=5)
            await async_task_queue.push_batch(tasks)
        async_time = time.perf_counter() - async_start

        async_memory = process.memory_info().rss / 1024 / 1024  # MB
        async_cpu = process.cpu_percent()
        async_memory_delta = async_memory - initial_memory

        print(f"\n{'='*60}")
        print("Resource Usage Comparison")
        print(f"{'='*60}")
        print(f"Operations: {num_operations} batches of {batch_size} tasks")
        print("\nSync Implementation:")
        print(f"  Time: {sync_time:.3f}s")
        print(f"  Memory delta: {sync_memory_delta:.2f} MB")
        print(f"  CPU usage: {sync_cpu:.1f}%")
        print("\nAsync Implementation:")
        print(f"  Time: {async_time:.3f}s")
        print(f"  Memory delta: {async_memory_delta:.2f} MB")
        print(f"  CPU usage: {async_cpu:.1f}%")
        print("\nEfficiency:")
        print(f"  Time efficiency: {sync_time/async_time:.2f}x faster with async")
        print(f"  Memory efficiency: {abs(sync_memory_delta/async_memory_delta):.2f}x")
