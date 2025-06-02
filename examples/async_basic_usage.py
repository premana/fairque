"""Basic async usage example for FairQueue."""

import asyncio
import logging

from fairque import AsyncTaskHandler, AsyncTaskQueue, AsyncWorker, FairQueueConfig, Priority, Task

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExampleAsyncTaskHandler(AsyncTaskHandler):
    """Example async task handler implementation."""

    async def process_task(self, task: Task) -> bool:
        """Process a task asynchronously.

        Args:
            task: Task to process

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Processing async task {task.task_id} for user {task.user_id}")

        # Simulate async work
        await asyncio.sleep(0.1)

        # Process based on task type in payload
        if isinstance(task.payload, dict):
            task_type = task.payload.get("type", "unknown")

            if task_type == "email":
                return await self._send_email(task.payload)
            elif task_type == "notification":
                return await self._send_notification(task.payload)
            elif task_type == "processing":
                return await self._process_data(task.payload)

        logger.info(f"Successfully processed async task {task.task_id}")
        return True

    async def _send_email(self, payload: dict) -> bool:
        """Simulate sending an email."""
        await asyncio.sleep(0.2)  # Simulate network delay
        logger.info(f"Email sent to {payload.get('recipient', 'unknown')}")
        return True

    async def _send_notification(self, payload: dict) -> bool:
        """Simulate sending a notification."""
        await asyncio.sleep(0.1)  # Simulate API call
        logger.info(f"Notification sent: {payload.get('message', 'No message')}")
        return True

    async def _process_data(self, payload: dict) -> bool:
        """Simulate data processing."""
        await asyncio.sleep(0.3)  # Simulate computation
        logger.info(f"Processed {payload.get('records', 0)} records")
        return True

    async def on_task_success(self, task: Task, duration: float) -> None:
        """Handle successful task completion."""
        logger.info(f"Async task {task.task_id} completed successfully in {duration:.2f}s")

    async def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        """Handle task failure."""
        logger.error(f"Async task {task.task_id} failed after {duration:.2f}s: {error}")

    async def on_task_timeout(self, task: Task, duration: float) -> None:
        """Handle task timeout."""
        logger.warning(f"Async task {task.task_id} timed out after {duration:.2f}s")


async def main() -> None:
    """Main async example function."""
    # Configuration
    config = FairQueueConfig.from_dict({
        "redis": {
            "host": "localhost",
            "port": 6379,
            "db": 0
        },
        "worker": {
            "id": "async-worker-example",
            "assigned_users": ["user1", "user2", "user3"],
            "steal_targets": ["user4", "user5"],
            "max_concurrent_tasks": 5,
            "poll_interval_seconds": 1.0,
            "task_timeout_seconds": 30.0
        },
        "queue": {
            "enable_pipeline_optimization": True,
            "batch_size": 10
        }
    })

    # Create task handler and worker
    task_handler = ExampleAsyncTaskHandler()

    async with AsyncTaskQueue(config) as queue:
        async with AsyncWorker(config, task_handler, queue) as worker:
            logger.info("Starting async example...")

            # Push some example tasks
            tasks = [
                Task(
                    task_id=f"email-{i}",
                    user_id=f"user{(i % 3) + 1}",
                    priority=Priority.NORMAL,
                    payload={
                        "type": "email",
                        "recipient": f"user{i}@example.com",
                        "subject": f"Hello {i}"
                    }
                ) for i in range(5)
            ]

            tasks.extend([
                Task(
                    task_id=f"notification-{i}",
                    user_id=f"user{(i % 3) + 1}",
                    priority=Priority.HIGH,
                    payload={
                        "type": "notification",
                        "message": f"Important notification {i}"
                    }
                ) for i in range(3)
            ])

            tasks.extend([
                Task(
                    task_id=f"processing-{i}",
                    user_id=f"user{(i % 3) + 1}",
                    priority=Priority.CRITICAL,
                    payload={
                        "type": "processing",
                        "records": 1000 * (i + 1)
                    }
                ) for i in range(2)
            ])

            # Push tasks concurrently
            logger.info(f"Pushing {len(tasks)} tasks concurrently...")
            results = await queue.push_batch_concurrent(tasks)

            successful_pushes = sum(1 for r in results if r.get("success", False))
            logger.info(f"Successfully pushed {successful_pushes}/{len(tasks)} tasks")

            # Let the worker process tasks
            logger.info("Processing tasks for 10 seconds...")
            await asyncio.sleep(10)

            # Get statistics
            worker_stats = worker.get_stats()
            queue_stats = await queue.get_stats()

            logger.info("Worker Statistics:")
            for key, value in worker_stats.items():
                logger.info(f"  {key}: {value}")

            logger.info("Queue Statistics:")
            for key, value in queue_stats.items():
                logger.info(f"  {key}: {value}")

            # Health check
            is_healthy = await worker.is_healthy()
            logger.info(f"Worker health status: {'Healthy' if is_healthy else 'Unhealthy'}")

    logger.info("Async example completed!")


if __name__ == "__main__":
    asyncio.run(main())
