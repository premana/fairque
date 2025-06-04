"""Async Worker implementation for FairQueue with task processing and work stealing."""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set

from fairque.core.config import FairQueueConfig
from fairque.core.exceptions import TaskValidationError
from fairque.core.models import Task
from fairque.queue.async_queue import AsyncTaskQueue

logger = logging.getLogger(__name__)


class AsyncTaskHandler(ABC):
    """Abstract base class for async task handlers."""

    @abstractmethod
    async def process_task(self, task: Task) -> bool:
        """Process a task asynchronously.

        Args:
            task: Task to process

        Returns:
            True if task processed successfully, False otherwise
        """
        pass

    async def on_task_success(self, task: Task, duration: float) -> None:
        """Called when task processing succeeds.

        Args:
            task: Successfully processed task
            duration: Processing duration in seconds
        """
        pass

    async def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        """Called when task processing fails.

        Args:
            task: Failed task
            error: Exception that occurred
            duration: Processing duration in seconds
        """
        pass

    async def on_task_timeout(self, task: Task, duration: float) -> None:
        """Called when task processing times out.

        Args:
            task: Timed out task
            duration: Processing duration in seconds
        """
        pass


class AsyncWorker:
    """Production-ready async worker for processing FairQueue tasks with work stealing."""

    def __init__(
        self,
        config: FairQueueConfig,
        task_handler: AsyncTaskHandler,
        queue: Optional[AsyncTaskQueue] = None
    ) -> None:
        """Initialize async worker.

        Args:
            config: FairQueue configuration
            task_handler: Async task processing handler
            queue: Optional AsyncTaskQueue instance (will create from config if not provided)
        """
        self.config = config
        self.task_handler = task_handler
        self.queue = queue or AsyncTaskQueue(config)

        # Worker state
        self.is_running = False
        self.is_stopping = False
        self.worker_task: Optional[asyncio.Task[None]] = None
        self.active_tasks: Set[asyncio.Task[Any]] = set()

        # Processing statistics
        self.stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_timeout": 0,
            "start_time": 0.0,
            "last_task_time": 0.0,
            "processing_time_total": 0.0,
        }

        # Lock for active tasks tracking
        self._active_tasks_lock = asyncio.Lock()

        logger.info(f"AsyncWorker initialized: {config.worker.id}")

    async def start(self) -> None:
        """Start the async worker."""
        if self.is_running:
            logger.warning("AsyncWorker is already running")
            return

        self.is_running = True
        self.is_stopping = False
        self.stats["start_time"] = time.time()

        # Initialize the queue if not already done
        if not hasattr(self.queue, 'redis') or not self.queue.redis:
            await self.queue.initialize()

        # Start worker task
        self.worker_task = asyncio.create_task(
            self._worker_loop(),
            name=f"async-worker-{self.config.worker.id}-main"
        )

        logger.info(f"AsyncWorker started: {self.config.worker.id}")

    async def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the async worker gracefully.

        Args:
            timeout: Maximum time to wait for shutdown (uses config default if not provided)
        """
        if not self.is_running:
            logger.warning("AsyncWorker is not running")
            return

        timeout = timeout or self.config.worker.graceful_shutdown_timeout
        logger.info(f"Stopping AsyncWorker {self.config.worker.id} (timeout: {timeout}s)")

        self.is_stopping = True

        # Wait for current tasks to complete
        await self._wait_for_active_tasks(timeout)

        # Cancel worker task
        if self.worker_task and not self.worker_task.done():
            self.worker_task.cancel()
            try:
                await asyncio.wait_for(self.worker_task, timeout=timeout)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        self.is_running = False

        # Close queue connection
        await self.queue.close()

        # Final statistics
        runtime = time.time() - self.stats["start_time"]
        logger.info(
            f"AsyncWorker stopped: {self.config.worker.id} "
            f"(runtime: {runtime:.1f}s, tasks: {self.stats['tasks_processed']})"
        )

    async def _wait_for_active_tasks(self, timeout: float) -> None:
        """Wait for active tasks to complete.

        Args:
            timeout: Maximum time to wait
        """
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            async with self._active_tasks_lock:
                # Remove completed tasks
                self.active_tasks = {task for task in self.active_tasks if not task.done()}
                active_count = len(self.active_tasks)

            if active_count == 0:
                break

            logger.info(f"Waiting for {active_count} active async tasks to complete...")
            await asyncio.sleep(0.5)

        async with self._active_tasks_lock:
            remaining_tasks = len(self.active_tasks)

        if remaining_tasks > 0:
            logger.warning(f"Shutdown timeout reached, {remaining_tasks} async tasks still active")
            # Cancel remaining tasks
            async with self._active_tasks_lock:
                for task in self.active_tasks:
                    if not task.done():
                        task.cancel()

    async def _worker_loop(self) -> None:
        """Main async worker loop - polls for tasks and processes them."""
        logger.info(f"AsyncWorker loop started: {self.config.worker.id}")

        try:
            while self.is_running and not self.is_stopping:
                try:
                    # Check if we can accept more tasks
                    async with self._active_tasks_lock:
                        # Remove completed tasks
                        self.active_tasks = {task for task in self.active_tasks if not task.done()}
                        active_count = len(self.active_tasks)

                    if active_count >= self.config.worker.max_concurrent_tasks:
                        logger.debug(f"Max concurrent tasks reached ({active_count}), waiting...")
                        await asyncio.sleep(self.config.worker.poll_interval_seconds)
                        continue

                    # Try to get a task
                    task = await self.queue.pop()

                    if task is None:
                        # No tasks available, wait before next poll
                        await asyncio.sleep(self.config.worker.poll_interval_seconds)
                        continue

                    # Submit task for processing
                    await self._submit_task(task)

                except Exception as e:
                    logger.error(f"Error in async worker loop: {e}", exc_info=True)
                    await asyncio.sleep(self.config.worker.poll_interval_seconds)

        except asyncio.CancelledError:
            logger.info(f"AsyncWorker loop cancelled: {self.config.worker.id}")
        except Exception as e:
            logger.error(f"Unexpected error in async worker loop: {e}", exc_info=True)
        finally:
            logger.info(f"AsyncWorker loop finished: {self.config.worker.id}")

    async def _submit_task(self, task: Task) -> None:
        """Submit task for async processing.

        Args:
            task: Task to process
        """
        # Create task for processing
        processing_task = asyncio.create_task(
            self._process_task_wrapper(task),
            name=f"task-{task.task_id}"
        )

        # Add to active tasks
        async with self._active_tasks_lock:
            self.active_tasks.add(processing_task)

        # Add callback to handle completion
        processing_task.add_done_callback(
            lambda t: asyncio.create_task(self._task_completed(task, t))
        )

        logger.debug(f"Async task submitted: {task.task_id}")

    async def _process_task_wrapper(self, task: Task) -> Dict[str, Any]:
        """Wrapper for async task processing with timeout and error handling.

        Args:
            task: Task to process

        Returns:
            Processing result dictionary
        """
        start_time = time.time()

        try:
            # Validate task is ready to execute
            if not task.is_ready_to_execute():
                raise TaskValidationError(f"Task {task.task_id} is not ready to execute")

            # Process the task with timeout
            success = await self._process_task_with_timeout(task)

            duration = time.time() - start_time

            if success:
                await self.task_handler.on_task_success(task, duration)
                return {
                    "success": True,
                    "duration": duration,
                    "task_id": task.task_id
                }
            else:
                await self.task_handler.on_task_failure(task, Exception("Task handler returned False"), duration)
                return {
                    "success": False,
                    "error": "Task handler returned False",
                    "duration": duration,
                    "task_id": task.task_id
                }

        except asyncio.TimeoutError:
            duration = time.time() - start_time
            await self.task_handler.on_task_timeout(task, duration)
            return {
                "success": False,
                "error": "Task processing timeout",
                "duration": duration,
                "task_id": task.task_id,
                "timeout": True
            }

        except Exception as e:
            duration = time.time() - start_time
            await self.task_handler.on_task_failure(task, e, duration)
            return {
                "success": False,
                "error": str(e),
                "duration": duration,
                "task_id": task.task_id
            }

    async def _process_task_with_timeout(self, task: Task) -> bool:
        """Process task with timeout.

        Args:
            task: Task to process

        Returns:
            True if successful, False otherwise

        Raises:
            asyncio.TimeoutError: If task processing times out
        """
        try:
            result = await asyncio.wait_for(
                self.task_handler.process_task(task),
                timeout=self.config.worker.task_timeout_seconds
            )
            return bool(result)
        except asyncio.TimeoutError:
            logger.warning(f"Task {task.task_id} timed out after {self.config.worker.task_timeout_seconds}s")
            raise

    async def _task_completed(self, task: Task, processing_task: asyncio.Task[Any]) -> None:
        """Handle async task completion.

        Args:
            task: Completed task
            processing_task: Asyncio task that processed the task
        """
        # Remove from active tasks
        async with self._active_tasks_lock:
            self.active_tasks.discard(processing_task)

        try:
            result = processing_task.result()

            # Update statistics
            self.stats["last_task_time"] = time.time()
            self.stats["processing_time_total"] += result.get("duration", 0)

            if result.get("success", False):
                self.stats["tasks_processed"] += 1
                logger.debug(f"Async task completed successfully: {task.task_id}")
            elif result.get("timeout", False):
                self.stats["tasks_timeout"] += 1
                logger.warning(f"Async task timed out: {task.task_id}")
            else:
                self.stats["tasks_failed"] += 1
                error = result.get("error", "Unknown error")
                logger.error(f"Async task failed: {task.task_id} - {error}")

            # Clean up task data
            await self.queue.delete_task(task.task_id)

        except asyncio.CancelledError:
            logger.info(f"Async task was cancelled: {task.task_id}")
            self.stats["tasks_failed"] += 1
        except Exception as e:
            logger.error(f"Error handling async task completion: {e}", exc_info=True)
            self.stats["tasks_failed"] += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get async worker statistics.

        Returns:
            Dictionary with worker statistics
        """
        current_time = time.time()
        runtime = current_time - self.stats["start_time"] if self.stats["start_time"] else 0

        # Get active count without async context (for sync method compatibility)
        try:
            # This is a sync method, so we can't use async operations
            # We'll just count the tasks that aren't done
            active_count = len([task for task in self.active_tasks if not task.done()])
        except Exception:
            active_count = len(self.active_tasks)

        total_tasks = self.stats["tasks_processed"] + self.stats["tasks_failed"] + self.stats["tasks_timeout"]
        avg_processing_time = (
            self.stats["processing_time_total"] / total_tasks
            if total_tasks > 0 else 0
        )

        return {
            "worker_id": self.config.worker.id,
            "worker_type": "async",
            "is_running": self.is_running,
            "is_stopping": self.is_stopping,
            "runtime_seconds": runtime,
            "active_tasks": active_count,
            "max_concurrent_tasks": self.config.worker.max_concurrent_tasks,
            "tasks_processed": self.stats["tasks_processed"],
            "tasks_failed": self.stats["tasks_failed"],
            "tasks_timeout": self.stats["tasks_timeout"],
            "total_tasks": total_tasks,
            "success_rate": (
                self.stats["tasks_processed"] / total_tasks
                if total_tasks > 0 else 0
            ),
            "average_processing_time": avg_processing_time,
            "last_task_time": self.stats["last_task_time"],
            "assigned_users": self.config.worker.assigned_users,
            "steal_targets": self.config.worker.steal_targets,
        }

    async def is_healthy(self) -> bool:
        """Check if async worker is healthy.

        Returns:
            True if worker is healthy, False otherwise
        """
        if not self.is_running:
            return False

        # Check if worker task is alive
        if self.worker_task and self.worker_task.done():
            return False

        # Check if we've processed tasks recently (if we should have)
        current_time = time.time()
        time_since_last_task = current_time - self.stats["last_task_time"]

        # Consider unhealthy if no tasks processed in last 5 minutes and we're running
        if self.stats["last_task_time"] > 0 and time_since_last_task > 300:
            # But only if there are tasks available
            try:
                queue_sizes = await self.queue.get_batch_queue_sizes()
                total_tasks = queue_sizes.get("totals", {}).get("total_size", 0)
                if total_tasks > 0:
                    return False
            except Exception:
                # If we can't check queue sizes, assume healthy
                pass

        return True

    async def __aenter__(self) -> "AsyncWorker":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self.is_running:
            await self.stop()
