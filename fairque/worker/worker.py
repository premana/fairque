"""Worker implementation for FairQueue with task processing and work stealing."""

import logging
import signal
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, Optional, Set

from fairque.core.config import FairQueueConfig
from fairque.core.exceptions import TaskValidationError
from fairque.core.models import Task
from fairque.queue.queue import TaskQueue

logger = logging.getLogger(__name__)


class TaskHandler(ABC):
    """Abstract base class for task handlers."""

    def process_task(self, task: Task) -> bool:
        """Process a task. If task has func set, execute it automatically.

        Args:
            task: Task to process

        Returns:
            True if task processed successfully, False otherwise
        """
        if task.func is not None:
            try:
                result = task()  # Use __call__ method
                logger.debug(f"Function task executed successfully: {task.task_id}, result: {result}")
                return True
            except Exception as e:
                logger.error(f"Function execution failed for task {task.task_id}: {e}")
                return False
        else:
            # Call the custom implementation
            return self._process_task(task)

    @abstractmethod
    def _process_task(self, task: Task) -> bool:
        """Override this method for custom task processing.

        Args:
            task: Task to process

        Returns:
            True if task processed successfully, False otherwise
        """
        pass

    def on_task_success(self, task: Task, duration: float) -> None:
        """Called when task processing succeeds.

        Args:
            task: Successfully processed task
            duration: Processing duration in seconds
        """
        pass

    def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        """Called when task processing fails.

        Args:
            task: Failed task
            error: Exception that occurred
            duration: Processing duration in seconds
        """
        pass

    def on_task_timeout(self, task: Task, duration: float) -> None:
        """Called when task processing times out.

        Args:
            task: Timed out task
            duration: Processing duration in seconds
        """
        pass


class Worker:
    """Production-ready worker for processing FairQueue tasks with work stealing."""

    def __init__(
        self,
        config: FairQueueConfig,
        task_handler: TaskHandler,
        queue: Optional[TaskQueue] = None
    ) -> None:
        """Initialize worker.

        Args:
            config: FairQueue configuration
            task_handler: Task processing handler
            queue: Optional FairQueue instance (will create from config if not provided)
        """
        self.config = config
        self.task_handler = task_handler
        self.queue = queue or TaskQueue(config)

        # Worker state
        self.is_running = False
        self.is_stopping = False
        self.worker_thread: Optional[threading.Thread] = None
        self.executor: Optional[ThreadPoolExecutor] = None

        # Processing statistics
        self.stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_timeout": 0,
            "start_time": 0.0,
            "last_task_time": 0.0,
            "processing_time_total": 0.0,
        }

        # Active tasks tracking
        self.active_tasks: Set[str] = set()
        self.active_tasks_lock = threading.Lock()

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        logger.info(f"Worker initialized: {config.worker.id}")

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum: int, frame: Any) -> None:
            logger.info(f"Received signal {signum}, initiating graceful shutdown")
            self.stop()

        # Register handlers for common termination signals
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def start(self) -> None:
        """Start the worker in a separate thread."""
        if self.is_running:
            logger.warning("Worker is already running")
            return

        self.is_running = True
        self.is_stopping = False
        self.stats["start_time"] = time.time()

        # Initialize thread pool executor
        self.executor = ThreadPoolExecutor(
            max_workers=self.config.worker.max_concurrent_tasks,
            thread_name_prefix=f"worker-{self.config.worker.id}"
        )

        # Start worker thread
        self.worker_thread = threading.Thread(
            target=self._worker_loop,
            name=f"worker-{self.config.worker.id}-main",
            daemon=True
        )
        self.worker_thread.start()

        logger.info(f"Worker started: {self.config.worker.id}")

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop the worker gracefully.

        Args:
            timeout: Maximum time to wait for shutdown (uses config default if not provided)
        """
        if not self.is_running:
            logger.warning("Worker is not running")
            return

        timeout = timeout or self.config.worker.graceful_shutdown_timeout
        logger.info(f"Stopping worker {self.config.worker.id} (timeout: {timeout}s)")

        self.is_stopping = True

        # Wait for current tasks to complete
        self._wait_for_active_tasks(timeout)

        # Shutdown executor
        if self.executor:
            self.executor.shutdown(wait=True, timeout=timeout)
            self.executor = None

        # Wait for worker thread to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=timeout)

        self.is_running = False

        # Final statistics
        runtime = time.time() - self.stats["start_time"]
        logger.info(
            f"Worker stopped: {self.config.worker.id} "
            f"(runtime: {runtime:.1f}s, tasks: {self.stats['tasks_processed']})"
        )

    def _wait_for_active_tasks(self, timeout: float) -> None:
        """Wait for active tasks to complete.

        Args:
            timeout: Maximum time to wait
        """
        start_time = time.time()

        while self.active_tasks and (time.time() - start_time) < timeout:
            with self.active_tasks_lock:
                active_count = len(self.active_tasks)

            if active_count > 0:
                logger.info(f"Waiting for {active_count} active tasks to complete...")
                time.sleep(0.5)
            else:
                break

        with self.active_tasks_lock:
            remaining_tasks = len(self.active_tasks)

        if remaining_tasks > 0:
            logger.warning(f"Shutdown timeout reached, {remaining_tasks} tasks still active")

    def _worker_loop(self) -> None:
        """Main worker loop - polls for tasks and processes them."""
        logger.info(f"Worker loop started: {self.config.worker.id}")

        while self.is_running and not self.is_stopping:
            try:
                # Check if we can accept more tasks
                with self.active_tasks_lock:
                    active_count = len(self.active_tasks)

                if active_count >= self.config.worker.max_concurrent_tasks:
                    logger.debug(f"Max concurrent tasks reached ({active_count}), waiting...")
                    time.sleep(self.config.worker.poll_interval_seconds)
                    continue

                # Try to get a task
                task = self.queue.pop()

                if task is None:
                    # No tasks available, wait before next poll
                    time.sleep(self.config.worker.poll_interval_seconds)
                    continue

                # Submit task for processing
                self._submit_task(task)

            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(self.config.worker.poll_interval_seconds)

        logger.info(f"Worker loop finished: {self.config.worker.id}")

    def _submit_task(self, task: Task) -> None:
        """Submit task for processing in thread pool.

        Args:
            task: Task to process
        """
        if not self.executor:
            logger.error("Executor not available for task submission")
            return

        # Add to active tasks
        with self.active_tasks_lock:
            self.active_tasks.add(task.task_id)

        # Submit to thread pool
        future = self.executor.submit(self._process_task_wrapper, task)

        # Add callback to handle completion
        future.add_done_callback(lambda f: self._task_completed(task, f))

        logger.debug(f"Task submitted: {task.task_id}")

    def _process_task_wrapper(self, task: Task) -> Dict[str, Any]:
        """Wrapper for task processing with timeout and error handling.

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
            success = self._process_task_with_timeout(task)

            duration = time.time() - start_time

            if success:
                self.task_handler.on_task_success(task, duration)
                return {
                    "success": True,
                    "duration": duration,
                    "task_id": task.task_id
                }
            else:
                self.task_handler.on_task_failure(task, Exception("Task handler returned False"), duration)
                return {
                    "success": False,
                    "error": "Task handler returned False",
                    "duration": duration,
                    "task_id": task.task_id
                }

        except FutureTimeoutError:
            duration = time.time() - start_time
            self.task_handler.on_task_timeout(task, duration)
            return {
                "success": False,
                "error": "Task processing timeout",
                "duration": duration,
                "task_id": task.task_id,
                "timeout": True
            }

        except Exception as e:
            duration = time.time() - start_time
            self.task_handler.on_task_failure(task, e, duration)
            return {
                "success": False,
                "error": str(e),
                "duration": duration,
                "task_id": task.task_id
            }

    def _process_task_with_timeout(self, task: Task) -> bool:
        """Process task with timeout.

        Args:
            task: Task to process

        Returns:
            True if successful, False otherwise

        Raises:
            FutureTimeoutError: If task processing times out
        """
        # Create a separate executor for timeout handling
        with ThreadPoolExecutor(max_workers=1) as timeout_executor:
            future = timeout_executor.submit(self.task_handler.process_task, task)

            try:
                result = future.result(timeout=self.config.worker.task_timeout_seconds)
                return bool(result)
            except Exception as e:
                # Cancel the future if it's still running
                future.cancel()
                raise e

    def _task_completed(self, task: Task, future: Any) -> None:
        """Handle task completion.

        Args:
            task: Completed task
            future: Future object with result
        """
        # Remove from active tasks
        with self.active_tasks_lock:
            self.active_tasks.discard(task.task_id)

        try:
            result = future.result()

            # Update statistics
            self.stats["last_task_time"] = time.time()
            self.stats["processing_time_total"] += result.get("duration", 0)

            if result.get("success", False):
                self.stats["tasks_processed"] += 1
                logger.debug(f"Task completed successfully: {task.task_id}")
            elif result.get("timeout", False):
                self.stats["tasks_timeout"] += 1
                logger.warning(f"Task timed out: {task.task_id}")
            else:
                self.stats["tasks_failed"] += 1
                error = result.get("error", "Unknown error")
                logger.error(f"Task failed: {task.task_id} - {error}")

            # Clean up task data
            self.queue.delete_task(task.task_id)

        except Exception as e:
            logger.error(f"Error handling task completion: {e}", exc_info=True)
            self.stats["tasks_failed"] += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics.

        Returns:
            Dictionary with worker statistics
        """
        current_time = time.time()
        runtime = current_time - self.stats["start_time"] if self.stats["start_time"] else 0

        with self.active_tasks_lock:
            active_count = len(self.active_tasks)

        total_tasks = self.stats["tasks_processed"] + self.stats["tasks_failed"] + self.stats["tasks_timeout"]
        avg_processing_time = (
            self.stats["processing_time_total"] / total_tasks
            if total_tasks > 0 else 0
        )

        return {
            "worker_id": self.config.worker.id,
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

    def is_healthy(self) -> bool:
        """Check if worker is healthy.

        Returns:
            True if worker is healthy, False otherwise
        """
        if not self.is_running:
            return False

        # Check if worker thread is alive
        if self.worker_thread and not self.worker_thread.is_alive():
            return False

        # Check if we've processed tasks recently (if we should have)
        current_time = time.time()
        time_since_last_task = current_time - self.stats["last_task_time"]

        # Consider unhealthy if no tasks processed in last 5 minutes and we're running
        if self.stats["last_task_time"] > 0 and time_since_last_task > 300:
            # But only if there are tasks available
            try:
                queue_sizes = self.queue.get_batch_queue_sizes()
                total_tasks = queue_sizes.get("totals", {}).get("total_size", 0)
                if total_tasks > 0:
                    return False
            except Exception:
                # If we can't check queue sizes, assume healthy
                pass

        return True

    def __enter__(self) -> "Worker":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        if self.is_running:
            self.stop()
