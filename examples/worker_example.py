"""Worker usage example demonstrating task processing with FairQueue."""

import logging
import time
from typing import Dict

from fairque import FairQueueConfig, Priority, Task, TaskQueue
from fairque.worker.worker import TaskHandler, Worker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExampleTaskHandler(TaskHandler):
    """Example task handler for demonstration purposes."""

    def _process_task(self, task: Task) -> bool:
        """Process a task based on its payload.

        Args:
            task: Task to process

        Returns:
            True if successful, False otherwise
        """
        payload = task.payload
        action = payload.get("action", "unknown")

        logger.info(f"Processing task {task.task_id}: {action}")

        try:
            if action == "process_order":
                return self._process_order(payload)
            elif action == "send_email":
                return self._send_email(payload)
            elif action == "generate_report":
                return self._generate_report(payload)
            elif action == "urgent_notification":
                return self._urgent_notification(payload)
            elif action == "cleanup_data":
                return self._cleanup_data(payload)
            else:
                logger.warning(f"Unknown action: {action}")
                return False

        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {e}")
            return False

    def _process_order(self, payload: Dict) -> bool:
        """Simulate order processing."""
        order_id = payload.get("order_id")
        logger.info(f"Processing order {order_id}")

        # Simulate processing time
        time.sleep(0.5)

        # Simulate success/failure based on order ID
        assert order_id is not None, "Order ID is required"
        success = (order_id % 10) != 0  # 90% success rate

        if success:
            logger.info(f"Order {order_id} processed successfully")
        else:
            logger.error(f"Order {order_id} processing failed")

        return success

    def _send_email(self, payload: Dict) -> bool:
        """Simulate email sending."""
        template = payload.get("template", "default")
        recipient = payload.get("recipient", "unknown")

        logger.info(f"Sending email template '{template}' to {recipient}")

        # Simulate email sending
        time.sleep(0.2)

        logger.info(f"Email sent successfully to {recipient}")
        return True

    def _generate_report(self, payload: Dict) -> bool:
        """Simulate report generation."""
        report_type = payload.get("report_type", "standard")

        logger.info(f"Generating {report_type} report")

        # Simulate longer processing for reports
        time.sleep(2.0)

        logger.info(f"Report generated: {report_type}")
        return True

    def _urgent_notification(self, payload: Dict) -> bool:
        """Simulate urgent notification processing."""
        message = payload.get("message", "Urgent alert")

        logger.warning(f"URGENT: {message}")

        # Immediate processing for urgent tasks
        time.sleep(0.1)

        return True

    def _cleanup_data(self, payload: Dict) -> bool:
        """Simulate data cleanup."""
        data_type = payload.get("data_type", "temp")

        logger.info(f"Cleaning up {data_type} data")

        # Simulate cleanup
        time.sleep(1.0)

        logger.info(f"Data cleanup completed: {data_type}")
        return True

    def on_task_success(self, task: Task, duration: float) -> None:
        """Handle successful task completion."""
        logger.info(f"✓ Task {task.task_id} completed successfully in {duration:.2f}s")

    def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        """Handle task failure."""
        logger.error(f"✗ Task {task.task_id} failed after {duration:.2f}s: {error}")

    def on_task_timeout(self, task: Task, duration: float) -> None:
        """Handle task timeout."""
        logger.warning(f"⏰ Task {task.task_id} timed out after {duration:.2f}s")


def create_sample_tasks(queue: TaskQueue) -> None:
    """Create and push sample tasks to the queue."""

    tasks = [
        # Critical urgent notifications
        Task.create(
            user_id="user:1",
            priority=Priority.CRITICAL,
            payload={"action": "urgent_notification", "message": "System alert: High CPU usage"}
        ),

        # High priority order processing
        Task.create(
            user_id="user:2",
            priority=Priority.HIGH,
            payload={"action": "process_order", "order_id": 12345, "customer": "John Doe"}
        ),
        Task.create(
            user_id="user:1",
            priority=Priority.HIGH,
            payload={"action": "process_order", "order_id": 12346, "customer": "Jane Smith"}
        ),

        # Normal priority tasks
        Task.create(
            user_id="user:3",
            priority=Priority.NORMAL,
            payload={"action": "send_email", "template": "welcome", "recipient": "new@example.com"}
        ),
        Task.create(
            user_id="user:2",
            priority=Priority.NORMAL,
            payload={"action": "send_email", "template": "newsletter", "recipient": "subscriber@example.com"}
        ),

        # Low priority cleanup tasks
        Task.create(
            user_id="user:4",
            priority=Priority.LOW,
            payload={"action": "cleanup_data", "data_type": "temp_files", "age_days": 30}
        ),

        # Very high priority report
        Task.create(
            user_id="user:1",
            priority=Priority.VERY_HIGH,
            payload={"action": "generate_report", "report_type": "financial", "period": "Q4"}
        ),

        # More orders with some that will fail
        Task.create(
            user_id="user:3",
            priority=Priority.HIGH,
            payload={"action": "process_order", "order_id": 12350, "customer": "Test User"}  # This will fail
        ),
    ]

    logger.info(f"Creating {len(tasks)} sample tasks...")

    for task in tasks:
        result = queue.push(task)
        if result["success"]:
            logger.info(f"Pushed task {task.task_id} (user: {task.user_id}, priority: {task.priority.name})")
        else:
            logger.error(f"Failed to push task {task.task_id}")


def main() -> None:
    """Main worker example."""

    # Create configuration for worker
    config = FairQueueConfig.create_default(
        worker_id="example-worker-001",
        assigned_users=["user:1", "user:2"],  # Primary responsibility
        steal_targets=["user:3", "user:4"]    # Can steal work from these users
    )

    # Override some settings for demo
    config.worker.max_concurrent_tasks = 3
    config.worker.poll_interval_seconds = 1.0
    config.worker.task_timeout_seconds = 10.0

    logger.info("=== FairQueue Worker Example ===")
    logger.info(f"Worker ID: {config.worker.id}")
    logger.info(f"Assigned users: {config.worker.assigned_users}")
    logger.info(f"Steal targets: {config.worker.steal_targets}")
    logger.info(f"Max concurrent tasks: {config.worker.max_concurrent_tasks}")

    # Create task handler and worker
    task_handler = ExampleTaskHandler()

    with TaskQueue(config) as queue:
        # Create sample tasks
        create_sample_tasks(queue)

        # Show initial queue state
        logger.info("\\n--- Initial Queue State ---")
        stats = queue.get_stats()
        logger.info(f"Total active tasks: {stats.get('tasks_active', 0)}")

        batch_sizes = queue.get_batch_queue_sizes()
        for user_id, sizes in batch_sizes.items():
            if user_id not in ["totals", "checked_at"]:
                logger.info(f"User {user_id}: {sizes['total_size']} tasks")

        # Start worker
        with Worker(config, task_handler, queue) as worker:
            logger.info("\\n--- Starting Worker ---")
            worker.start()

            # Let worker process tasks
            logger.info("Worker processing tasks... (will run for 15 seconds)")

            # Monitor progress
            for i in range(15):
                time.sleep(1)

                if i % 3 == 0:  # Every 3 seconds
                    worker_stats = worker.get_stats()
                    queue_stats = queue.get_stats()

                    logger.info(f"[{i+1:2d}s] Worker: {worker_stats['tasks_processed']} processed, "
                              f"{worker_stats['active_tasks']} active, "
                              f"Queue: {queue_stats.get('tasks_active', 0)} total active")

                    if queue_stats.get('tasks_active', 0) == 0:
                        logger.info("All tasks completed!")
                        break

            # Final statistics
            logger.info("\\n--- Final Statistics ---")

            worker_stats = worker.get_stats()
            logger.info("Worker Statistics:")
            logger.info(f"  Tasks processed: {worker_stats['tasks_processed']}")
            logger.info(f"  Tasks failed: {worker_stats['tasks_failed']}")
            logger.info(f"  Tasks timeout: {worker_stats['tasks_timeout']}")
            logger.info(f"  Success rate: {worker_stats['success_rate']:.1%}")
            logger.info(f"  Average processing time: {worker_stats['average_processing_time']:.2f}s")
            logger.info(f"  Runtime: {worker_stats['runtime_seconds']:.1f}s")
            logger.info(f"  Healthy: {worker.is_healthy()}")

            queue_stats = queue.get_stats()
            logger.info("Queue Statistics:")
            logger.info(f"  Total pushed: {queue_stats.get('tasks_pushed_total', 0)}")
            logger.info(f"  Total popped: {queue_stats.get('tasks_popped_total', 0)}")
            logger.info(f"  Remaining active: {queue_stats.get('tasks_active', 0)}")

            # Health check
            health = queue.get_health()
            logger.info(f"Queue Health: {health.get('status', 'unknown')}")

            logger.info("\\n--- Worker will stop automatically when exiting context ---")


if __name__ == "__main__":
    main()
