"""Task scheduler implementation with distributed locking and cron-based scheduling."""

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

import redis

from fairque.core.exceptions import RedisConnectionError
from fairque.core.models import Priority
from fairque.queue.queue import TaskQueue
from fairque.scheduler.models import ScheduledTask

logger = logging.getLogger(__name__)


class TaskScheduler:
    """Task scheduler with cron-based scheduling and distributed locking.

    This scheduler integrates with FairQueue to provide scheduled task execution
    based on cron expressions. It supports distributed deployments with Redis-based
    locking to ensure only one scheduler instance processes tasks at a time.

    Attributes:
        queue: FairQueue instance for task execution
        scheduler_id: Unique identifier for this scheduler instance
        redis: Redis client for persistence
        schedules_key: Redis key for storing schedules
        lock_key: Redis key for distributed locking
        check_interval: Interval in seconds between schedule checks
        lock_timeout: Lock timeout in seconds
        is_running: Whether the scheduler is currently running
        _scheduler_thread: Background thread for scheduler loop
    """

    def __init__(
        self,
        queue: TaskQueue,
        scheduler_id: str,
        schedules_key: str = "fairque:schedules",
        lock_key: str = "fairque:scheduler_lock",
        check_interval: int = 60,
        lock_timeout: int = 300,
    ) -> None:
        """Initialize task scheduler.

        Args:
            queue: FairQueue instance for task execution
            scheduler_id: Unique identifier for this scheduler
            schedules_key: Redis key for storing schedules
            lock_key: Redis key for distributed locking
            check_interval: Interval between schedule checks (seconds)
            lock_timeout: Lock timeout in seconds
        """
        self.queue = queue
        self.scheduler_id = scheduler_id
        self.redis = queue.redis
        self.schedules_key = schedules_key
        self.lock_key = lock_key
        self.check_interval = check_interval
        self.lock_timeout = lock_timeout
        self.is_running = False
        self._scheduler_thread: Optional[threading.Thread] = None

        # Lua script for atomic lock operations
        self._register_lua_scripts()

        logger.info(
            f"TaskScheduler initialized: id={scheduler_id}, "
            f"check_interval={check_interval}s, lock_timeout={lock_timeout}s"
        )

    def _register_lua_scripts(self) -> None:
        """Register Lua scripts for atomic operations."""
        # Script for atomic lock release
        self._release_lock_script = self.redis.register_script("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        """)

    def add_schedule(
        self,
        cron_expr: str,
        user_id: str,
        priority: Priority,
        payload: Dict[str, Any],
        timezone: str = "UTC",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add a new scheduled task.

        Args:
            cron_expr: Cron expression (e.g., "0 9 * * *" for daily at 9 AM)
            user_id: User ID for the task
            priority: Task priority
            payload: Task payload
            timezone: Timezone for cron expression
            metadata: Additional metadata

        Returns:
            Schedule ID of the created scheduled task

        Raises:
            ValueError: If cron expression is invalid
            RedisConnectionError: If Redis operation fails
        """
        # Create scheduled task
        scheduled_task = ScheduledTask.create(
            cron_expr=cron_expr,
            user_id=user_id,
            priority=priority,
            payload=payload,
            timezone=timezone,
            metadata=metadata,
        )

        # Save to Redis
        try:
            self.redis.hset(
                self.schedules_key,
                scheduled_task.schedule_id,
                scheduled_task.to_json(),
            )

            logger.info(
                f"Added schedule {scheduled_task.schedule_id}: "
                f"cron='{cron_expr}', user={user_id}, priority={priority.name}"
            )

            return scheduled_task.schedule_id

        except redis.RedisError as e:
            logger.error(f"Failed to save schedule: {e}")
            raise RedisConnectionError(f"Failed to save schedule: {e}") from e

    def update_schedule(
        self,
        schedule_id: str,
        cron_expr: Optional[str] = None,
        priority: Optional[Priority] = None,
        payload: Optional[Dict[str, Any]] = None,
        timezone: Optional[str] = None,
        is_active: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update an existing scheduled task.

        Args:
            schedule_id: ID of the schedule to update
            cron_expr: New cron expression (optional)
            priority: New priority (optional)
            payload: New payload (optional)
            timezone: New timezone (optional)
            is_active: New active status (optional)
            metadata: New metadata (optional)

        Returns:
            True if update was successful, False if schedule not found

        Raises:
            ValueError: If new cron expression is invalid
            RedisConnectionError: If Redis operation fails
        """
        # Load existing schedule
        scheduled_task = self.get_schedule(schedule_id)
        if not scheduled_task:
            return False

        # Update fields
        if cron_expr is not None:
            scheduled_task.cron_expression = cron_expr
            # Recalculate next run time
            scheduled_task.next_run = scheduled_task.calculate_next_run()

        if priority is not None:
            scheduled_task.priority = priority

        if payload is not None:
            scheduled_task.payload = payload

        if timezone is not None:
            scheduled_task.timezone = timezone
            # Recalculate next run time with new timezone
            scheduled_task.next_run = scheduled_task.calculate_next_run()

        if is_active is not None:
            scheduled_task.is_active = is_active

        if metadata is not None:
            scheduled_task.metadata = metadata

        scheduled_task.updated_at = time.time()

        # Save updated schedule
        try:
            self.redis.hset(
                self.schedules_key,
                schedule_id,
                scheduled_task.to_json(),
            )

            logger.info(f"Updated schedule {schedule_id}")
            return True

        except redis.RedisError as e:
            logger.error(f"Failed to update schedule: {e}")
            raise RedisConnectionError(f"Failed to update schedule: {e}") from e

    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a scheduled task.

        Args:
            schedule_id: ID of the schedule to remove

        Returns:
            True if schedule was removed, False if not found

        Raises:
            RedisConnectionError: If Redis operation fails
        """
        try:
            result = self.redis.hdel(self.schedules_key, schedule_id)
            if int(result) > 0:
                logger.info(f"Removed schedule {schedule_id}")
                return True

            return False

        except redis.RedisError as e:
            logger.error(f"Failed to remove schedule: {e}")
            raise RedisConnectionError(f"Failed to remove schedule: {e}") from e

    def get_schedule(self, schedule_id: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by ID.

        Args:
            schedule_id: Schedule ID

        Returns:
            ScheduledTask instance or None if not found

        Raises:
            RedisConnectionError: If Redis operation fails
        """
        try:
            data = self.redis.hget(self.schedules_key, schedule_id)

            if not data:
                return None

            return ScheduledTask.from_json(data)

        except redis.RedisError as e:
            logger.error(f"Failed to get schedule: {e}")
            raise RedisConnectionError(f"Failed to get schedule: {e}") from e
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Invalid schedule data for {schedule_id}: {e}")
            return None

    def list_schedules(
        self,
        user_id: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> List[ScheduledTask]:
        """List all scheduled tasks with optional filters.

        Args:
            user_id: Filter by user ID (optional)
            is_active: Filter by active status (optional)

        Returns:
            List of ScheduledTask instances

        Raises:
            RedisConnectionError: If Redis operation fails
        """
        try:
            all_data = self.redis.hgetall(self.schedules_key)
            schedules = []

            for schedule_id, data in all_data.items():
                try:
                    scheduled_task = ScheduledTask.from_json(data)

                    # Apply filters
                    if user_id is not None and scheduled_task.user_id != user_id:
                        continue

                    if is_active is not None and scheduled_task.is_active != is_active:
                        continue

                    schedules.append(scheduled_task)

                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Invalid schedule data for {schedule_id}: {e}")

            return schedules

        except redis.RedisError as e:
            logger.error(f"Failed to list schedules: {e}")
            raise RedisConnectionError(f"Failed to list schedules: {e}") from e

    def start(self) -> None:
        """Start the scheduler in a background thread.

        The scheduler will run until stop() is called. Only one scheduler
        instance should be active at a time in a distributed deployment.
        """
        if self.is_running:
            logger.warning("Scheduler is already running")
            return

        self.is_running = True
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name=f"TaskScheduler-{self.scheduler_id}",
            daemon=True,
        )
        self._scheduler_thread.start()

        logger.info(f"Scheduler {self.scheduler_id} started")

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return

        self.is_running = False

        if self._scheduler_thread and self._scheduler_thread.is_alive():
            # Wait for thread to finish current iteration
            self._scheduler_thread.join(timeout=self.check_interval + 5)

        logger.info(f"Scheduler {self.scheduler_id} stopped")

    def _scheduler_loop(self) -> None:
        """Main scheduler loop with distributed locking."""
        logger.info(f"Scheduler loop started for {self.scheduler_id}")

        while self.is_running:
            try:
                # Try to acquire distributed lock
                if self._try_acquire_lock():
                    try:
                        # Process scheduled tasks
                        self._process_scheduled_tasks()
                    finally:
                        # Always release lock
                        self._release_lock()
                else:
                    logger.debug(
                        f"Scheduler {self.scheduler_id} could not acquire lock, "
                        "another instance may be active"
                    )

                # Sleep until next check
                time.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                # Continue running after error
                time.sleep(self.check_interval)

        logger.info(f"Scheduler loop stopped for {self.scheduler_id}")

    def _try_acquire_lock(self) -> bool:
        """Try to acquire distributed lock.

        Returns:
            True if lock was acquired, False otherwise
        """
        lock_value = f"{self.scheduler_id}:{time.time()}"

        try:
            # Try to set lock with expiration
            result = self.redis.set(
                self.lock_key,
                lock_value,
                nx=True,  # Only set if doesn't exist
                ex=self.lock_timeout,  # Expiration time
            )

            return result is True

        except redis.RedisError as e:
            logger.error(f"Failed to acquire lock: {e}")
            return False

    def _release_lock(self) -> bool:
        """Release distributed lock atomically.

        Returns:
            True if lock was released, False otherwise
        """
        lock_value = f"{self.scheduler_id}:{time.time()}"

        try:
            # Use Lua script for atomic check-and-delete
            result = self._release_lock_script(
                keys=[self.lock_key],
                args=[lock_value],
            )

            return result == 1

        except redis.RedisError as e:
            logger.error(f"Failed to release lock: {e}")
            return False

    def _process_scheduled_tasks(self) -> None:
        """Process all scheduled tasks that are ready for execution."""
        current_time = time.time()
        processed_count = 0
        error_count = 0

        logger.debug(f"Processing scheduled tasks at {current_time}")

        # Get all active schedules
        schedules = self._get_ready_schedules(current_time)

        for scheduled_task in schedules:
            try:
                # Create and push task to queue
                task = scheduled_task.create_task()
                result = self.queue.push(task)

                # Update schedule with last run time
                scheduled_task.update_after_run(current_time)

                # Save updated schedule
                self.redis.hset(
                    self.schedules_key,
                    scheduled_task.schedule_id,
                    scheduled_task.to_json(),
                )

                processed_count += 1

                logger.info(
                    f"Executed scheduled task: "
                    f"schedule_id={scheduled_task.schedule_id}, "
                    f"task_id={task.task_id}, "
                    f"user={scheduled_task.user_id}"
                )

            except Exception as e:
                error_count += 1
                logger.error(
                    f"Failed to execute scheduled task {scheduled_task.schedule_id}: {e}",
                    exc_info=True,
                )

        if processed_count > 0 or error_count > 0:
            logger.info(
                f"Scheduled task processing complete: "
                f"processed={processed_count}, errors={error_count}"
            )

    def _get_ready_schedules(self, current_time: float) -> List[ScheduledTask]:
        """Get all schedules ready for execution.

        Args:
            current_time: Current timestamp

        Returns:
            List of ScheduledTask instances ready for execution
        """
        ready_schedules = []

        try:
            # Get all schedules
            all_schedules = self.list_schedules(is_active=True)

            for scheduled_task in all_schedules:
                # Check if schedule is ready
                if scheduled_task.next_run and scheduled_task.next_run <= current_time:
                    ready_schedules.append(scheduled_task)

            return ready_schedules

        except Exception as e:
            logger.error(f"Failed to get ready schedules: {e}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics.

        Returns:
            Dictionary with scheduler statistics
        """
        try:
            all_schedules = self.list_schedules()
            active_schedules = [s for s in all_schedules if s.is_active]

            # Group by user
            by_user: Dict[str, int] = {}
            for schedule in active_schedules:
                by_user[schedule.user_id] = by_user.get(schedule.user_id, 0) + 1

            # Group by priority
            by_priority: Dict[str, int] = {}
            for schedule in active_schedules:
                priority_name = schedule.priority.name
                by_priority[priority_name] = by_priority.get(priority_name, 0) + 1

            return {
                "scheduler_id": self.scheduler_id,
                "is_running": self.is_running,
                "total_schedules": len(all_schedules),
                "active_schedules": len(active_schedules),
                "inactive_schedules": len(all_schedules) - len(active_schedules),
                "schedules_by_user": by_user,
                "schedules_by_priority": by_priority,
                "check_interval": self.check_interval,
                "lock_timeout": self.lock_timeout,
            }

        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {
                "scheduler_id": self.scheduler_id,
                "is_running": self.is_running,
                "error": str(e),
            }
