"""Main FairQueue implementation with Redis-based operations."""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import redis

from fairque.core.config import FairQueueConfig
from fairque.core.exceptions import (
    LuaScriptError,
    RedisConnectionError,
    TaskSerializationError,
    TaskValidationError,
)
from fairque.core.models import Priority, Task
from fairque.utils.lua_manager import LuaScriptManager

logger = logging.getLogger(__name__)


class TaskQueue:
    """Production-ready fair queue implementation using Redis with work stealing and priority scheduling."""

    def __init__(
        self,
        config: FairQueueConfig,
        redis_client: Optional[redis.Redis] = None
    ) -> None:
        """Initialize FairQueue with configuration.

        Args:
            config: FairQueue configuration
            redis_client: Optional Redis client (will create from config if not provided)

        Raises:
            RedisConnectionError: If Redis connection fails
        """
        self.config = config
        config.validate_all()

        # Initialize Redis client
        if redis_client:
            self.redis = redis_client
        else:
            try:
                self.redis = config.create_redis_client()
                # Test connection
                self.redis.ping()
            except redis.RedisError as e:
                raise RedisConnectionError(f"Failed to connect to Redis: {e}") from e

        # Initialize Lua script manager
        self.lua_manager = LuaScriptManager(self.redis)

        # Load required scripts
        self._load_scripts()

        logger.info(f"FairQueue initialized with worker ID: {config.worker.id}")

    def _load_scripts(self) -> None:
        """Load all required Lua scripts."""
        try:
            required_scripts = ["push", "pop", "stats"]
            for script_name in required_scripts:
                self.lua_manager.load_script(script_name)
            logger.debug(f"Loaded {len(required_scripts)} Lua scripts")
        except LuaScriptError as e:
            logger.error(f"Failed to load Lua scripts: {e}")
            raise

    def push(self, task: Task) -> Dict[str, Any]:
        """Push a task to the appropriate queue based on priority.

        Args:
            task: Task to push to queue

        Returns:
            Dictionary with operation result and statistics

        Raises:
            TaskValidationError: If task validation fails
            LuaScriptError: If Lua script execution fails
        """
        # Validate task
        if not task.task_id:
            raise TaskValidationError("Task ID cannot be empty")

        if not task.user_id:
            raise TaskValidationError("User ID cannot be empty")

        # Convert task to Lua arguments
        try:
            lua_args = task.to_lua_args()
        except Exception as e:
            raise TaskSerializationError(f"Failed to serialize task: {e}") from e

        # Execute push script
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("push", args=lua_args)
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("push", {
                    "error_code": error_code,
                    "message": error_message,
                    "task_id": task.task_id
                })

            logger.debug(f"Pushed task {task.task_id} for user {task.user_id} with priority {task.priority}")
            return dict(response)

        except json.JSONDecodeError as e:
            raise LuaScriptError("push", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def pop(self, user_list: Optional[List[str]] = None) -> Optional[Task]:
        """Pop a task from queues using fair scheduling and work stealing.

        Args:
            user_list: List of user IDs to check (defaults to worker's assigned_users + steal_targets)

        Returns:
            Task if available, None if no tasks ready

        Raises:
            LuaScriptError: If Lua script execution fails
        """
        # Use worker configuration if no user list provided
        if user_list is None:
            user_list = self.config.worker.get_all_users()

        if not user_list:
            logger.warning("No users to check for tasks")
            return None

        # Convert user list to comma-separated string for Lua script
        user_list_str = ",".join(user_list)
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("pop", args=[user_list_str])
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("pop", {
                    "error_code": error_code,
                    "message": error_message,
                    "user_list": user_list
                })

            # Check if task was found
            task_data = response.get("data")
            if not task_data:
                logger.debug("No ready tasks available")
                return None

            # Convert response to Task object
            try:
                task = Task(
                    task_id=task_data["task_id"],
                    user_id=task_data["user_id"],
                    priority=Priority(task_data["priority"]),
                    payload=task_data["payload"],
                    retry_count=task_data["retry_count"],
                    max_retries=task_data["max_retries"],
                    created_at=task_data["created_at"],
                    execute_after=task_data["execute_after"]
                )

                logger.debug(f"Popped task {task.task_id} from user {task_data['selected_user']}")
                return task

            except (KeyError, ValueError) as e:
                raise TaskSerializationError(f"Failed to deserialize task from pop result: {e}") from e

        except json.JSONDecodeError as e:
            raise LuaScriptError("pop", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics.

        Returns:
            Dictionary with queue statistics

        Raises:
            LuaScriptError: If Lua script execution fails
        """
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("stats", args=["get_stats"])
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("stats", {
                    "error_code": error_code,
                    "message": error_message,
                    "operation": "get_stats"
                })

            return dict(response.get("data", {}))

        except json.JSONDecodeError as e:
            raise LuaScriptError("stats", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def get_queue_sizes(self, user_id: str) -> Dict[str, int]:
        """Get queue sizes for a specific user.

        Args:
            user_id: User identifier

        Returns:
            Dictionary with critical_size, normal_size, and total_size

        Raises:
            LuaScriptError: If Lua script execution fails
        """
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("stats", args=["get_queue_sizes", user_id])
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("stats", {
                    "error_code": error_code,
                    "message": error_message,
                    "operation": "get_queue_sizes",
                    "user_id": user_id
                })

            # Convert to dict[str, int] explicitly
            data = response.get("data", {})
            return {str(k): int(v) for k, v in data.items()}

        except json.JSONDecodeError as e:
            raise LuaScriptError("stats", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def get_batch_queue_sizes(self, user_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get queue sizes for multiple users.

        Args:
            user_list: List of user IDs (defaults to worker's assigned_users + steal_targets)

        Returns:
            Dictionary with per-user queue sizes and totals

        Raises:
            LuaScriptError: If Lua script execution fails
        """
        if user_list is None:
            user_list = self.config.worker.get_all_users()

        if not user_list:
            return {"totals": {"critical_size": 0, "normal_size": 0, "total_size": 0}}

        user_list_str = ",".join(user_list)
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("stats", args=["get_batch_sizes", user_list_str])
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("stats", {
                    "error_code": error_code,
                    "message": error_message,
                    "operation": "get_batch_sizes",
                    "user_list": user_list
                })

            return dict(response.get("data", {}))

        except json.JSONDecodeError as e:
            raise LuaScriptError("stats", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def get_health(self) -> Dict[str, Any]:
        """Get health check information.

        Returns:
            Dictionary with health status and metrics

        Raises:
            LuaScriptError: If Lua script execution fails
        """
        result = "N/A"  # Default value for result in case of failure
        try:
            result = self.lua_manager.execute_script("stats", args=["get_health"])
            response = json.loads(result)

            if not response.get("success", False):
                error_code = response.get("error_code", "UNKNOWN")
                error_message = response.get("message", "Unknown error")
                raise LuaScriptError("stats", {
                    "error_code": error_code,
                    "message": error_message,
                    "operation": "get_health"
                })

            return dict(response.get("data", {}))

        except json.JSONDecodeError as e:
            raise LuaScriptError("stats", {
                "error": "json_decode_error",
                "details": str(e),
                "raw_result": str(result)
            }) from e

    def push_batch(self, tasks: List[Task]) -> List[Dict[str, Any]]:
        """Push multiple tasks efficiently.

        Args:
            tasks: List of tasks to push

        Returns:
            List of operation results

        Raises:
            TaskValidationError: If any task validation fails
        """
        if not tasks:
            return []

        results = []

        # Use pipeline for efficiency if enabled
        if self.config.queue.enable_pipeline_optimization and len(tasks) > 1:
            # TODO: Implement pipeline-based batch push
            # For now, fall back to individual pushes
            pass

        # Individual push operations
        for task in tasks:
            try:
                result = self.push(task)
                results.append(result)
            except Exception as e:
                # Include error in results for batch processing
                results.append({
                    "success": False,
                    "error": str(e),
                    "task_id": getattr(task, "task_id", "unknown")
                })

        return results

    def delete_task(self, task_id: str) -> bool:
        """Delete a task and its data.

        Args:
            task_id: Task identifier

        Returns:
            True if task was deleted, False if not found
        """
        try:
            # Delete task hash
            deleted = self.redis.delete(f"task:{task_id}")
            return bool(deleted)
        except redis.RedisError as e:
            logger.error(f"Failed to delete task {task_id}: {e}")
            return False

    def cleanup_expired_tasks(self, max_age_seconds: int = 86400) -> int:
        """Clean up old completed tasks (maintenance operation).

        Args:
            max_age_seconds: Maximum age for task data retention

        Returns:
            Number of tasks cleaned up
        """
        # This is a simple cleanup - in production you might want more sophisticated logic
        current_time = time.time()
        cutoff_time = current_time - max_age_seconds

        # Scan for old task keys (this is a simplified implementation)
        # In production, you might want to use Redis streams or separate cleanup tracking

        cleanup_count = 0
        try:
            for key in self.redis.scan_iter(match="task:*"):
                # This is inefficient for large datasets - consider batch operations
                created_at = self.redis.hget(key, "created_at")
                if created_at is None:
                    logger.warning(f"Task {key} has no created_at field, skipping cleanup")
                    continue

                # Ensure created_at is a string and convert to float
                created_at_fp = float(str(created_at))
                if created_at_fp < cutoff_time:
                    self.redis.delete(key)
                    cleanup_count += 1

        except redis.RedisError as e:
            logger.error(f"Error during cleanup: {e}")

        if cleanup_count > 0:
            logger.info(f"Cleaned up {cleanup_count} expired tasks")

        return cleanup_count

    def close(self) -> None:
        """Close Redis connection and cleanup resources."""
        try:
            if hasattr(self.redis, "close"):
                self.redis.close()
            logger.info("FairQueue connection closed")
        except Exception as e:
            logger.warning(f"Error closing FairQueue: {e}")

    def __enter__(self) -> "TaskQueue":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
