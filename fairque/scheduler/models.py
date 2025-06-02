"""Models for task scheduling functionality."""

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from fairque.core.models import Priority, Task


@dataclass
class ScheduledTask:
    """Scheduled task with cron expression.

    Attributes:
        schedule_id: Unique identifier for the scheduled task
        cron_expression: Cron expression for scheduling (e.g., "0 9 * * *")
        user_id: Target user ID for the task
        priority: Task priority level
        payload: Task payload data
        timezone: Timezone for cron expression (default: UTC)
        is_active: Whether the schedule is active
        last_run: Timestamp of last execution
        next_run: Timestamp of next scheduled execution
        created_at: Timestamp when schedule was created
        updated_at: Timestamp when schedule was last updated
        metadata: Additional metadata for the schedule
    """

    schedule_id: str
    cron_expression: str
    user_id: str
    priority: Priority
    payload: Dict[str, Any]
    timezone: str = "UTC"
    is_active: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        cron_expr: str,
        user_id: str,
        priority: Priority,
        payload: Dict[str, Any],
        timezone: str = "UTC",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ScheduledTask":
        """Create a new scheduled task.

        Args:
            cron_expr: Cron expression for scheduling
            user_id: User ID for the task
            priority: Task priority
            payload: Task payload
            timezone: Timezone for cron expression
            metadata: Additional metadata

        Returns:
            New ScheduledTask instance
        """
        scheduled_task = cls(
            schedule_id=str(uuid.uuid4()),
            cron_expression=cron_expr,
            user_id=user_id,
            priority=priority,
            payload=payload,
            timezone=timezone,
            is_active=True,
            last_run=None,
            next_run=None,
            metadata=metadata or {},
        )

        # Calculate initial next run time
        scheduled_task.next_run = scheduled_task.calculate_next_run()

        return scheduled_task

    def calculate_next_run(self, from_time: Optional[float] = None) -> float:
        """Calculate next run time using croniter.

        Args:
            from_time: Base time for calculation (defaults to current time or last_run)

        Returns:
            Timestamp of next scheduled execution

        Raises:
            ImportError: If croniter is not installed
            ValueError: If cron expression is invalid
        """
        try:
            import pytz
            from croniter import croniter
        except ImportError as e:
            raise ImportError(
                "croniter and pytz are required for scheduling. "
                "Install with: pip install fairque[scheduler]"
            ) from e

        # Get timezone
        try:
            tz = pytz.timezone(self.timezone)
        except pytz.exceptions.UnknownTimeZoneError as e:
            raise ValueError(f"Unknown timezone: {self.timezone}") from e

        # Determine base time
        base_time = from_time or self.last_run or time.time()
        base_dt = datetime.fromtimestamp(base_time, tz)

        # Calculate next run
        try:
            cron = croniter(self.cron_expression, base_dt)
            next_dt = cron.get_next(datetime)
            return next_dt.timestamp()
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid cron expression '{self.cron_expression}': {e}")

    def create_task(self) -> Task:
        """Convert scheduled task to regular Task for queue processing.

        Returns:
            Task instance ready for queue processing
        """
        return Task.create(
            user_id=self.user_id,
            priority=self.priority,
            payload={
                **self.payload,
                "__scheduled__": True,
                "__schedule_id__": self.schedule_id,
            },
        )

    def update_after_run(self, run_time: float) -> None:
        """Update schedule after successful execution.

        Args:
            run_time: Timestamp when the task was executed
        """
        self.last_run = run_time
        self.next_run = self.calculate_next_run(from_time=run_time)
        self.updated_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage.

        Returns:
            Dictionary representation of the scheduled task
        """
        return {
            "schedule_id": self.schedule_id,
            "cron_expression": self.cron_expression,
            "user_id": self.user_id,
            "priority": self.priority.value,
            "payload": self.payload,
            "timezone": self.timezone,
            "is_active": self.is_active,
            "last_run": self.last_run,
            "next_run": self.next_run,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScheduledTask":
        """Create ScheduledTask from dictionary.

        Args:
            data: Dictionary representation

        Returns:
            ScheduledTask instance

        Raises:
            KeyError: If required fields are missing
            ValueError: If data is invalid
        """
        try:
            return cls(
                schedule_id=data["schedule_id"],
                cron_expression=data["cron_expression"],
                user_id=data["user_id"],
                priority=Priority(data["priority"]),
                payload=data["payload"],
                timezone=data.get("timezone", "UTC"),
                is_active=data.get("is_active", True),
                last_run=data.get("last_run"),
                next_run=data.get("next_run"),
                created_at=data.get("created_at", time.time()),
                updated_at=data.get("updated_at", time.time()),
                metadata=data.get("metadata", {}),
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid scheduled task data: {e}") from e

    def to_json(self) -> str:
        """Convert to JSON string for Redis storage.

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_json(cls, json_str: str) -> "ScheduledTask":
        """Create ScheduledTask from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            ScheduledTask instance

        Raises:
            json.JSONDecodeError: If JSON is invalid
            ValueError: If data is invalid
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
