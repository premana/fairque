"""Task decorator for converting functions into FairQueue tasks."""

import functools
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

from fairque.core.function_registry import FunctionRegistry
from fairque.core.models import Priority, Task

F = TypeVar('F', bound=Callable[..., Any])


def task(
    priority: Priority = Priority.NORMAL,
    max_retries: int = 3,
    user_id: Optional[str] = None,
    execute_after: Optional[float] = None,
    payload: Optional[Dict[str, Any]] = None
) -> Callable[[F], Callable[..., Task]]:
    """Decorator to convert a function into a FairQueue task factory.

    Args:
        priority: Task priority (default: NORMAL)
        max_retries: Maximum retry attempts (default: 3)
        user_id: User identifier (default: from environment USER)
        execute_after: Timestamp when task should be executed (default: now)
        payload: Additional task payload data (default: empty dict)

    Returns:
        Decorator function that converts a function into a task factory

    Example:
        @fairque.task(max_retries=2)
        def fib(n):
            if n <= 1:
                return 1
            else:
                return fib(n - 1) + fib(n - 2)

        # Usage:
        task_instance = fib(10)  # Returns Task instance
        result = task_instance()  # Execute function
        queue.push(task_instance)  # Push to queue
    """
    def decorator(func: F) -> Callable[..., Task]:
        # Auto-register function for fallback resolution
        registry_name = f"{func.__module__}.{func.__name__}"
        FunctionRegistry.register(registry_name, func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Task:
            # Create task with function reference
            effective_user_id = user_id or os.environ.get("USER", "unknown")
            effective_execute_after = execute_after or time.time()
            effective_payload = payload.copy() if payload else {}
            effective_payload.update({
                "function_name": func.__name__,
                "module": func.__module__,
            })

            return Task.create(
                user_id=effective_user_id,
                priority=priority,
                payload=effective_payload,
                max_retries=max_retries,
                execute_after=effective_execute_after,
                func=func,
                args=args,
                kwargs=kwargs
            )

        # Add metadata to the wrapper
        wrapper.__fairque_task__ = True
        wrapper.__fairque_priority__ = priority
        wrapper.__fairque_max_retries__ = max_retries
        wrapper.__original_function__ = func

        return wrapper

    return decorator


def is_fairque_task(func: Callable) -> bool:
    """Check if a function is decorated with @fairque.task.

    Args:
        func: Function to check

    Returns:
        True if function is a FairQueue task, False otherwise
    """
    return hasattr(func, '__fairque_task__') and func.__fairque_task__


def get_task_metadata(func: Callable) -> Dict[str, Any]:
    """Get metadata from a FairQueue task function.

    Args:
        func: Function decorated with @fairque.task

    Returns:
        Dictionary with task metadata

    Raises:
        ValueError: If function is not a FairQueue task
    """
    if not is_fairque_task(func):
        raise ValueError(f"Function {func.__name__} is not a FairQueue task")

    return {
        "priority": getattr(func, '__fairque_priority__', Priority.NORMAL),
        "max_retries": getattr(func, '__fairque_max_retries__', 3),
        "original_function": getattr(func, '__original_function__', None),
        "function_name": func.__name__,
        "module": func.__module__,
    }


class TaskFactory:
    """Factory class for creating tasks from functions dynamically."""

    @staticmethod
    def create_task(
        func: Callable,
        args: Tuple[Any, ...] = (),
        kwargs: Dict[str, Any] = {},  # noqa: B006
        priority: Priority = Priority.NORMAL,
        max_retries: int = 3,
        user_id: Optional[str] = None,
        execute_after: Optional[float] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> Task:
        """Create a task from any function without using decorator.

        Args:
            func: Function to convert to task
            args: Function arguments
            kwargs: Function keyword arguments
            priority: Task priority
            max_retries: Maximum retry attempts
            user_id: User identifier
            execute_after: Timestamp when task should be executed
            payload: Additional task payload data

        Returns:
            Task instance with function and arguments
        """
        if kwargs is None:
            kwargs = {}

        # Determine user_id
        effective_user_id = user_id
        if effective_user_id is None:
            effective_user_id = os.environ.get("USER", "unknown")

        # Determine execute_after
        effective_execute_after = execute_after
        if effective_execute_after is None:
            effective_execute_after = time.time()

        # Create payload
        effective_payload = payload.copy() if payload else {}
        effective_payload.update({
            "function_name": func.__name__,
            "module": func.__module__,
        })

        # Create task
        return Task.create(
            user_id=effective_user_id,
            priority=priority,
            payload=effective_payload,
            max_retries=max_retries,
            execute_after=effective_execute_after,
            func=func,
            args=args,
            kwargs=kwargs
        )


# Convenience aliases
fairque_task = task
create_task = TaskFactory.create_task
