"""Task decorator for converting functions into FairQueue tasks with XCom support."""

import functools
import logging
import os
import time
from typing import Any, Callable, Dict, Optional, TypeVar

from fairque.core.function_registry import FunctionRegistry
from fairque.core.models import Priority, Task

F = TypeVar('F', bound=Callable[..., Any])

logger = logging.getLogger(__name__)


def xcom_pull(
    *,
    keys: Optional[Dict[str, str]] = None,
    defaults: Optional[Dict[str, Any]] = None,
    namespace: Optional[str] = None
) -> Callable[[F], F]:
    """Decorator to automatically inject XCom values by key only.

    Args:
        keys: Mapping of function parameter names to XCom keys
              e.g., {"input_data": "processed_data", "config": "task_config"}
        defaults: Default values for XCom keys if not found
                 e.g., {"config": {"timeout": 30}}
        namespace: Specific namespace to pull from

    Usage:
        @xcom_pull(
            keys={"input_data": "processed_data", "config": "task_config"},
            defaults={"config": {"timeout": 30}},
            namespace="processing"
        )
        def my_function(input_data, config, other_param):
            # input_data will be injected from XCom key "processed_data"
            # config will be injected from XCom key "task_config" with default
            # other_param remains as normal parameter
            pass
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Check if this is being called from a Task with XCom manager
            task = None
            if args and hasattr(args[0], '_xcom_manager') and hasattr(args[0], 'task_id'):
                task = args[0]  # First argument is the Task instance

            if task and task._xcom_manager and keys:
                # Inject XCom values into kwargs
                for param_name, xcom_key in keys.items():
                    if param_name not in kwargs:  # Don't override existing kwargs
                        try:
                            # Use specified namespace or task's default namespace
                            xcom_value = task.xcom_pull(xcom_key, namespace=namespace)
                            kwargs[param_name] = xcom_value
                            ns_info = f"namespace={namespace}" if namespace else f"namespace={task.xcom_namespace}"
                            logger.debug(f"Injected XCom: {param_name}={xcom_value} from {xcom_key}, {ns_info}")
                        except KeyError:
                            if defaults and param_name in defaults:
                                kwargs[param_name] = defaults[param_name]
                                logger.debug(f"Used default value for {param_name}: {defaults[param_name]}")
                            else:
                                logger.warning(f"XCom key not found: {xcom_key}")

            return func(*args, **kwargs)

        # Store metadata on the function
        wrapper._xcom_pull_config = {  # type: ignore
            'keys': keys or {},
            'defaults': defaults or {},
            'namespace': namespace
        }

        return wrapper  # type: ignore
    return decorator


def xcom_push(
    key: str,
    ttl_seconds: int = 3600,
    namespace: Optional[str] = None,
    condition: Optional[Callable[[Any], bool]] = None
) -> Callable[[F], F]:
    """Decorator to automatically save function return value to XCom.

    Args:
        key: XCom key name to store the return value
        ttl_seconds: TTL in seconds (default=3600, 0=no TTL, >0=specific TTL)
        namespace: Custom namespace (if None, use task's namespace)
        condition: Optional condition function to determine if value should be stored

    Usage:
        @xcom_push("calculation_result", ttl_seconds=3600)
        def calculate_data():
            return 42  # Will be stored in XCom with key "calculation_result"

        @xcom_push("positive_result", condition=lambda x: x > 0)
        def maybe_positive():
            return -1  # Won't be stored because condition fails
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)

            # Check if this is being called from a Task with XCom manager
            task = None
            if args and hasattr(args[0], '_xcom_manager') and hasattr(args[0], 'task_id'):
                task = args[0]  # First argument is the Task instance

            if task and task._xcom_manager:
                # Check condition if provided
                should_store = True
                if condition:
                    try:
                        should_store = condition(result)
                    except Exception as e:
                        logger.warning(f"XCom push condition failed: {e}")
                        should_store = False

                if should_store:
                    try:
                        task.xcom_push(key, result, ttl_seconds=ttl_seconds, namespace=namespace)
                        ns_info = f"namespace={namespace}" if namespace else f"namespace={task.xcom_namespace}"
                        ttl_info = "no TTL" if ttl_seconds == 0 else f"TTL {ttl_seconds}s"
                        logger.debug(f"Auto-pushed to XCom: key={key}, {ns_info}, {ttl_info}")
                    except Exception as e:
                        logger.error(f"Failed to auto-push to XCom: {e}")

            return result

        # Store metadata on the function
        wrapper._xcom_push_config = {  # type: ignore
            'key': key,
            'ttl_seconds': ttl_seconds,
            'namespace': namespace,
            'condition': condition
        }

        return wrapper  # type: ignore
    return decorator


def xcom_task(
    push_key: Optional[str] = None,
    pull_keys: Optional[Dict[str, str]] = None,
    pull_defaults: Optional[Dict[str, Any]] = None,
    ttl_seconds: int = 3600,
    namespace: Optional[str] = None
) -> Callable[[F], F]:
    """Combined decorator for XCom pull and push functionality.

    Args:
        push_key: XCom key to store return value
        pull_keys: Mapping of function parameters to XCom keys for injection
        pull_defaults: Default values for XCom keys if not found
        ttl_seconds: TTL in seconds (default=3600, 0=no TTL, >0=specific TTL)
        namespace: Custom namespace for this task

    Usage:
        @xcom_task(
            push_key="final_result",
            pull_keys={"input_data": "raw_data", "config": "settings"},
            pull_defaults={"config": {"timeout": 30}},
            ttl_seconds=3600,
            namespace="my_workflow"
        )
        def process_data(input_data, config, multiplier=2):
            return input_data * multiplier * config.get("factor", 1)
    """
    def decorator(func: F) -> F:
        # Apply pull decorator if pull_keys specified
        if pull_keys:
            func = xcom_pull(
                keys=pull_keys,
                defaults=pull_defaults,
                namespace=namespace
            )(func)

        # Apply push decorator if push_key specified
        if push_key:
            func = xcom_push(push_key, ttl_seconds=ttl_seconds, namespace=namespace)(func)

        # Store combined metadata
        func._xcom_task_config = {  # type: ignore
            'push_key': push_key,
            'pull_keys': pull_keys or {},
            'pull_defaults': pull_defaults or {},
            'ttl_seconds': ttl_seconds,
            'namespace': namespace
        }

        return func
    return decorator


def task(
    priority: Priority = Priority.NORMAL,
    max_retries: int = 3,
    user_id: Optional[str] = None,
    execute_after: Optional[float] = None,
    payload: Optional[Dict[str, Any]] = None,
    # XCom parameters with default namespace
    enable_xcom: bool = False,
    push_key: Optional[str] = None,
    pull_keys: Optional[Dict[str, str]] = None,
    pull_defaults: Optional[Dict[str, Any]] = None,
    xcom_ttl_seconds: int = 3600,
    xcom_namespace: str = "default"
) -> Callable[[Callable[..., Any]], Callable[..., Task]]:
    """Enhanced task decorator with XCom support.

    Args:
        priority: Task priority (default: NORMAL)
        max_retries: Maximum retry attempts (default: 3)
        user_id: User identifier (default: from environment USER)
        execute_after: Timestamp when task should be executed (default: now)
        payload: Additional task payload data (default: empty dict)
        enable_xcom: Enable XCom functionality
        push_key: XCom key to store return value
        pull_keys: Mapping of function parameters to XCom keys
        pull_defaults: Default values for XCom keys if not found
        xcom_ttl_seconds: Default TTL for XCom data
        xcom_namespace: XCom namespace for data storage

    Returns:
        Decorator function that converts a function into a task factory

    Example:
        # Standard task without XCom (default)
        @fairque.task()
        def simple_task():
            return "processed"

        # XCom-enabled task (explicit)
        @fairque.task(
            enable_xcom=True,
            push_key="processed_data",
            pull_keys={"raw_data": "input_data"},
            xcom_ttl_seconds=3600,
            xcom_namespace="workflow_a"
        )
        def xcom_task(raw_data, multiplier=2):
            return raw_data * multiplier
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Task]:
        # Apply XCom decorators only if explicitly enabled
        if enable_xcom:
            if pull_keys:
                func = xcom_pull(
                    keys=pull_keys,
                    defaults=pull_defaults,
                    namespace=xcom_namespace
                )(func)

            if push_key:
                func = xcom_push(push_key, ttl_seconds=xcom_ttl_seconds, namespace=xcom_namespace)(func)

        # Auto-register function for fallback resolution
        registry_name = f"{func.__module__}.{func.__name__}"
        FunctionRegistry.register(registry_name, func)

        # Store task metadata
        func._fairque_task_config = {  # type: ignore
            'priority': priority,
            'max_retries': max_retries,
            'user_id': user_id,
            'execute_after': execute_after,
            'payload': payload,
            'xcom_enabled': enable_xcom,
            'xcom_ttl_seconds': xcom_ttl_seconds,
            'xcom_namespace': xcom_namespace
        }

        @functools.wraps(func)
        def task_factory(*args: Any, **kwargs: Any) -> Task:
            """Factory function that creates Task instances."""
            # Determine user_id
            effective_user_id = user_id or os.environ.get("USER", "unknown")

            # Determine execution time
            effective_execute_after = execute_after or time.time()

            # Create payload with any additional data
            effective_payload = payload.copy() if payload else {}

            # Create task with XCom configuration
            return Task.create(
                user_id=effective_user_id,
                priority=priority,
                payload=effective_payload,
                max_retries=max_retries,
                execute_after=effective_execute_after,
                func=func,
                args=args,
                kwargs=kwargs,
                enable_xcom=enable_xcom,
                xcom_namespace=xcom_namespace,
                xcom_ttl_seconds=xcom_ttl_seconds,
                auto_xcom=True  # Auto-configure from decorators
            )

        return task_factory
    return decorator
