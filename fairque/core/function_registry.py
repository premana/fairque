"""Function registry and resolution with fallback strategy."""

import importlib
import logging
from typing import Any, Callable, Dict, Optional, Tuple

from fairque.core.exceptions import FunctionResolutionError

logger = logging.getLogger(__name__)


class FunctionRegistry:
    """Global function registry for fallback resolution."""

    _registry: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[..., Any]) -> None:
        """Register function in global registry."""
        cls._registry[name] = func
        logger.debug(f"Registered function: {name}")

    @classmethod
    def get(cls, name: str) -> Optional[Callable[..., Any]]:
        """Get function from registry."""
        return cls._registry.get(name)

    @classmethod
    def clear(cls) -> None:
        """Clear registry (mainly for testing)."""
        cls._registry.clear()


def serialize_function(func: Callable[..., Any]) -> Dict[str, str]:
    """Serialize function to string reference."""
    return {
        "module": func.__module__,
        "name": func.__name__,
        "qualname": getattr(func, "__qualname__", func.__name__)
    }


def deserialize_function(func_data: Dict[str, str]) -> Tuple[Callable[..., Any], str]:
    """
    Deserialize function with fallback strategy.

    Strategy: auto (import first, then registry)
    1. Try import from module
    2. Try registry lookup
    3. Raise FunctionResolutionError if both fail

    Returns:
        (function_object, resolution_method)
        resolution_method: "import" or "registry"

    Raises:
        FunctionResolutionError: When function cannot be resolved
    """
    func_ref = f"{func_data.get('module', '')}.{func_data.get('name', '')}"

    # Step 1: Try import
    try:
        module = importlib.import_module(func_data["module"])
        func = getattr(module, func_data["name"])
        logger.debug(f"Function imported: {func_ref}")
        return func, "import"
    except (ImportError, AttributeError) as e:
        logger.debug(f"Import failed for {func_ref}: {e}")

    # Step 2: Try registry
    func = FunctionRegistry.get(func_ref)
    if func is not None:
        logger.debug(f"Function found in registry: {func_ref}")
        return func, "registry"

    # Step 3: Failed - raise exception
    logger.warning(f"Function resolution failed: {func_ref}")
    raise FunctionResolutionError(f"Cannot resolve function: {func_ref}")


def try_deserialize_function(func_data: Dict[str, str]) -> Tuple[Optional[Callable[..., Any]], str]:
    """
    Safe wrapper that returns None instead of raising exception.
    Used when we want to handle failures gracefully.

    Returns:
        (function_object_or_none, resolution_method)
        resolution_method: "import", "registry", or "failed"
    """
    try:
        func, strategy = deserialize_function(func_data)
        return func, strategy
    except FunctionResolutionError:
        return None, "failed"
