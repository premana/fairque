"""Exception classes for FairQueue."""

from typing import Any, Dict


class FairQueueError(Exception):
    """Base FairQueue exception class."""

    pass


class LuaScriptError(FairQueueError):
    """Lua script execution error."""

    def __init__(self, script_name: str, error_details: Dict[str, Any]) -> None:
        """Initialize Lua script error.

        Args:
            script_name: Name of the failed Lua script
            error_details: Dictionary containing error details
        """
        self.script_name = script_name
        self.error_details = error_details
        super().__init__(f"Lua script '{script_name}' failed: {error_details}")


class TaskValidationError(FairQueueError):
    """Task validation error."""

    pass


class RedisConnectionError(FairQueueError):
    """Redis connection error."""

    pass


class TaskSerializationError(FairQueueError):
    """Task serialization/deserialization error."""

    pass


class ConfigurationError(FairQueueError):
    """Configuration validation error."""

    pass
