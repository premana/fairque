"""Lua script management utilities for FairQueue."""

import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import redis
from redis.commands.core import Script

from fairque.core.exceptions import LuaScriptError

logger = logging.getLogger(__name__)


class LuaScriptManager:
    """Manages Lua script loading, caching, and execution for FairQueue operations."""

    def __init__(self, redis_client: redis.Redis, script_dir: Optional[Path] = None) -> None:
        """Initialize Lua script manager.

        Args:
            redis_client: Redis client instance
            script_dir: Directory containing Lua scripts (defaults to package scripts/)
        """
        self.redis = redis_client
        self.script_dir = script_dir or Path(__file__).parent.parent / "scripts"
        self.loaded_scripts: Dict[str, Script] = {}
        self.script_hashes: Dict[str, str] = {}

    def load_script(self, script_name: str) -> Script:
        """Load and register a Lua script.

        Args:
            script_name: Name of the script file (without .lua extension)

        Returns:
            Loaded Redis script object

        Raises:
            LuaScriptError: If script loading fails
        """
        if script_name in self.loaded_scripts:
            return self.loaded_scripts[script_name]

        script_path = self.script_dir / f"{script_name}.lua"

        if not script_path.exists():
            raise LuaScriptError(
                script_name,
                {"error": "script_not_found", "path": str(script_path)}
            )

        try:
            with open(script_path, "r", encoding="utf-8") as f:
                script_content = f.read()

            # Calculate script hash for debugging
            script_hash = hashlib.sha1(script_content.encode()).hexdigest()
            self.script_hashes[script_name] = script_hash

            # Register script with Redis
            script = self.redis.register_script(script_content)
            self.loaded_scripts[script_name] = script

            logger.debug(f"Loaded Lua script '{script_name}' with hash {script_hash}")
            return script

        except OSError as e:
            raise LuaScriptError(
                script_name,
                {"error": "file_read_error", "details": str(e)}
            ) from e
        except redis.RedisError as e:
            raise LuaScriptError(
                script_name,
                {"error": "redis_registration_error", "details": str(e)}
            ) from e

    def execute_script(
        self,
        script_name: str,
        keys: Optional[List[str]] = None,
        args: Optional[List[str]] = None
    ) -> Any:
        """Execute a loaded Lua script.

        Args:
            script_name: Name of the script to execute
            keys: Redis keys for the script (KEYS array)
            args: Arguments for the script (ARGV array)

        Returns:
            Script execution result

        Raises:
            LuaScriptError: If script execution fails
        """
        keys = keys or []
        args = args or []

        try:
            script = self.load_script(script_name)
            result = script(keys=keys, args=args)

            logger.debug(
                f"Executed script '{script_name}' with {len(keys)} keys and {len(args)} args"
            )
            return result

        except redis.ResponseError as e:
            # Parse Redis Lua error for better error reporting
            error_msg = str(e)
            if "ERR Error running script" in error_msg:
                # Extract actual Lua error from Redis error message
                if ":" in error_msg:
                    lua_error = error_msg.split(":", 1)[1].strip()
                else:
                    lua_error = error_msg
            else:
                lua_error = error_msg

            raise LuaScriptError(
                script_name,
                {
                    "error": "execution_error",
                    "lua_error": lua_error,
                    "keys": keys,
                    "args": args,
                    "script_hash": self.script_hashes.get(script_name)
                }
            ) from e
        except redis.RedisError as e:
            raise LuaScriptError(
                script_name,
                {
                    "error": "redis_error",
                    "details": str(e),
                    "keys": keys,
                    "args": args
                }
            ) from e

    def reload_script(self, script_name: str) -> Script:
        """Reload a script (useful for development/debugging).

        Args:
            script_name: Name of the script to reload

        Returns:
            Reloaded Redis script object
        """
        if script_name in self.loaded_scripts:
            del self.loaded_scripts[script_name]
        if script_name in self.script_hashes:
            del self.script_hashes[script_name]

        logger.info(f"Reloading Lua script '{script_name}'")
        return self.load_script(script_name)

    def load_all_scripts(self) -> None:
        """Load all available Lua scripts in the scripts directory."""
        if not self.script_dir.exists():
            logger.warning(f"Scripts directory not found: {self.script_dir}")
            return

        script_files = list(self.script_dir.glob("*.lua"))
        logger.info(f"Loading {len(script_files)} Lua scripts from {self.script_dir}")

        for script_path in script_files:
            script_name = script_path.stem
            try:
                self.load_script(script_name)
            except LuaScriptError as e:
                logger.error(f"Failed to load script '{script_name}': {e}")
                raise

    def get_loaded_scripts(self) -> List[str]:
        """Get list of currently loaded script names.

        Returns:
            List of loaded script names
        """
        return list(self.loaded_scripts.keys())

    def get_script_info(self, script_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a loaded script.

        Args:
            script_name: Name of the script

        Returns:
            Script information dictionary or None if not loaded
        """
        if script_name not in self.loaded_scripts:
            return None

        script_path = self.script_dir / f"{script_name}.lua"

        return {
            "name": script_name,
            "path": str(script_path),
            "hash": self.script_hashes.get(script_name),
            "exists": script_path.exists(),
            "loaded": True
        }

    def clear_cache(self) -> None:
        """Clear all loaded scripts from cache."""
        logger.info("Clearing Lua script cache")
        self.loaded_scripts.clear()
        self.script_hashes.clear()
