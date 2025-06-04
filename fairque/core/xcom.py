"""XCom (Cross Communication) implementation for fairque.

Provides namespace-based data exchange between tasks using Redis storage.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, cast

import redis

logger = logging.getLogger(__name__)


@dataclass
class XComValue:
    """XCom value with namespace support."""
    key: str
    value: Any
    user_id: str
    namespace: str
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: int = 3600  # Default 1 hour

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "key": self.key,
            "value": self.value,
            "user_id": self.user_id,
            "namespace": self.namespace,
            "timestamp": self.timestamp,
            "ttl_seconds": self.ttl_seconds
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "XComValue":
        """Create XComValue from dictionary."""
        return cls(
            key=data["key"],
            value=data["value"],
            user_id=data["user_id"],
            namespace=data["namespace"],
            timestamp=data.get("timestamp", time.time()),
            ttl_seconds=data.get("ttl_seconds", 3600)
        )


class XComManager:
    """XCom manager with namespace-based storage (no task_id)."""

    def __init__(self, redis_client: redis.Redis, default_ttl: int = 3600):
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.default_namespace = "default"  # Default namespace

    def push(self, key: str, value: Any, user_id: str, namespace: str, ttl_seconds: int) -> None:
        """Store data in XCom using namespace.

        Args:
            key: XCom key name
            value: Value to store
            user_id: User ID
            namespace: Storage namespace (e.g., "default", "workflow_001")
            ttl_seconds: TTL in seconds (0=no TTL, >0=specific TTL)
        """
        redis_key = f"fairque:xcom:{user_id}:{namespace}:{key}"
        namespace_keys_key = f"fairque:xcom:namespace_keys:{user_id}:{namespace}"

        pipe = self.redis.pipeline()

        # Store XCom data
        xcom_value = XComValue(
            key=key,
            value=value,
            user_id=user_id,
            namespace=namespace,
            ttl_seconds=ttl_seconds
        )
        serialized = json.dumps(xcom_value.to_dict())

        if ttl_seconds > 0:
            # Set with TTL
            pipe.setex(redis_key, ttl_seconds, serialized)
            pipe.sadd(namespace_keys_key, key)
            pipe.expire(namespace_keys_key, ttl_seconds)
            logger.debug(f"XCom stored with TTL: {redis_key} (TTL: {ttl_seconds}s)")
        else:
            # Set without TTL (unlimited)
            pipe.set(redis_key, serialized)
            pipe.sadd(namespace_keys_key, key)
            logger.debug(f"XCom stored without TTL: {redis_key} (unlimited)")

        pipe.execute()

    def pull(self, key: str, user_id: str, namespace: Optional[str] = None) -> Any:
        """Retrieve data from XCom by namespace.

        Args:
            key: XCom key name
            user_id: User ID
            namespace: Specific namespace (if None, searches all namespaces for latest)

        Returns:
            Value from XCom

        Raises:
            KeyError: If key not found
        """
        if namespace:
            # Get from specific namespace
            redis_key = f"fairque:xcom:{user_id}:{namespace}:{key}"
            data = self.redis.get(redis_key)
            if not data:
                raise KeyError(f"XCom key not found or expired: {key} (namespace: {namespace})")
        else:
            # Search all namespaces for this key (get latest)
            pattern = f"fairque:xcom:{user_id}:*:{key}"
            keys = cast(List[Union[str, bytes]], self.redis.keys(pattern))
            if not keys:
                raise KeyError(f"XCom key not found: {key}")

            # Get most recent key (assuming namespace contains timestamp or is ordered)
            redis_key_raw = max(keys)
            redis_key = redis_key_raw.decode('utf-8') if isinstance(redis_key_raw, bytes) else redis_key_raw
            data = cast(Optional[Union[str, bytes]], self.redis.get(redis_key))
            if not data:
                raise KeyError(f"XCom key expired: {key}")

        xcom_data = json.loads(cast(Union[str, bytes], data))
        return xcom_data["value"]

    def pull_from_namespace(self, namespace: str, user_id: str) -> Dict[str, Any]:
        """Pull all keys from a specific namespace.

        Args:
            namespace: Target namespace
            user_id: User ID

        Returns:
            Dictionary mapping keys to values
        """
        pattern = f"fairque:xcom:{user_id}:{namespace}:*"
        keys = cast(List[Union[str, bytes]], self.redis.keys(pattern))

        if not keys:
            return {}

        pipe = self.redis.pipeline()
        for key in keys:
            pipe.get(key)

        values = cast(List[Optional[Union[str, bytes]]], pipe.execute())
        result = {}

        for i, key in enumerate(keys):
            if values[i]:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                xcom_key = key_str.split(':')[-1]  # Extract key part
                xcom_data = json.loads(cast(Union[str, bytes], values[i]))
                result[xcom_key] = xcom_data["value"]

        return result

    def cleanup_namespace(self, namespace: str, user_id: str, reason: str = "namespace_cleanup") -> int:
        """Clean up all XCom data in a specific namespace.

        Args:
            namespace: Target namespace
            user_id: User ID
            reason: Cleanup reason for logging

        Returns:
            Number of entries cleaned up
        """
        pattern = f"fairque:xcom:{user_id}:{namespace}:*"
        keys = cast(List[Union[str, bytes]], self.redis.keys(pattern))

        if not keys:
            return 0

        pipe = self.redis.pipeline()
        for key in keys:
            pipe.delete(key)

        # Also delete namespace tracking key
        namespace_keys_key = f"fairque:xcom:namespace_keys:{user_id}:{namespace}"
        pipe.delete(namespace_keys_key)

        pipe.execute()
        cleaned_count = len(keys)

        logger.info(f"Cleaned {cleaned_count} XCom entries from namespace '{namespace}' (reason: {reason})")
        return cleaned_count

    def list_namespaces(self, user_id: str) -> List[str]:
        """List all available namespaces for a user."""
        pattern = f"fairque:xcom:{user_id}:*"
        keys = cast(List[Union[str, bytes]], self.redis.keys(pattern))

        namespaces = set()
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
            parts = key_str.split(':')
            if len(parts) >= 4:
                namespace = parts[3]  # Extract namespace part
                namespaces.add(namespace)

        return sorted(namespaces)

    def list_keys_in_namespace(self, namespace: str, user_id: str) -> List[str]:
        """List all keys in a specific namespace."""
        pattern = f"fairque:xcom:{user_id}:{namespace}:*"
        keys = cast(List[Union[str, bytes]], self.redis.keys(pattern))

        xcom_keys = []
        for key in keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
            parts = key_str.split(':')
            if len(parts) >= 5:
                xcom_key = parts[4]  # Extract key part
                xcom_keys.append(xcom_key)

        return sorted(xcom_keys)
