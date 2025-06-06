"""Centralized Redis key naming with fq: prefix for unified namespace management."""


class RedisKeys:
    """Centralized Redis key naming with fq: prefix for namespace consistency.

    This class provides a unified interface for all Redis key generation,
    ensuring consistent naming across the entire FairQueue system.
    """

    # Base prefix for all FairQueue Redis keys
    PREFIX = "fq:"

    # Component prefixes
    QUEUE_PREFIX = f"{PREFIX}queue:user:"
    STATE_PREFIX = f"{PREFIX}state:"
    TASK_PREFIX = f"{PREFIX}task:"
    DEPS_PREFIX = f"{PREFIX}deps:"
    XCOM_PREFIX = f"{PREFIX}xcom:"

    # Singleton keys
    STATS = f"{PREFIX}stats"
    SCHEDULES = f"{PREFIX}schedules"
    SCHEDULER_LOCK = f"{PREFIX}scheduler:lock"

    @staticmethod
    def user_critical_queue(user_id: str) -> str:
        """Get Redis key for user's critical priority queue.

        Args:
            user_id: User identifier

        Returns:
            Redis key for critical priority queue
        """
        return f"{RedisKeys.QUEUE_PREFIX}{user_id}:critical"

    @staticmethod
    def user_normal_queue(user_id: str) -> str:
        """Get Redis key for user's normal priority queue.

        Args:
            user_id: User identifier

        Returns:
            Redis key for normal priority queue
        """
        return f"{RedisKeys.QUEUE_PREFIX}{user_id}:normal"

    @staticmethod
    def state_registry(state: str) -> str:
        """Get Redis key for state registry.

        Args:
            state: Task state (queued, started, finished, failed, canceled, scheduled)

        Returns:
            Redis key for state registry
        """
        return f"{RedisKeys.STATE_PREFIX}{state}"

    @staticmethod
    def task_data(task_id: str) -> str:
        """Get Redis key for task data storage.

        Args:
            task_id: Task identifier

        Returns:
            Redis key for task data hash
        """
        return f"{RedisKeys.TASK_PREFIX}{task_id}"

    @staticmethod
    def deps_waiting(task_id: str) -> str:
        """Get Redis key for tasks waiting on this task's completion.

        Args:
            task_id: Task identifier

        Returns:
            Redis key for dependency waiting set
        """
        return f"{RedisKeys.DEPS_PREFIX}waiting:{task_id}"

    @staticmethod
    def deps_blocked(task_id: str) -> str:
        """Get Redis key for tasks this task is blocked by.

        Args:
            task_id: Task identifier

        Returns:
            Redis key for dependency blocking set
        """
        return f"{RedisKeys.DEPS_PREFIX}blocked:{task_id}"

    @staticmethod
    def xcom_key(key: str) -> str:
        """Get Redis key for XCom data storage.

        Args:
            key: XCom key name

        Returns:
            Redis key for XCom data
        """
        return f"{RedisKeys.XCOM_PREFIX}{key}"

    @staticmethod
    def xcom_data(user_id: str, namespace: str, key: str) -> str:
        """Get Redis key for XCom data storage with namespace.

        Args:
            user_id: User identifier
            namespace: XCom namespace
            key: XCom key name

        Returns:
            Redis key for namespaced XCom data
        """
        return f"{RedisKeys.XCOM_PREFIX}{user_id}:{namespace}:{key}"

    @staticmethod
    def xcom_namespace_keys(user_id: str, namespace: str) -> str:
        """Get Redis key for XCom namespace tracking.

        Args:
            user_id: User identifier
            namespace: XCom namespace

        Returns:
            Redis key for namespace tracking set
        """
        return f"{RedisKeys.XCOM_PREFIX}namespace_keys:{user_id}:{namespace}"

    @classmethod
    def get_all_patterns(cls) -> list[str]:
        """Get all Redis key patterns for cleanup and monitoring.

        Returns:
            List of Redis key patterns
        """
        return [
            f"{cls.QUEUE_PREFIX}*",
            f"{cls.STATE_PREFIX}*",
            f"{cls.TASK_PREFIX}*",
            f"{cls.DEPS_PREFIX}*",
            f"{cls.XCOM_PREFIX}*",
            cls.STATS,
            cls.SCHEDULES,
            cls.SCHEDULER_LOCK,
        ]

    @classmethod
    def cleanup_pattern(cls) -> str:
        """Get Redis pattern for bulk cleanup operations.

        Returns:
            Redis pattern matching all FairQueue keys
        """
        return f"{cls.PREFIX}*"
