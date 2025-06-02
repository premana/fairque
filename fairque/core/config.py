"""Configuration classes for FairQueue."""

from dataclasses import dataclass, field
from typing import List, Optional

import yaml
from redis import Redis

from fairque.core.exceptions import ConfigurationError


@dataclass
class RedisConfig:
    """Redis connection configuration."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    username: Optional[str] = None
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    health_check_interval: int = 30
    decode_responses: bool = True

    def validate(self) -> None:
        """Validate Redis configuration parameters.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.host:
            raise ConfigurationError("Redis host cannot be empty")

        if not (1 <= self.port <= 65535):
            raise ConfigurationError(f"Invalid Redis port: {self.port}. Must be 1-65535.")

        if not (0 <= self.db <= 15):
            raise ConfigurationError(f"Invalid Redis database: {self.db}. Must be 0-15.")

        if self.socket_timeout <= 0:
            raise ConfigurationError("socket_timeout must be positive")

        if self.socket_connect_timeout <= 0:
            raise ConfigurationError("socket_connect_timeout must be positive")

        if self.health_check_interval <= 0:
            raise ConfigurationError("health_check_interval must be positive")

    def create_redis_client(self) -> Redis:
        """Create Redis client from this configuration.

        Returns:
            Configured Redis client instance
        """
        self.validate()
        return Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            username=self.username,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
            health_check_interval=self.health_check_interval,
            decode_responses=self.decode_responses,
        )


@dataclass
class WorkerConfig:
    """Worker configuration with validation."""

    id: str                                    # Unique worker identifier
    assigned_users: List[str]                  # Users this worker is responsible for
    steal_targets: List[str]                   # Users this worker can steal from
    poll_interval_seconds: float = 1.0        # Polling interval
    task_timeout_seconds: float = 300.0       # Task execution timeout
    max_concurrent_tasks: int = 10             # Maximum concurrent tasks
    graceful_shutdown_timeout: float = 30.0   # Graceful shutdown timeout

    def __post_init__(self) -> None:
        """Validate configuration after initialization.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        self.validate()

    def validate(self) -> None:
        """Validate worker configuration parameters.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.id:
            raise ConfigurationError("Worker ID cannot be empty")

        if not self.assigned_users:
            raise ConfigurationError("Worker must have at least one assigned user")

        # Check for duplicates in assigned users
        if len(self.assigned_users) != len(set(self.assigned_users)):
            raise ConfigurationError("Assigned users cannot contain duplicates")

        # Check for duplicates in steal targets
        if len(self.steal_targets) != len(set(self.steal_targets)):
            raise ConfigurationError("Steal targets cannot contain duplicates")

        # Check for overlap between assigned and steal targets
        assigned_set = set(self.assigned_users)
        steal_set = set(self.steal_targets)
        overlap = assigned_set & steal_set
        if overlap:
            raise ConfigurationError(
                f"Assigned users and steal targets cannot overlap: {overlap}"
            )

        if self.poll_interval_seconds <= 0:
            raise ConfigurationError("poll_interval_seconds must be positive")

        if self.task_timeout_seconds <= 0:
            raise ConfigurationError("task_timeout_seconds must be positive")

        if self.max_concurrent_tasks <= 0:
            raise ConfigurationError("max_concurrent_tasks must be positive")

        if self.graceful_shutdown_timeout <= 0:
            raise ConfigurationError("graceful_shutdown_timeout must be positive")

    def get_all_users(self) -> List[str]:
        """Get all users this worker can process (assigned + steal targets).

        Returns:
            List of all user IDs this worker can process
        """
        return list(set(self.assigned_users + self.steal_targets))


@dataclass
class QueueConfig:
    """Queue configuration with performance settings."""

    stats_prefix: str = "fq"
    lua_script_cache_size: int = 100
    max_retry_attempts: int = 3
    default_task_timeout: float = 300.0
    default_max_retries: int = 3

    # Pipeline settings
    enable_pipeline_optimization: bool = True
    pipeline_batch_size: int = 100
    pipeline_timeout: float = 5.0

    # Performance settings
    queue_cleanup_interval: int = 3600  # seconds
    stats_aggregation_interval: int = 300  # seconds

    def validate(self) -> None:
        """Validate configuration parameters.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.stats_prefix:
            raise ConfigurationError("stats_prefix cannot be empty")

        if self.lua_script_cache_size <= 0:
            raise ConfigurationError("lua_script_cache_size must be positive")

        if self.max_retry_attempts < 0:
            raise ConfigurationError("max_retry_attempts cannot be negative")

        if self.default_task_timeout <= 0:
            raise ConfigurationError("default_task_timeout must be positive")

        if self.default_max_retries < 0:
            raise ConfigurationError("default_max_retries cannot be negative")

        if self.pipeline_batch_size <= 0:
            raise ConfigurationError("pipeline_batch_size must be positive")

        if self.pipeline_timeout <= 0:
            raise ConfigurationError("pipeline_timeout must be positive")

        if self.queue_cleanup_interval <= 0:
            raise ConfigurationError("queue_cleanup_interval must be positive")

        if self.stats_aggregation_interval <= 0:
            raise ConfigurationError("stats_aggregation_interval must be positive")

    def get_stats_key(self, key_suffix: str) -> str:
        """Get prefixed statistics key.

        Args:
            key_suffix: Suffix to append to stats prefix

        Returns:
            Full statistics key with prefix
        """
        return f"{self.stats_prefix}:{key_suffix}"


@dataclass
class FairQueueConfig:
    """Unified configuration class."""

    redis: RedisConfig
    worker: WorkerConfig
    queue: QueueConfig = field(default_factory=QueueConfig)

    @classmethod
    def from_yaml(cls, path: str) -> "FairQueueConfig":
        """Load unified configuration from single YAML file.

        Args:
            path: Path to YAML configuration file

        Returns:
            FairQueueConfig instance loaded from YAML

        Raises:
            ConfigurationError: If YAML loading or parsing fails
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except (OSError, yaml.YAMLError) as e:
            raise ConfigurationError(f"Failed to load configuration from {path}: {e}") from e

        if not isinstance(data, dict):
            raise ConfigurationError("Configuration file must contain a YAML dictionary")

        try:
            return cls(
                redis=RedisConfig(**data.get("redis", {})),
                worker=WorkerConfig(**data.get("worker", {})),
                queue=QueueConfig(**data.get("queue", {})),
            )
        except TypeError as e:
            raise ConfigurationError(f"Invalid configuration structure: {e}") from e

    def to_yaml(self, path: str) -> None:
        """Save unified configuration to YAML file.

        Args:
            path: Path where to save YAML configuration file

        Raises:
            ConfigurationError: If YAML saving fails
        """
        data = {
            "redis": self.redis.__dict__,
            "worker": self.worker.__dict__,
            "queue": self.queue.__dict__,
        }

        try:
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=True)
        except OSError as e:
            raise ConfigurationError(f"Failed to save configuration to {path}: {e}") from e

    def create_redis_client(self) -> Redis:
        """Create Redis client from configuration.

        Returns:
            Configured Redis client instance
        """
        return self.redis.create_redis_client()

    def validate_all(self) -> None:
        """Validate all configuration sections.

        Raises:
            ConfigurationError: If any configuration section is invalid
        """
        self.redis.validate()
        self.worker.validate()
        self.queue.validate()

        # Cross-section validation
        if self.worker.task_timeout_seconds > self.queue.default_task_timeout:
            raise ConfigurationError(
                "Worker task_timeout_seconds should not exceed queue default_task_timeout"
            )

    @classmethod
    def create_default(
        cls,
        worker_id: str,
        assigned_users: List[str],
        steal_targets: Optional[List[str]] = None,
    ) -> "FairQueueConfig":
        """Create default configuration with minimal required parameters.

        Args:
            worker_id: Unique worker identifier
            assigned_users: List of users this worker is responsible for
            steal_targets: List of users this worker can steal from (optional)

        Returns:
            FairQueueConfig with default settings
        """
        return cls(
            redis=RedisConfig(),
            worker=WorkerConfig(
                id=worker_id,
                assigned_users=assigned_users,
                steal_targets=steal_targets or [],
            ),
            queue=QueueConfig(),
        )
