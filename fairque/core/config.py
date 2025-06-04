"""Configuration classes for FairQueue with support for separated config files."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

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
class CommonConfig:
    """Common configuration shared across processes."""

    # Redis configuration
    redis: RedisConfig = field(default_factory=RedisConfig)

    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Queue configuration
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
        """Validate common configuration parameters.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        self.redis.validate()

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

    def create_redis_client(self) -> Redis:
        """Create Redis client from configuration.

        Returns:
            Configured Redis client instance
        """
        return self.redis.create_redis_client()


@dataclass
class WorkerConfig:
    """Worker-specific configuration."""

    # Common configuration (will be merged from common_config)
    common: CommonConfig = field(default_factory=CommonConfig)

    # Worker-specific settings
    id: str = "worker-001"
    assigned_users: List[str] = field(default_factory=list)
    steal_targets: List[str] = field(default_factory=list)
    poll_interval_seconds: float = 1.0
    task_timeout_seconds: float = 300.0
    max_concurrent_tasks: int = 10
    graceful_shutdown_timeout: float = 30.0

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
        self.common.validate()

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

    def create_redis_client(self) -> Redis:
        """Create Redis client from configuration.

        Returns:
            Configured Redis client instance
        """
        return self.common.create_redis_client()


@dataclass
class SchedulerConfig:
    """Scheduler-specific configuration."""

    # Common configuration (will be merged from common_config)
    common: CommonConfig = field(default_factory=CommonConfig)

    # Scheduler-specific settings
    id: str = "scheduler-001"
    check_interval: int = 60  # seconds
    max_scheduled_tasks: int = 1000
    lock_timeout: int = 300  # seconds
    timezone: str = "UTC"
    enable_distributed_locking: bool = True
    lock_renewal_interval: int = 30  # seconds

    def __post_init__(self) -> None:
        """Validate configuration after initialization.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        self.validate()

    def validate(self) -> None:
        """Validate scheduler configuration parameters.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        self.common.validate()

        if not self.id:
            raise ConfigurationError("Scheduler ID cannot be empty")

        if self.check_interval <= 0:
            raise ConfigurationError("check_interval must be positive")

        if self.max_scheduled_tasks <= 0:
            raise ConfigurationError("max_scheduled_tasks must be positive")

        if self.lock_timeout <= 0:
            raise ConfigurationError("lock_timeout must be positive")

        if self.lock_renewal_interval <= 0:
            raise ConfigurationError("lock_renewal_interval must be positive")

        if self.lock_renewal_interval >= self.lock_timeout:
            raise ConfigurationError("lock_renewal_interval must be less than lock_timeout")

    def create_redis_client(self) -> Redis:
        """Create Redis client from configuration.

        Returns:
            Configured Redis client instance
        """
        return self.common.create_redis_client()


# Configuration loading functions

def _load_config_file(filename: str, config_dir: Optional[Path] = None) -> Dict:
    """Load configuration from YAML file.

    Args:
        filename: Name of the configuration file
        config_dir: Directory containing config files (defaults to ./config)

    Returns:
        Dictionary containing configuration data

    Raises:
        ConfigurationError: If file loading fails
    """
    if config_dir is None:
        config_dir = Path("config")

    file_path = config_dir / filename

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}
    except FileNotFoundError:
        # Return empty dict if file doesn't exist
        return {}
    except (OSError, yaml.YAMLError) as e:
        raise ConfigurationError(f"Failed to load configuration from {file_path}: {e}") from e


def _merge_configs(base: Dict, override: Dict) -> Dict:
    """Deep merge two configuration dictionaries.

    Args:
        base: Base configuration dictionary
        override: Override configuration dictionary

    Returns:
        Merged configuration dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_configs(result[key], value)
        else:
            result[key] = value

    return result


def load_worker_config(
    config_name: str = "worker",
    config_dir: Optional[Path] = None,
    common_config_name: str = "common"
) -> WorkerConfig:
    """Load worker configuration with automatic common config merging.

    Args:
        config_name: Worker config name (loads {config_name}_config.yaml)
        config_dir: Configuration directory (defaults to ./config)
        common_config_name: Common config name (loads {common_config_name}_config.yaml)

    Returns:
        WorkerConfig instance with merged configuration

    Raises:
        ConfigurationError: If configuration loading or validation fails
    """
    # Load common configuration
    common_data = _load_config_file(f"{common_config_name}_config.yaml", config_dir)

    # Load worker-specific configuration
    worker_data = _load_config_file(f"{config_name}_config.yaml", config_dir)

    # Merge configurations
    merged_data = _merge_configs(common_data, worker_data)

    try:
        # Create CommonConfig from merged data
        common_config = CommonConfig()
        if "redis" in merged_data:
            common_config.redis = RedisConfig(**merged_data["redis"])

        # Update common config with other common fields
        for field_name in ["log_level", "log_format", "stats_prefix", "lua_script_cache_size",
                          "max_retry_attempts", "default_task_timeout", "default_max_retries",
                          "enable_pipeline_optimization", "pipeline_batch_size", "pipeline_timeout",
                          "queue_cleanup_interval", "stats_aggregation_interval"]:
            if field_name in merged_data:
                setattr(common_config, field_name, merged_data[field_name])

        # Create WorkerConfig
        worker_config_data = merged_data.copy()
        worker_config_data["common"] = common_config

        # Remove common fields from worker data to avoid conflicts
        for field_name in ["redis", "log_level", "log_format", "stats_prefix", "lua_script_cache_size",
                          "max_retry_attempts", "default_task_timeout", "default_max_retries",
                          "enable_pipeline_optimization", "pipeline_batch_size", "pipeline_timeout",
                          "queue_cleanup_interval", "stats_aggregation_interval"]:
            worker_config_data.pop(field_name, None)

        return WorkerConfig(**worker_config_data)

    except TypeError as e:
        raise ConfigurationError(f"Invalid worker configuration structure: {e}") from e


def load_scheduler_config(
    config_name: str = "scheduler",
    config_dir: Optional[Path] = None,
    common_config_name: str = "common"
) -> SchedulerConfig:
    """Load scheduler configuration with automatic common config merging.

    Args:
        config_name: Scheduler config name (loads {config_name}_config.yaml)
        config_dir: Configuration directory (defaults to ./config)
        common_config_name: Common config name (loads {common_config_name}_config.yaml)

    Returns:
        SchedulerConfig instance with merged configuration

    Raises:
        ConfigurationError: If configuration loading or validation fails
    """
    # Load common configuration
    common_data = _load_config_file(f"{common_config_name}_config.yaml", config_dir)

    # Load scheduler-specific configuration
    scheduler_data = _load_config_file(f"{config_name}_config.yaml", config_dir)

    # Merge configurations
    merged_data = _merge_configs(common_data, scheduler_data)

    try:
        # Create CommonConfig from merged data
        common_config = CommonConfig()
        if "redis" in merged_data:
            common_config.redis = RedisConfig(**merged_data["redis"])

        # Update common config with other common fields
        for field_name in ["log_level", "log_format", "stats_prefix", "lua_script_cache_size",
                          "max_retry_attempts", "default_task_timeout", "default_max_retries",
                          "enable_pipeline_optimization", "pipeline_batch_size", "pipeline_timeout",
                          "queue_cleanup_interval", "stats_aggregation_interval"]:
            if field_name in merged_data:
                setattr(common_config, field_name, merged_data[field_name])

        # Create SchedulerConfig
        scheduler_config_data = merged_data.copy()
        scheduler_config_data["common"] = common_config

        # Remove common fields from scheduler data to avoid conflicts
        for field_name in ["redis", "log_level", "log_format", "stats_prefix", "lua_script_cache_size",
                          "max_retry_attempts", "default_task_timeout", "default_max_retries",
                          "enable_pipeline_optimization", "pipeline_batch_size", "pipeline_timeout",
                          "queue_cleanup_interval", "stats_aggregation_interval"]:
            scheduler_config_data.pop(field_name, None)

        return SchedulerConfig(**scheduler_config_data)

    except TypeError as e:
        raise ConfigurationError(f"Invalid scheduler configuration structure: {e}") from e


# Legacy support for existing FairQueueConfig
@dataclass
class QueueConfig:
    """Queue configuration with performance settings (legacy support)."""

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
    """Unified configuration class (legacy support)."""

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
