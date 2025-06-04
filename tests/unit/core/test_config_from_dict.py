"""Tests for FairQueueConfig.from_dict and to_dict methods."""

import pytest

from fairque.core.config import FairQueueConfig, WorkerConfig
from fairque.core.exceptions import ConfigurationError


class TestFairQueueConfigFromDict:
    """Test FairQueueConfig.from_dict method."""

    def test_from_dict_single_worker_legacy_format(self):
        """Test from_dict with single worker (legacy format)."""
        config_dict = {
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 15
            },
            "worker": {
                "id": "worker1",
                "assigned_users": ["user_0", "user_1"],
                "steal_targets": ["user_2", "user_3"]
            },
            "queue": {
                "stats_prefix": "test_fq"
            }
        }

        config = FairQueueConfig.from_dict(config_dict)

        assert len(config.workers) == 1
        assert config.workers[0].id == "worker1"
        assert config.workers[0].assigned_users == ["user_0", "user_1"]
        assert config.workers[0].steal_targets == ["user_2", "user_3"]
        assert config.redis.port == 6379
        assert config.redis.db == 15
        assert config.queue.stats_prefix == "test_fq"

    def test_from_dict_multi_worker_format(self):
        """Test from_dict with multiple workers."""
        config_dict = {
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 15
            },
            "workers": [
                {
                    "id": "worker1",
                    "assigned_users": ["user_0", "user_1"],
                    "steal_targets": ["user_2", "user_3"]
                },
                {
                    "id": "worker2",
                    "assigned_users": ["user_2", "user_3"],
                    "steal_targets": ["user_0", "user_1"]
                }
            ],
            "queue": {
                "stats_prefix": "test_fq"
            }
        }

        config = FairQueueConfig.from_dict(config_dict)

        assert len(config.workers) == 2
        worker1 = config.get_worker_by_id("worker1")
        assert worker1 is not None and worker1.id == "worker1"
        worker2 = config.get_worker_by_id("worker2")
        assert worker2 is not None and worker2.id == "worker2"
        assert config.redis.port == 6379
        assert config.queue.stats_prefix == "test_fq"

    def test_from_dict_empty_config_sections(self):
        """Test from_dict with empty redis and queue sections (should use defaults)."""
        config_dict = {
            "worker": {
                "id": "worker1",
                "assigned_users": ["user1", "user2"]
            }
        }

        config = FairQueueConfig.from_dict(config_dict)

        # Should use default values
        assert config.redis.host == "localhost"
        assert config.redis.port == 6379
        assert config.queue.stats_prefix == "fq"
        assert len(config.workers) == 1

    def test_from_dict_both_worker_and_workers(self):
        """Test from_dict with both 'worker' and 'workers' specified."""
        config_dict = {
            "redis": {"host": "localhost"},
            "worker": {"id": "worker1", "assigned_users": ["user1"]},
            "workers": [{"id": "worker2", "assigned_users": ["user2"]}],
            "queue": {}
        }

        with pytest.raises(ConfigurationError, match="Cannot specify both 'worker' and 'workers'"):
            FairQueueConfig.from_dict(config_dict)

    def test_from_dict_workers_not_list(self):
        """Test from_dict with 'workers' that is not a list."""
        config_dict = {
            "redis": {"host": "localhost"},
            "workers": {"id": "worker1", "assigned_users": ["user1"]},  # Should be list
            "queue": {}
        }

        with pytest.raises(ConfigurationError, match="'workers' must be a list"):
            FairQueueConfig.from_dict(config_dict)

    def test_from_dict_worker_not_dict(self):
        """Test from_dict with worker configuration that is not a dict."""
        config_dict = {
            "redis": {"host": "localhost"},
            "workers": ["not a dict"],  # Should be dict
            "queue": {}
        }

        with pytest.raises(ConfigurationError, match="Worker 0 configuration must be a dictionary"):
            FairQueueConfig.from_dict(config_dict)

    def test_from_dict_no_worker_section(self):
        """Test from_dict without worker or workers section."""
        config_dict = {
            "redis": {"host": "localhost"},
            "queue": {}
        }

        with pytest.raises(ConfigurationError, match="Configuration must contain either 'worker' or 'workers' section"):
            FairQueueConfig.from_dict(config_dict)

    def test_from_dict_invalid_worker_config(self):
        """Test from_dict with invalid worker configuration."""
        config_dict = {
            "redis": {"host": "localhost"},
            "worker": {
                "id": "worker1",
                "assigned_users": ["user1"],
                "invalid_field": "invalid_value"  # This should cause TypeError
            },
            "queue": {}
        }

        with pytest.raises(ConfigurationError, match="Invalid configuration structure"):
            FairQueueConfig.from_dict(config_dict)


class TestFairQueueConfigToDict:
    """Test FairQueueConfig.to_dict method."""

    def test_to_dict_always_multi_worker_format(self):
        """Test to_dict always uses multi-worker format."""
        # Single worker configuration
        single_worker_config = FairQueueConfig.create_default(
            worker_id="worker1",
            assigned_users=["user_0", "user_1"],
            steal_targets=["user_2", "user_3"]
        )

        config_dict = single_worker_config.to_dict()

        # Should always use 'workers' array, even for single worker
        assert "workers" in config_dict
        assert "worker" not in config_dict  # No legacy format
        assert len(config_dict["workers"]) == 1
        assert config_dict["workers"][0]["id"] == "worker1"

    def test_to_dict_multi_worker(self):
        """Test to_dict with multiple workers."""
        config = FairQueueConfig.create_multi_worker([
            WorkerConfig(
                id="worker1",
                assigned_users=["user_0", "user_1"],
                steal_targets=["user_2", "user_3"]
            ),
            WorkerConfig(
                id="worker2",
                assigned_users=["user_2", "user_3"],
                steal_targets=["user_0", "user_1"]
            )
        ])

        config_dict = config.to_dict()

        assert "workers" in config_dict
        assert len(config_dict["workers"]) == 2
        assert config_dict["workers"][0]["id"] == "worker1"
        assert config_dict["workers"][1]["id"] == "worker2"

    def test_to_dict_contains_all_sections(self):
        """Test to_dict contains redis, queue, and workers sections."""
        config = FairQueueConfig.create_default(
            worker_id="test_worker",
            assigned_users=["user1"]
        )

        config_dict = config.to_dict()

        assert "redis" in config_dict
        assert "queue" in config_dict
        assert "workers" in config_dict

        # Check redis section
        assert "host" in config_dict["redis"]
        assert "port" in config_dict["redis"]
        assert "db" in config_dict["redis"]

        # Check queue section
        assert "stats_prefix" in config_dict["queue"]
        assert "lua_script_cache_size" in config_dict["queue"]

        # Check workers section
        assert len(config_dict["workers"]) == 1
        assert "id" in config_dict["workers"][0]
        assert "assigned_users" in config_dict["workers"][0]


class TestFairQueueConfigRoundtrip:
    """Test roundtrip conversion: dict -> config -> dict."""

    def test_roundtrip_single_worker(self):
        """Test complete roundtrip for single worker configuration."""
        original_dict = {
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 15
            },
            "worker": {
                "id": "worker1",
                "assigned_users": ["user_0", "user_1"],
                "steal_targets": ["user_2", "user_3"]
            },
            "queue": {
                "stats_prefix": "test_fq"
            }
        }

        # Dict -> Config -> Dict
        config = FairQueueConfig.from_dict(original_dict)
        result_dict = config.to_dict()

        # Should maintain same data but in multi-worker format
        assert result_dict["redis"]["host"] == "localhost"
        assert result_dict["redis"]["port"] == 6379
        assert result_dict["redis"]["db"] == 15
        assert len(result_dict["workers"]) == 1
        assert result_dict["workers"][0]["id"] == "worker1"
        assert result_dict["workers"][0]["assigned_users"] == ["user_0", "user_1"]
        assert result_dict["workers"][0]["steal_targets"] == ["user_2", "user_3"]
        assert result_dict["queue"]["stats_prefix"] == "test_fq"

    def test_roundtrip_multi_worker(self):
        """Test complete roundtrip for multi-worker configuration."""
        original_dict = {
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 15
            },
            "workers": [
                {
                    "id": "worker1",
                    "assigned_users": ["user_0", "user_1"],
                    "steal_targets": ["user_2", "user_3"]
                },
                {
                    "id": "worker2",
                    "assigned_users": ["user_2", "user_3"],
                    "steal_targets": ["user_0", "user_1"]
                }
            ],
            "queue": {
                "stats_prefix": "test_fq"
            }
        }

        # Dict -> Config -> Dict
        config = FairQueueConfig.from_dict(original_dict)
        result_dict = config.to_dict()

        # Should maintain same structure
        assert result_dict["redis"]["host"] == "localhost"
        assert result_dict["redis"]["port"] == 6379
        assert len(result_dict["workers"]) == 2
        assert result_dict["workers"][0]["id"] == "worker1"
        assert result_dict["workers"][1]["id"] == "worker2"
        assert result_dict["queue"]["stats_prefix"] == "test_fq"

    def test_roundtrip_preserves_worker_validation(self):
        """Test that roundtrip preserves worker validation."""
        # Create config with valid multiple workers
        config_dict = {
            "redis": {"host": "localhost"},
            "workers": [
                {
                    "id": "worker1",
                    "assigned_users": ["user_0", "user_1"],
                    "steal_targets": ["user_2", "user_3"]
                },
                {
                    "id": "worker2",
                    "assigned_users": ["user_2", "user_3"],
                    "steal_targets": ["user_0", "user_1"]
                }
            ],
            "queue": {}
        }

        config = FairQueueConfig.from_dict(config_dict)

        # Should pass validation
        config.validate_all()

        # Convert back to dict and recreate - should still be valid
        result_dict = config.to_dict()
        recreated_config = FairQueueConfig.from_dict(result_dict)
        recreated_config.validate_all()  # Should not raise
