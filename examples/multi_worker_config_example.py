"""Example demonstrating FairQueueConfig with multiple workers."""

from fairque.core.config import FairQueueConfig, WorkerConfig


def main():
    """Demonstrate multiple worker configuration creation and usage."""

    print("=== FairQueue Multi-Worker Configuration Example ===")

    # Example 1: Using create_multi_worker class method
    print("\n--- Example 1: Using create_multi_worker ---")

    config = FairQueueConfig.create_multi_worker([
        WorkerConfig(
            id="worker1",
            assigned_users=[f"user_{i}" for i in range(5)],
            steal_targets=[f"user_{i}" for i in range(5, 10)],
        ),
        WorkerConfig(
            id="worker2",
            assigned_users=[f"user_{i}" for i in range(5, 10)],
            steal_targets=[f"user_{i}" for i in range(5)],
        ),
    ])

    print(f"Created configuration with {len(config.workers)} workers")
    print("Worker coverage info:")
    coverage = config.get_coverage_info()
    for detail in coverage["worker_details"]:
        print(f"  - {detail['id']}: {detail['assigned_count']} assigned, "
              f"{detail['steal_targets_count']} steal targets")

    # Example 2: Using from_dict with multi-worker format
    print("\n--- Example 2: Using from_dict with multi-worker format ---")

    config_dict = {
        "redis": {
            "host": "localhost",
            "port": 6379,
            "db": 15,
            "decode_responses": False,
        },
        "workers": [
            {
                "id": "worker1",
                "assigned_users": [f"user_{i}" for i in range(5)],
                "steal_targets": [f"user_{i}" for i in range(5, 10)],
            },
            {
                "id": "worker2",
                "assigned_users": [f"user_{i}" for i in range(5, 10)],
                "steal_targets": [f"user_{i}" for i in range(5)],
            },
        ],
        "queue": {
            "max_queue_size": 100000,
            "stats_prefix": "fq",
        },
    }

    config_from_dict = FairQueueConfig.from_dict(config_dict)
    print(f"Loaded configuration from dict with {len(config_from_dict.workers)} workers")

    # Example 3: Converting to dict (always uses multi-worker format)
    print("\n--- Example 3: Converting to dict ---")

    result_dict = config_from_dict.to_dict()
    print("Configuration as dictionary (multi-worker format):")
    print(f"  Redis: {result_dict['redis']['host']}:{result_dict['redis']['port']}")
    print(f"  Workers: {len(result_dict['workers'])}")
    for i, worker in enumerate(result_dict['workers']):
        print(f"    Worker {i+1}: {worker['id']} "
              f"({len(worker['assigned_users'])} assigned, "
              f"{len(worker['steal_targets'])} steal targets)")

    # Example 4: Legacy single worker support
    print("\n--- Example 4: Legacy single worker support ---")

    legacy_config = FairQueueConfig.create_default(
        worker_id="legacy_worker",
        assigned_users=["user1", "user2", "user3"],
        steal_targets=["user4", "user5"]
    )

    print(f"Legacy config has {len(legacy_config.workers)} worker")
    print(f"Can access via legacy property: {legacy_config.worker.id}")

    # Convert legacy to dict - still uses multi-worker format
    legacy_dict = legacy_config.to_dict()
    print(f"Legacy config as dict has 'workers' array with {len(legacy_dict['workers'])} items")

    # Example 5: Loading legacy format from dict
    print("\n--- Example 5: Loading legacy format from dict ---")

    legacy_dict_format = {
        "redis": {"host": "localhost", "port": 6379},
        "worker": {  # Note: singular 'worker' for legacy support
            "id": "legacy_from_dict",
            "assigned_users": ["user1", "user2"],
            "steal_targets": ["user3", "user4"]
        },
        "queue": {"stats_prefix": "legacy_fq"}
    }

    legacy_from_dict = FairQueueConfig.from_dict(legacy_dict_format)
    print(f"Loaded legacy format: {legacy_from_dict.worker.id}")
    print(f"Internally stored as {len(legacy_from_dict.workers)} workers")

    # Example 6: Worker lookup and utilities
    print("\n--- Example 6: Worker lookup and utilities ---")

    worker1 = config.get_worker_by_id("worker1")
    if worker1:
        print(f"Found worker: {worker1.id}")
        print(f"  Assigned users: {worker1.assigned_users}")
        print(f"  All users (assigned + steal): {worker1.get_all_users()}")

    all_users = config.get_all_users()
    print(f"All users covered by configuration: {all_users}")

    coverage_info = config.get_coverage_info()
    print(f"Total coverage: {coverage_info['unique_users_covered']} unique users "
          f"across {coverage_info['total_workers']} workers")

    # Example 7: Validation
    print("\n--- Example 7: Configuration validation ---")

    try:
        config.validate_all()
        print("✓ Configuration is valid")
    except Exception as e:
        print(f"✗ Configuration validation failed: {e}")

    print("\n=== Examples completed successfully ===")


if __name__ == "__main__":
    main()
