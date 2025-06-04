# FairQueue Project

## Project Overview
**fairque** is a production-ready fair queue implementation using Redis/Valkey with work stealing and priority scheduling.

## Key Features
- Fair Scheduling: Round-robin user selection with work stealing
- Priority Queues: Type-safe priority system (1-6) with critical/normal separation
- Atomic Operations: Lua scripts ensure consistency and performance
- Configuration-Based: No Redis state for user management, fully configurable
- Pipeline Optimization: Batch operations for high throughput
- Comprehensive Monitoring: Built-in statistics and alerting
- Production Ready: Error handling, graceful shutdown, health checks
- Cron-Based Scheduling: Task scheduling with croniter for recurring tasks
## Architecture
- **Redis/Valkey as persistent storage** (not in-memory cache)
- **Lua scripts for server-side atomic operations** with integrated statistics
- **Configuration-based user management** (no all_users Redis key)
- **Dual implementation**: Synchronous and Asynchronous versions
- **Type-safe Priority system** using IntEnum
- **Work stealing strategy** for load balancing

## Technology Stack
- Python 3.10+
- Redis 7.2.5+ / Valkey 7.2.6+ / Amazon MemoryDB
- Package Manager: uv
- Type Annotations: Required (Full Typing)
- Language: English only

## Project Structure
```
fairque/
├── fairque/                    # Main package
│   ├── __init__.py
│   ├── core/                   # Core models and types
│   │   ├── __init__.py
│   │   ├── models.py          # Task, Priority, DLQEntry
│   │   ├── config.py          # Configuration classes
│   │   └── exceptions.py      # Exception classes
│   ├── queue/                  # Queue implementation
│   │   ├── __init__.py
│   │   ├── fairqueue.py       # Main FairQueue class
│   │   └── async_fairqueue.py # Async implementation
│   ├── worker/                 # Worker implementation
│   │   ├── __init__.py
│   │   ├── worker.py          # Sync worker
│   │   └── async_worker.py    # Async worker
│   ├── scripts/               # Lua scripts
│   ├── scheduler/             # Task scheduling
│   │   ├── __init__.py
│   │   ├── models.py          # ScheduledTask model
│   │   └── scheduler.py       # TaskScheduler implementation
│   ├── scripts/               # Lua scripts
│   │   ├── common.lua         # Shared functions
│   │   ├── push.lua           # Push operation
│   │   ├── pop.lua            # Pop operation
│   │   └── stats.lua          # Statistics operations
│   └── utils/                 # Utilities
│       ├── __init__.py
│       ├── stats.py           # Statistics formatting
│       └── monitoring.py      # Monitoring and alerting
├── tests/                     # Test suite
├── examples/                  # Usage examples
├── docs/                      # Documentation
├── pyproject.toml            # Project configuration
└── README.md                 # Project documentation
```

## Implementation Status
- [x] Project setup and structure
- [x] Core models implementation (Priority, Task, DLQEntry)
- [x] Configuration system (RedisConfig, WorkerConfig, QueueConfig, FairQueueConfig)
- [x] Exception handling system
- [x] Lua scripts implementation (common.lua, push.lua, pop.lua, stats.lua)
- [x] FairQueue core implementation (FairQueue class, LuaScriptManager)
- [x] Worker implementation (TaskHandler, Worker classes with work stealing)
- [x] Basic testing suite (unit tests and integration tests)
- [x] **Async implementation (AsyncTaskQueue, AsyncWorker, AsyncTaskHandler)**
- [x] Task Scheduler implementation (Cron-based scheduling with distributed locking)
- [x] **Performance testing suite** (throughput, worker, Redis operations, async comparison)
- [x] **Function execution support** (Task with func, args, kwargs and __call__ method)
- [x] **Task decorator system** (@fairque.task decorator for converting functions to tasks)
- [ ] Complete documentation

## Current Phase
**Phase 8: Exception-based Function Resolution** - ✅ **FUNCTION FALLBACK SYSTEM COMPLETED**

**Status**: **🎉 EXCEPTION-BASED FUNCTION RESOLUTION SYSTEM COMPLETE 🎉**

### Function Resolution Features
- **Exception-based Resolution**: Clear failure indication with FunctionResolutionError
- **Automatic Fallback Strategy**: Import → registry → exception pattern
- **Function Registry**: Auto-registration via decorators with global registry
- **Strict Error Handling**: Failed function tasks are explicitly failed in TaskHandler
- **Task & ScheduledTask Support**: Unified function execution across both systems
- **Safe Recovery**: try_deserialize_function for graceful handling
- **Enhanced Logging**: Detailed logs for debugging function resolution issues

### Performance Testing Details
- **Queue Throughput Tests**: Single/batch operations, concurrent pushes, work stealing
- **Worker Performance Tests**: Single/concurrent workers, processing efficiency
- **Redis Operations Tests**: Lua script performance, pipeline optimization, memory efficiency
- **Async vs Sync Comparison**: Throughput, concurrency, resource usage comparisons
- **Benchmarking Utilities**: Reusable performance metrics and reporting
- **Run Script**: `python tests/performance/run_benchmarks.py` for easy execution

### Async Implementation Details
- **AsyncTaskQueue**: Full async version of TaskQueue using `redis.asyncio`
- **AsyncWorker**: Async worker with `asyncio.Task` based concurrency
- **AsyncTaskHandler**: Abstract base class for async task processing
- **AsyncLuaScriptManager**: Async version of Lua script management
- **Concurrent Operations**: `push_batch_concurrent()` for high-throughput scenarios
- **Async Context Managers**: Full support for `async with` statements
- **Health Checks**: Async health monitoring and statistics
- **Graceful Shutdown**: Proper async task cleanup and resource management

### Dual Architecture Benefits
- **Synchronous Version**: Thread-based concurrency, familiar patterns
- **Asynchronous Version**: Event-loop based, higher throughput potential
- **Shared Core**: Same Lua scripts, models, and configuration system
- **API Compatibility**: Similar interfaces for easy migration
- **Performance Options**: Choose based on use case and environment

## Current Phase
**Phase 10: Multi-Worker Configuration Support** - ✅ **MULTI-WORKER CONFIGURATION COMPLETED**

**Status**: **🎉 MULTI-WORKER FAIRQUEUECONFIG SUPPORT COMPLETE 🎉**

### Multi-Worker Configuration Features
- **Multiple Worker Support**: FairQueueConfig now accepts multiple WorkerConfig instances via `workers` field
- **Legacy Compatibility**: Backward-compatible `worker` property for single worker access
- **from_dict Method**: Supports both legacy single-worker and new multi-worker dictionary formats
- **to_dict Method**: Always outputs modern multi-worker format with `workers` array
- **Worker Validation**: Comprehensive validation including duplicate ID checks and user assignment verification
- **Worker Utilities**: Methods for worker lookup, user coverage analysis, and statistics
- **YAML Support**: Updated from_yaml and to_yaml methods support both formats
- **Configuration Factory Methods**: `create_default()` for single worker, `create_multi_worker()` for multiple workers

### Configuration Format Examples
```python
# Modern multi-worker format
config = FairQueueConfig.create_multi_worker([
    WorkerConfig(
        id="worker1",
        assigned_users=["user_0", "user_1", "user_2"],
        steal_targets=["user_3", "user_4"],
    ),
    WorkerConfig(
        id="worker2",
        assigned_users=["user_3", "user_4"],
        steal_targets=["user_0", "user_1"],
    ),
])

# from_dict with multi-worker format
config_dict = {
    "redis": {"host": "localhost", "port": 6379, "db": 15},
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
    "queue": {"stats_prefix": "fq"}
}
config = FairQueueConfig.from_dict(config_dict)

# Legacy single worker support (backward compatible)
legacy_config = FairQueueConfig.create_default(
    worker_id="worker1",
    assigned_users=["user_0", "user_1"],
    steal_targets=["user_2", "user_3"]
)
# Access via legacy property
worker = legacy_config.worker
```

### Multi-Worker Validation and Utilities
- **Worker ID Validation**: Prevents duplicate worker IDs across configuration
- **User Assignment Validation**: Ensures no user is assigned to multiple workers
- **Coverage Analysis**: `get_coverage_info()` provides comprehensive statistics
- **Worker Lookup**: `get_worker_by_id()` for efficient worker retrieval
- **User Coverage**: `get_all_users()` returns all users covered by all workers
- **Cross-validation**: Enhanced validation checks worker timeout vs queue timeout across all workers

### Implementation Details
- **Enhanced validate_all()**: Now validates all workers and cross-worker constraints
- **Improved Error Messages**: Worker-specific error messages include worker ID for clarity
- **Factory Methods**: Type-safe configuration creation with validation
- **Comprehensive Test Suite**: Full test coverage for all new functionality
- **Example Code**: Complete multi-worker usage examples in `examples/multi_worker_config_example.py`

## Next Steps
1. Implement core models (Priority, Task, Configuration)
2. Create Lua scripts with common functions
3. Implement basic FairQueue functionality
4. Add comprehensive error handling

## Development Guidelines
- All code and documentation in English
- Full type annotations required
- Comprehensive docstrings
- Follow PEP 8 style guidelines
- Use uv for package management
- Incremental implementation with checkpoints
