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
- [ ] Complete documentation

## Current Phase
**Phase 7: Performance Testing** - ✅ **PERFORMANCE TESTING COMPLETED**

**Status**: **🎉 COMPREHENSIVE PERFORMANCE TESTING SUITE COMPLETE 🎉**

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
**Phase 5: Testing and Documentation** - ✅ **CORE TESTING COMPLETED** - Comprehensive test suite covering all major functionality.

**Status**: **🎉 PRODUCTION-READY CORE SYSTEM COMPLETE 🎉**

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
