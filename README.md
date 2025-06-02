# FairQueue

Production-ready fair queue implementation using Redis with work stealing and priority scheduling.

## Features

- 🎯 **Fair Scheduling**: Round-robin user selection with work stealing
- 🏆 **Priority Queues**: Type-safe priority system (1-6) with critical/normal separation
- ⚡ **Atomic Operations**: Lua scripts ensure consistency and performance
- ⚙️ **Configuration-Based**: No Redis state for user management, fully configurable
- 🚀 **Pipeline Optimization**: Batch operations for high throughput
- 📊 **Comprehensive Monitoring**: Built-in statistics and alerting
- 🛡️ **Production Ready**: Error handling, graceful shutdown, health checks

## Quick Start

### Installation

```bash
# Using uv (recommended)
uv add fairque

# Using pip
pip install fairque
```

### Basic Usage

```python
from fairque import FairQueue, FairQueueConfig, Priority, Task

# Create configuration
config = FairQueueConfig.create_default(
    worker_id="worker-001",
    assigned_users=["user:1", "user:2", "user:3"],
    steal_targets=["user:4", "user:5"]
)

# Initialize FairQueue
with FairQueue(config) as queue:
    # Create and push a task
    task = Task.create(
        user_id="user:1",
        priority=Priority.HIGH,
        payload={"action": "process_data", "data_id": 123}
    )
    
    # Push task to queue
    result = queue.push(task)
    print(f"Task pushed: {result['success']}")
    
    # Pop and process tasks
    task = queue.pop()
    if task:
        print(f"Processing task {task.task_id} from user {task.user_id}")
        print(f"Payload: {task.payload}")
        
        # Clean up after processing
        queue.delete_task(task.task_id)
    
    # Get statistics
    stats = queue.get_stats()
    print(f"Active tasks: {stats.get('tasks_active', 0)}")
```

### Worker Usage

```python
from fairque import FairQueue, FairQueueConfig, Priority, Task, TaskHandler, Worker

# Create a custom task handler
class MyTaskHandler(TaskHandler):
    def process_task(self, task: Task) -> bool:
        action = task.payload.get("action")
        
        if action == "process_order":
            order_id = task.payload.get("order_id")
            print(f"Processing order {order_id}")
            # Your business logic here
            return True
        elif action == "send_email":
            recipient = task.payload.get("recipient")
            print(f"Sending email to {recipient}")
            # Your email logic here
            return True
        else:
            print(f"Unknown action: {action}")
            return False
    
    def on_task_success(self, task: Task, duration: float) -> None:
        print(f"✓ Task {task.task_id} completed in {duration:.2f}s")
    
    def on_task_failure(self, task: Task, error: Exception, duration: float) -> None:
        print(f"✗ Task {task.task_id} failed: {error}")

# Create configuration and worker
config = FairQueueConfig.create_default(
    worker_id="worker-001",
    assigned_users=["user:1", "user:2"],
    steal_targets=["user:3", "user:4"]
)

# Start worker
with Worker(config, MyTaskHandler()) as worker:
    worker.start()
    
    # Worker will continuously process tasks
    # Use worker.get_stats() to monitor progress
    # Worker stops automatically when exiting context
```

### Configuration

```yaml
# fairque_config.yaml
redis:
  host: "localhost"
  port: 6379
  db: 0

worker:
  id: "worker-001"
  assigned_users: ["user:1", "user:3", "user:5"]
  steal_targets: ["user:2", "user:4", "user:6"]
  poll_interval_seconds: 1.0
  max_concurrent_tasks: 10

queue:
  stats_prefix: "fq"
  default_max_retries: 3
  enable_pipeline_optimization: true
```

Load configuration:

```python
config = FairQueueConfig.from_yaml("fairque_config.yaml")
```

## Why "Fair"?

The name **FairQueue** reflects the core principle of **fairness** in task distribution and processing:

### 🎯 **Fair Task Distribution**
- **Round-robin user selection**: Each user gets equal opportunity for their tasks to be processed
- **No user starvation**: High-volume users cannot monopolize worker resources
- **Balanced workload**: Tasks are distributed evenly across workers through work stealing

### ⚖️ **Fair Resource Allocation**
- **Priority-aware fairness**: Critical tasks get immediate attention while maintaining fairness among normal priorities
- **Worker equity**: All workers have equal opportunity to process tasks from their assigned users
- **Dynamic load balancing**: Work stealing ensures optimal resource utilization without unfair advantage

### 🔄 **Fair Processing Order**
- **Within user fairness**: Tasks from the same user are processed in priority order
- **Cross-user fairness**: No single user can dominate the queue regardless of task volume
- **Temporal fairness**: Tasks are processed in a predictable, fair manner based on submission time and priority

This fairness model makes FairQueue ideal for multi-tenant systems, SaaS platforms, and any application where equitable resource sharing is crucial for user experience and system stability.

## Architecture

### Priority System

```python
from fairque import Priority

# Priority levels (1-6)
Priority.VERY_LOW    # 1 - Lowest priority
Priority.LOW         # 2 - Low priority  
Priority.NORMAL      # 3 - Standard priority
Priority.HIGH        # 4 - High priority
Priority.VERY_HIGH   # 5 - Very high priority
Priority.CRITICAL    # 6 - Critical priority (separate FIFO queue)
```

### Queue Structure

```
queue:user:{user_id}:critical  # Priority.CRITICAL tasks (List, FIFO)
queue:user:{user_id}:normal    # Priority 1-5 tasks (Sorted Set, Score-based)
dlq                           # Single DLQ for all failure types (List)
queue:stats                   # Unified statistics (Hash)
```

### Work Stealing Strategy

Workers have:
- **assigned_users**: Primary responsibility users
- **steal_targets**: Users they can steal work from when idle

Processing order:
1. Try assigned_users (round-robin)
2. If empty, try steal_targets (round-robin)
3. For each user: critical queue first, then normal queue

## Development Status

This project is currently in **Phase 1: Core Infrastructure**.

### ✅ Completed
- [x] Project setup and structure
- [x] Core models (Priority, Task, DLQEntry)
- [x] Configuration system
- [x] Exception handling
- [x] Lua scripts implementation
- [x] FairQueue core implementation
- [x] Worker implementation with work stealing
- [x] Comprehensive testing suite
- [x] Async implementation (AsyncFairQueue, AsyncWorker)

### 🚧 Optional Extensions
- [ ] Advanced monitoring and alerting
- [ ] Extended documentation and tutorials

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=fairque --cov-report=html

# Run specific test categories
pytest tests/unit/          # Unit tests only
pytest tests/integration/   # Integration tests only
```

**Note**: Tests require Redis running on `localhost:6379` with database 15 available for testing.

## Requirements

- Python 3.10+
- Redis 7.2.5+ / Valkey 7.2.6+ / Amazon MemoryDB for Redis

## License

MIT License - see LICENSE file for details.

## Contributing

This project follows strict development guidelines:
- All code and documentation in English
- Full type annotations required
- Comprehensive docstrings
- Follow PEP 8 style guidelines
- Use uv for package management

Please see the development documentation for more details.
