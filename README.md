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
- 🔧 **Function Decorators**: @task decorator for seamless function-to-task conversion
- 📡 **XCom Support**: Cross-task communication with automatic data management

## Quick Start

### Installation

```bash
# Using uv (recommended)
uv add fairque

# Using pip
pip install fairque
```

### Basic Usage with @task Decorator

```python
from fairque import task, TaskQueue, FairQueueConfig, Priority, xcom_push, xcom_pull

# Define tasks using the @task decorator
@task(priority=Priority.HIGH, max_retries=3)
def process_order(order_id: int, customer_id: str) -> dict:
    """Process customer order."""
    print(f"Processing order {order_id} for customer {customer_id}")
    
    # Simulate order processing
    result = {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": "processed",
        "total": 99.99
    }
    
    # Store result in XCom for other tasks
    xcom_push("order_result", result)
    return result

@task(priority=Priority.NORMAL)
def send_confirmation(order_id: int):
    """Send order confirmation email."""
    # Pull order result from XCom
    order_data = xcom_pull("order_result")
    print(f"Sending confirmation for order {order_id}: {order_data}")

# Create configuration
config = FairQueueConfig.create_default(
    worker_id="worker-001",
    assigned_users=["user:1", "user:2", "user:3"],
    steal_targets=["user:4", "user:5"]
)

# Create and execute tasks
with TaskQueue(config) as queue:
    # Create tasks by calling decorated functions
    order_id = 12345
    customer_id = "customer@example.com"
    order_task = process_order(order_id, customer_id)
    confirmation_task = send_confirmation(order_id)
    
    # Execute immediately or push to queue
    result = order_task()  # Execute directly
    print(f"Order result: {result}")
    
    # Push to queue for worker processing
    queue.push(confirmation_task, user_id="user:1")
    
    # Get queue statistics
    stats = queue.get_stats()
    print(f"Active tasks: {stats.get('tasks_active', 0)}")
```

### XCom (Cross Communication) Usage

```python
from fairque import xcom_task, xcom_push, xcom_pull

@xcom_task("data_processing")
def extract_data(source: str) -> dict:
    """Extract data and automatically store in XCom."""
    data = {"source": source, "records": 1000}
    return data  # Automatically stored in XCom with task key

@task()
def transform_data():
    """Transform data using XCom."""
    # Pull data from previous task
    raw_data = xcom_pull("data_processing")
    transformed = {
        "processed_records": raw_data["records"] * 2,
        "source": raw_data["source"]
    }
    # Store transformed data
    xcom_push("transformed_data", transformed)
    return transformed

@task()
def load_data():
    """Load transformed data."""
    data = xcom_pull("transformed_data")
    print(f"Loading {data['processed_records']} records from {data['source']}")
```

### Worker Usage

```python
from fairque import TaskHandler, Worker, FairQueueConfig

class MyTaskHandler(TaskHandler):
    def _process_task(self, task) -> bool:
        """Process tasks with automatic function execution."""
        # TaskHandler automatically executes task.func if available
        # Only implement custom logic for non-function tasks
        action = task.payload.get("action")
        
        if action == "custom_processing":
            print(f"Custom processing for task {task.task_id}")
            return True
        
        # For function tasks, parent class handles execution
        return super()._process_task(task)
    
    def on_task_success(self, task, duration: float) -> None:
        print(f"✓ Task {task.task_id} completed in {duration:.2f}s")
    
    def on_task_failure(self, task, error: Exception, duration: float) -> None:
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
    # Worker automatically processes function tasks and custom tasks
```

### Legacy Manual Task Creation

```python
from fairque import TaskQueue, FairQueueConfig, Priority, Task

# Create configuration
config = FairQueueConfig.create_default(
    worker_id="worker-001",
    assigned_users=["user:1", "user:2", "user:3"],
    steal_targets=["user:4", "user:5"]
)

# Initialize TaskQueue
with TaskQueue(config) as queue:
    # Create and push a task manually
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

### Configuration

```yaml
# fairque_config.yaml
redis:
  host: "localhost"
  port: 6379
  db: 0

workers:
  - id: "worker-001"
    assigned_users: ["user:1", "user:3", "user:5"]
    steal_targets: ["user:2", "user:4", "user:6"]
    poll_interval_seconds: 1.0
    max_concurrent_tasks: 10
  - id: "worker-002"
    assigned_users: ["user:2", "user:4", "user:6"]
    steal_targets: ["user:1", "user:3", "user:5"]
    poll_interval_seconds: 1.0
    max_concurrent_tasks: 10

queue:
  stats_prefix: "fq"
  default_max_retries: 3
  enable_pipeline_optimization: true
  xcom_ttl_seconds: 3600
```

Load configuration:

```python
# Multi-worker configuration
config = FairQueueConfig.from_yaml("fairque_config.yaml")

# Single worker configuration (legacy)
config = FairQueueConfig.create_default(
    worker_id="worker-001",
    assigned_users=["user:1", "user:2"],
    steal_targets=["user:3", "user:4"]
)
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
xcom:{xcom_key}              # XCom data storage (Hash with TTL)
```

### Work Stealing Strategy

Workers have:
- **assigned_users**: Primary responsibility users
- **steal_targets**: Users they can steal work from when idle

Processing order:
1. Try assigned_users (round-robin)
2. If empty, try steal_targets (round-robin)
3. For each user: critical queue first, then normal queue

### Function Task System

FairQueue provides a decorator-based system for converting functions into tasks:

```python
@task(priority=Priority.HIGH, max_retries=3)
def my_function(arg1: str, arg2: int = 10) -> str:
    return f"Processed {arg1} with {arg2}"

# Creates a Task object that can be executed or queued
task_obj = my_function("data", arg2=20)

# Execute directly
result = task_obj()

# Or push to queue
queue.push(task_obj, user_id="user:1")
```

### XCom (Cross Communication)

XCom enables data sharing between tasks:

```python
# Store data
xcom_push("my_key", {"data": "value"})

# Retrieve data
data = xcom_pull("my_key")

# Automatic XCom storage
@xcom_task("result_key")
def compute_data():
    return {"computed": True}  # Automatically stored in XCom
```

## Development Status

This project is currently in **Phase 10: Multi-Worker Configuration Support**.

### ✅ Completed
- [x] Project setup and structure
- [x] Core models (Priority, Task, DLQEntry)
- [x] Configuration system with multi-worker support
- [x] Exception handling
- [x] Lua scripts implementation
- [x] TaskQueue core implementation
- [x] Worker implementation with work stealing
- [x] Comprehensive testing suite
- [x] Async implementation (AsyncTaskQueue, AsyncWorker)
- [x] Function execution support with @task decorator
- [x] XCom (Cross Communication) system
- [x] Performance testing suite
- [x] Multi-worker configuration support

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
pytest tests/performance/   # Performance tests only

# Run benchmarks
python tests/performance/run_benchmarks.py
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
