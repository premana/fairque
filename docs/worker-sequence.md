# FairQueue Worker Process Sequence

## Overview

The FairQueue Worker is responsible for processing tasks from the queue with work stealing capability. It implements fair scheduling, concurrent task processing, and robust error handling to ensure reliable task execution.

## Table of Contents
1. [Worker Initialization](#worker-initialization)
2. [Main Worker Loop](#main-worker-loop)
3. [Task Processing Flow](#task-processing-flow)
4. [Work Stealing Strategy](#work-stealing-strategy)
5. [Error Handling and Recovery](#error-handling-and-recovery)
6. [Graceful Shutdown](#graceful-shutdown)
7. [Performance Monitoring](#performance-monitoring)

## Worker Initialization

The worker initialization process sets up the necessary components and validates the configuration.

```mermaid
sequenceDiagram
    participant A as Application
    participant W as Worker
    participant TH as TaskHandler
    participant Q as TaskQueue
    participant R as Redis
    participant E as ThreadPoolExecutor
    
    A->>W: Worker(config, task_handler, queue)
    W->>W: validate configuration
    W->>TH: verify task handler
    TH-->>W: handler validated
    
    alt Queue provided
        W->>Q: use provided queue
    else No queue provided
        W->>Q: TaskQueue(config)
        Q->>R: test connection
        R-->>Q: connection confirmed
    end
    
    W->>W: initialize worker state
    W->>W: setup signal handlers
    W->>E: create thread pool executor
    E-->>W: executor ready
    W->>W: initialize statistics
    W-->>A: worker ready
```

### Initialization Components

1. **Configuration Validation**: Verify worker ID, assigned users, and steal targets
2. **Task Handler Setup**: Validate the provided task handler implementation
3. **Queue Connection**: Establish or verify TaskQueue connection
4. **Thread Pool**: Create executor for concurrent task processing
5. **Signal Handlers**: Setup graceful shutdown on SIGTERM/SIGINT
6. **Statistics**: Initialize performance tracking metrics

## Main Worker Loop

The worker runs a continuous loop that polls for tasks and processes them concurrently.

```mermaid
sequenceDiagram
    participant W as Worker
    participant Q as TaskQueue
    participant L as Lua Script
    participant R as Redis
    participant E as ThreadPoolExecutor
    participant T as TaskHandler
    
    Note over W: Worker loop starts
    
    loop While running and not stopping
        W->>W: check active task count
        
        alt Below max concurrent tasks
            W->>Q: pop(user_list)
            Q->>L: execute pop.lua
            L->>R: check assigned users first
            
            alt Task found in assigned users
                L->>R: pop from user queue
                R-->>L: task data
            else No tasks in assigned users
                L->>R: check steal targets
                alt Task found in steal targets
                    L->>R: pop from steal target queue
                    R-->>L: task data
                else No tasks available
                    L-->>Q: null
                end
            end
            
            L->>R: update statistics
            L-->>Q: task or null
            Q-->>W: task or null
            
            alt Task available
                W->>E: submit task for processing
                E->>T: process_task_wrapper(task)
                Note over E,T: Async processing
                W->>W: track active task
            else No task available
                W->>W: sleep(poll_interval)
            end
        else Max concurrent tasks reached
            W->>W: sleep(poll_interval)
            Note over W: Wait for tasks to complete
        end
    end
    
    Note over W: Worker loop ends
```

### Task Polling Strategy

```mermaid
sequenceDiagram
    participant W as Worker
    participant C as Config
    participant Q as TaskQueue
    
    W->>C: get assigned_users
    C-->>W: [user:1, user:3, user:5]
    W->>C: get steal_targets  
    C-->>W: [user:2, user:4, user:6]
    
    W->>W: combine user lists
    Note over W: user_list = assigned_users + steal_targets
    
    W->>Q: pop(user_list)
    Note over Q: Lua script handles priority:<br/>1. Try assigned users first<br/>2. Then try steal targets<br/>3. Critical queues before normal
    
    Q-->>W: task or null
```

## Task Processing Flow

Each task is processed in a separate thread with timeout and error handling.

```mermaid
sequenceDiagram
    participant E as ThreadPoolExecutor
    participant W as Worker
    participant TH as TaskHandler
    participant Q as TaskQueue
    participant M as Metrics
    
    E->>W: process_task_wrapper(task)
    W->>W: start timer
    W->>W: log task start
    
    alt Within timeout
        W->>TH: process_task(task)
        
        alt Task processing successful
            TH-->>W: True
            W->>W: calculate duration
            W->>TH: on_task_success(task, duration)
            W->>Q: delete_task(task.task_id)
            Q-->>W: deletion confirmed
            W->>M: increment success metrics
            W->>W: log success
            
        else Task processing failed
            TH-->>W: False or Exception
            W->>W: calculate duration
            W->>TH: on_task_failure(task, error, duration)
            W->>W: handle task failure
            
            alt Retry available
                W->>Q: requeue task with retry
                Q-->>W: requeue confirmed
                W->>M: increment retry metrics
            else Max retries exceeded
                W->>Q: send to DLQ
                Q-->>W: DLQ confirmed
                W->>M: increment DLQ metrics
            end
            
            W->>M: increment failure metrics
            W->>W: log failure
        end
        
    else Task timeout
        W->>W: cancel task execution
        W->>TH: on_task_failure(task, TimeoutError, timeout_duration)
        W->>W: handle timeout
        W->>M: increment timeout metrics
        W->>W: log timeout
    end
    
    W->>W: remove from active tasks
    W->>W: update statistics
```

### Detailed Task Execution

```mermaid
sequenceDiagram
    participant W as Worker
    participant F as Future
    participant TH as TaskHandler
    participant Q as TaskQueue
    
    Note over W: Task execution with timeout
    
    W->>F: executor.submit(process_task_with_timeout, task)
    F->>TH: process_task(task)
    
    par Task Processing
        TH->>TH: execute business logic
        alt Success
            TH-->>F: return True
        else Exception
            TH-->>F: raise Exception
        end
    and Timeout Monitoring
        W->>W: wait with timeout
        alt Task completes within timeout
            F-->>W: result (success/failure)
        else Timeout exceeded
            W->>F: cancel()
            F-->>W: TimeoutError
        end
    end
    
    alt Task successful
        W->>Q: delete_task(task_id)
        W->>W: update success stats
    else Task failed but retriable
        W->>W: increment retry count
        alt Retries remaining
            W->>Q: push(updated_task)
            W->>W: update retry stats
        else Max retries exceeded
            W->>Q: send_to_dlq(task)
            W->>W: update DLQ stats
        end
    else Task timeout
        W->>Q: send_to_dlq(task)
        W->>W: update timeout stats
    end
```

## Work Stealing Strategy

Workers implement intelligent work stealing to balance load across users.

```mermaid
sequenceDiagram
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant Q as TaskQueue
    participant L as pop.lua
    participant R as Redis
    
    Note over W1,W2: Worker 1: assigned=[user:1,user:2] steal=[user:3,user:4]<br/>Worker 2: assigned=[user:3,user:4] steal=[user:1,user:2]
    
    par Worker 1 Processing
        W1->>Q: pop([user:1, user:2, user:3, user:4])
        Q->>L: execute with user priority
        
        alt Tasks in assigned users (user:1, user:2)
            L->>R: check user:1 queues
            alt Critical queue has tasks
                L->>R: RPOP from user:1:critical
                R-->>L: task data
            else Normal queue has ready tasks
                L->>R: ZPOPMIN from user:1:normal (score <= now)
                R-->>L: task data
            else Check user:2
                L->>R: check user:2 queues
                Note over L,R: Similar process for user:2
            end
        else No tasks in assigned users
            L->>R: check steal targets (user:3, user:4)
            Note over L,R: Only if assigned users are empty
        end
        
        L-->>Q: task or null
        Q-->>W1: task
        
    and Worker 2 Processing
        W2->>Q: pop([user:3, user:4, user:1, user:2])
        Note over W2: Similar process but different priority order
        Q-->>W2: task
    end
```

### Work Stealing Algorithm

```mermaid
graph TD
    Start[Worker polls for task] --> AU[Check Assigned Users]
    AU --> AUE{Any tasks in assigned users?}
    
    AUE -->|Yes| PRT[Process by priority]
    AUE -->|No| ST[Check Steal Targets]
    
    PRT --> CR{Critical queue has tasks?}
    CR -->|Yes| PC[Pop from Critical]
    CR -->|No| NR{Normal queue has ready tasks?}
    NR -->|Yes| PN[Pop from Normal]
    NR -->|No| NU[Next User]
    NU --> AUE
    
    ST --> STE{Any tasks in steal targets?}
    STE -->|Yes| PRT2[Process by priority]
    STE -->|No| Sleep[Sleep and retry]
    
    PRT2 --> CR2{Critical queue has tasks?}
    CR2 -->|Yes| PC2[Pop from Critical]
    CR2 -->|No| NR2{Normal queue has ready tasks?}
    NR2 -->|Yes| PN2[Pop from Normal]
    NR2 -->|No| NU2[Next Steal Target]
    NU2 --> STE
    
    PC --> Process[Process Task]
    PN --> Process
    PC2 --> Process
    PN2 --> Process
    Sleep --> Start
    
    Process --> Success{Task Success?}
    Success -->|Yes| Delete[Delete Task]
    Success -->|No| Retry{Retries Left?}
    Retry -->|Yes| Requeue[Requeue Task]
    Retry -->|No| DLQ[Send to DLQ]
    
    Delete --> Start
    Requeue --> Start
    DLQ --> Start
```

## Error Handling and Recovery

The worker implements comprehensive error handling for various failure scenarios.

```mermaid
sequenceDiagram
    participant W as Worker
    participant TH as TaskHandler
    participant Q as TaskQueue
    participant R as Redis
    participant L as Logger
    participant M as Metrics
    
    Note over W: Error handling scenarios
    
    alt Task Handler Exception
        W->>TH: process_task(task)
        TH-->>W: raise Exception
        W->>L: log exception details
        W->>M: increment task_error_count
        W->>TH: on_task_failure(task, exception, duration)
        
        alt Retriable error
            W->>W: increment retry count
            W->>Q: push(task) # requeue
        else Non-retriable or max retries
            W->>Q: send_to_dlq(task)
        end
        
    else Queue Connection Error
        W->>Q: pop()
        Q-->>W: Redis connection error
        W->>L: log connection error
        W->>M: increment connection_error_count
        W->>W: sleep(error_backoff_time)
        
    else Task Timeout
        W->>W: task execution timeout
        W->>L: log timeout
        W->>M: increment timeout_count
        W->>TH: on_task_failure(task, TimeoutError, timeout_duration)
        W->>Q: send_to_dlq(task)
        
    else Task Deserialization Error
        W->>Q: pop()
        Q-->>W: malformed task data
        W->>L: log deserialization error
        W->>M: increment deserialization_error_count
        W->>Q: send_to_dlq(malformed_task)
        
    else Thread Pool Exhaustion
        W->>W: submit task to executor
        W->>W: executor queue full
        W->>L: log thread pool exhaustion
        W->>M: increment thread_pool_error_count
        W->>Q: push(task) # requeue for later
    end
```

### Error Recovery Strategies

```mermaid
graph TD
    E[Error Detected] --> T{Error Type}
    
    T -->|Task Processing Error| R1[Check Retry Count]
    T -->|Connection Error| R2[Backoff and Retry]
    T -->|Timeout Error| R3[Send to DLQ]
    T -->|Deserialization Error| R4[Send to DLQ]
    T -->|Thread Pool Error| R5[Requeue Task]
    
    R1 --> RC{Retries < Max?}
    RC -->|Yes| RQ[Requeue with Increment]
    RC -->|No| DLQ[Send to DLQ]
    
    R2 --> B[Exponential Backoff]
    B --> C[Continue Polling]
    
    R3 --> DLQ
    R4 --> DLQ
    R5 --> RQ
    
    RQ --> L[Log Recovery Action]
    DLQ --> L
    C --> L
    
    L --> M[Update Metrics]
    M --> N[Continue Operation]
```

## Graceful Shutdown

The worker supports graceful shutdown to ensure running tasks complete safely.

```mermaid
sequenceDiagram
    participant A as Application
    participant W as Worker
    participant T as WorkerThread
    participant E as ThreadPoolExecutor
    participant AT as ActiveTasks
    participant Q as TaskQueue
    
    A->>W: stop(timeout)
    W->>W: set is_stopping = True
    W->>W: log shutdown initiated
    
    alt Worker thread is running
        W->>T: signal shutdown
        T->>T: finish current poll cycle
        T->>T: stop accepting new tasks
        
        W->>W: wait for active tasks
        W->>E: executor.shutdown(wait=False)
        
        loop Check active tasks
            W->>AT: get active task count
            AT-->>W: count
            
            alt Tasks still running
                W->>W: wait(check_interval)
                alt Timeout not exceeded
                    W->>W: continue waiting
                else Timeout exceeded
                    W->>E: force shutdown
                    W->>W: log forced termination
                    break
                end
            else All tasks completed
                W->>W: log graceful completion
                break
            end
        end
        
        W->>T: join thread
        T-->>W: thread finished
    end
    
    W->>Q: close connection (if owned)
    W->>W: cleanup resources
    W->>W: log shutdown complete
    W-->>A: shutdown finished
```

### Shutdown State Management

```mermaid
stateDiagram-v2
    [*] --> Stopped
    Stopped --> Starting : start()
    Starting --> Running : thread started
    Running --> Stopping : stop() called
    Running --> Error : critical error
    Stopping --> WaitingForTasks : active tasks exist
    Stopping --> Stopped : no active tasks
    WaitingForTasks --> Stopped : tasks completed or timeout
    Error --> Stopped : error handled
    
    Running : Polling for tasks
    Running : Processing tasks
    Running : Updating statistics
    
    Stopping : No new tasks accepted
    Stopping : Current cycle completes
    
    WaitingForTasks : Monitor active tasks
    WaitingForTasks : Graceful timeout
    
    Error : Handle error
    Error : Cleanup resources
```

## Performance Monitoring

The worker tracks comprehensive performance metrics for monitoring and optimization.

```mermaid
sequenceDiagram
    participant W as Worker
    participant M as Metrics
    participant S as Statistics
    participant H as HealthCheck
    
    loop Performance Monitoring
        W->>M: increment tasks_processed
        W->>M: record processing_time
        W->>M: update success/failure rates
        
        W->>S: calculate throughput
        W->>S: calculate average latency
        W->>S: calculate error rates
        
        W->>H: check worker health
        H->>H: validate queue connection
        H->>H: check thread pool status
        H->>H: monitor error rates
        H-->>W: health status
        
        alt Health check fails
            W->>W: log health issues
            W->>W: trigger alerts
        end
    end
```

### Key Performance Metrics

```mermaid
graph LR
    subgraph "Throughput Metrics"
        T1[Tasks Processed/sec]
        T2[Tasks Failed/sec]
        T3[Tasks Timeout/sec]
        T4[Queue Operations/sec]
    end
    
    subgraph "Latency Metrics"
        L1[Average Processing Time]
        L2[Queue Poll Latency]
        L3[Task Execution Time]
        L4[End-to-end Latency]
    end
    
    subgraph "Resource Metrics"
        R1[Thread Pool Utilization]
        R2[Memory Usage]
        R3[CPU Usage]
        R4[Redis Connection Count]
    end
    
    subgraph "Error Metrics"
        E1[Task Failure Rate]
        E2[Timeout Rate]
        E3[Connection Error Rate]
        E4[Retry Rate]
    end
    
    subgraph "Business Metrics"
        B1[Tasks by User]
        B2[Tasks by Priority]
        B3[Work Stealing Efficiency]
        B4[Queue Size Trends]
    end
```

### Health Check Implementation

```mermaid
sequenceDiagram
    participant H as HealthCheck
    participant W as Worker
    participant Q as TaskQueue
    participant R as Redis
    participant E as ThreadPoolExecutor
    
    H->>W: get_health()
    W->>W: check worker state
    
    W->>Q: ping connection
    Q->>R: ping()
    R-->>Q: pong
    Q-->>W: connection healthy
    
    W->>E: check thread pool
    E-->>W: pool status
    
    W->>W: calculate error rates
    W->>W: check last activity
    
    W->>W: aggregate health data
    W-->>H: health report
    
    H->>H: evaluate health status
    
    alt All checks pass
        H-->>W: status: healthy
    else Some issues detected
        H-->>W: status: degraded + warnings
    else Critical issues
        H-->>W: status: unhealthy + errors
    end
```

This comprehensive sequence documentation provides detailed insight into how FairQueue workers operate, from initialization through task processing to graceful shutdown, with robust error handling and performance monitoring throughout the lifecycle.
