# FairQueue Scheduler Process Sequence

## Overview

The FairQueue Scheduler is responsible for managing cron-based task scheduling with distributed locking. It ensures that scheduled tasks are executed at the right time across multiple scheduler instances without conflicts.

## Table of Contents
1. [Scheduler Initialization](#scheduler-initialization)
2. [Schedule Management](#schedule-management)
3. [Task Execution Cycle](#task-execution-cycle)
4. [Distributed Locking](#distributed-locking)
5. [Error Handling](#error-handling)
6. [Shutdown Process](#shutdown-process)

## Scheduler Initialization

The scheduler initialization process sets up the necessary components and validates the configuration.

```mermaid
sequenceDiagram
    participant A as Application
    participant S as TaskScheduler
    participant Q as TaskQueue
    participant R as Redis
    participant C as Croniter
    
    A->>S: TaskScheduler(queue, scheduler_id, ...)
    S->>S: validate parameters
    S->>Q: verify queue connection
    Q->>R: ping()
    R-->>Q: pong
    Q-->>S: connection confirmed
    S->>R: test Redis connection
    R-->>S: connection successful
    S->>S: initialize scheduler state
    S-->>A: scheduler ready
```

### Initialization Steps

1. **Parameter Validation**: Validate scheduler ID, queue instance, and configuration
2. **Queue Connection**: Verify that the TaskQueue is properly connected
3. **Redis Connection**: Test direct Redis connection for schedule storage
4. **State Initialization**: Set up internal state and threading components

## Schedule Management

### Adding a Schedule

```mermaid
sequenceDiagram
    participant C as Client
    participant S as TaskScheduler
    participant ST as ScheduledTask
    participant CR as Croniter
    participant R as Redis
    
    C->>S: add_schedule(cron_expr, user_id, priority, payload, timezone)
    S->>S: validate cron expression
    S->>ST: ScheduledTask.create(...)
    ST->>CR: validate cron expression
    CR-->>ST: validation result
    ST->>ST: calculate next_run time
    ST-->>S: scheduled task created
    S->>S: serialize scheduled task
    S->>R: HSET schedules_key schedule_id serialized_task
    R-->>S: success
    S->>S: log schedule addition
    S-->>C: return schedule_id
```

### Updating a Schedule

```mermaid
sequenceDiagram
    participant C as Client
    participant S as TaskScheduler
    participant R as Redis
    participant ST as ScheduledTask
    participant CR as Croniter
    
    C->>S: update_schedule(schedule_id, **kwargs)
    S->>R: HGET schedules_key schedule_id
    R-->>S: serialized_task or null
    
    alt Schedule exists
        S->>S: deserialize scheduled task
        S->>ST: update fields with kwargs
        
        alt Cron expression changed
            ST->>CR: validate new cron expression
            CR-->>ST: validation result
            ST->>ST: recalculate next_run time
        end
        
        S->>S: serialize updated task
        S->>R: HSET schedules_key schedule_id serialized_task
        R-->>S: success
        S-->>C: return True
    else Schedule not found
        S-->>C: return False
    end
```

### Removing a Schedule

```mermaid
sequenceDiagram
    participant C as Client
    participant S as TaskScheduler
    participant R as Redis
    
    C->>S: remove_schedule(schedule_id)
    S->>R: HDEL schedules_key schedule_id
    R-->>S: deletion count
    
    alt Schedule deleted
        S->>S: log schedule removal
        S-->>C: return True
    else Schedule not found
        S-->>C: return False
    end
```

## Task Execution Cycle

The main scheduler loop runs continuously, checking for due tasks and executing them.

```mermaid
sequenceDiagram
    participant S as TaskScheduler
    participant R as Redis
    participant L as DistributedLock
    participant ST as ScheduledTask
    participant Q as TaskQueue
    participant T as Task
    
    loop Scheduler Loop
        S->>S: sleep(check_interval)
        S->>L: acquire_lock(lock_key, lock_timeout)
        
        alt Lock acquired
            L-->>S: lock acquired
            S->>R: HGETALL schedules_key
            R-->>S: all schedules
            
            loop For each schedule
                S->>S: deserialize schedule
                S->>ST: check if due (next_run <= current_time)
                
                alt Task is due and active
                    ST-->>S: task is due
                    S->>T: Task.create(user_id, priority, payload)
                    T-->>S: task created
                    S->>Q: push(task)
                    Q-->>S: push result
                    
                    alt Push successful
                        S->>ST: update last_run = current_time
                        S->>ST: calculate new next_run
                        S->>S: serialize updated schedule
                        S->>R: HSET schedules_key schedule_id serialized_schedule
                        R-->>S: success
                        S->>S: log task execution
                    else Push failed
                        S->>S: log push error
                    end
                else Task not due or inactive
                    ST-->>S: skip task
                end
            end
            
            S->>L: release_lock()
            L-->>S: lock released
        else Lock not acquired
            L-->>S: lock acquisition failed
            S->>S: log lock failure (another scheduler running)
        end
    end
```

### Detailed Task Execution Flow

```mermaid
sequenceDiagram
    participant S as TaskScheduler
    participant ST as ScheduledTask
    participant CR as Croniter
    participant T as Task
    participant Q as TaskQueue
    participant R as Redis
    
    Note over S: Processing a due schedule
    
    S->>ST: check if task is due
    ST->>ST: current_time >= next_run?
    
    alt Task is due
        S->>T: Task.create(user_id, priority, payload)
        Note over T: Create task with unique ID
        T-->>S: task instance
        
        S->>Q: push(task)
        Q->>R: execute push.lua script
        R-->>Q: push result
        Q-->>S: success/failure
        
        alt Push successful
            S->>ST: update last_run = current_time
            S->>ST: set updated_at = current_time
            S->>CR: calculate next execution time
            CR-->>S: next_run timestamp
            S->>ST: update next_run
            
            S->>S: serialize updated schedule
            S->>R: HSET schedules_key schedule_id serialized_data
            R-->>S: confirmation
            
            S->>S: log successful execution
        else Push failed
            S->>S: log error but don't update schedule
            Note over S: Schedule will retry on next cycle
        end
    else Task not due
        Note over S: Skip this schedule
    end
```

## Distributed Locking

The scheduler uses Redis-based distributed locking to ensure only one scheduler instance processes tasks at a time.

```mermaid
sequenceDiagram
    participant S1 as Scheduler 1
    participant S2 as Scheduler 2
    participant R as Redis
    
    Note over S1,S2: Both schedulers attempt to acquire lock
    
    par Scheduler 1
        S1->>R: SET lock_key scheduler_id_1 PX lock_timeout NX
        R-->>S1: OK (lock acquired)
        S1->>S1: process schedules
        Note over S1: Processing schedules...
        S1->>R: DEL lock_key
        R-->>S1: 1 (lock released)
    and Scheduler 2
        S2->>R: SET lock_key scheduler_id_2 PX lock_timeout NX
        R-->>S2: nil (lock not acquired)
        S2->>S2: log: another scheduler is running
        S2->>S2: wait for next cycle
    end
    
    Note over S1,S2: Next cycle - roles may reverse
    
    par Scheduler 1
        S1->>S1: sleep(check_interval)
    and Scheduler 2
        S2->>R: SET lock_key scheduler_id_2 PX lock_timeout NX
        R-->>S2: OK (lock acquired)
        S2->>S2: process schedules
    end
```

### Lock Implementation Details

```mermaid
sequenceDiagram
    participant S as TaskScheduler
    participant R as Redis
    
    S->>S: prepare lock attempt
    S->>R: SET lock_key scheduler_id PX timeout NX
    
    alt Lock acquired (returns OK)
        R-->>S: "OK"
        S->>S: set lock_acquired = True
        S->>S: record lock_acquired_at = current_time
        Note over S: Proceed with schedule processing
        
        S->>S: process all schedules
        
        S->>R: DEL lock_key
        R-->>S: 1 (deleted)
        S->>S: set lock_acquired = False
        S->>S: log lock released
        
    else Lock not acquired (returns nil)
        R-->>S: nil
        S->>S: log: lock held by another scheduler
        S->>S: continue to next cycle
        
    else Redis error
        R-->>S: error
        S->>S: log Redis error
        S->>S: continue to next cycle
    end
```

## Error Handling

The scheduler implements comprehensive error handling for various failure scenarios.

```mermaid
sequenceDiagram
    participant S as TaskScheduler
    participant R as Redis
    participant Q as TaskQueue
    participant L as Logger
    participant M as Metrics
    
    Note over S: Error handling scenarios
    
    alt Redis Connection Error
        S->>R: connection attempt
        R-->>S: connection error
        S->>L: log error with details
        S->>M: increment redis_error_count
        S->>S: continue to next cycle
        
    else Lock Acquisition Timeout
        S->>R: SET lock_key ... (timeout)
        R-->>S: timeout
        S->>L: log timeout
        S->>M: increment lock_timeout_count
        S->>S: continue to next cycle
        
    else Schedule Deserialization Error
        S->>S: deserialize schedule data
        S->>S: JSON decode error
        S->>L: log malformed schedule data
        S->>M: increment deserialization_error_count
        S->>S: skip this schedule, continue
        
    else Task Push Error
        S->>Q: push(task)
        Q-->>S: push failed
        S->>L: log push failure
        S->>M: increment push_error_count
        Note over S: Don't update schedule, will retry
        
    else Cron Expression Error
        S->>S: calculate next run
        S->>S: invalid cron expression
        S->>L: log cron error
        S->>M: increment cron_error_count
        S->>S: disable schedule (set is_active = False)
        S->>R: update schedule in Redis
    end
```

### Error Recovery Strategies

```mermaid
graph TD
    E[Error Detected] --> T{Error Type}
    
    T -->|Connection Error| R1[Retry Connection]
    T -->|Lock Timeout| R2[Wait Next Cycle]
    T -->|Invalid Schedule| R3[Disable Schedule]
    T -->|Push Failure| R4[Keep Schedule Active]
    T -->|Cron Error| R5[Disable Schedule]
    
    R1 --> B1[Backoff Strategy]
    R2 --> C1[Continue Normal Operation]
    R3 --> U1[Update Schedule in Redis]
    R4 --> C2[Retry Next Cycle]
    R5 --> U2[Mark as Inactive]
    
    B1 --> C1
    U1 --> C1
    C2 --> C1
    U2 --> C1
    
    C1 --> L[Log Recovery Action]
    L --> M[Update Metrics]
```

## Shutdown Process

The scheduler supports graceful shutdown to ensure clean termination.

```mermaid
sequenceDiagram
    participant A as Application
    participant S as TaskScheduler
    participant T as SchedulerThread
    participant R as Redis
    participant L as DistributedLock
    
    A->>S: stop(timeout)
    S->>S: set is_running = False
    S->>S: log shutdown initiated
    
    alt Scheduler thread is running
        S->>T: signal shutdown
        T->>T: finish current cycle
        
        alt Has acquired lock
            T->>L: release_lock()
            L->>R: DEL lock_key
            R-->>L: confirmation
            L-->>T: lock released
        end
        
        T->>T: cleanup resources
        T-->>S: thread finished
        
        S->>S: wait for thread join (with timeout)
        
        alt Thread joined within timeout
            S->>S: log graceful shutdown completed
        else Timeout exceeded
            S->>S: log forced shutdown
            S->>T: force terminate (if possible)
        end
    else Thread not running
        S->>S: log already stopped
    end
    
    S->>S: cleanup scheduler resources
    S-->>A: shutdown complete
```

### Shutdown State Transitions

```mermaid
stateDiagram-v2
    [*] --> Stopped
    Stopped --> Starting : start()
    Starting --> Running : initialization complete
    Running --> Stopping : stop() called
    Running --> Error : critical error
    Stopping --> Stopped : graceful shutdown
    Error --> Stopped : error handling complete
    
    Running : Processing schedules
    Running : Lock management
    Running : Task execution
    
    Stopping : Finishing current cycle
    Stopping : Releasing locks
    Stopping : Cleanup resources
    
    Error : Handle error
    Error : Log error details
    Error : Update metrics
```

## Performance Considerations

### Optimization Strategies

1. **Schedule Caching**: Keep frequently accessed schedules in memory
2. **Batch Processing**: Process multiple due schedules in a single Redis transaction
3. **Lock Timeout Tuning**: Balance between scheduler availability and safety
4. **Check Interval Optimization**: Adjust based on schedule precision requirements

### Monitoring Points

```mermaid
graph LR
    subgraph "Scheduler Metrics"
        M1[Schedules Processed/sec]
        M2[Lock Acquisition Success Rate]
        M3[Task Push Success Rate]
        M4[Average Processing Time]
        M5[Error Rates by Type]
    end
    
    subgraph "Health Indicators"
        H1[Scheduler Uptime]
        H2[Last Successful Cycle]
        H3[Redis Connection Status]
        H4[Lock Contention]
    end
    
    subgraph "Performance Alerts"
        A1[High Error Rate]
        A2[Lock Acquisition Failures]
        A3[Processing Delays]
        A4[Redis Connection Issues]
    end
```

This sequence documentation provides a comprehensive understanding of how the FairQueue scheduler operates, from initialization through task execution to graceful shutdown, with robust error handling and performance monitoring throughout the process.
