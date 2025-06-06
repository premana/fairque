# FairQueue Internal Data Structures and Dependency Management

## Overview

This document provides detailed technical documentation of FairQueue's internal Redis data structures, with a focus on the dependency management system. Understanding these internals is essential for developers working on FairQueue core features or debugging complex dependency scenarios.

## Table of Contents

1. [Redis Key Structure](#redis-key-structure)
2. [Dependency Management System](#dependency-management-system)
3. [Key Usage Patterns](#key-usage-patterns)
4. [Bidirectional Dependency Tracking](#bidirectional-dependency-tracking)
5. [State Transitions and Cleanup](#state-transitions-and-cleanup)
6. [Examples and Use Cases](#examples-and-use-cases)

## Redis Key Structure

FairQueue uses a unified namespace with the `fq:` prefix for all Redis keys to ensure consistency and avoid conflicts.

### Core Queue Keys

```
# Task queues by user and priority
fq:queue:user:{user_id}:critical    # Priority.CRITICAL tasks (List, FIFO)
fq:queue:user:{user_id}:normal      # Priority 1-5 tasks (Sorted Set, Score-based)

# State management
fq:state:{state}                    # Task state registries (Set)
fq:task:{task_id}                   # Task metadata & dependencies (Hash)

# Dependency tracking (bidirectional)
fq:deps:waiting:{task_id}           # Tasks waiting on this task (Set)
fq:deps:blocked:{task_id}           # Tasks this task is blocked by (Set)

# Cross-task communication and scheduling
fq:xcom:{key}                       # XCom data storage (Hash with TTL)
fq:stats                           # Unified statistics (Hash)
fq:schedules                       # Scheduled tasks (Hash)
fq:scheduler:lock                  # Scheduler distributed lock
```

## Dependency Management System

FairQueue implements a sophisticated dependency management system using bidirectional Redis sets to track task relationships efficiently.

### Core Concepts

- **Dependency**: A relationship where Task B cannot execute until Task A completes
- **Waiting Set**: Tasks that are waiting for a specific task to complete
- **Blocked Set**: Tasks that are blocking a specific task from executing
- **Deferred State**: Tasks waiting for dependencies remain in this state until ready

### Key Components

#### 1. `fq:deps:waiting:{task_id}`

**Purpose**: Stores task IDs that are waiting for `{task_id}` to complete.

**Data Type**: Redis SET

**Usage**:
- **Creation**: When Task B depends on Task A, Task B's ID is added to `fq:deps:waiting:A`
- **Reading**: When Task A completes, read this set to find all dependent tasks
- **Cleanup**: When Task A is canceled or completed, this set is processed and deleted

**Implementation Location**: 
- Created in: `fairque/scripts/push.lua` (line 115)
- Read in: `fairque/scripts/state_ops.lua` (line 86)
- Cleaned in: `fairque/scripts/state_ops.lua` (lines 213-216)

#### 2. `fq:deps:blocked:{task_id}`

**Purpose**: Stores task IDs that are blocking `{task_id}` from executing.

**Data Type**: Redis SET

**Usage**:
- **Creation**: When Task B depends on Task A, Task A's ID is added to `fq:deps:blocked:B`
- **Reading**: To check if a task has remaining dependencies (`SCARD`)
- **Modification**: Remove dependency IDs as they complete
- **Cleanup**: Delete when task completes or is canceled

**Implementation Location**:
- Created in: `fairque/scripts/push.lua` (line 116)
- Read in: `fairque/scripts/state_ops.lua` (lines 94, 277)
- Modified in: `fairque/scripts/state_ops.lua` (line 91)
- Cleaned in: `fairque/scripts/state_ops.lua` (lines 121, 223, 247)

## Key Usage Patterns

### 1. Task Enqueuing with Dependencies

```lua
-- In push.lua (lines 114-117)
for _, dep_task_id in ipairs(depends_on) do
    redis.call("SADD", DEPS_PREFIX .. "waiting:" .. dep_task_id, task_id)
    redis.call("SADD", DEPS_PREFIX .. "blocked:" .. task_id, dep_task_id)
end
```

**Process**:
1. For each dependency, add the new task to the dependency's waiting set
2. Add each dependency to the new task's blocked set
3. Set task state to `DEFERRED` instead of `QUEUED`

### 2. Dependency Resolution on Task Completion

```lua
-- In state_ops.lua (lines 86-102)
local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)

for _, waiting_task_id in ipairs(waiting_tasks) do
    -- Remove this dependency from waiting task
    redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
    
    -- Check if all dependencies are now resolved
    local remaining_deps = redis.call("SCARD", DEPS_PREFIX .. "blocked:" .. waiting_task_id)
    
    if remaining_deps == 0 then
        -- All dependencies resolved - transition DEFERRED -> QUEUED
        -- Add task to user queue for execution
    end
end
```

**Process**:
1. Find all tasks waiting for the completed task
2. Remove the completed task from each waiting task's blocked set
3. Check if any waiting tasks now have zero dependencies
4. Move ready tasks from `DEFERRED` to `QUEUED` state

### 3. Cleanup on Task Cancellation

```lua
-- In state_ops.lua (lines 213-223)
-- Clean up forward references (tasks waiting for this one)
local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)
for _, waiting_task_id in ipairs(waiting_tasks) do
    redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
end
redis.call("DEL", DEPS_PREFIX .. "waiting:" .. task_id)

-- Clean up reverse references (tasks this one was waiting for)
local blocked_by = redis.call("SMEMBERS", DEPS_PREFIX .. "blocked:" .. task_id)
for _, blocking_task_id in ipairs(blocked_by) do
    redis.call("SREM", DEPS_PREFIX .. "waiting:" .. blocking_task_id, task_id)
end
redis.call("DEL", DEPS_PREFIX .. "blocked:" .. task_id)
```

**Process**:
1. Clean up forward references (remove canceled task from waiting sets)
2. Clean up reverse references (remove canceled task from blocked sets)
3. Delete both dependency sets for the canceled task

## Bidirectional Dependency Tracking

The system uses complementary data structures to enable efficient dependency management:

### Relationship Structure

```
If Task B depends on Task A:

fq:deps:waiting:A = {B}    # Task A has Task B waiting for it
fq:deps:blocked:B = {A}    # Task B is blocked by Task A
```

### Benefits of Bidirectional Design

1. **Forward Resolution**: When Task A completes, quickly find all dependent tasks
2. **Reverse Lookup**: When Task B is canceled, quickly clean up references
3. **Dependency Counting**: Use `SCARD fq:deps:blocked:B` to check remaining dependencies
4. **Atomic Operations**: All dependency operations are atomic within Lua scripts

### Complex Dependency Example

```
Scenario:
- Task D depends on Tasks A, B, C
- Task E depends on Tasks A, B  
- Task F depends on Task C

Redis Structure:
fq:deps:waiting:A = {D, E}     # A is waited on by D, E
fq:deps:waiting:B = {D, E}     # B is waited on by D, E
fq:deps:waiting:C = {D, F}     # C is waited on by D, F

fq:deps:blocked:D = {A, B, C}  # D is blocked by A, B, C
fq:deps:blocked:E = {A, B}     # E is blocked by A, B
fq:deps:blocked:F = {C}        # F is blocked by C
```

### Resolution Flow Example

1. **Task A Completes**:
   - Read `fq:deps:waiting:A = {D, E}`
   - Remove A from `fq:deps:blocked:D` → `{B, C}`
   - Remove A from `fq:deps:blocked:E` → `{B}`
   - Both D and E still have dependencies, remain `DEFERRED`

2. **Task B Completes**:
   - Read `fq:deps:waiting:B = {D, E}`
   - Remove B from `fq:deps:blocked:D` → `{C}`
   - Remove B from `fq:deps:blocked:E` → `{}` (empty)
   - **Task E transitions to `QUEUED` state** (no remaining dependencies)

3. **Task C Completes**:
   - Read `fq:deps:waiting:C = {D, F}`
   - Remove C from `fq:deps:blocked:D` → `{}` (empty)
   - Remove C from `fq:deps:blocked:F` → `{}` (empty)
   - **Both Task D and F transition to `QUEUED` state**

## State Transitions and Cleanup

### Task States Related to Dependencies

- **`SCHEDULED`**: Waiting for `execute_after` time (independent of dependencies)
- **`DEFERRED`**: Has unresolved dependencies, cannot be queued yet
- **`QUEUED`**: Ready for execution (all dependencies resolved)
- **`STARTED`**: Currently executing
- **`FINISHED`**: Completed successfully (triggers dependency resolution)
- **`FAILED`**: Failed execution (dependencies are not resolved)
- **`CANCELED`**: Manually canceled (triggers cleanup)

### State Transition Rules

1. **Task with dependencies** → `DEFERRED` state initially
2. **All dependencies resolved** → `DEFERRED` → `QUEUED`
3. **Task completion** → Resolve dependencies for waiting tasks
4. **Task failure** → Dependencies remain unresolved, waiting tasks stay `DEFERRED`
5. **Task cancellation** → Clean up all dependency references

### Cleanup Mechanisms

1. **Completion Cleanup**: Delete dependency sets when task finishes
2. **Cancellation Cleanup**: Bidirectional cleanup of all references
3. **TTL Cleanup**: Automatic expiration of failed task data (7 days)
4. **Maintenance Cleanup**: Periodic cleanup of orphaned dependency data

## Examples and Use Cases

### Example 1: Linear Pipeline

```python
@task(task_id="extract")
def extract_data():
    return {"records": 1000}

@task(task_id="transform", depends_on=["extract"])
def transform_data():
    return {"processed": 2000}

@task(task_id="load", depends_on=["transform"])  
def load_data():
    return {"loaded": True}
```

**Redis Structure**:
```
fq:deps:waiting:extract = {transform}
fq:deps:waiting:transform = {load}

fq:deps:blocked:transform = {extract}
fq:deps:blocked:load = {transform}
```

**Execution Flow**:
1. All tasks created in `DEFERRED` state (except `extract`)
2. `extract` completes → `transform` moves to `QUEUED`
3. `transform` completes → `load` moves to `QUEUED`

### Example 2: Fan-out/Fan-in Pattern

```python
@task(task_id="prepare")
def prepare_data():
    return {"data": "prepared"}

@task(task_id="process_a", depends_on=["prepare"])
def process_a():
    return {"result_a": "done"}

@task(task_id="process_b", depends_on=["prepare"])
def process_b():
    return {"result_b": "done"}

@task(task_id="combine", depends_on=["process_a", "process_b"])
def combine_results():
    return {"combined": "complete"}
```

**Redis Structure**:
```
fq:deps:waiting:prepare = {process_a, process_b}
fq:deps:waiting:process_a = {combine}
fq:deps:waiting:process_b = {combine}

fq:deps:blocked:process_a = {prepare}
fq:deps:blocked:process_b = {prepare}
fq:deps:blocked:combine = {process_a, process_b}
```

**Execution Flow**:
1. `prepare` completes → both `process_a` and `process_b` move to `QUEUED`
2. `process_a` completes → `combine` still has dependency on `process_b`
3. `process_b` completes → `combine` moves to `QUEUED`

### Example 3: Cycle Detection

FairQueue prevents dependency cycles during task creation. If a cycle is detected, the task creation fails with a validation error, preventing deadlock situations.

## Performance Considerations

### Optimization Strategies

1. **Atomic Operations**: All dependency operations use Lua scripts for atomicity
2. **Efficient Set Operations**: Redis SET operations are O(1) for add/remove
3. **Minimal Data Storage**: Only store task IDs, not full task objects
4. **Lazy Cleanup**: Dependency data is cleaned up when tasks complete
5. **TTL Management**: Failed tasks and their dependencies auto-expire

### Monitoring Points

- **Dependency Set Sizes**: Monitor `SCARD` of dependency sets
- **Deferred Task Count**: Track tasks in `DEFERRED` state
- **Dependency Resolution Time**: Time from dependency completion to task queuing
- **Orphaned Dependencies**: Dependency sets without corresponding tasks

This internal documentation provides the technical foundation for understanding and working with FairQueue's sophisticated dependency management system.