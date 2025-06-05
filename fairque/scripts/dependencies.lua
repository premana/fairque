-- dependencies.lua: Task dependency management for FairQueue
-- Handles dependency resolution, deferred task management, and dependent task enqueueing

-- Load common utilities
local common = dofile("common.lua")

-- Additional constants for dependencies
local DEPENDENCY_PREFIX = "task:"
local DEPENDENT_SUFFIX = ":dependents"
local DEPENDENCIES_SUFFIX = ":dependencies"
local STATE_SUFFIX = ":state"
local DEFERRED_QUEUE = "queue:deferred"

-- Task states
local STATE_QUEUED = "queued"
local STATE_STARTED = "started"
local STATE_DEFERRED = "deferred"
local STATE_FINISHED = "finished"
local STATE_FAILED = "failed"
local STATE_CANCELED = "canceled"
local STATE_SCHEDULED = "scheduled"

-- Setup dependencies for a task
-- KEYS[1]: task_id
-- ARGV[1]: user_id
-- ARGV[2]: dependencies (JSON array of task IDs)
-- Returns: "deferred" if task should be deferred, "ready" if ready to queue
local function setup_dependencies(task_id, user_id, dependencies_json)
    local dependencies = cjson.decode(dependencies_json or "[]")
    
    if #dependencies == 0 then
        return "ready"
    end
    
    -- Check if all dependencies are finished
    local unfinished_deps = {}
    for _, dep_id in ipairs(dependencies) do
        local dep_state_key = DEPENDENCY_PREFIX .. dep_id .. STATE_SUFFIX
        local dep_state = redis.call("GET", dep_state_key)
        
        if dep_state ~= STATE_FINISHED then
            table.insert(unfinished_deps, dep_id)
            
            -- Register this task as dependent on the dependency
            local dep_dependents_key = DEPENDENCY_PREFIX .. dep_id .. DEPENDENT_SUFFIX
            redis.call("SADD", dep_dependents_key, task_id)
        end
    end
    
    if #unfinished_deps > 0 then
        -- Store remaining dependencies for this task
        local task_deps_key = DEPENDENCY_PREFIX .. task_id .. DEPENDENCIES_SUFFIX
        redis.call("DEL", task_deps_key)
        for _, dep_id in ipairs(unfinished_deps) do
            redis.call("SADD", task_deps_key, dep_id)
        end
        
        -- Set task state to deferred
        local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
        redis.call("SET", task_state_key, STATE_DEFERRED)
        
        -- Add to deferred queue for tracking
        redis.call("SADD", DEFERRED_QUEUE .. ":" .. user_id, task_id)
        
        return "deferred"
    end
    
    return "ready"
end

-- Mark task as started
-- KEYS[1]: task_id
-- Returns: "ok"
local function mark_task_started(task_id)
    local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    redis.call("SET", task_state_key, STATE_STARTED)
    return "ok"
end

-- Mark task as finished and enqueue dependents
-- KEYS[1]: task_id
-- ARGV[1]: user_id
-- ARGV[2]: auto_xcom (optional, "1" if enabled)
-- ARGV[3]: result_key (optional, XCom key for result)
-- Returns: array of dependent task IDs that were enqueued
local function mark_task_finished(task_id, user_id, auto_xcom, result_key)
    -- Set task state to finished
    local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    redis.call("SET", task_state_key, STATE_FINISHED)
    
    -- Get all dependents of this task
    local dependents_key = DEPENDENCY_PREFIX .. task_id .. DEPENDENT_SUFFIX
    local dependents = redis.call("SMEMBERS", dependents_key)
    
    local enqueued_dependents = {}
    
    -- Process each dependent
    for _, dependent_id in ipairs(dependents) do
        -- Remove this dependency from the dependent task
        local dependent_deps_key = DEPENDENCY_PREFIX .. dependent_id .. DEPENDENCIES_SUFFIX
        redis.call("SREM", dependent_deps_key, task_id)
        
        -- Check if dependent has any remaining dependencies
        local remaining_deps = redis.call("SCARD", dependent_deps_key)
        
        if remaining_deps == 0 then
            -- No more dependencies, move from deferred to ready
            local dependent_state_key = DEPENDENCY_PREFIX .. dependent_id .. STATE_SUFFIX
            local dependent_state = redis.call("GET", dependent_state_key)
            
            if dependent_state == STATE_DEFERRED then
                -- Set state to queued
                redis.call("SET", dependent_state_key, STATE_QUEUED)
                
                -- Remove from deferred queue
                redis.call("SREM", DEFERRED_QUEUE .. ":" .. user_id, dependent_id)
                
                table.insert(enqueued_dependents, dependent_id)
            end
        end
    end
    
    -- Clean up dependents list for this task
    redis.call("DEL", dependents_key)
    
    -- If auto_xcom is enabled and result_key provided, store result
    if auto_xcom == "1" and result_key and result_key ~= "" then
        common.update_stats("auto_xcom_pushes", 1)
    end
    
    return enqueued_dependents
end

-- Mark task as failed
-- KEYS[1]: task_id
-- ARGV[1]: user_id
-- ARGV[2]: error_message (optional)
-- Returns: "ok"
local function mark_task_failed(task_id, user_id, error_message)
    local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    redis.call("SET", task_state_key, STATE_FAILED)
    
    -- Store error message if provided
    if error_message and error_message ~= "" then
        local error_key = DEPENDENCY_PREFIX .. task_id .. ":error"
        redis.call("SET", error_key, error_message)
    end
    
    -- Update statistics
    common.update_stats("failed_tasks", 1)
    
    return "ok"
end

-- Cancel task and its dependents
-- KEYS[1]: task_id
-- ARGV[1]: user_id
-- Returns: array of canceled dependent task IDs
local function cancel_task_and_dependents(task_id, user_id)
    -- Set task state to canceled
    local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    redis.call("SET", task_state_key, STATE_CANCELED)
    
    -- Get all dependents (recursively)
    local function get_all_dependents(tid)
        local dependents_key = DEPENDENCY_PREFIX .. tid .. DEPENDENT_SUFFIX
        local direct_dependents = redis.call("SMEMBERS", dependents_key)
        local all_dependents = {}
        
        for _, dep_id in ipairs(direct_dependents) do
            table.insert(all_dependents, dep_id)
            -- Recursively get dependents of dependents
            local nested_deps = get_all_dependents(dep_id)
            for _, nested_dep in ipairs(nested_deps) do
                table.insert(all_dependents, nested_dep)
            end
        end
        
        return all_dependents
    end
    
    local canceled_dependents = get_all_dependents(task_id)
    
    -- Cancel all dependents
    for _, dep_id in ipairs(canceled_dependents) do
        local dep_state_key = DEPENDENCY_PREFIX .. dep_id .. STATE_SUFFIX
        redis.call("SET", dep_state_key, STATE_CANCELED)
        
        -- Remove from deferred queue if present
        redis.call("SREM", DEFERRED_QUEUE .. ":" .. user_id, dep_id)
    end
    
    -- Clean up dependency relationships
    local dependents_key = DEPENDENCY_PREFIX .. task_id .. DEPENDENT_SUFFIX
    redis.call("DEL", dependents_key)
    
    return canceled_dependents
end

-- Get task state
-- KEYS[1]: task_id
-- Returns: task state string
local function get_task_state(task_id)
    local task_state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    local state = redis.call("GET", task_state_key)
    return state or STATE_QUEUED
end

-- Get deferred tasks for user
-- KEYS[1]: user_id
-- Returns: array of deferred task IDs
local function get_deferred_tasks(user_id)
    return redis.call("SMEMBERS", DEFERRED_QUEUE .. ":" .. user_id)
end

-- Clean up completed task dependencies
-- KEYS[1]: task_id
-- Returns: "ok"
local function cleanup_task_dependencies(task_id)
    -- Clean up all dependency-related keys for this task
    local task_deps_key = DEPENDENCY_PREFIX .. task_id .. DEPENDENCIES_SUFFIX
    local dependents_key = DEPENDENCY_PREFIX .. task_id .. DEPENDENT_SUFFIX
    local state_key = DEPENDENCY_PREFIX .. task_id .. STATE_SUFFIX
    local error_key = DEPENDENCY_PREFIX .. task_id .. ":error"
    
    redis.call("DEL", task_deps_key, dependents_key, state_key, error_key)
    
    return "ok"
end

-- Main script execution
local operation = ARGV[1]

if operation == "setup" then
    return setup_dependencies(KEYS[1], ARGV[2], ARGV[3])
elseif operation == "start" then
    return mark_task_started(KEYS[1])
elseif operation == "finish" then
    return mark_task_finished(KEYS[1], ARGV[2], ARGV[3], ARGV[4])
elseif operation == "fail" then
    return mark_task_failed(KEYS[1], ARGV[2], ARGV[3])
elseif operation == "cancel" then
    return cancel_task_and_dependents(KEYS[1], ARGV[2])
elseif operation == "state" then
    return get_task_state(KEYS[1])
elseif operation == "deferred" then
    return get_deferred_tasks(KEYS[1])
elseif operation == "cleanup" then
    return cleanup_task_dependencies(KEYS[1])
else
    error("Invalid operation: " .. tostring(operation))
end