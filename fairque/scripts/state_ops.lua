-- State operations with unified state management and dependency resolution
-- This script handles state transitions, dependency resolution, and cleanup

-- Redis key prefixes with fq: namespace
local QUEUE_PREFIX = "fq:queue:user:"
local STATE_PREFIX = "fq:state:"
local TASK_PREFIX = "fq:task:"
local DEPS_PREFIX = "fq:deps:"
local STATS_KEY = "fq:stats"

-- TTL configuration (seconds)
local FINISHED_TTL = 86400   -- 1 day
local FAILED_TTL = 604800    -- 7 days
local CANCELED_TTL = 3600    -- 1 hour

-- Error codes
local ERR_INVALID_ARGS = "ERR_INVALID_ARGS"
local ERR_TASK_NOT_FOUND = "ERR_TASK_NOT_FOUND"
local ERR_INVALID_STATE = "ERR_INVALID_STATE"

-- Helper functions
local function get_current_time()
    local time_result = redis.call("TIME")
    return tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
end

local function update_stats(stat_key, increment)
    increment = increment or 1
    local current_time = get_current_time()
    
    redis.call("HINCRBY", STATS_KEY, stat_key, increment)
    redis.call("HSET", STATS_KEY, stat_key .. ":last_update", current_time)
end

local function get_queue_key(user_id, priority)
    if tonumber(priority) == 6 then
        return QUEUE_PREFIX .. user_id .. ":critical"
    else
        return QUEUE_PREFIX .. user_id .. ":normal"
    end
end

local function calculate_score(created_at, priority, current_time)
    local elapsed_time = current_time - created_at
    local priority_weight = priority / 5.0
    return created_at + (priority_weight * elapsed_time)
end

-- Main command routing
local command = ARGV[1]
if not command then
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"Command is required"}'
end

local current_time = get_current_time()

if command == "finish" then
    -- ARGV: [command, task_id, result_data]
    if #ARGV ~= 3 then
        return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"finish requires task_id and result_data"}'
    end
    
    local task_id = ARGV[2]
    local result_data = ARGV[3]
    
    -- Verify task exists and is in started state
    local current_state = redis.call("HGET", TASK_PREFIX .. task_id, "state")
    if not current_state then
        return '{"success":false,"error_code":"' .. ERR_TASK_NOT_FOUND .. '","message":"Task not found"}'
    end
    
    if current_state ~= "started" then
        return '{"success":false,"error_code":"' .. ERR_INVALID_STATE .. '","message":"Task must be in started state to finish"}'
    end
    
    -- Transition STARTED -> FINISHED
    redis.call("ZREM", STATE_PREFIX .. "started", task_id)
    redis.call("ZADD", STATE_PREFIX .. "finished", current_time, task_id)
    redis.call("HMSET", TASK_PREFIX .. task_id,
        "state", "finished",
        "finished_at", current_time,
        "result", result_data)
    redis.call("EXPIRE", TASK_PREFIX .. task_id, FINISHED_TTL)
    
    -- Resolve dependencies - check tasks waiting for this one
    local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)
    local ready_tasks = {}
    
    for _, waiting_task_id in ipairs(waiting_tasks) do
        -- Remove this dependency from waiting task
        redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
        
        -- Check if all dependencies are now resolved
        local remaining_deps = redis.call("SCARD", DEPS_PREFIX .. "blocked:" .. waiting_task_id)
        
        if remaining_deps == 0 then
            -- All dependencies resolved - transition DEFERRED -> QUEUED and add to user queue
            redis.call("HSET", TASK_PREFIX .. waiting_task_id, "state", "queued")
            
            local user_id = redis.call("HGET", TASK_PREFIX .. waiting_task_id, "user_id")
            local priority = tonumber(redis.call("HGET", TASK_PREFIX .. waiting_task_id, "priority"))
            local created_at = tonumber(redis.call("HGET", TASK_PREFIX .. waiting_task_id, "created_at"))
            
            if user_id and priority then
                local queue_key = get_queue_key(user_id, priority)
                
                if priority == 6 then
                    redis.call("LPUSH", queue_key, waiting_task_id)
                else
                    local score = calculate_score(created_at, priority, current_time)
                    redis.call("ZADD", queue_key, score, waiting_task_id)
                end
                
                table.insert(ready_tasks, waiting_task_id)
            end
        end
    end
    
    -- Clean up dependency tracking for completed task
    redis.call("DEL", DEPS_PREFIX .. "waiting:" .. task_id)
    redis.call("DEL", DEPS_PREFIX .. "blocked:" .. task_id)
    
    -- Update statistics
    update_stats("tasks_finished", 1)
    
    return '{"success":true,"ready_tasks":' .. cjson.encode(ready_tasks) .. '}'

elseif command == "fail" then
    -- ARGV: [command, task_id, error_message, failure_type]
    if #ARGV < 3 then
        return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"fail requires task_id and error_message"}'
    end
    
    local task_id = ARGV[2]
    local error_msg = ARGV[3]
    local failure_type = ARGV[4] or "failed"
    
    -- Verify task exists
    local current_state = redis.call("HGET", TASK_PREFIX .. task_id, "state")
    if not current_state then
        return '{"success":false,"error_code":"' .. ERR_TASK_NOT_FOUND .. '","message":"Task not found"}'
    end
    
    -- Get current retry info for enhanced failure tracking
    local retry_count = tonumber(redis.call("HGET", TASK_PREFIX .. task_id, "retry_count") or "0")
    local retry_history = redis.call("HGET", TASK_PREFIX .. task_id, "retry_history") or "[]"
    local history = cjson.decode(retry_history)
    
    -- Add new retry entry
    table.insert(history, {
        attempt = retry_count + 1,
        failed_at = current_time,
        error = error_msg,
        failure_type = failure_type
    })
    
    -- Transition to failed state
    redis.call("ZREM", STATE_PREFIX .. "started", task_id)
    redis.call("ZREM", STATE_PREFIX .. "queued", task_id)  -- In case it was queued
    redis.call("ZADD", STATE_PREFIX .. "failed", current_time, task_id)
    
    -- Store enhanced failure data
    redis.call("HMSET", TASK_PREFIX .. task_id,
        "state", "failed",
        "failure_type", failure_type,
        "error_message", error_msg,
        "retry_history", cjson.encode(history),
        "finished_at", current_time,
        "retry_count", retry_count + 1)
    
    -- Apply TTL
    redis.call("EXPIRE", TASK_PREFIX .. task_id, FAILED_TTL)
    
    -- Update statistics
    update_stats("tasks_failed", 1)
    
    return '{"success":true,"failure_type":"' .. failure_type .. '"}'

elseif command == "cancel" then
    -- ARGV: [command, task_id, reason]
    if #ARGV < 2 then
        return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"cancel requires task_id"}'
    end
    
    local task_id = ARGV[2]
    local reason = ARGV[3] or "Manually canceled"
    
    -- Verify task exists
    local current_state = redis.call("HGET", TASK_PREFIX .. task_id, "state")
    if not current_state then
        return '{"success":false,"error_code":"' .. ERR_TASK_NOT_FOUND .. '","message":"Task not found"}'
    end
    
    -- Remove from current state
    redis.call("ZREM", STATE_PREFIX .. current_state, task_id)
    
    -- Remove from user queues if present
    local user_id = redis.call("HGET", TASK_PREFIX .. task_id, "user_id")
    if user_id then
        redis.call("LREM", QUEUE_PREFIX .. user_id .. ":critical", 0, task_id)
        redis.call("ZREM", QUEUE_PREFIX .. user_id .. ":normal", task_id)
    end
    
    -- Add to canceled state
    redis.call("ZADD", STATE_PREFIX .. "canceled", current_time, task_id)
    redis.call("HMSET", TASK_PREFIX .. task_id,
        "state", "canceled",
        "canceled_at", current_time,
        "reason", reason)
    redis.call("EXPIRE", TASK_PREFIX .. task_id, CANCELED_TTL)
    
    -- Clean up dependencies
    local waiting_tasks = redis.call("SMEMBERS", DEPS_PREFIX .. "waiting:" .. task_id)
    for _, waiting_task_id in ipairs(waiting_tasks) do
        redis.call("SREM", DEPS_PREFIX .. "blocked:" .. waiting_task_id, task_id)
    end
    redis.call("DEL", DEPS_PREFIX .. "waiting:" .. task_id)
    
    local blocked_by = redis.call("SMEMBERS", DEPS_PREFIX .. "blocked:" .. task_id)
    for _, blocking_task_id in ipairs(blocked_by) do
        redis.call("SREM", DEPS_PREFIX .. "waiting:" .. blocking_task_id, task_id)
    end
    redis.call("DEL", DEPS_PREFIX .. "blocked:" .. task_id)
    
    -- Update statistics
    update_stats("tasks_canceled", 1)
    
    return '{"success":true}'

elseif command == "cleanup" then
    -- ARGV: [command, state, max_age_seconds]
    if #ARGV ~= 3 then
        return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"cleanup requires state and max_age_seconds"}'
    end
    
    local state = ARGV[2]
    local max_age = tonumber(ARGV[3])
    local cutoff = current_time - max_age
    
    -- Get expired task IDs for cleanup
    local expired_tasks = redis.call("ZRANGEBYSCORE", STATE_PREFIX .. state, 0, cutoff)
    
    -- Clean up task data and dependency tracking
    for _, task_id in ipairs(expired_tasks) do
        redis.call("DEL", TASK_PREFIX .. task_id)
        redis.call("DEL", DEPS_PREFIX .. "waiting:" .. task_id)
        redis.call("DEL", DEPS_PREFIX .. "blocked:" .. task_id)
    end
    
    -- Remove from state registry
    local removed = redis.call("ZREMRANGEBYSCORE", STATE_PREFIX .. state, 0, cutoff)
    
    return '{"success":true,"removed":' .. removed .. '}'

elseif command == "stats" then
    -- Get comprehensive state statistics
    local stats = {
        queued = redis.call("ZCARD", STATE_PREFIX .. "queued"),
        started = redis.call("ZCARD", STATE_PREFIX .. "started"),
        scheduled = redis.call("ZCARD", STATE_PREFIX .. "scheduled"),
        finished = redis.call("ZCARD", STATE_PREFIX .. "finished"),
        failed = redis.call("ZCARD", STATE_PREFIX .. "failed"),
        canceled = redis.call("ZCARD", STATE_PREFIX .. "canceled")
    }
    
    return '{"success":true,"stats":' .. cjson.encode(stats) .. '}'

elseif command == "process_scheduled" then
    -- ARGV: [command]
    -- Process scheduled tasks that are ready to run
    
    local ready_scheduled = redis.call("ZRANGEBYSCORE", STATE_PREFIX .. "scheduled", 0, current_time)
    local processed = 0
    
    for _, task_id in ipairs(ready_scheduled) do
        -- Check if task still has dependencies
        local remaining_deps = redis.call("SCARD", DEPS_PREFIX .. "blocked:" .. task_id)
        
        -- Move from scheduled to queued registry
        redis.call("ZREM", STATE_PREFIX .. "scheduled", task_id)
        redis.call("ZADD", STATE_PREFIX .. "queued", current_time, task_id)
        
        if remaining_deps == 0 then
            -- No dependencies - set QUEUED state and add to user queue immediately
            redis.call("HSET", TASK_PREFIX .. task_id, "state", "queued")
            
            local user_id = redis.call("HGET", TASK_PREFIX .. task_id, "user_id")
            local priority = tonumber(redis.call("HGET", TASK_PREFIX .. task_id, "priority"))
            local created_at = tonumber(redis.call("HGET", TASK_PREFIX .. task_id, "created_at"))
            
            if user_id and priority then
                local queue_key = get_queue_key(user_id, priority)
                
                if priority == 6 then
                    redis.call("LPUSH", queue_key, task_id)
                else
                    local score = calculate_score(created_at, priority, current_time)
                    redis.call("ZADD", queue_key, score, task_id)
                end
            end
        else
            -- Still has dependencies - set DEFERRED state but stay in queued registry
            redis.call("HSET", TASK_PREFIX .. task_id, "state", "deferred")
        end
        
        processed = processed + 1
    end
    
    return '{"success":true,"processed":' .. processed .. '}'

else
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"Unknown command: ' .. command .. '"}'
end