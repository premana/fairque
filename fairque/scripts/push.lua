-- push.lua: Push task to appropriate queue with priority-based routing
-- This script atomically adds a task to the correct queue and updates statistics

-- Load common functions (note: in practice, this would be loaded via external mechanism)
-- For now, we'll inline the necessary functions

-- Constants
local CRITICAL_SUFFIX = ":critical"
local NORMAL_SUFFIX = ":normal"
local STATS_KEY = "queue:stats"

-- Error codes
local ERR_INVALID_ARGS = "ERR_INVALID_ARGS"
local ERR_INVALID_PRIORITY = "ERR_INVALID_PRIORITY"
local ERR_QUEUE_OPERATION = "ERR_QUEUE_OPERATION"

-- Helper functions (simplified inline versions)
local function get_queue_key(user_id, priority)
    if tonumber(priority) == 6 then
        return "queue:user:" .. user_id .. CRITICAL_SUFFIX
    else
        return "queue:user:" .. user_id .. NORMAL_SUFFIX
    end
end

local function is_valid_priority(priority)
    local p = tonumber(priority)
    return p and p >= 1 and p <= 6
end

local function calculate_score(created_at, priority, current_time)
    local priority_weight = tonumber(priority) / 5.0
    local elapsed_time = current_time - tonumber(created_at)
    return tonumber(created_at) + (priority_weight * elapsed_time)
end

local function get_current_time()
    local time_result = redis.call("TIME")
    return tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
end

local function update_stats(stat_key, increment)
    increment = increment or 1
    local current_time = get_current_time()
    
    -- Update counter
    redis.call("HINCRBY", STATS_KEY, stat_key, increment)
    
    -- Update timestamp for rate calculations
    redis.call("HSET", STATS_KEY, stat_key .. ":last_update", current_time)
end

local function validate_task_args(task_args)
    if #task_args ~= 8 then
        return false, "Task must have exactly 8 arguments"
    end
    
    local task_id, user_id, priority, payload, retry_count, max_retries, created_at, execute_after = 
        task_args[1], task_args[2], task_args[3], task_args[4], task_args[5], task_args[6], task_args[7], task_args[8]
    
    -- Validate required fields
    if not task_id or task_id == "" then
        return false, "task_id cannot be empty"
    end
    
    if not user_id or user_id == "" then
        return false, "user_id cannot be empty"
    end
    
    if not is_valid_priority(priority) then
        return false, "Invalid priority: " .. tostring(priority)
    end
    
    if not payload then
        return false, "payload cannot be nil"
    end
    
    -- Validate numeric fields
    if not tonumber(retry_count) or tonumber(retry_count) < 0 then
        return false, "retry_count must be non-negative number"
    end
    
    if not tonumber(max_retries) or tonumber(max_retries) < 0 then
        return false, "max_retries must be non-negative number"
    end
    
    if not tonumber(created_at) or tonumber(created_at) <= 0 then
        return false, "created_at must be positive number"
    end
    
    if not tonumber(execute_after) or tonumber(execute_after) <= 0 then
        return false, "execute_after must be positive number"
    end
    
    return true, nil
end

-- Main push operation
-- ARGV: [task_id, user_id, priority, payload, retry_count, max_retries, created_at, execute_after]
-- Returns: JSON-like response with success status and details

-- Validate arguments
if #ARGV ~= 8 then
    return '{"success":false,"error_code":"ERR_INVALID_ARGS","message":"Expected 8 arguments, got ' .. #ARGV .. '"}'
end

local task_args = ARGV
local valid, error_msg = validate_task_args(task_args)
if not valid then
    return '{"success":false,"error_code":"ERR_INVALID_ARGS","message":"' .. error_msg .. '"}'
end

-- Extract task data
local task_id = task_args[1]
local user_id = task_args[2]
local priority = tonumber(task_args[3])
local payload = task_args[4]
local retry_count = tonumber(task_args[5])
local max_retries = tonumber(task_args[6])
local created_at = tonumber(task_args[7])
local execute_after = tonumber(task_args[8])

-- Get current time
local current_time = get_current_time()

-- Create task hash for storage
local task_hash_key = "task:" .. task_id
local task_hash_data = {
    "task_id", task_id,
    "user_id", user_id,
    "priority", tostring(priority),
    "payload", payload,
    "retry_count", tostring(retry_count),
    "max_retries", tostring(max_retries),
    "created_at", tostring(created_at),
    "execute_after", tostring(execute_after)
}

-- Check if task already exists
local existing_task = redis.call("EXISTS", task_hash_key)
if existing_task == 1 then
    return '{"success":false,"error_code":"ERR_QUEUE_OPERATION","message":"Task with ID ' .. task_id .. ' already exists"}'
end

-- Get appropriate queue key
local queue_key = get_queue_key(user_id, priority)

-- Store task data
redis.call("HMSET", task_hash_key, unpack(task_hash_data))

-- Add task to appropriate queue
local queue_operation_success = false
local score = nil

if priority == 6 then
    -- Critical priority: Add to FIFO list (left push for FIFO with right pop)
    local list_length = redis.call("LPUSH", queue_key, task_id)
    queue_operation_success = (list_length > 0)
    update_stats("tasks_pushed_critical", 1)
else
    -- Normal priority: Add to sorted set with calculated score
    score = calculate_score(created_at, priority, current_time)
    local zadd_result = redis.call("ZADD", queue_key, score, task_id)
    queue_operation_success = (zadd_result == 1)
    update_stats("tasks_pushed_normal", 1)
end

if not queue_operation_success then
    -- Rollback: Remove task data if queue operation failed
    redis.call("DEL", task_hash_key)
    return '{"success":false,"error_code":"ERR_QUEUE_OPERATION","message":"Failed to add task to queue"}'
end

-- Update general statistics
update_stats("tasks_pushed_total", 1)
update_stats("tasks_active", 1)

-- Update user-specific statistics
local user_stat_key = "user:" .. user_id .. ":tasks_pushed"
update_stats(user_stat_key, 1)

-- Update priority-specific statistics
local priority_stat_key = "priority:" .. tostring(priority) .. ":tasks_pushed"
update_stats(priority_stat_key, 1)

-- Build success response
local response_data = {
    task_id = task_id,
    user_id = user_id,
    priority = priority,
    queue_key = queue_key,
    pushed_at = current_time
}

if score then
    response_data.score = score
end

-- Return success response
local response = {
    success = true,
    data = response_data,
    stats = {
        tasks_active = redis.call("HGET", STATS_KEY, "tasks_active"),
        tasks_pushed_total = redis.call("HGET", STATS_KEY, "tasks_pushed_total")
    }
}

-- Simple JSON serialization
local function serialize_table(tbl)
    local items = {}
    for k, v in pairs(tbl) do
        local value_str
        if type(v) == "string" then
            value_str = '"' .. v .. '"'
        elseif type(v) == "table" then
            value_str = serialize_table(v)
        else
            value_str = tostring(v)
        end
        table.insert(items, '"' .. k .. '":' .. value_str)
    end
    return "{" .. table.concat(items, ",") .. "}"
end

return serialize_table(response)
