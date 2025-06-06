-- push.lua with unified state management and fq: prefix
-- This script implements task enqueue with dependency handling and state tracking

-- Redis key prefixes with fq: namespace
local QUEUE_PREFIX = "fq:queue:user:"
local STATE_PREFIX = "fq:state:"
local TASK_PREFIX = "fq:task:"
local DEPS_PREFIX = "fq:deps:"
local STATS_KEY = "fq:stats"

-- Error codes
local ERR_INVALID_ARGS = "ERR_INVALID_ARGS"
local ERR_SERIALIZATION = "ERR_SERIALIZATION"

-- Helper functions
local function get_current_time()
    local time_result = redis.call("TIME")
    return tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
end

local function get_queue_key(user_id, priority)
    if tonumber(priority) == 6 then
        return QUEUE_PREFIX .. user_id .. ":critical"
    else
        return QUEUE_PREFIX .. user_id .. ":normal"
    end
end

local function calculate_score(task_data, current_time)
    local created_at = tonumber(task_data.created_at)
    local priority = tonumber(task_data.priority)
    local elapsed_time = current_time - created_at
    local priority_weight = priority / 5.0
    return created_at + (priority_weight * elapsed_time)
end

local function update_stats(stat_key, increment)
    increment = increment or 1
    local current_time = get_current_time()
    
    redis.call("HINCRBY", STATS_KEY, stat_key, increment)
    redis.call("HSET", STATS_KEY, stat_key .. ":last_update", current_time)
end

-- Main push operation
-- ARGV: [task_json]

-- Validate arguments
if #ARGV ~= 1 then
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"Expected 1 argument (task_json), got ' .. #ARGV .. '"}'
end

local task_json = ARGV[1]
if not task_json or task_json == "" then
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"task_json cannot be empty"}'
end

-- Parse task data
local task_data
local ok, err = pcall(function()
    task_data = cjson.decode(task_json)
end)

if not ok then
    return '{"success":false,"error_code":"' .. ERR_SERIALIZATION .. '","message":"Failed to parse task JSON: ' .. tostring(err) .. '"}'
end

-- Extract task fields
local task_id = task_data.task_id
local user_id = task_data.user_id
local priority = tonumber(task_data.priority)
local depends_on = task_data.depends_on or {}
local execute_after = tonumber(task_data.execute_after) or 0
local current_time = get_current_time()

-- Validate required fields
if not task_id or task_id == "" then
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"task_id is required"}'
end

if not user_id or user_id == "" then
    return '{"success":false,"error_code":"' .. ERR_INVALID_ARGS .. '","message":"user_id is required"}'
end

-- Store task data with enhanced fields
redis.call("HMSET", TASK_PREFIX .. task_id,
    "task_id", task_id,
    "user_id", user_id,
    "priority", tostring(priority),
    "payload", task_json,
    "depends_on", cjson.encode(depends_on),
    "created_at", current_time,
    "execute_after", execute_after,
    "state", "queued",
    "retry_count", "0",
    "max_retries", tostring(task_data.max_retries or 3))

-- Determine placement based on scheduling and dependencies
local initial_state = "queued"

if execute_after > current_time then
    -- Scheduled task - goes to scheduled state
    initial_state = "scheduled"
    redis.call("ZADD", STATE_PREFIX .. "scheduled", execute_after, task_id)
    redis.call("HSET", TASK_PREFIX .. task_id, "state", "scheduled")
    
elseif #depends_on > 0 then
    -- Has dependencies - goes to queued registry with deferred state
    initial_state = "deferred"
    redis.call("ZADD", STATE_PREFIX .. "queued", current_time, task_id)
    redis.call("HSET", TASK_PREFIX .. task_id, "state", "deferred")
    
    -- Setup dependency tracking but don't add to user queue yet
    for _, dep_task_id in ipairs(depends_on) do
        redis.call("SADD", DEPS_PREFIX .. "waiting:" .. dep_task_id, task_id)
        redis.call("SADD", DEPS_PREFIX .. "blocked:" .. task_id, dep_task_id)
    end
    
else
    -- Ready for immediate execution - goes to queued registry with queued state
    redis.call("ZADD", STATE_PREFIX .. "queued", current_time, task_id)
    redis.call("HSET", TASK_PREFIX .. task_id, "state", "queued")
    
    -- Add to user queue for immediate execution
    local queue_key = get_queue_key(user_id, priority)
    
    if priority == 6 then
        redis.call("LPUSH", queue_key, task_id)
    else
        local score = calculate_score(task_data, current_time)
        redis.call("ZADD", queue_key, score, task_id)
    end
end

-- Update statistics
update_stats("tasks_pushed_total", 1)
update_stats("tasks_" .. initial_state, 1)

local user_stat_key = "user:" .. user_id .. ":tasks_pushed"
update_stats(user_stat_key, 1)

local priority_stat_key = "priority:" .. tostring(priority) .. ":tasks_pushed"
update_stats(priority_stat_key, 1)

-- Build success response
local response = {
    success = true,
    task_id = task_id,
    state = initial_state,
    stats = {
        tasks_queued = redis.call("HGET", STATS_KEY, "tasks_queued"),
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
        elseif v == nil then
            value_str = "null"
        else
            value_str = tostring(v)
        end
        table.insert(items, '"' .. k .. '":' .. value_str)
    end
    return "{" .. table.concat(items, ",") .. "}"
end

return serialize_table(response)