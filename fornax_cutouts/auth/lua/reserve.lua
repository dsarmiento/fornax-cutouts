-- Rolling-window cutout budget: events ZSET tracks job admission times (score = timestamp),
-- counts HASH maps job_id -> reserved/actual cutout count for jobs still in the window.
--
-- KEYS: 1=events (zset), 2=counts (hash)
-- ARGV: 1=job_id, 2=now, 3=window_seconds, 4=limit, 5=requested

local events_key = KEYS[1]
local counts_key = KEYS[2]

local job_id = ARGV[1]
local now = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local requested = tonumber(ARGV[5])

-- Sum cutouts currently reserved across all in-window jobs.
local used = sum_cutout_counts(events_key, counts_key, now, window)

-- Job already in the window; check how many cutouts are already reserved.
local reserved = 0
if redis.call('ZSCORE', events_key, job_id) ~= false then
    reserved = tonumber(redis.call('HGET', counts_key, job_id)) or 0
end
used = used - reserved

-- Reject if this request would exceed the limit; return oldest event time for retry-after.
if limit and used + requested > limit then
    drop_reservation(events_key, counts_key, job_id)
    local oldest_score = oldest_event_score(events_key)
    return {0, used, oldest_score}
end

-- Add the job to the window and set its cutout count.
add_reservation(events_key, counts_key, job_id, requested, now, window)

return {1, used, 0}
