-- Refund a reservation when job creation fails after a successful reserve.
--
-- KEYS: 1=events (zset), 2=counts (hash)
-- ARGV: 1=job_id

local events_key = KEYS[1]
local counts_key = KEYS[2]
local job_id = ARGV[1]

drop_reservation(events_key, counts_key, job_id)
return 1
