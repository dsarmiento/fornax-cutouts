from __future__ import annotations

import time
from typing import Any, Callable

from redis import Redis as SyncRedisClient
from redis import RedisCluster as SyncRedisCluster
from redis.asyncio import Redis as AsyncRedisClient
from redis.asyncio import RedisCluster as AsyncRedisCluster

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import CutoutLimitKeys
from fornax_cutouts.models.auth import Principal
from fornax_cutouts.utils.exceptions import CutoutLimitExceededError
from fornax_cutouts.utils.logging import get_logger

# KEYS: 1=events (zset), 2=counts (hash)
# ARGV: 1=job_id, 2=now, 3=window_seconds, 4=limit, 5=requested
# Expires old entries out of the window, sums what's left, and admits the request only if
# used + requested <= limit. Reservation (ZADD + HSET) happens atomically with the check so
# concurrent requests can't both slip under the cap.
_RESERVE_SCRIPT = """
local events_key = KEYS[1]
local counts_key = KEYS[2]
local job_id = ARGV[1]
local now = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local requested = tonumber(ARGV[5])

local cutoff = now - window
local expired = redis.call('ZRANGEBYSCORE', events_key, '-inf', cutoff)
if #expired > 0 then
    redis.call('ZREMRANGEBYSCORE', events_key, '-inf', cutoff)
    for _, jid in ipairs(expired) do
        redis.call('HDEL', counts_key, jid)
    end
end

local used = 0
local counts = redis.call('HVALS', counts_key)
for _, v in ipairs(counts) do
    used = used + tonumber(v)
end

if used + requested > limit then
    local oldest = redis.call('ZRANGE', events_key, 0, 0, 'WITHSCORES')
    local oldest_score = 0
    if #oldest > 0 then
        oldest_score = tonumber(oldest[2])
    end
    return {0, used, oldest_score}
end

redis.call('ZADD', events_key, now, job_id)
redis.call('HSET', counts_key, job_id, requested)
local ttl = window * 2
redis.call('EXPIRE', events_key, ttl)
redis.call('EXPIRE', counts_key, ttl)

return {1, used, 0}
"""

# KEYS: 1=events (zset), 2=counts (hash)
# ARGV: 1=job_id, 2=actual_count, 3=limit ('' if the principal is unlimited), 4=window_seconds,
#       5=now
# Only updates the count if the job's reservation is still within the window; a job that
# already rolled off the window must not resurrect a stale entry (returns allowed=1, a no-op,
# in that case). When `actual` is higher than what was reserved and a limit applies, re-checks
# the identity's current usage (excluding this job's own reservation) against the limit before
# admitting the increase; rejects (allowed=0) and leaves the old reservation untouched if it
# would exceed the limit, so the caller can raise rather than silently over-run the budget.
_RECONCILE_SCRIPT = """
local events_key = KEYS[1]
local counts_key = KEYS[2]
local job_id = ARGV[1]
local actual = tonumber(ARGV[2])
local limit = ARGV[3]

if redis.call('ZSCORE', events_key, job_id) == false then
    return {1, 0, 0}
end

if limit ~= '' then
    limit = tonumber(limit)
    local old = tonumber(redis.call('HGET', counts_key, job_id)) or 0

    if actual > old then
        local used = 0
        local counts = redis.call('HGETALL', counts_key)
        for i = 1, #counts, 2 do
            if counts[i] ~= job_id then
                used = used + tonumber(counts[i + 1])
            end
        end

        if used + actual > limit then
            local window = tonumber(ARGV[4])
            local now = tonumber(ARGV[5])
            local oldest = redis.call('ZRANGE', events_key, 0, 0, 'WITHSCORES')
            local oldest_score = 0
            if #oldest > 0 then
                oldest_score = tonumber(oldest[2])
            end
            return {0, used, oldest_score}
        end
    end
end

redis.call('HSET', counts_key, job_id, actual)
return {1, 0, 0}
"""

# KEYS: 1=events (zset), 2=counts (hash)
# ARGV: 1=job_id
_RELEASE_SCRIPT = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""


class CutoutLimiter:
    """
    Rolling-window cutout budget per identity, backed by a Redis ZSET+HASH pair.

    Construct with an async Redis client to use `reserve`/`release` (API side).
    Construct with a sync Redis client to use `reconcile` (Celery worker side).
    The `redis.register_script` method binds each Lua script to the client it's registered against,
    so mixing them on the wrong client type will produce an unawaited coroutine rather than a result.
    """

    def __init__(self, redis_client: AsyncRedisClient | AsyncRedisCluster | SyncRedisClient | SyncRedisCluster):
        self._redis_client = redis_client
        self._reserve_script: Callable = redis_client.register_script(_RESERVE_SCRIPT)
        self._reconcile_script: Callable = redis_client.register_script(_RECONCILE_SCRIPT)
        self._release_script: Callable = redis_client.register_script(_RELEASE_SCRIPT)
        self.logger = get_logger()

    async def reserve(self, principal: Principal, job_id: str, requested: int) -> None:
        """
        Reserve `requested` cutouts against the principal's rolling-window budget.

        No-op when cutout limiting is disabled or the principal is unlimited. Raises
        `CutoutLimitExceededError` if the reservation would exceed the limit.
        """
        if not CONFIG.cutout_limit.enabled or principal.cutout_limit is None:
            return

        window = principal.window_seconds or CONFIG.cutout_limit.window_seconds
        keys = CutoutLimitKeys(identity=principal.identity)
        now = time.time()

        allowed, used, oldest_score = await self._reserve_script(
            keys=[keys.events, keys.counts],
            args=[job_id, now, window, principal.cutout_limit, requested],
        )

        if not allowed:
            retry_after = max(1, int(oldest_score + window - now)) if oldest_score else window
            self.logger.info(
                f"Cutout limit exceeded for identity={principal.identity}: "
                f"used={used} minimum_requested={requested} limit={principal.cutout_limit}",
                extra={
                    "event": "cutout_limit_exceeded",
                    "identity": principal.identity,
                    "is_anonymous": principal.is_anonymous,
                    "used": used,
                    "minimum_requested": requested,
                    "limit": principal.cutout_limit,
                    "retry_after": retry_after,
                },
            )
            raise CutoutLimitExceededError(
                limit=principal.cutout_limit,
                used=int(used),
                requested=requested,
                retry_after=retry_after,
            )

    async def release(self, identity: str, job_id: str) -> None:
        """Refund a reservation, e.g. when job creation fails after a successful reserve."""
        keys = CutoutLimitKeys(identity=identity)
        await self._release_script(keys=[keys.events, keys.counts], args=[job_id])

    def reconcile(
        self,
        identity: str,
        job_id: str,
        actual: int,
        cutout_limit: int | None = None,
        window_seconds: int | None = None,
    ) -> None:
        """Update a reservation to the true cutout count once it's known (sync, worker-side).

        No-op if the job's reservation already rolled off the rolling window. If `actual` is
        higher than what was reserved and `cutout_limit` is set, re-checks the identity's
        current usage against the limit before admitting the increase; raises
        `CutoutLimitExceededError` (leaving the old reservation untouched) if it would
        exceed the limit, rather than letting the overage silently spill into future requests.
        """
        keys = CutoutLimitKeys(identity=identity)
        window = window_seconds or CONFIG.cutout_limit.window_seconds
        now = time.time()

        allowed, used, oldest_score = self._reconcile_script(
            keys=[keys.events, keys.counts],
            args=[job_id, actual, cutout_limit if cutout_limit is not None else "", window, now],
        )

        if not allowed:
            retry_after = max(1, int(oldest_score + window - now)) if oldest_score else window
            self.logger.info(
                f"Cutout limit exceeded on reconcile for identity={identity} job={job_id}: "
                f"used={used} actual={actual} limit={cutout_limit}",
                extra={
                    "event": "cutout_limit_exceeded_on_reconcile",
                    "identity": identity,
                    "job_id": job_id,
                    "used": used,
                    "actual": actual,
                    "limit": cutout_limit,
                    "retry_after": retry_after,
                },
            )
            raise CutoutLimitExceededError(
                limit=cutout_limit,
                used=int(used),
                requested=actual,
                retry_after=retry_after,
            )


def cutout_limiter_factory(redis_client: Any) -> CutoutLimiter:
    return CutoutLimiter(redis_client)
