from __future__ import annotations

import time
from importlib.resources import files
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

_LUA_DIR = files("fornax_cutouts.auth") / "lua"


def _load_lua(name: str) -> str:
    """
    Load a Lua script from the given name.

    Args:
        name (str): The name of the script to load

    Returns:
        str: The loaded Lua script
    """
    return (_LUA_DIR / name).read_text(encoding="utf-8")


def _compose_lua(name: str) -> str:
    """
    Compose a Lua script by concatenating the library and the given script.

    Args:
        name (str): The name of the script to compose

    Returns:
        str: The composed Lua script
    """
    return _load_lua("_lib.lua") + _load_lua(name)


_RESERVE_SCRIPT = _compose_lua("reserve.lua")
_RELEASE_SCRIPT = _compose_lua("release.lua")


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
        self._release_script: Callable = redis_client.register_script(_RELEASE_SCRIPT)
        self.logger = get_logger()

    async def reserve(self, principal: Principal, job_id: str, requested: int) -> None:
        """
        Reserve `requested` cutouts against the principal's rolling-window budget.

        Args:
            principal (Principal): The principal to reserve cutouts against
            job_id (str): The ID of the job
            requested (int): The number of cutouts to reserve

        Raises:
            CutoutLimitExceededError: If the cutout limit is exceeded
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
        """
        Update a reservation to the true cutout count once it's known.

        Args:
            identity (str): The identity of the principal
            job_id (str): The ID of the job
            actual (int): The actual number of cutouts
            cutout_limit (int | None): The cutout limit
            window_seconds (int | None): The window seconds

        Raises:
            CutoutLimitExceededError: If the cutout limit is exceeded
        """
        keys = CutoutLimitKeys(identity=identity)
        window = window_seconds or CONFIG.cutout_limit.window_seconds
        now = time.time()

        allowed, used, oldest_score = self._reserve_script(
            keys=[keys.events, keys.counts],
            args=[job_id, now, window, cutout_limit, actual],
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
    """
    Create a CutoutLimiter instance.

    Args:
        redis_client (Any): The Redis client to use

    Returns:
        CutoutLimiter: A CutoutLimiter instance
    """
    return CutoutLimiter(redis_client)
