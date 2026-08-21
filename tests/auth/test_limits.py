"""
Tests for CutoutLimiter (fornax_cutouts/auth/limits.py).

Covers the Python-side logic in reserve / release / reconcile, including the
CONFIG-gated early returns, error construction, and retry_after calculation.
The Lua script correctness is tested separately in test_lua_scripts.py.
"""

from __future__ import annotations

import time

import pytest
import redis
import redis.asyncio as aioredis

import fornax_cutouts.auth.limits as limits_module
from fornax_cutouts.auth.limits import CutoutLimiter
from fornax_cutouts.models.auth import Principal
from fornax_cutouts.utils.exceptions import CutoutLimitExceededError

WINDOW = 60
LIMIT = 100


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def limiter(async_redis: aioredis.Redis) -> CutoutLimiter:
    return CutoutLimiter(async_redis)


@pytest.fixture
def sync_limiter(sync_redis: redis.Redis) -> CutoutLimiter:
    return CutoutLimiter(sync_redis)


@pytest.fixture(autouse=True)
def enable_cutout_limits(monkeypatch):
    """Enable the cutout limit feature for all tests in this module."""
    monkeypatch.setattr(limits_module.CONFIG.cutout_limit, "enabled", True)
    monkeypatch.setattr(limits_module.CONFIG.cutout_limit, "window_seconds", WINDOW)


def _principal(cutout_limit: int | None = LIMIT, window: int | None = WINDOW) -> Principal:
    return Principal(identity="test-user", cutout_limit=cutout_limit, window_seconds=window)


# ---------------------------------------------------------------------------
# CutoutLimiter.reserve
# ---------------------------------------------------------------------------


class TestReserve:
    async def test_reserve_disabled_config_is_noop(self, limiter, monkeypatch):
        monkeypatch.setattr(limits_module.CONFIG.cutout_limit, "enabled", False)
        # Should not raise even with a completely full budget.
        await limiter.reserve(_principal(cutout_limit=1), "job-1", 9999)

    async def test_reserve_no_cutout_limit_is_noop(self, limiter):
        """principal.cutout_limit=None means unlimited; must not raise."""
        await limiter.reserve(_principal(cutout_limit=None), "job-1", 9999)

    async def test_reserve_within_budget(self, limiter):
        await limiter.reserve(_principal(), "job-1", 10)

    async def test_reserve_at_exact_limit(self, limiter):
        await limiter.reserve(_principal(), "job-1", LIMIT)

    async def test_reserve_over_limit_raises(self, limiter):
        await limiter.reserve(_principal(), "job-1", LIMIT)
        with pytest.raises(CutoutLimitExceededError) as exc_info:
            await limiter.reserve(_principal(), "job-2", 1)
        err = exc_info.value
        assert err.limit == LIMIT
        assert err.used == LIMIT
        assert err.requested == 1

    async def test_reserve_retry_after_positive(self, limiter):
        await limiter.reserve(_principal(), "job-1", LIMIT)
        with pytest.raises(CutoutLimitExceededError) as exc_info:
            await limiter.reserve(_principal(), "job-2", 1)
        assert exc_info.value.retry_after >= 1

    async def test_reserve_retry_after_bounded_by_window(self, limiter):
        await limiter.reserve(_principal(), "job-1", LIMIT)
        with pytest.raises(CutoutLimitExceededError) as exc_info:
            await limiter.reserve(_principal(), "job-2", 1)
        assert exc_info.value.retry_after <= WINDOW

    async def test_reserve_uses_principal_window(self, limiter):
        """principal.window_seconds overrides CONFIG default."""
        short_window = 5
        p = _principal(window=short_window)
        old_timestamp = time.time() - short_window - 1
        # Pre-populate with an expired reservation using the raw Lua script.
        from fornax_cutouts.auth.limits import _RESERVE_SCRIPT
        from fornax_cutouts.jobs.redis import CutoutLimitKeys

        keys = CutoutLimitKeys(identity=p.identity)
        # A non-awaited sync call won't work here; use the async client directly.
        async_client: aioredis.Redis = limiter._redis_client
        raw_script = async_client.register_script(_RESERVE_SCRIPT)
        await raw_script(keys=[keys.events, keys.counts], args=["old-job", old_timestamp, short_window, LIMIT, LIMIT])
        # Now the old job is outside the short window; reserve should succeed.
        await limiter.reserve(p, "new-job", LIMIT)

    async def test_reserve_multiple_identities_isolated(self, limiter):
        """Two principals must not share budget."""
        p1 = Principal(identity="alice", cutout_limit=10, window_seconds=WINDOW)
        p2 = Principal(identity="bob", cutout_limit=10, window_seconds=WINDOW)
        await limiter.reserve(p1, "job-a1", 10)
        # bob's budget is independent; this must not raise.
        await limiter.reserve(p2, "job-b1", 10)


# ---------------------------------------------------------------------------
# CutoutLimiter.release
# ---------------------------------------------------------------------------


class TestRelease:
    async def test_release_refunds_budget(self, limiter):
        await limiter.reserve(_principal(), "job-1", LIMIT)
        await limiter.release("test-user", "job-1")
        # Budget should now be free again.
        await limiter.reserve(_principal(), "job-2", LIMIT)

    async def test_release_nonexistent_job_does_not_raise(self, limiter):
        await limiter.release("test-user", "ghost-job")


# ---------------------------------------------------------------------------
# CutoutLimiter.reconcile  (sync path used by Celery workers)
# ---------------------------------------------------------------------------


class TestReconcile:
    def test_reconcile_within_budget(self, sync_limiter):
        sync_limiter.reconcile("test-user", "job-1", actual=10, cutout_limit=LIMIT, window_seconds=WINDOW)

    def test_reconcile_over_limit_raises(self, sync_limiter):
        sync_limiter.reconcile("test-user", "job-1", actual=LIMIT, cutout_limit=LIMIT, window_seconds=WINDOW)
        with pytest.raises(CutoutLimitExceededError) as exc_info:
            sync_limiter.reconcile("test-user", "job-2", actual=1, cutout_limit=LIMIT, window_seconds=WINDOW)
        err = exc_info.value
        assert err.limit == LIMIT
        assert err.used == LIMIT

    def test_reconcile_updates_existing_reservation(self, sync_limiter):
        """reconcile with a lower actual count frees up budget."""
        sync_limiter.reconcile("test-user", "job-1", actual=LIMIT, cutout_limit=LIMIT, window_seconds=WINDOW)
        # Update job-1 to a smaller actual count; job-2 should now fit.
        sync_limiter.reconcile("test-user", "job-1", actual=10, cutout_limit=LIMIT, window_seconds=WINDOW)
        sync_limiter.reconcile("test-user", "job-2", actual=LIMIT - 10, cutout_limit=LIMIT, window_seconds=WINDOW)

    def test_reconcile_no_limit_does_not_raise(self, sync_limiter):
        sync_limiter.reconcile("test-user", "job-1", actual=9999, cutout_limit=None, window_seconds=WINDOW)
