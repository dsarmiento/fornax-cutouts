"""
Tests for the Lua scripts in fornax_cutouts/auth/lua/.

These tests exercise the scripts directly via a real Redis client to validate
server-side behavior: ZSET/HASH mutations, pruning, TTL assignment, and the
allow/deny decision logic. Python-level CutoutLimiter integration is covered
separately in test_limits.py.
"""

from __future__ import annotations

import time

import pytest
import redis

from fornax_cutouts.auth.limits import _RELEASE_SCRIPT, _RESERVE_SCRIPT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EVENTS_KEY = "test:cutout_limit:{user1}:events"
COUNTS_KEY = "test:cutout_limit:{user1}:counts"
KEYS = [EVENTS_KEY, COUNTS_KEY]

WINDOW = 60  # seconds
LIMIT = 100


def _reserve(script, job_id: str, now: float, requested: int, limit: int = LIMIT, window: int = WINDOW):
    return script(keys=KEYS, args=[job_id, now, window, limit, requested])


def _release(script, job_id: str):
    return script(keys=KEYS, args=[job_id])


# ---------------------------------------------------------------------------
# reserve.lua
# ---------------------------------------------------------------------------


class TestReserveScript:
    @pytest.fixture(autouse=True)
    def scripts(self, sync_redis: redis.Redis):
        self.reserve = sync_redis.register_script(_RESERVE_SCRIPT)
        self.release = sync_redis.register_script(_RELEASE_SCRIPT)
        self.r = sync_redis

    def test_reserve_within_budget(self):
        now = time.time()
        allowed, used, _ = _reserve(self.reserve, "job-1", now, requested=10)
        assert allowed == 1
        assert used == 0  # nothing was reserved before this call

    def test_reserve_at_exact_limit(self):
        now = time.time()
        allowed, _, _ = _reserve(self.reserve, "job-1", now, requested=LIMIT)
        assert allowed == 1

    def test_reserve_over_limit_rejected(self):
        now = time.time()
        # Fill the budget completely.
        _reserve(self.reserve, "job-1", now, requested=LIMIT)
        # Any further reservation must be rejected.
        allowed, used, oldest_score = _reserve(self.reserve, "job-2", now, requested=1)
        assert allowed == 0
        assert used == LIMIT
        assert oldest_score > 0

    def test_reserve_stores_in_zset_and_hash(self):
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=42)
        assert self.r.zscore(EVENTS_KEY, "job-1") == pytest.approx(now, abs=0.1)
        assert int(self.r.hget(COUNTS_KEY, "job-1")) == 42

    def test_reserve_sets_ttl_on_keys(self):
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=10)
        assert self.r.ttl(EVENTS_KEY) > 0
        assert self.r.ttl(COUNTS_KEY) > 0

    def test_reserve_existing_job_replaces_count(self):
        """Re-reserving the same job_id must not double-count the old reservation."""
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=50)
        # Update the same job to a new count.
        allowed, used_before_update, _ = _reserve(self.reserve, "job-1", now, requested=30)
        assert allowed == 1
        # used_before_update reflects the count before the re-reserve (the old 50 was
        # subtracted from the sum prior to the limit check).
        assert used_before_update == 0
        assert int(self.r.hget(COUNTS_KEY, "job-1")) == 30

    def test_reserve_multiple_jobs_sum_correctly(self):
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=30)
        _reserve(self.reserve, "job-2", now, requested=30)
        allowed, used, _ = _reserve(self.reserve, "job-3", now, requested=30)
        assert allowed == 1
        assert used == 60  # job-1 + job-2

    def test_prune_expired_jobs_not_counted(self):
        """Jobs older than the window must be pruned and excluded from the budget."""
        old_now = time.time() - WINDOW - 1
        _reserve(self.reserve, "old-job", old_now, requested=LIMIT)
        # The old job is outside the window; a fresh reservation should be allowed.
        fresh_now = time.time()
        allowed, used, _ = _reserve(self.reserve, "new-job", fresh_now, requested=LIMIT)
        assert allowed == 1
        assert used == 0

    def test_rejected_job_not_left_in_zset(self):
        """A rejected reservation must not leave the job_id in the ZSET."""
        now = time.time()
        _reserve(self.reserve, "filler", now, requested=LIMIT)
        _reserve(self.reserve, "rejected", now, requested=1)
        assert self.r.zscore(EVENTS_KEY, "rejected") is None

    def test_oldest_score_returned_on_rejection(self):
        """oldest_score drives retry-after; it must equal the oldest event timestamp.

        Redis Lua scripts return numbers as integers (floats are truncated), so
        the score will be within 1 second of the recorded timestamp.
        """
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=LIMIT)
        _, _, oldest_score = _reserve(self.reserve, "job-2", now + 1, requested=1)
        assert oldest_score == pytest.approx(now, abs=1.0)

    def test_no_limit_always_allowed(self):
        """Passing an empty string as limit (serialised None) must always allow."""
        now = time.time()
        # redis-py serialises Python None as b"" which Lua's tonumber converts to nil.
        result = self.reserve(keys=KEYS, args=["job-1", now, WINDOW, "", 9999])
        assert result[0] == 1


# ---------------------------------------------------------------------------
# release.lua
# ---------------------------------------------------------------------------


class TestReleaseScript:
    @pytest.fixture(autouse=True)
    def scripts(self, sync_redis: redis.Redis):
        self.reserve = sync_redis.register_script(_RESERVE_SCRIPT)
        self.release = sync_redis.register_script(_RELEASE_SCRIPT)
        self.r = sync_redis

    def test_release_removes_from_zset_and_hash(self):
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=10)
        assert self.r.zscore(EVENTS_KEY, "job-1") is not None
        _release(self.release, "job-1")
        assert self.r.zscore(EVENTS_KEY, "job-1") is None
        assert self.r.hget(COUNTS_KEY, "job-1") is None

    def test_release_frees_budget(self):
        now = time.time()
        _reserve(self.reserve, "job-1", now, requested=LIMIT)
        _release(self.release, "job-1")
        allowed, _, _ = _reserve(self.reserve, "job-2", now, requested=LIMIT)
        assert allowed == 1

    def test_release_nonexistent_job_is_noop(self):
        """Releasing a job that was never reserved must not raise or corrupt state."""
        _release(self.release, "ghost-job")
