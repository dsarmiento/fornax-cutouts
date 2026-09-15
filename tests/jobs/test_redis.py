"""Tests for RedisKeys and SyncRedisCutoutJob TTL behavior."""

from __future__ import annotations

import pytest
from vo_models.uws.types import ExecutionPhase

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import AsyncRedisCutoutJob, RedisKeys, SyncRedisCutoutJob

_JOB_ID = "abcd1234"
_POSITIONS = ["10.0, 20.0", "30.0, 40.0"]


@pytest.fixture
async def job_id(async_redis):
    job = AsyncRedisCutoutJob(redis_client=async_redis, job_id=_JOB_ID)
    await job.create_job(
        run_id="test-run-id",
        parameters={"position": _POSITIONS, "size": 256},
    )
    return _JOB_ID


class TestAsyncJobTTL:
    async def test_job_creation_sets_ttl(self, sync_redis, async_redis):
        job = AsyncRedisCutoutJob(redis_client=async_redis, job_id="newjob01")
        await job.create_job(parameters={"position": _POSITIONS, "size": 256})

        keys = RedisKeys("newjob01")
        for key in keys.job_keys:
            if sync_redis.exists(key):
                ttl = sync_redis.ttl(key)
                assert 0 < ttl <= CONFIG.async_ttl

    def test_uws_ttl_survives_phase_update(self, sync_redis, job_id):
        keys = RedisKeys(job_id)
        ttl_before = sync_redis.ttl(keys.uws)
        assert ttl_before > 0
        sync_redis.json().set(keys.uws, "$.phase", ExecutionPhase.EXECUTING)
        ttl_after = sync_redis.ttl(keys.uws)
        assert ttl_after > 0
        assert abs(ttl_after - ttl_before) <= 2


class TestSyncRedisCutoutJobLifecycle:
    def test_start_job_sets_executing_and_start_time(self, sync_redis, job_id):
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        job.start_job()
        uws = sync_redis.json().get(RedisKeys(job_id).uws)
        assert uws["phase"] == ExecutionPhase.EXECUTING
        assert uws["start_time"] is not None

    def test_start_job_is_idempotent(self, sync_redis, job_id):
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        job.start_job()
        start_time = sync_redis.json().get(RedisKeys(job_id).uws, "$.start_time")[0]
        job.start_job()
        assert sync_redis.json().get(RedisKeys(job_id).uws, "$.start_time")[0] == start_time


class TestSyncRedisCutoutJobTTL:
    def test_set_total_task_count_preserves_ttl(self, sync_redis, job_id):
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        keys = RedisKeys(job_id)
        ttl_before = sync_redis.ttl(keys.total_task_count)
        assert ttl_before > 0
        job.set_total_task_count(42)
        ttl_after = sync_redis.ttl(keys.total_task_count)
        assert ttl_after > 0
        assert abs(ttl_after - ttl_before) == 0

    def test_push_pending_tasks_ttl(self, sync_redis, job_id):
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        keys = RedisKeys(job_id)
        job.push_pending_tasks([{"job_id": job_id, "source_file": "x.fits"}])
        assert 0 < sync_redis.ttl(keys.pending_tasks) <= CONFIG.async_ttl

    def test_batch_keys_ttl(self, sync_redis, job_id):
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        keys = RedisKeys(job_id)
        job.set_batch_outstanding(1, 5)
        job.set_batch_descriptors(1, [{"x": 1}])
        for key in (keys.batch_outstanding(1), keys.batch_descriptors(1)):
            assert 0 < sync_redis.ttl(key) <= CONFIG.async_ttl
