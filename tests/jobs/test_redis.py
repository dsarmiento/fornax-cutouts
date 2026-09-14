"""Tests for RedisKeys and SyncRedisCutoutJob TTL behavior."""

from __future__ import annotations

from types import SimpleNamespace

import fakeredis
import pytest
from vo_models.uws.types import ExecutionPhase

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import AsyncRedisCutoutJob, RedisKeys, SyncRedisCutoutJob

_JOB_ID = "abcd1234"
_POSITIONS = ["10.0, 20.0", "30.0, 40.0"]


@pytest.fixture
def redis_clients():
    server = fakeredis.FakeServer()
    sync_redis = fakeredis.FakeRedis(server=server, decode_responses=True)
    async_redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    yield SimpleNamespace(sync=sync_redis, async_client=async_redis)
    sync_redis.flushall()


@pytest.fixture
async def job_id(redis_clients):
    job = AsyncRedisCutoutJob(redis_client=redis_clients.async_client, job_id=_JOB_ID)
    await job.create_job(
        run_id="test-run-id",
        parameters={"position": _POSITIONS, "size": 256},
    )
    return _JOB_ID


class TestAsyncJobTTL:
    async def test_job_creation_sets_ttl(self, redis_clients):
        job = AsyncRedisCutoutJob(redis_client=redis_clients.async_client, job_id="newjob01")
        await job.create_job(parameters={"position": _POSITIONS, "size": 256})

        keys = RedisKeys("newjob01")
        for key in keys.job_keys:
            if redis_clients.sync.exists(key):
                ttl = redis_clients.sync.ttl(key)
                assert 0 < ttl <= CONFIG.async_ttl

    def test_uws_ttl_survives_phase_update(self, redis_clients, job_id):
        keys = RedisKeys(job_id)
        ttl_before = redis_clients.sync.ttl(keys.uws)
        assert ttl_before > 0
        redis_clients.sync.json().set(keys.uws, "$.phase", ExecutionPhase.EXECUTING)
        ttl_after = redis_clients.sync.ttl(keys.uws)
        assert ttl_after > 0
        assert abs(ttl_after - ttl_before) <= 2


class TestSyncRedisCutoutJobTTL:
    def test_set_total_task_count_preserves_ttl(self, redis_clients, job_id):
        job = SyncRedisCutoutJob(redis_client=redis_clients.sync, job_id=job_id)
        keys = RedisKeys(job_id)
        ttl_before = redis_clients.sync.ttl(keys.total_task_count)
        assert ttl_before > 0
        job.set_total_task_count(42)
        ttl_after = redis_clients.sync.ttl(keys.total_task_count)
        assert ttl_after > 0
        assert abs(ttl_after - ttl_before) == 0

    def test_push_pending_tasks_ttl(self, redis_clients, job_id):
        job = SyncRedisCutoutJob(redis_client=redis_clients.sync, job_id=job_id)
        keys = RedisKeys(job_id)
        job.push_pending_tasks([{"job_id": job_id, "source_file": "x.fits"}])
        assert 0 < redis_clients.sync.ttl(keys.pending_tasks) <= CONFIG.async_ttl

    def test_batch_keys_ttl(self, redis_clients, job_id):
        job = SyncRedisCutoutJob(redis_client=redis_clients.sync, job_id=job_id)
        keys = RedisKeys(job_id)
        job.set_batch_outstanding(1, 5)
        job.set_batch_descriptors(1, [{"x": 1}])
        for key in (keys.batch_outstanding(1), keys.batch_descriptors(1)):
            assert 0 < redis_clients.sync.ttl(key) <= CONFIG.async_ttl
