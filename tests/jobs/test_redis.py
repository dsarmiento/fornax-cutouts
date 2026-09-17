"""Tests for RedisKeys and SyncRedisCutoutJob TTL behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from vo_models.uws.types import ExecutionPhase

from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import (
    AsyncRedisCutoutJob,
    RedisKeys,
    SyncRedisCutoutJob,
    _build_uws_jobs_search_query,
    _filter_uws_jobs_by_phase,
)
from tests.jobs.helpers import descriptor

_JOB_ID = "abcd1234"
_POSITIONS = ["10.0, 20.0", "30.0, 40.0"]


class TestUWSJobPhaseFiltering:
    def test_build_search_query_single_phase(self):
        query = _build_uws_jobs_search_query([ExecutionPhase.QUEUED])
        assert query == "@phase:{QUEUED} -@phase:{ARCHIVED} "

    def test_build_search_query_multiple_phases(self):
        query = _build_uws_jobs_search_query([ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING])
        assert query == "@phase:{QUEUED | EXECUTING} -@phase:{ARCHIVED} "

    def test_build_search_query_archived_only(self):
        query = _build_uws_jobs_search_query([ExecutionPhase.ARCHIVED])
        assert query == "@phase:{ARCHIVED} "

    def test_build_search_query_without_phase_excludes_archived(self):
        query = _build_uws_jobs_search_query([])
        assert query == "-@phase:{ARCHIVED} "

    def test_build_search_query_with_after(self):
        after = datetime(2024, 1, 1, tzinfo=UTC)
        query = _build_uws_jobs_search_query([ExecutionPhase.QUEUED], after=after)
        assert query.endswith(f"@creation_time:[{after.timestamp()} +inf]")

    def test_filter_jobs_by_single_phase(self):
        jobs = [
            {"job_id": "a", "phase": ExecutionPhase.PENDING},
            {"job_id": "b", "phase": ExecutionPhase.EXECUTING},
        ]
        filtered = _filter_uws_jobs_by_phase(jobs, [ExecutionPhase.EXECUTING])
        assert [job["job_id"] for job in filtered] == ["b"]

    def test_filter_jobs_excludes_archived_by_default(self):
        jobs = [
            {"job_id": "a", "phase": ExecutionPhase.PENDING},
            {"job_id": "b", "phase": ExecutionPhase.ARCHIVED},
        ]
        filtered = _filter_uws_jobs_by_phase(jobs, [])
        assert [job["job_id"] for job in filtered] == ["a"]


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
        sync_redis.rpush(keys.pending_tasks, json.dumps({"job_id": job_id, "source_file": "x.fits"}))
        job.prepare_batch(1, 1)
        for key in (keys.batch_outstanding(1), keys.batch_descriptors(1)):
            assert 0 < sync_redis.ttl(key) <= CONFIG.async_ttl


def _prepare_batch(sync_redis, job_id: str, num_tasks: int) -> SyncRedisCutoutJob:
    job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
    job.push_pending_tasks([descriptor(job_id, f"{chr(ord('a') + i)}.fits") for i in range(num_tasks)])
    job.prepare_batch(1, num_tasks)
    return job


class TestSyncRedisCutoutJobTaskOperations:
    def test_skip_task_updates_counters_and_result(self, sync_redis, job_id):
        job = _prepare_batch(sync_redis, job_id, 2)
        keys = RedisKeys(job_id)
        job.start_task(1, 0)

        remaining = job.skip_task(1, 0)

        assert remaining == 1
        assert sync_redis.hget(keys.batch_results(1), "0") == "null"
        assert int(sync_redis.get(keys.skipped_task_count)) == 1
        assert int(sync_redis.get(keys.executing_task_count)) == 0
        assert job.get_batch_outstanding(1) == 1
        assert job.get_batch_results(1) == [None]

    def test_skip_task_returns_zero_for_last_task(self, sync_redis, job_id):
        job = _prepare_batch(sync_redis, job_id, 1)
        job.start_task(1, 0)

        remaining = job.skip_task(1, 0)

        assert remaining == 0
        assert job.get_batch_outstanding(1) == 0

    def test_fail_task_records_failure_and_updates_counters(self, sync_redis, job_id):
        job = _prepare_batch(sync_redis, job_id, 2)
        keys = RedisKeys(job_id)
        job.start_task(1, 0)
        task_kwargs = descriptor(job_id, "a.fits")

        remaining = job.fail_task(1, 0, task_kwargs, "cutout failed")

        assert remaining == 1
        assert task_kwargs["error_message"] == "cutout failed"
        assert sync_redis.hget(keys.batch_results(1), "0") == "null"
        assert int(sync_redis.get(keys.executing_task_count)) == 0
        assert job.get_batch_outstanding(1) == 1
        failed = json.loads(sync_redis.lrange(keys.failed_tasks, 0, -1)[0])
        assert failed["source_file"] == "a.fits"
        assert failed["error_message"] == "cutout failed"
