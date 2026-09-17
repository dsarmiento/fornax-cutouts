"""Tests for Celery task helpers."""

from __future__ import annotations

import json
from unittest.mock import patch

from fornax_cutouts.jobs.redis import TOTAL_PENDING_TASKS_KEY, RedisKeys, SyncRedisCutoutJob
from fornax_cutouts.jobs.tasks import batch_watchdog
from tests.jobs.helpers import descriptor


def _store_batch_descriptors(sync_redis, job_id: str, batch_num: int, descriptors: list[dict]) -> None:
    keys = RedisKeys(job_id)
    sync_redis.set(keys.batch_descriptors(batch_num), json.dumps(descriptors))


@patch("fornax_cutouts.jobs.tasks.write_results")
@patch("fornax_cutouts.jobs.tasks.redis_client_factory")
class TestBatchWatchdog:
    def test_noop_when_batch_has_no_outstanding_tasks(self, mock_redis_factory, mock_write_results, sync_redis, job_id):
        mock_redis_factory.return_value = sync_redis
        job = SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id)
        job.reset_batch_outstanding(1)

        batch_watchdog.run(job_id=job_id, batch_num=1, expected_count=2)

        mock_write_results.run.assert_not_called()

    def test_requeues_queued_stranded_tasks(self, mock_redis_factory, mock_write_results, sync_redis, job_id):
        mock_redis_factory.return_value = sync_redis
        keys = RedisKeys(job_id)
        descriptors = [descriptor(job_id, "a.fits"), descriptor(job_id, "b.fits"), descriptor(job_id, "c.fits")]
        _store_batch_descriptors(sync_redis, job_id, 1, descriptors)
        sync_redis.set(keys.batch_outstanding(1), 2)
        sync_redis.set(keys.queued_task_count, 2)
        sync_redis.hset(keys.batch_results(1), "0", '{"mission": "test"}')

        batch_watchdog.run(job_id=job_id, batch_num=1, expected_count=3)

        pending = [json.loads(raw) for raw in sync_redis.lrange(keys.pending_tasks, 0, -1)]
        assert pending == [descriptors[1], descriptors[2]]
        assert int(sync_redis.get(keys.queued_task_count)) == 0
        assert int(sync_redis.get(TOTAL_PENDING_TASKS_KEY)) == 2
        assert SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id).get_batch_outstanding(1) == 0
        mock_write_results.run.assert_called_once_with(job_id=job_id, batch_num=1)

    def test_requeues_started_stranded_tasks(self, mock_redis_factory, mock_write_results, sync_redis, job_id):
        mock_redis_factory.return_value = sync_redis
        keys = RedisKeys(job_id)
        descriptors = [descriptor(job_id, "a.fits"), descriptor(job_id, "b.fits")]
        _store_batch_descriptors(sync_redis, job_id, 1, descriptors)
        sync_redis.set(keys.batch_outstanding(1), 1)
        sync_redis.set(keys.executing_task_count, 1)
        sync_redis.hset(keys.batch_results(1), "0", '{"mission": "test"}')
        sync_redis.hset(keys.batch_started(1), "1", "1")

        batch_watchdog.run(job_id=job_id, batch_num=1, expected_count=2)

        pending = [json.loads(raw) for raw in sync_redis.lrange(keys.pending_tasks, 0, -1)]
        assert pending == [descriptors[1]]
        assert int(sync_redis.get(keys.executing_task_count)) == 0
        assert int(sync_redis.get(TOTAL_PENDING_TASKS_KEY)) == 1
        assert SyncRedisCutoutJob(redis_client=sync_redis, job_id=job_id).get_batch_outstanding(1) == 0
        mock_write_results.run.assert_called_once_with(job_id=job_id, batch_num=1)
