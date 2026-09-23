from __future__ import annotations

import pytest

from fornax_cutouts.jobs.redis import AsyncRedisCutoutJob

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
