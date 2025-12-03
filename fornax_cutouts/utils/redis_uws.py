from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from redis import ResponseError
from redis.asyncio import Redis as RedisClient
from redis.asyncio import RedisCluster
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from vo_models.uws.models import ExecutionPhase, Jobs, JobSummary, Parameters, ShortJobDescription

from fornax_cutouts.async_results import AsyncCutoutResults
from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.cutouts import CutoutResponse
from fornax_cutouts.models.uws import create_job_summary, create_parameters

JOB_SUMMARY_TIME_FIELDS = ["quote", "creation_time", "start_time", "end_time", "destruction"]
CUTOUT_INDEX_NAME = "cutoutJobsIdx"
CUTOUT_JOB_PREFIX = f"{CONFIG.worker.redis_prefix}:jobs"


def json_dumps_with_encoders(obj: Any) -> str:
    """
    JSON dumps with datetime serialization support.
    """
    def custom_encoders(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat(timespec="seconds").replace("+00:00", "Z")
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    return json.dumps(obj, default=custom_encoders)


def redis_uws_client():
    return RedisUWS()


class RedisUWS:
    def __init__(self):
        redis_kwargs = {
            "host": CONFIG.redis.host,
            "port": CONFIG.redis.port,
            "ssl": CONFIG.redis.use_ssl,
        }

        if CONFIG.redis.is_cluster:
            r = RedisCluster(**redis_kwargs)
        else:
            r = RedisClient(**redis_kwargs)

        self.__redis_client = r

    async def _setup_index(self):
        if not CONFIG.redis.search_en:
            return

        try:
            print("Setting up redis indexes")
            await self.__redis_client.ft(CUTOUT_INDEX_NAME).create_index(
                fields=[
                    TagField("$.phase", as_name="phase"),
                    NumericField("$.creation_time", as_name="creation_time"),
                ],
                definition=IndexDefinition(
                    prefix=[f"{CUTOUT_JOB_PREFIX}:*:uws"],
                    index_type=IndexType.JSON,
                ),
            )

        except ResponseError as e:
            if "Index already exists" not in str(e):
                raise e

    async def close(self):
        await self.__redis_client.close()

    async def ping(self):
        await self.__redis_client.ping()

    async def __update_job(self, job_id: str, obj: Any, path: str = "$"):
        await self.__redis_client.json().set(
            name=f"{CUTOUT_JOB_PREFIX}:{job_id}:uws",
            path=path,
            obj=obj,
        )

    async def __set_time(
        self,
        job_id: str,
        time_field: str,
        time: datetime | None = None,
    ):
        """
        Using the linux timestamp to save the time into redis for efficient redis querying when filtering on dates

        Args:
            job_id (str): Job ID to set the time for
            time_field (str): Timestamp field to set
            time (datetime, optional): Time object to set. Defaults to datetime.now().
        """
        if time is None:
            time = datetime.now()

        await self.__update_job(
            job_id=job_id,
            path=f"$.{time_field}",
            obj=time.timestamp(),
        )

    async def create_job(
        self,
        job_id: str,
        run_id: str | None = None,
        parameters: dict = {},
    ):
        job_obj = {
            "job_id": job_id,
            "phase": ExecutionPhase.PENDING,
        }

        if run_id:
            job_obj["run_id"] = run_id

        if parameters:
            job_obj["parameters"] = parameters

        await self.__redis_client.json().set(
            name=f"{CUTOUT_JOB_PREFIX}:{job_id}:uws",
            path="$",
            obj=job_obj,
        )
        await self.set_create_time(job_id)

    async def get_job(self, job_id: str) -> JobSummary:
        job_json: dict = await self.__redis_client.json().get(f"{CUTOUT_JOB_PREFIX}:{job_id}:uws")
        job_json.pop("results", None)
        return create_job_summary(**job_json)

    async def get_jobs(
        self,
        phase: ExecutionPhase | None = None,
        after: datetime | None = None,
        last: int = 100,
    ) -> Jobs:
        jobref = []

        if CONFIG.redis.search_en:
            query_str = ""

            if phase:
                query_str += f'@phase:"{phase}" '

            if phase != ExecutionPhase.ARCHIVED:
                query_str += f'-@phase:"{ExecutionPhase.ARCHIVED}" '

            if after:
                query_str += f"@creation_time:[{after.timestamp()} +inf]"

            query = Query(query_str).sort_by("creation_time", asc=False).paging(0, last)

            results = await self.__redis_client.ft(CUTOUT_INDEX_NAME).search(query)

            for doc in results.docs:
                job_obj = json.loads(doc.json)
                job_obj["href"] = f"/cutouts/async/{job_obj['job_id']}"
                jobref.append(ShortJobDescription(**job_obj))

        else:
            keys = []
            async for key in self.__redis_client.scan_iter(match=f"{CUTOUT_JOB_PREFIX}:*", count=100):
                keys.append(key.decode())

            if keys:
                for i in range(0, len(keys), 100):
                    batch_keys = keys[i : i + 100]
                    value = await self.__redis_client.json().mget(batch_keys, Path.root_path())
                    jobref.extend(value)

                jobref.sort(key=lambda job: job["creation_time"], reverse=True)
                jobref = jobref[:last]

        jobs = Jobs(jobref=jobref)
        return jobs

    async def update_job_phase(self, job_id: str, new_phase: ExecutionPhase):
        await self.__update_job(
            job_id=job_id,
            path="$.phase",
            obj=new_phase,
        )

    async def get_job_parameters(self, job_id: str) -> Parameters:
        job_parameters = await self.__redis_client.json().get(
            f"{CUTOUT_JOB_PREFIX}:{job_id}:uws",
            "$.parameters",
        )
        return create_parameters(**job_parameters[0])

    def get_job_cutout_results(self, job_id: str) -> AsyncCutoutResults:
        return AsyncCutoutResults(job_id=job_id)

    def append_job_cutout_result(self, job_id: str, job_results: list[CutoutResponse], batch_num: int):
        results = self.get_job_cutout_results(job_id)
        results.add_results(job_results, batch_num)

    async def set_quote(self, job_id: str, quote: datetime):
        await self.__set_time(job_id=job_id, time_field="quote", time=quote)

    async def set_create_time(self, job_id: str):
        await self.__set_time(
            job_id=job_id,
            time_field="creation_time",
        )

    async def set_start_time(self, job_id: str):
        await self.__set_time(
            job_id=job_id,
            time_field="start_time",
        )

    async def set_end_time(self, job_id: str):
        await self.__set_time(
            job_id=job_id,
            time_field="end_time",
        )

    async def set_destruction(self, job_id: str, destruction: datetime):
        await self.__set_time(job_id=job_id, time_field="destruction", time=destruction)

    # Chunked scheduling helpers
    async def set_expected_results(self, job_id: str, expected_count: int):
        """Store the expected number of cutout results for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:expected_results"
        await self.__redis_client.set(key, expected_count)

    async def get_expected_results(self, job_id: str) -> int:
        """Get the expected number of cutout results for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:expected_results"
        count = await self.__redis_client.get(key)
        return int(count) if count else 0

    async def push_pending_descriptor(self, job_id: str, descriptor: dict):
        """Push a cutout descriptor to the pending queue for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:pending"
        await self.__redis_client.rpush(key, json_dumps_with_encoders(descriptor))

    async def pop_pending_descriptors(self, job_id: str, max_items: int) -> list[dict]:
        """Pop up to max_items descriptors from the pending queue."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:pending"
        descriptors = []
        for _ in range(max_items):
            result = await self.__redis_client.lpop(key)
            if result is None:
                break
            descriptors.append(json.loads(result))
        return descriptors

    async def has_pending_descriptors(self, job_id: str) -> bool:
        """Check if there are pending descriptors for a job."""
        length = await self.get_pending_count(job_id)
        return length > 0

    async def get_pending_count(self, job_id: str) -> int:
        """Get the current pending count for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:pending"
        length = await self.__redis_client.llen(key)
        return length

    async def get_failed_count(self, job_id: str) -> int:
        """Get the current failed count for a job."""
        length = await self.get_task_failures(job_id, "generate_cutout")
        return length

    async def push_result(self, job_id: str, result: dict):
        """Push a cutout result to the results queue for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:results"
        await self.__redis_client.rpush(key, json_dumps_with_encoders(result))

    async def pop_results(self, job_id: str, max_items: int) -> list[dict]:
        """Pop up to max_items results from the results queue."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:results"
        results = []
        for _ in range(max_items):
            result = await self.__redis_client.lpop(key)
            if result is None:
                break
            results.append(json.loads(result))
        return results

    async def increment_completed(self, job_id: str, amount: int = 1) -> int:
        """Increment the completed counter for a job by the specified amount and return the new value."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:completed"
        return await self.__redis_client.incrby(key, amount)

    async def get_completed_count(self, job_id: str) -> int:
        """Get the current completed count for a job."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:completed"
        count = await self.__redis_client.get(key)
        return int(count) if count else 0

    async def reset_completed_count(self, job_id: str):
        """Reset the completed counter for a job (used when starting a new job)."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:completed"
        await self.__redis_client.delete(key)

    async def get_batch_num(self, job_id: str) -> int:
        """Get the current batch number without incrementing."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:batch_num"
        count = await self.__redis_client.get(key)
        return int(count) if count else 0

    async def get_and_increment_batch_num(self, job_id: str) -> int:
        """Get the current batch number and increment it for the next batch."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:batch_num"
        batch_num = await self.__redis_client.incr(key)
        return batch_num - 1  # Return the batch_num before increment (0-indexed)

    async def reset_batch_num(self, job_id: str):
        """Reset the batch number counter for a job (used when starting a new job)."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:batch_num"
        await self.__redis_client.delete(key)

    async def get_results_queue_length(self, job_id: str) -> int:
        """Get the number of results currently in the results queue."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:results"
        return await self.__redis_client.llen(key)

    async def increment_task_failures(self, job_id: str, task_type: str) -> int:
        """Increment failure counter for a specific task type and return the new count."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:failures:{task_type}"
        return await self.__redis_client.incr(key)

    async def get_task_failures(self, job_id: str, task_type: str) -> int:
        """Get the current failure count for a specific task type."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:failures:{task_type}"
        count = await self.__redis_client.get(key)
        return int(count) if count else 0

    async def reset_task_failures(self, job_id: str, task_type: str):
        """Reset failure counter for a specific task type."""
        key = f"{CUTOUT_JOB_PREFIX}:{job_id}:failures:{task_type}"
        await self.__redis_client.delete(key)
