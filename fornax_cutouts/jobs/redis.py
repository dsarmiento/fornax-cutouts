from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from redis import Redis as SyncRedisClient
from redis import RedisCluster as SyncRedisCluster
from redis import ResponseError
from redis.asyncio import Redis as AsyncRedisClient
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from vo_models.uws.models import ExecutionPhase, Jobs, JobSummary, Parameters, ShortJobDescription

from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.uws import create_job_summary, create_parameters

JOB_SUMMARY_TIME_FIELDS = ["quote", "creation_time", "start_time", "end_time", "destruction"]
CUTOUT_INDEX_NAME = "cutoutJobsIdx"
CUTOUT_JOB_PREFIX = f"{CONFIG.worker.redis_prefix}:jobs"


@dataclass
class RedisKeys:
    job_id: str

    @property
    def uws(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:uws"

    @property
    def pending_tasks(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:pending_tasks"

    @property
    def failed_tasks(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:failed_tasks"

    @property
    def current_batch_num(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:current_batch_num"

    @property
    def queued_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:queued_task_count"

    @property
    def executing_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:executing_task_count"

    @property
    def completed_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:completed_task_count"

    @property
    def total_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:total_task_count"


def async_redis_client_factory():
    if CONFIG.redis.is_cluster:
        redis_client = AsyncRedisCluster(**CONFIG.redis.connection_kwargs, decode_responses=True)
    else:
        redis_client = AsyncRedisClient(**CONFIG.redis.connection_kwargs, decode_responses=True)

    return redis_client


def sync_redis_client_factory():
    if CONFIG.redis.is_cluster:
        redis_client = SyncRedisCluster(**CONFIG.redis.connection_kwargs, decode_responses=True)
    else:
        redis_client = SyncRedisClient(**CONFIG.redis.connection_kwargs, decode_responses=True)

    return redis_client


def setup_index(redis_client: SyncRedisClient | SyncRedisCluster):
    if not CONFIG.redis.search_en:
        return

    try:
        print("Setting up redis indexes")
        redis_client.ft(CUTOUT_INDEX_NAME).create_index(
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


def json_dumps_with_encoders(obj: Any) -> str:
    """
    JSON dumps with datetime serialization support.
    """

    def custom_encoders(o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat(timespec="seconds").replace("+00:00", "Z")
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    return json.dumps(obj, default=custom_encoders)


async def async_get_uws_jobs(
    redis_client: AsyncRedisClient | AsyncRedisCluster,
    phase: ExecutionPhase | None = None,
    after: datetime | None = None,
    last: int = 100,
) -> Jobs:
    uws_jobs = []

    if CONFIG.redis.search_en:
        query_str = ""

        if phase:
            query_str += f'@phase:"{phase}" '

        if phase != ExecutionPhase.ARCHIVED:
            query_str += f'-@phase:"{ExecutionPhase.ARCHIVED}" '

        if after:
            query_str += f"@creation_time:[{after.timestamp()} +inf]"

        query = Query(query_str).sort_by("creation_time", asc=False).paging(0, last)

        results = await redis_client.ft(CUTOUT_INDEX_NAME).search(query)

        for doc in results.docs:
            job_obj = json.loads(doc.json)
            job_obj["href"] = f"/cutouts/async/{job_obj['job_id']}"
            uws_jobs.append(ShortJobDescription(**job_obj))

    else:
        keys = []
        async for key in redis_client.scan_iter(match=f"{CUTOUT_JOB_PREFIX}:*:uws", count=100):
            keys.append(key.decode())

        if keys:
            for i in range(0, len(keys), 100):
                batch_keys = keys[i : i + 100]
                value = await redis_client.json().mget(batch_keys, Path.root_path())
                uws_jobs.extend(value)

            uws_jobs.sort(key=lambda job: job["creation_time"], reverse=True)
            uws_jobs = uws_jobs[:last]

    jobs = Jobs(jobref=uws_jobs)
    return jobs

class AsyncRedisCutoutJob:
    def __init__(
        self,
        redis_client: AsyncRedisClient | AsyncRedisCluster,
        job_id: str,
    ):
        self.job_id = job_id
        self.__redis_client = redis_client
        self.__keys = RedisKeys(job_id)

    async def __update_uws(self, path: str, obj: Any):
        await self.__redis_client.json().set(
            name=self.__keys.uws,
            path=path,
            obj=obj,
        )

    async def __set_time(self, time_field: str, time: datetime | None = None):
        """
        Using the linux timestamp to save the time into redis for efficient redis querying when filtering on dates

        Args:
            job_id (str): Job ID to set the time for
            time_field (str): Timestamp field to set
            time (datetime, optional): Time object to set. Defaults to datetime.now().
        """
        if time is None:
            time = datetime.now()

        await self.__update_uws(path=f"$.{time_field}", obj=time.timestamp())

    async def __set_create_time(self):
        await self.__set_time(time_field="creation_time")

    async def __set_quote(self, quote: datetime):
        await self.__set_time(time_field="quote", time=quote)

    async def __set_destruction(self, destruction: datetime):
        await self.__set_time(time_field="destruction", time=destruction)

    async def create_job(self, run_id: str | None = None, parameters: dict = {}):
        job_obj = {
            "job_id": self.job_id,
            "phase": ExecutionPhase.PENDING,
        }

        if run_id:
            job_obj["run_id"] = run_id

        if parameters:
            job_obj["parameters"] = parameters

        await self.__update_uws(path="$", obj=job_obj)
        await self.__set_create_time()

        async with self.__redis_client.pipeline() as pipe:
            pipe.set(self.__keys.total_task_count, 0)
            pipe.set(self.__keys.queued_task_count, 0)
            pipe.set(self.__keys.executing_task_count, 0)
            pipe.set(self.__keys.completed_task_count, 0)
            pipe.set(self.__keys.current_batch_num, 0)
            await pipe.execute()

    async def get_job_summary(self) -> JobSummary:
        job_json: dict = await self.__redis_client.json().get(self.__keys.uws)
        job_json.pop("results", None)
        return create_job_summary(**job_json)

    async def get_job_result_status(self):
        async with self.__redis_client.pipeline() as pipe:
            pipe.llen(self.__keys.pending_tasks)
            pipe.get(self.__keys.queued_task_count)
            pipe.get(self.__keys.executing_task_count)
            pipe.get(self.__keys.completed_task_count)
            pipe.llen(self.__keys.failed_tasks)
            pipe.get(self.__keys.total_task_count)

            pending_tasks, queued_tasks, executing_tasks, completed_tasks, failed_tasks, total_tasks = await pipe.execute()

        pending_tasks = int(pending_tasks) if pending_tasks else 0
        queued_tasks = int(queued_tasks) if queued_tasks else 0
        executing_tasks = int(executing_tasks) if executing_tasks else 0
        completed_tasks = int(completed_tasks) if completed_tasks else 0
        failed_tasks = int(failed_tasks) if failed_tasks else 0
        total_tasks = int(total_tasks) if total_tasks else 0

        return {
            "pending_jobs": pending_tasks,
            "queued_jobs": queued_tasks,
            "executing_jobs": executing_tasks,
            "completed_jobs": completed_tasks,
            "failed_jobs": failed_tasks,
            "total_jobs": total_tasks,
        }

    async def get_job_parameters(self) -> Parameters:
        job_parameters = await self.__redis_client.json().get(
            self.__keys.uws,
            "$.parameters",
        )
        return create_parameters(**job_parameters[0])


# get_and_increment_batch_num
# get_job_cutout_results
# get_expected_results
# get_completed_count
# get_task_failures
# get_pending_count
# get_batch_num
# increment_task_failures
# increment_completed


class SyncRedisCutoutJob:
    def __init__(self, redis_client: SyncRedisClient | SyncRedisCluster, job_id: str):
        self.job_id = job_id
        self.__redis_client = redis_client
        self.__keys = RedisKeys(job_id)

    def __update_uws(self, path: str, obj: Any):
        self.__redis_client.json().set(
            name=self.__keys.uws,
            path=path,
            obj=obj,
        )

    def __set_time(self, time_field: str, time: datetime | None = None):
        """
        Using the linux timestamp to save the time into redis for efficient redis querying when filtering on dates

        Args:
            job_id (str): Job ID to set the time for
            time_field (str): Timestamp field to set
            time (datetime, optional): Time object to set. Defaults to datetime.now().
        """
        if time is None:
            time = datetime.now()

        self.__update_uws(path=f"$.{time_field}", obj=time.timestamp())

    def update_job_phase(self, new_phase: ExecutionPhase):
        self.__update_uws(
            path="$.phase",
            obj=new_phase,
        )

    def set_start_time(self):
        self.__set_time(time_field="start_time")

    def set_end_time(self):
        self.__set_time(time_field="end_time")

    def push_pending_tasks(self, all_task_kwargs: list[dict]):
        all_tasks = [json_dumps_with_encoders(task_kwargs) for task_kwargs in all_task_kwargs]
        self.__redis_client.rpush(self.__keys.pending_tasks, *all_tasks)

    def push_failed_task(self, task_kwargs: dict, error_message: str):
        task_kwargs["error_message"] = error_message
        self.__redis_client.rpush(self.__keys.failed_tasks, json_dumps_with_encoders(task_kwargs))

    def pop_pending_tasks(self, max_tasks: int) -> list[dict]:
        all_task_kwargs = self.__redis_client.lpop(self.__keys.pending_tasks, max_tasks)
        all_task_kwargs = [json.loads(task_kwargs) for task_kwargs in all_task_kwargs]
        return all_task_kwargs

    def set_total_task_count(self, total_count: int):
        self.__redis_client.set(self.__keys.total_task_count, total_count)

    def get_batch_num(self) -> int:
        count = self.__redis_client.get(self.__keys.current_batch_num)
        return int(count) if count else 0

    def increment_batch_num(self) -> int:
        return self.__redis_client.incr(self.__keys.current_batch_num)

    def increment_queued_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.incrby(self.__keys.queued_task_count, amount)

    def decrement_queued_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.decrby(self.__keys.queued_task_count, amount)

    def increment_executing_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.incrby(self.__keys.executing_task_count, amount)

    def decrement_executing_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.decrby(self.__keys.executing_task_count, amount)

    def increment_completed_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.incrby(self.__keys.completed_task_count, amount)

    def decrement_completed_task_count(self, amount: int = 1) -> int:
        return self.__redis_client.decrby(self.__keys.completed_task_count, amount)

    def get_job_result_status(self):
        with self.__redis_client.pipeline() as pipe:
            pipe.llen(self.__keys.pending_tasks)
            pipe.get(self.__keys.queued_task_count)
            pipe.get(self.__keys.executing_task_count)
            pipe.get(self.__keys.completed_task_count)
            pipe.llen(self.__keys.failed_tasks)
            pipe.get(self.__keys.total_task_count)

            pending_tasks, queued_tasks, executing_tasks, completed_tasks, failed_tasks, total_tasks = pipe.execute()

        pending_tasks = int(pending_tasks) if pending_tasks else 0
        queued_tasks = int(queued_tasks) if queued_tasks else 0
        executing_tasks = int(executing_tasks) if executing_tasks else 0
        completed_tasks = int(completed_tasks) if completed_tasks else 0
        failed_tasks = int(failed_tasks) if failed_tasks else 0
        total_tasks = int(total_tasks) if total_tasks else 0

        return {
            "pending_jobs": pending_tasks,
            "queued_jobs": queued_tasks,
            "executing_jobs": executing_tasks,
            "completed_jobs": completed_tasks,
            "failed_jobs": failed_tasks,
            "total_jobs": total_tasks,
        }
