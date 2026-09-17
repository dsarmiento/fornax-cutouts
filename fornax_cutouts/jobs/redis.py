from __future__ import annotations

import json
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from vo_models.uws.types import ErrorType
from vo_models.voresource.types import UTCTimestamp

from fornax_cutouts.auth.registry import _UNKNOWN_CLIENT_BUCKET
from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.uws import create_job_summary, create_parameters
from fornax_cutouts.utils.exceptions import CutoutJobNotFoundError, NoTasksRemainingInJobError
from fornax_cutouts.utils.pagination import get_pagination_metadata

JOB_SUMMARY_TIME_FIELDS = ["quote", "creation_time", "start_time", "end_time", "destruction"]
CUTOUT_INDEX_NAME = "cutoutJobsIdx"
CUTOUT_JOB_PREFIX = f"{CONFIG.worker.redis_prefix}:jobs"
TOTAL_PENDING_TASKS_KEY = f"{CONFIG.worker.redis_prefix}:total_pending_tasks"
CUTOUT_LIMIT_PREFIX = f"{CONFIG.worker.redis_prefix}:cutout_limit"
POSITIONS_BATCH_SIZE = 100_000


@dataclass
class RedisKeys:
    job_id: str

    @property
    def uws(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:uws"

    @property
    def positions(self):
        return f"{self.uws}:positions"

    @property
    def cutout_limit_identity(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:cutout_limit_identity"

    @property
    def cutout_limit_max(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:cutout_limit_max"

    @property
    def cutout_limit_window_seconds(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:cutout_limit_window_seconds"

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
    def skipped_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:skipped_task_count"

    @property
    def total_task_count(self):
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:total_task_count"

    def batch_outstanding(self, batch_num: int) -> str:
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:batch:{batch_num}:outstanding"

    def batch_descriptors(self, batch_num: int) -> str:
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:batch:{batch_num}:descriptors"

    def batch_results(self, batch_num: int) -> str:
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:batch:{batch_num}:results"

    def batch_started(self, batch_num: int) -> str:
        return f"{CUTOUT_JOB_PREFIX}:{self.job_id}:batch:{batch_num}:started"

    @property
    def job_keys(self) -> list[str]:
        return [
            self.uws,
            self.positions,
            self.pending_tasks,
            self.failed_tasks,
            self.cutout_limit_identity,
            self.cutout_limit_max,
            self.cutout_limit_window_seconds,
            self.current_batch_num,
            self.queued_task_count,
            self.executing_task_count,
            self.completed_task_count,
            self.skipped_task_count,
            self.total_task_count,
        ]

    def batch_keys(self, batch_num: int) -> list[str]:
        return [
            self.batch_outstanding(batch_num),
            self.batch_descriptors(batch_num),
            self.batch_results(batch_num),
            self.batch_started(batch_num),
        ]


@dataclass
class CutoutLimitKeys:
    """
    Redis keys for one identity's rolling-window cutout budget.

    Both keys share a `{identity}` hash tag so the reserve/reconcile/release Lua scripts stay
    single-slot under Redis Cluster.
    """

    identity: str

    @property
    def events(self):
        return f"{CUTOUT_LIMIT_PREFIX}:{{{self.identity}}}:events"

    @property
    def counts(self):
        return f"{CUTOUT_LIMIT_PREFIX}:{{{self.identity}}}:counts"


def recalculate_total_pending_tasks(redis_client: SyncRedisClient | SyncRedisCluster) -> int:
    """Sum pending-task queue lengths across jobs and reset the global metric key."""
    total = 0
    for key in redis_client.scan_iter(match=f"{CUTOUT_JOB_PREFIX}:*:pending_tasks", count=500):
        total += redis_client.llen(key)
    redis_client.set(TOTAL_PENDING_TASKS_KEY, total)
    return total


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
    JSON dumps with UTCTimestamp serialization support.
    """

    def custom_encoders(o: Any) -> Any:
        if isinstance(o, datetime):
            return UTCTimestamp(o).isoformat()
        if isinstance(o, UTCTimestamp):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    return json.dumps(obj, default=custom_encoders)


def _build_uws_jobs_search_query(
    phases: list[ExecutionPhase],
    after: UTCTimestamp | None = None,
) -> str:
    query_str = ""

    if phases:
        phase_filter = " | ".join(phase.value for phase in phases)
        query_str += f"@phase:{{{phase_filter}}} "

    if not phases or ExecutionPhase.ARCHIVED not in phases:
        query_str += f"-@phase:{{{ExecutionPhase.ARCHIVED.value}}} "

    if after:
        query_str += f"@creation_time:[{after.timestamp()} +inf]"

    return query_str


def _filter_uws_jobs_by_phase(
    jobs: list[dict[str, Any]],
    phases: list[ExecutionPhase],
) -> list[dict[str, Any]]:
    if phases:
        return [job for job in jobs if job and job.get("phase") in phases]
    return [job for job in jobs if job and job.get("phase") != ExecutionPhase.ARCHIVED]


async def async_get_uws_jobs(
    redis_client: AsyncRedisClient | AsyncRedisCluster,
    phases: list[ExecutionPhase],
    after: datetime | None = None,
    last: int = 100,
) -> Jobs:
    uws_jobs = []

    if CONFIG.redis.search_en:
        query_str = _build_uws_jobs_search_query(phases, after=after)
        query = Query(query_str).sort_by("creation_time", asc=False).paging(0, last)

        results = await redis_client.ft(CUTOUT_INDEX_NAME).search(query)

        for doc in results.docs:
            job_obj = json.loads(doc.json)
            job_obj["href"] = f"/cutouts/async/{job_obj['job_id']}"
            uws_jobs.append(ShortJobDescription(**job_obj))

    else:
        keys = []
        async for key in redis_client.scan_iter(match=f"{CUTOUT_JOB_PREFIX}:*:uws", count=100):
            keys.append(key)

        if keys:
            for i in range(0, len(keys), 100):
                batch_keys = keys[i : i + 100]
                value = await redis_client.json().mget(batch_keys, Path.root_path())
                uws_jobs.extend(value)

            uws_jobs = _filter_uws_jobs_by_phase(uws_jobs, phases)
            uws_jobs.sort(key=lambda job: job["creation_time"], reverse=True)
            uws_jobs = uws_jobs[:last]

    jobs = Jobs(jobref=uws_jobs)
    return jobs


class AsyncRedisCutoutJob:
    """
    Async Redis accessor for a single UWS cutout job.

    Used by the API layer to create jobs and serve job metadata. Mutations that drive
    task execution (batching, counters, phase transitions) live on `SyncRedisCutoutJob`.
    """

    def __init__(
        self,
        redis_client: AsyncRedisClient | AsyncRedisCluster,
        job_id: str,
    ):
        """
        Args:
            redis_client: Async Redis client used for all job key access.
            job_id: UWS job identifier.
        """
        self.job_id = job_id
        self.__redis_client = redis_client
        self.__keys = RedisKeys(job_id)

    async def ensure_exists(self) -> dict:
        """
        Return the UWS job document.

        Raises:
            CutoutJobNotFoundError: If no UWS document exists for this job.

        Returns:
            dict: The full UWS job JSON object.
        """
        job_json = await self.__redis_client.json().get(self.__keys.uws)
        if not job_json:
            raise CutoutJobNotFoundError(self.job_id)
        return job_json

    async def __update_uws(self, path: str, obj: Any):
        """
        Write ``obj`` to the UWS JSON document at JSONPath ``path``.

        Args:
            path (str): JSONPath within the UWS document (e.g. ``$.phase``).
            obj (Any): Value to store at ``path``.
        """
        await self.__redis_client.json().set(
            name=self.__keys.uws,
            path=path,
            obj=obj,
        )

    async def __set_time(self, time_field: str, time: UTCTimestamp | None = None) -> UTCTimestamp:
        """
        Set a UWS timestamp field as a Unix timestamp for efficient Redis date filtering.

        Args:
            time_field (str): UWS field name to set (e.g. ``creation_time``).
            time (UTCTimestamp, optional): Time to store. Defaults to ``UTCTimestamp.now(timezone.utc)``.

        Returns:
            UTCTimestamp: The time that was written.
        """
        if time is None:
            time = UTCTimestamp.now(timezone.utc)

        await self.__update_uws(path=f"$.{time_field}", obj=time.timestamp())

        return time

    async def __set_create_time(self) -> UTCTimestamp:
        """
        Set the UWS ``creation_time`` to the current time.

        Returns:
            UTCTimestamp: The time that was written.
        """
        return await self.__set_time(time_field="creation_time")

    async def __set_quote(self, quote: UTCTimestamp) -> UTCTimestamp:
        """
        Set the UWS ``quote`` (estimated completion) timestamp.

        Args:
            quote (UTCTimestamp): Estimated completion time.

        Returns:
            UTCTimestamp: The time that was written.
        """
        return await self.__set_time(time_field="quote", time=quote)

    async def __set_destruction(self, destruction: UTCTimestamp) -> UTCTimestamp:
        """
        Set the UWS ``destruction`` (TTL expiry) timestamp.

        Args:
            destruction (UTCTimestamp): Job destruction / key-expiry time.

        Returns:
            UTCTimestamp: The time that was written.
        """
        return await self.__set_time(time_field="destruction", time=destruction)

    async def create_job(
        self,
        run_id: str | None = None,
        parameters: dict = {},
        identity: str = _UNKNOWN_CLIENT_BUCKET,
        cutout_limit: int | None = None,
        window_seconds: int | None = None,
    ):
        """
        Create a new cutout UWS job in Redis.

        Initializes the UWS job document, enqueues positions, resets task counters, and
        optionally snapshots cutout-limit settings for the worker.

        Args:
            run_id (str | None): Optional run ID from the UWS spec.
            parameters (dict): Job parameters; ``position`` (list) is extracted into a
                separate Redis list and replaced with ``position_count``.
            identity (str): Principal identity for cutout-limit tracking (default: _UNKNOWN_CLIENT_BUCKET).
            cutout_limit (int | None): Max cutouts allowed within the rate window (default: None, unlimited).
            window_seconds (int | None): Rolling rate-limit window in seconds (default: None, unlimited).
        """

        job_obj = {
            "job_id": self.job_id,
            "phase": ExecutionPhase.PENDING,
        }
        positions_obj = []

        if run_id:
            job_obj["run_id"] = run_id

        if parameters:
            # Extract the positions from the parameters as this can be a large list.
            positions_obj = parameters.pop("position", [])
            parameters["position_count"] = len(positions_obj)
            job_obj["parameters"] = parameters

        await self.__update_uws(path="$", obj=job_obj)

        # Push the positions to the Redis positions queue in batches.
        async with self.__redis_client.pipeline() as pipe:
            for idx in range(0, len(positions_obj), POSITIONS_BATCH_SIZE):
                batch_positions = positions_obj[idx : idx + POSITIONS_BATCH_SIZE]
                pipe.rpush(self.__keys.positions, *batch_positions)
            await pipe.execute()

        create_time = await self.__set_create_time()

        destruction_time = create_time + timedelta(seconds=CONFIG.async_ttl)
        await self.__set_destruction(destruction_time)

        async with self.__redis_client.pipeline() as pipe:
            pipe.set(self.__keys.total_task_count, 0)
            pipe.set(self.__keys.queued_task_count, 0)
            pipe.set(self.__keys.executing_task_count, 0)
            pipe.set(self.__keys.completed_task_count, 0)
            pipe.set(self.__keys.current_batch_num, 0)
            pipe.set(self.__keys.cutout_limit_identity, identity)
            if cutout_limit is not None:
                pipe.set(self.__keys.cutout_limit_max, cutout_limit)
            if window_seconds is not None:
                pipe.set(self.__keys.cutout_limit_window_seconds, window_seconds)
            for key in self.__keys.job_keys:
                pipe.expireat(key, int(destruction_time.timestamp()))
            await pipe.execute()

    async def get_job_summary(self, base_url: str = "") -> JobSummary:
        """
        Return a UWS ``JobSummary``.

        When ``base_url`` is set, replaces the positions parameter with a link to
        ``{base_url}/parameters/position``.

        Args:
            base_url (str): Optional URL prefix for the positions parameter link.

        Raises:
            CutoutJobNotFoundError: If no UWS document exists for this job.

        Returns:
            JobSummary: Parsed UWS job summary.
        """
        job_json = await self.ensure_exists()
        # job_json.pop("results", None)
        if base_url:
            job_json["parameters"]["position"] = f"{base_url}/parameters/position"

        return create_job_summary(**job_json)

    async def get_job_result_status(self) -> dict:
        """
        Return per-task counters for this job.

        Raises:
            CutoutJobNotFoundError: If no UWS document exists for this job.

        Returns:
            dict: Counter mapping with keys ``pending_jobs``, ``queued_jobs``,
            ``executing_jobs``, ``completed_jobs``, ``skipped_jobs``, ``failed_jobs``,
            and ``total_jobs``.
        """
        await self.ensure_exists()
        async with self.__redis_client.pipeline() as pipe:
            pipe.llen(self.__keys.pending_tasks)
            pipe.get(self.__keys.queued_task_count)
            pipe.get(self.__keys.executing_task_count)
            pipe.get(self.__keys.completed_task_count)
            pipe.get(self.__keys.skipped_task_count)
            pipe.llen(self.__keys.failed_tasks)
            pipe.get(self.__keys.total_task_count)

            (
                pending_tasks,
                queued_tasks,
                executing_tasks,
                completed_tasks,
                skipped_tasks,
                failed_tasks,
                total_tasks,
            ) = await pipe.execute()

        pending_tasks = int(pending_tasks) if pending_tasks else 0
        queued_tasks = int(queued_tasks) if queued_tasks else 0
        executing_tasks = int(executing_tasks) if executing_tasks else 0
        completed_tasks = int(completed_tasks) if completed_tasks else 0
        skipped_tasks = int(skipped_tasks) if skipped_tasks else 0
        failed_tasks = int(failed_tasks) if failed_tasks else 0
        total_tasks = int(total_tasks) if total_tasks else 0

        return {
            "pending_jobs": pending_tasks,
            "queued_jobs": queued_tasks,
            "executing_jobs": executing_tasks,
            "completed_jobs": completed_tasks,
            "skipped_jobs": skipped_tasks,
            "failed_jobs": failed_tasks,
            "total_jobs": total_tasks,
        }

    async def get_job_parameters(self, position_base_url: str) -> Parameters:
        """
        Return job parameters with ``position`` replaced by a URL.

        The positions list can be large, so callers receive a link rather than inline data.

        Args:
            position_base_url (str): URL pointing to the paginated positions endpoint.

        Raises:
            CutoutJobNotFoundError: If no UWS document exists for this job.

        Returns:
            Parameters: Parsed UWS parameters model.
        """
        job_json = await self.ensure_exists()
        job_parameters = job_json["parameters"]
        job_parameters["position"] = f"{position_base_url}"
        return create_parameters(**job_parameters)

    async def get_job_positions(self, page: int = 0, limit: int = 100, base_url: str = "") -> dict:
        """
        Return a paginated slice of position strings for this job due to possible large number of positions.

        Args:
            page (int): Zero-based page index.
            limit (int): Maximum positions per page.
            base_url (str): Base URL used to build pagination links in the response metadata.

        Raises:
            CutoutJobNotFoundError: If no UWS document exists for this job.

        Returns:
            dict: Pagination metadata plus a ``positions`` list of position strings.
        """
        await self.ensure_exists()
        start = page * limit
        end = (page + 1) * limit - 1
        positions = await self.__redis_client.lrange(self.__keys.positions, start, end)
        metadata = get_pagination_metadata(page, limit, len(positions), base_url)
        metadata["positions"] = positions
        return metadata


class SyncRedisCutoutJob:
    """
    Sync Redis accessor for celery worker-side cutout job lifecycle.

    Handles task queuing, batch preparation, per-task counters, UWS phase transitions,
    and result aggregation. The API layer uses `AsyncRedisCutoutJob` for job creation and
    read-only metadata.
    """

    def __init__(self, redis_client: SyncRedisClient | SyncRedisCluster, job_id: str):
        """
        Args:
            redis_client: Sync Redis client used for all job key access.
            job_id: UWS job identifier.
        """
        self.job_id = job_id
        self.__redis_client = redis_client
        self.__keys = RedisKeys(job_id)
        self.__destruction_ts: float | None = None

    def __get_destruction_ts(self) -> float:
        """
        Resolve and cache the job destruction timestamp used for TTL extension.

        Falls back to ``creation_time + CONFIG.async_ttl``, then to ``now + CONFIG.async_ttl``.

        Returns:
            float: Unix timestamp when job keys should expire.
        """
        if self.__destruction_ts is None:
            destruction = self.__redis_client.json().get(self.__keys.uws, "$.destruction")
            if destruction and destruction[0] is not None:
                self.__destruction_ts = float(destruction[0])
            else:
                creation = self.__redis_client.json().get(self.__keys.uws, "$.creation_time")
                if creation and creation[0] is not None:
                    self.__destruction_ts = float(creation[0]) + CONFIG.async_ttl
                else:
                    self.__destruction_ts = UTCTimestamp.now(timezone.utc).timestamp() + CONFIG.async_ttl
        return self.__destruction_ts

    def __expire(self, pipe, *keys: str) -> None:
        """
        Queue EXPIREAT on ``keys`` using the cached destruction timestamp.

        Args:
            pipe: Active Redis pipeline to enqueue commands on.
            *keys (str): Redis keys to expire.
        """
        destruction_ts = int(self.__get_destruction_ts())
        for key in keys:
            pipe.expireat(key, destruction_ts)

    def __update_uws(self, path: str, obj: Any):
        """
        Write ``obj`` to the UWS JSON document at JSONPath ``path``.

        Args:
            path (str): JSONPath within the UWS document (e.g. ``$.phase``).
            obj (Any): Value to store at ``path``.
        """
        self.__redis_client.json().set(
            name=self.__keys.uws,
            path=path,
            obj=obj,
        )

    def __set_time(self, time_field: str, time: UTCTimestamp | None = None):
        """
        Set a UWS timestamp field as a Unix timestamp for efficient Redis date filtering.

        Args:
            time_field (str): UWS field name to set (e.g. ``start_time``).
            time (UTCTimestamp, optional): Time to store. Defaults to ``UTCTimestamp.now(timezone.utc)``.
        """
        if time is None:
            time = UTCTimestamp.now(timezone.utc)

        self.__update_uws(path=f"$.{time_field}", obj=time.timestamp())

    def get_job_parameters(self) -> dict:
        """
        Return the UWS parameters dict for this job.

        Returns:
            dict: Job parameters as stored in the UWS document.
        """
        job_parameters = self.__redis_client.json().get(self.__keys.uws, "$.parameters")
        return job_parameters[0]

    def get_cutout_limit_budget(self) -> tuple[str | None, int | None, int | None]:
        """
        Return the cutout-limit snapshot recorded at job creation.

        ``cutout_limit`` and ``window_seconds`` are ``None`` when the principal was
        unlimited (or cutout limiting was disabled), signaling the worker does not need
        to recheck on reconcile.

        Returns:
            tuple[str | None, int | None, int | None]: ``(identity, cutout_limit,
            window_seconds)``.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.get(self.__keys.cutout_limit_identity)
            pipe.get(self.__keys.cutout_limit_max)
            pipe.get(self.__keys.cutout_limit_window_seconds)
            identity, cutout_limit, window_seconds = pipe.execute()

        return (
            identity,
            int(cutout_limit) if cutout_limit is not None else None,
            int(window_seconds) if window_seconds is not None else None,
        )

    def scan_job_positions(self) -> Generator[list[str], None, None]:
        """
        Yield position strings from the Redis list in batches.

        Returns:
            Generator[list[str], None, None]: Batches of up to ``POSITIONS_BATCH_SIZE``
            position strings.
        """
        start = 0
        while True:
            end = start + POSITIONS_BATCH_SIZE - 1
            batch = self.__redis_client.lrange(self.__keys.positions, start, end)
            if not batch:
                # No more positions to scan.
                break
            yield batch
            if len(batch) < POSITIONS_BATCH_SIZE:
                # Last batch is less than `POSITIONS_BATCH_SIZE`; we are done scanning.
                break
            start += POSITIONS_BATCH_SIZE

    def update_job_phase(self, new_phase: ExecutionPhase):
        """
        Set the UWS execution phase.

        Pipeline consolidation: candidate for ``start_job``, ``complete_job``, ``fail_job``,
        and ``queue_job`` (each issues a separate round-trip today).

        Args:
            new_phase (ExecutionPhase): Phase to write to the UWS document.
        """
        self.__update_uws(
            path="$.phase",
            obj=new_phase,
        )

    def set_start_time(self):
        """
        Set UWS ``start_time`` to the current time.

        Pipeline consolidation: candidate for ``start_job``.
        """
        self.__set_time(time_field="start_time")

    def set_end_time(self):
        """
        Set UWS ``end_time`` to the current time.

        Pipeline consolidation: candidate for ``complete_job`` and ``fail_job``.
        """
        self.__set_time(time_field="end_time")

    def push_pending_tasks(self, all_task_kwargs: list[dict]):
        """
        Append serialized task descriptors to the pending-tasks queue.

        Pipeline consolidation: ``decrement_executing_task_count``,
        ``decrement_queued_task_count``, and ``increment_total_pending_tasks`` are
        candidates to merge here for the batch-watchdog requeue path.

        Args:
            all_task_kwargs (list[dict]): Task descriptor dicts to enqueue.
        """
        all_tasks = [json_dumps_with_encoders(task_kwargs) for task_kwargs in all_task_kwargs]
        with self.__redis_client.pipeline() as pipe:
            pipe.rpush(self.__keys.pending_tasks, *all_tasks)
            self.__expire(pipe, self.__keys.pending_tasks)
            pipe.execute()

    def clear_pending_tasks(self) -> int:
        """
        Discard all not-yet-dispatched pending tasks for this job.

        Pipeline consolidation: already pipelined; ``decrement_total_pending_tasks`` in
        ``fail_job`` is a candidate to join this pipeline.

        Returns:
            int: Number of tasks removed from the pending queue.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.llen(self.__keys.pending_tasks)
            pipe.delete(self.__keys.pending_tasks)
            count = pipe.execute()[0]

        return count

    def set_total_task_count(self, total_count: int):
        """
        Set the expected total task count, preserving the key TTL.

        Pipeline consolidation: ``increment_total_pending_tasks`` is a candidate to merge
        here (both are called together from ``schedule_job``).

        Args:
            total_count (int): Total number of cutout tasks expected for this job.
        """
        self.__redis_client.set(self.__keys.total_task_count, total_count, keepttl=True)

    def increment_batch_num(self) -> int:
        """
        Increment and return the current batch number for this job.

        Returns:
            int: Batch number after incrementing.
        """
        return self.__redis_client.incr(self.__keys.current_batch_num)

    def decrement_queued_task_count(self, amount: int = 1) -> int:
        """
        Decrement the queued-task counter by ``amount``.

        Pipeline consolidation: candidate for ``push_pending_tasks`` (batch-watchdog
        requeue path).

        Args:
            amount (int): Number to subtract from the counter.

        Returns:
            int: Counter value after decrementing.
        """
        return self.__redis_client.decrby(self.__keys.queued_task_count, amount)

    def decrement_executing_task_count(self, amount: int = 1) -> int:
        """
        Decrement the executing-task counter by ``amount``.

        Pipeline consolidation: candidate for ``push_pending_tasks`` (batch-watchdog
        requeue path).

        Args:
            amount (int): Number to subtract from the counter.

        Returns:
            int: Counter value after decrementing.
        """
        return self.__redis_client.decrby(self.__keys.executing_task_count, amount)

    def increment_total_pending_tasks(self, amount: int = 1):
        """
        Increment the global pending-tasks metric across all jobs.

        Pipeline consolidation: candidate for ``set_total_task_count`` (schedule path)
        and ``push_pending_tasks`` (batch-watchdog requeue path).

        Args:
            amount (int): Number to add to the global counter.
        """
        self.__redis_client.incrby(TOTAL_PENDING_TASKS_KEY, amount)

    def decrement_total_pending_tasks(self, amount: int = 1):
        """
        Decrement the global pending-tasks metric across all jobs.

        Pipeline consolidation: candidate for ``fail_job``.

        Args:
            amount (int): Number to subtract from the global counter.
        """
        self.__redis_client.decrby(TOTAL_PENDING_TASKS_KEY, amount)

    def get_batch_outstanding(self, batch_num: int) -> int:
        """
        Return the number of tasks still outstanding in a batch.

        Args:
            batch_num (int): Batch identifier within this job.

        Returns:
            int: Outstanding task count (``0`` when unset).
        """
        raw = self.__redis_client.get(self.__keys.batch_outstanding(batch_num))
        return int(raw) if raw is not None else 0

    def reset_batch_outstanding(self, batch_num: int):
        """
        Reset a batch's outstanding counter to zero.

        Pipeline consolidation: batch-watchdog counter adjustments
        (``decrement_executing_task_count``, ``decrement_queued_task_count``,
        ``increment_total_pending_tasks``) are candidates to merge here.

        Args:
            batch_num (int): Batch identifier within this job.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.set(self.__keys.batch_outstanding(batch_num), 0)
            self.__expire(pipe, self.__keys.batch_outstanding(batch_num))
            pipe.execute()

    def get_batch_descriptors(self, batch_num: int) -> list[dict]:
        """
        Return the task descriptors stored for a batch.

        Args:
            batch_num (int): Batch identifier within this job.

        Returns:
            list[dict]: Task descriptor dicts (empty when unset).
        """
        raw = self.__redis_client.get(self.__keys.batch_descriptors(batch_num))
        if not raw:
            return []
        return json.loads(raw)

    def batch_result_hexists(self, batch_num: int, increment_id: int) -> bool:
        """
        Check if a result exists for a specific task in the batch.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.

        Returns:
            bool: True if a result exists for the provided increment_id in the batch, False otherwise.
        """
        return bool(self.__redis_client.hexists(self.__keys.batch_results(batch_num), str(increment_id)))

    def batch_task_was_started(self, batch_num: int, increment_id: int) -> bool:
        """
        Check if a task was started in the batch.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.

        Returns:
            bool: True if the task was started in the batch, False otherwise.
        """
        return bool(self.__redis_client.hexists(self.__keys.batch_started(batch_num), str(increment_id)))

    def get_batch_results(self, batch_num: int) -> list[Any]:
        """
        Return all per-task results collected for a batch.

        Args:
            batch_num (int): Batch identifier within this job.

        Returns:
            list[Any]: Parsed result objects from the batch results hash (order not
            guaranteed).
        """
        results = self.__redis_client.hgetall(self.__keys.batch_results(batch_num))
        return [json.loads(raw) for raw in results.values()]

    # Task operations
    def start_task(self, batch_num: int, increment_id: int):
        """
        Mark a task as started and move it from queued to executing.

        Atomically updates the batch-started hash and job-level counters in one pipeline.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.hset(self.__keys.batch_started(batch_num), str(increment_id), "1")
            pipe.decr(self.__keys.queued_task_count)
            pipe.incr(self.__keys.executing_task_count)
            self.__expire(pipe, self.__keys.batch_started(batch_num))
            pipe.execute()

    def skip_task(self, batch_num: int, increment_id: int) -> int:
        """
        Record a skipped task (no data) and update batch/job counters.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.

        Returns:
            int: Remaining outstanding count for the batch after this update.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.hset(self.__keys.batch_results(batch_num), str(increment_id), "null")
            pipe.decr(self.__keys.batch_outstanding(batch_num))
            pipe.decr(self.__keys.executing_task_count)
            pipe.incr(self.__keys.skipped_task_count)
            self.__expire(pipe, self.__keys.batch_results(batch_num), self.__keys.skipped_task_count)
            remaining = pipe.execute()[1]
        return int(remaining) if remaining else 0

    def fail_task(self, batch_num: int, increment_id: int, task_kwargs: dict, error_message: str) -> int:
        """
        Record a failed task, append it to the failed-tasks list, and update counters.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.
            task_kwargs (dict): Original task descriptor; mutated in place with
                ``error_message``.
            error_message (str): Failure reason to persist with the descriptor.

        Returns:
            int: Remaining outstanding count for the batch after this update.
        """
        task_kwargs["error_message"] = error_message
        with self.__redis_client.pipeline() as pipe:
            pipe.hset(self.__keys.batch_results(batch_num), str(increment_id), "null")
            pipe.decr(self.__keys.batch_outstanding(batch_num))
            pipe.decr(self.__keys.executing_task_count)
            pipe.rpush(self.__keys.failed_tasks, json_dumps_with_encoders(task_kwargs))
            self.__expire(pipe, self.__keys.batch_results(batch_num), self.__keys.failed_tasks)
            remaining = pipe.execute()[1]
        return int(remaining) if remaining else 0

    def complete_task(self, batch_num: int, increment_id: int, result_json: str) -> int:
        """
        Record a completed task result and update batch/job counters.

        Args:
            batch_num (int): Batch identifier within this job.
            increment_id (int): Task index within the batch.
            result_json (str): Serialized cutout result JSON (or ``None`` serialized).

        Returns:
            int: Remaining outstanding count for the batch after this update.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.hset(self.__keys.batch_results(batch_num), str(increment_id), result_json)
            pipe.decr(self.__keys.batch_outstanding(batch_num))
            pipe.decr(self.__keys.executing_task_count)
            pipe.incr(self.__keys.completed_task_count)
            self.__expire(pipe, self.__keys.batch_results(batch_num))
            remaining = pipe.execute()[1]
        return int(remaining) if remaining else 0

    # Job operations
    def queue_job(self):
        """
        Transition a job from PENDING to QUEUED if it has not advanced further.

        No-op when the job is already queued, executing, or terminal.
        """
        current_phase = self.__redis_client.json().get(self.__keys.uws, "$.phase")
        if current_phase and current_phase[0] in (
            ExecutionPhase.QUEUED,
            ExecutionPhase.EXECUTING,
            ExecutionPhase.COMPLETED,
            ExecutionPhase.ERROR,
            ExecutionPhase.ABORTED,
        ):
            return
        self.update_job_phase(ExecutionPhase.QUEUED)

    def start_job(self):
        """
        Transition a job to EXECUTING and record ``start_time``.

        Idempotent once the job is executing or terminal. Pipeline consolidation:
        ``update_job_phase`` and ``set_start_time`` are candidates to merge here.
        """
        current_phase = self.__redis_client.json().get(self.__keys.uws, "$.phase")
        if current_phase and current_phase[0] in (
            ExecutionPhase.EXECUTING,
            ExecutionPhase.COMPLETED,
            ExecutionPhase.ERROR,
            ExecutionPhase.ABORTED,
        ):
            return
        self.update_job_phase(ExecutionPhase.EXECUTING)
        self.set_start_time()

    def complete_job(self):
        """
        Transition a job to COMPLETED and record ``end_time``.

        Pipeline consolidation: ``update_job_phase`` and ``set_end_time`` are candidates
        to merge here.
        """
        self.update_job_phase(ExecutionPhase.COMPLETED)
        self.set_end_time()

    def fail_job(self, message: str, error_type: ErrorType = ErrorType.FATAL):
        """
        Terminally fail the job with an error summary (e.g. cutout limit exceeded).

        Clears pending tasks and adjusts the global pending-tasks metric. Pipeline
        consolidation: ``update_job_phase``, ``set_end_time``, ``clear_pending_tasks``,
        and ``decrement_total_pending_tasks`` are candidates to merge into one pipeline
        under ``fail_job``.

        Args:
            message (str): Human-readable error summary for the UWS document.
            error_type (ErrorType): UWS error classification. Defaults to ``FATAL``.
        """
        self.__update_uws(
            path="$.error_summary",
            obj={"message": message, "type": error_type, "has_detail": False},
        )
        self.update_job_phase(ExecutionPhase.ERROR)
        self.set_end_time()
        cleared = self.clear_pending_tasks()
        self.decrement_total_pending_tasks(cleared)

    def get_job_result_status(self):
        """
        Return per-task counters for this job.

        Keys use the ``*_jobs`` naming convention expected by the UWS result model.

        Returns:
            dict: Counter mapping with keys ``pending_jobs``, ``queued_jobs``,
            ``executing_jobs``, ``completed_jobs``, ``skipped_jobs``, ``failed_jobs``,
            and ``total_jobs``.
        """
        with self.__redis_client.pipeline() as pipe:
            pipe.llen(self.__keys.pending_tasks)
            pipe.get(self.__keys.queued_task_count)
            pipe.get(self.__keys.executing_task_count)
            pipe.get(self.__keys.completed_task_count)
            pipe.get(self.__keys.skipped_task_count)
            pipe.llen(self.__keys.failed_tasks)
            pipe.get(self.__keys.total_task_count)

            pending_tasks, queued_tasks, executing_tasks, completed_tasks, skipped_tasks, failed_tasks, total_tasks = (
                pipe.execute()
            )

        pending_tasks = int(pending_tasks) if pending_tasks else 0
        queued_tasks = int(queued_tasks) if queued_tasks else 0
        executing_tasks = int(executing_tasks) if executing_tasks else 0
        completed_tasks = int(completed_tasks) if completed_tasks else 0
        skipped_tasks = int(skipped_tasks) if skipped_tasks else 0
        failed_tasks = int(failed_tasks) if failed_tasks else 0
        total_tasks = int(total_tasks) if total_tasks else 0

        return {
            "pending_jobs": pending_tasks,
            "queued_jobs": queued_tasks,
            "executing_jobs": executing_tasks,
            "completed_jobs": completed_tasks,
            "skipped_jobs": skipped_tasks,
            "failed_jobs": failed_tasks,
            "total_jobs": total_tasks,
        }

    # Batch operations
    def delete_batch_keys(self, batch_num: int):
        """
        Delete all Redis keys scoped to a batch.

        Pipeline consolidation: candidate to merge with post-batch cleanup in
        ``write_results`` (called from its ``finally`` block today).

        Args:
            batch_num (int): Batch identifier within this job.
        """
        self.__redis_client.delete(*self.__keys.batch_keys(batch_num))

    def prepare_batch(self, batch_num: int, batch_size: int):
        """
        Pop pending tasks and initialize Redis state for a new batch.

        Clears any prior batch keys, moves tasks from pending to queued, and stores
        batch descriptors and the outstanding counter.

        Args:
            batch_num (int): Batch identifier within this job.
            batch_size (int): Maximum number of tasks to pop from the pending queue.

        Raises:
            NoTasksRemainingInBatchError: When the pending queue is empty.

        Returns:
            list[dict]: Task descriptor dicts for the batch.
        """
        batch_tasks = self.__redis_client.lpop(self.__keys.pending_tasks, batch_size) or []

        if not batch_tasks:
            self.delete_batch_keys(batch_num)
            raise NoTasksRemainingInJobError(self.job_id)

        batch_tasks = [json.loads(task_kwargs) for task_kwargs in batch_tasks]
        num_descriptors = len(batch_tasks)

        self.delete_batch_keys(batch_num)
        with self.__redis_client.pipeline() as pipe:
            pipe.decrby(TOTAL_PENDING_TASKS_KEY, num_descriptors)
            pipe.incrby(self.__keys.queued_task_count, num_descriptors)
            pipe.set(self.__keys.batch_outstanding(batch_num), num_descriptors)
            pipe.set(self.__keys.batch_descriptors(batch_num), json.dumps(batch_tasks))
            self.__expire(pipe, self.__keys.batch_outstanding(batch_num), self.__keys.batch_descriptors(batch_num))
            pipe.execute()

        return batch_tasks
