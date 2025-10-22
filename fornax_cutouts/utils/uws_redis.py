from __future__ import annotations

import json
from datetime import datetime
from pprint import pprint
from typing import Any

from redis import ResponseError
from redis.asyncio import Redis as RedisClient
from redis.asyncio import RedisCluster
from redis.commands.json.path import Path
from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from vo_models.uws.models import ExecutionPhase, Jobs, ShortJobDescription

from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.cutouts import CutoutJobSummary

JOB_SUMMARY_TIME_FIELDS = ["quote", "creation_time", "start_time", "end_time", "destruction"]
CUTOUT_INDEX_NAME = "cutoutIdx"
CUTOUT_PREFIX = "cutout"
CUTOUT_JOB_PREFIX = f"{CUTOUT_PREFIX}:jobs"


def uws_redis_client():
    return UWSRedis()


class UWSRedis:
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
                    prefix=[CUTOUT_JOB_PREFIX],
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
            name=f"{CUTOUT_JOB_PREFIX}:{job_id}",
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

    async def create_job(self, job: CutoutJobSummary) -> CutoutJobSummary:
        job_id = job.job_id

        await self.__redis_client.json().set(
            name=f"{CUTOUT_JOB_PREFIX}:{job_id}",
            path="$",
            obj=job.model_dump(),
        )
        await self.set_create_time(job_id)

        return await self.get_job(job_id)

    async def get_job(self, job_id: str) -> CutoutJobSummary:
        job_json = await self.__redis_client.json().get(f"{CUTOUT_JOB_PREFIX}:{job_id}")
        del job_json["results"]["results"]
        return CutoutJobSummary(**job_json)

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
            for key in self.__redis_client.scan_iter(match=f"{CUTOUT_JOB_PREFIX}:*", count=100):
                keys.append(key.decode())

            if keys:
                for i in range(0, len(keys), 100):
                    batch_keys = keys[i : i + 100]
                    value = self.__redis_client.json().mget(batch_keys, Path.root_path())
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

    async def get_job_result(self, job_id: str):
        return await self.__redis_client.json().get(
            f"{CUTOUT_JOB_PREFIX}:{job_id}",
            "$.results.results",
        )

    async def append_job_result(self, job_id: str, result: Any):
        await self.__redis_client.json().arrappend(
            f"{CUTOUT_JOB_PREFIX}:{job_id}",
            "$.results.results",
            result,
        )

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
