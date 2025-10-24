import json
import uuid
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Path, Query, Request, Response, status
from fastapi.responses import RedirectResponse
from fastapi_utils.cbv import cbv
from vo_models.uws.models import Jobs
from vo_models.uws.types import ExecutionPhase
from vo_models.voresource.types import UTCTimestamp

from fornax_cutouts.models.cutouts import CutoutJobSummary
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.tasks import schedule_job
from fornax_cutouts.utils.uws_redis import UWSRedis, uws_redis_client

uws_router = APIRouter(prefix="/cutouts")


class PhaseAction(StrEnum):
    """Enum for async job phase requests"""

    ABORT = "ABORT"
    RUN = "RUN"


class XmlResponse(Response):
    media_type = "text/xml"


@cbv(uws_router)
class CutoutsUWSHandler:
    uws_redis: UWSRedis = Depends(uws_redis_client)

    @uws_router.get("/async")
    async def get_jobs(
        self,
        request: Request,
        phase: Annotated[
            list[ExecutionPhase] | None,
            Query(description="The current execution phase to filter jobs by."),
        ] = None,
        after: Annotated[
            UTCTimestamp | None,
            Query(description="Return jobs with creation times after the given date."),
        ] = None,
        last: Annotated[
            int | None,
            Query(description="Return the given number of most recently created jobs.", gt=0),
        ] = None,
    ) -> Jobs:
        """
        Returns Jobs list per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        if last is None:
            # If the user isn't limiting the number of jobs, we'll do it for them
            query_params = dict(request.query_params)
            query_params["last"] = 100
            new_query = urlencode(query_params)
            redirect_url = f"{request.url.path}?{new_query}"
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_303_SEE_OTHER)

        jobs = await self.uws_redis.get_jobs(phase=phase, after=after, last=last)
        return XmlResponse(jobs.to_xml())

    @uws_router.post("/async")
    async def post_job(
        self,
        request: Request,
        position: Annotated[list[str], Form()],
        size: Annotated[int, Form()],
        output_format: Annotated[list[str], Form()] = ["fits"],
        # relevant spec: https://www.ivoa.net/documents/DALI/20170517/REC-DALI-1.1.html#tth_sEc3.4.6
        run_id: Annotated[
            str,
            Form(
                max_length=64,
                description="RUNID for the request",
                alias="RUNID",
            ),
        ] = "",
    ):
        job_summary = CutoutJobSummary(
            job_id=uuid.uuid4().hex[:8],
            run_id=run_id,
            phase=ExecutionPhase.PENDING,
        )
        job_summary = await self.uws_redis.create_job(job_summary)

        form = await request.form()
        mission_params = {
            mission: json.loads(params)
            for mission, params in form.items()
            if mission in cutout_registry.get_source_names()
        }

        schedule_job.apply_async(
            task_id=f"cutout-{job_summary.job_id}",
            kwargs={
                "job_id": job_summary.job_id,
                "position": position,
                "size": size,
                "mission_params": mission_params,
                "output_format": output_format,
            },
        )
        redirect_url = f"{request.url.path}/{job_summary.job_id}"
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    @uws_router.get("/async/{job_id}")
    async def get_job(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ) -> CutoutJobSummary:
        try:
            job_summary = await self.uws_redis.get_job(job_id)
            return XmlResponse(job_summary.to_xml())

        except TypeError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cutout job {job_id!r} not found.",
            )

    @uws_router.delete("/async/{job_id}")
    def delete_job(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Deletes specific Job info per UWS spec. Cancels if running. Note the /jobs path for TAP is just /async
        Per https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#d1e1390:
        Sending a HTTP DELETE to a Job resource destroys that job, with the meaning noted in the definition
        of the Job object, above. No other resource of the UWS may be directly deleted by the client.
        The response to this request must have code 303 “See other” and the Location header of the response
        must point to the Job List at the /{jobs} URI.
        """
        return

    @uws_router.get("/async/{job_id}/phase")
    async def get_job_phase(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.phase

    @uws_router.post("/async{job_id}/phase")
    def post_job_phase(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
        phase: Annotated[
            PhaseAction,
            Form(alias="PHASE", description="Execution Phase"),
        ] = None,
    ):
        """
        Job control for existing jobs via POST per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        return

    @uws_router.get("/async/{job_id}/executionduration")
    async def get_job_executionduration(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Execution Duration not estimated, returns 0.
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.execution_duration

    @uws_router.get("/async/{job_id}/destruction")
    def get_job_destruction(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        return

    @uws_router.post("/async{job_id}/destruction")
    def post_job_destruction(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
        destruction: Annotated[
            UTCTimestamp | None,
            Form(alias="DESTRUCTION", description="ISO 8601 UTC datetime for proposed job destruction"),
        ] = None,
    ):
        """
        Job control for existing jobs via POST per UWS spec:
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#DestructionTime
        The service may forbid changes, or may set limits on the allowed destruction time.
        Destruction datetime is expected in ISO 8601 UTC format with Z for UTC times:
        https://www.ivoa.net/documents/VOResource/20180625/REC-VOResource-1.1.html#tth_sEc2.2.4
        """
        return

    @uws_router.get("/async/{job_id}/error")
    async def get_job_error(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        In case of a valid job with no errors, an empty 200 OK response is returned.
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.error_summary

    @uws_router.get("/async/{job_id}/quote")
    async def get_job_quote(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Execution Time quotes not provided, returns empty string.
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.quote if job_summary.quote is not None else ""

    @uws_router.get("/async/{job_id}/results")
    async def get_job_results(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.results

    @uws_router.get("/async/{job_id}/results/results")
    async def get_job_json_results(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_results = await self.uws_redis.get_job_result(job_id)
        return job_results


    @uws_router.get("/async/{job_id}/parameters")
    async def get_job_parameters(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.parameters

    @uws_router.post("/async{job_id}/parameters")
    def post_job_parameters(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
        runid: Annotated[
            str | None,
            Form(description="Run ID to be used", alias="RUNID"),
        ] = None,
    ):
        """
        Update job parameters by submitting a POST of key-value pairs.
        """
        return

    @uws_router.get("/async/{job_id}/owner")
    async def get_job_owner(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_summary = await self.uws_redis.get_job(job_id)
        return job_summary.owner_id
