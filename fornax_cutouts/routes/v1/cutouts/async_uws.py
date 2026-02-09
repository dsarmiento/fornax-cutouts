import json
import uuid
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Path, Query, Request, Response, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi_utils.cbv import cbv
from vo_models.uws.models import Jobs, JobSummary, Parameters, ResultReference, Results
from vo_models.uws.types import ExecutionPhase
from vo_models.voresource.types import UTCTimestamp
from vo_models.xlink import XlinkType

from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.tasks import schedule_job
from fornax_cutouts.utils.redis_uws import RedisUWS, redis_uws_client

uws_router = APIRouter(prefix="/cutouts")


class PhaseAction(StrEnum):
    """Enum for async job phase requests"""

    ABORT = "ABORT"
    RUN = "RUN"


class XmlResponse(Response):
    media_type = "application/xml"


class CsvResponse(Response):
    media_type = "text/csv"


@cbv(uws_router)
class CutoutsUWSHandler:
    uws_redis: RedisUWS = Depends(redis_uws_client)

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
        form = await request.form()

        mission_params = {}
        source_names = cutout_registry.get_source_names()

        for key, value in form.multi_items():
            # Case 1: key is a source name with JSON string value
            if key in source_names:
                mission_params[key] = json.loads(value)
            # Case 2: key is in format "source_name.parameter"
            elif "." in key:
                parts = key.split(".", 1)  # Split only on first dot
                source_name = parts[0]
                param_name = parts[1]

                if source_name in source_names:
                    if source_name not in mission_params:
                        mission_params[source_name] = {}

                    if param_name not in mission_params[source_name]:
                        mission_params[source_name][param_name] = value
                    elif not isinstance(mission_params[source_name][param_name], list):
                        mission_params[source_name][param_name] = [mission_params[source_name][param_name], value]
                    else:
                        mission_params[source_name][param_name].append(value)

        request_params = {
            "position": position,
            "size": size,
            "output_format": output_format,
            **mission_params,
        }

        job_id = uuid.uuid4().hex[:8]
        job_kwargs = {
            "job_id": job_id,
            "position": position,
            "size": size,
            "output_format": output_format,
            "mission_params": mission_params,
        }

        await self.uws_redis.create_job(
            job_id=job_id,
            run_id=run_id,
            parameters=request_params,
        )

        schedule_job.apply_async(
            task_id=f"schedule_job-{job_id}",
            kwargs=job_kwargs,
        )
        redirect_url = f"{request.url.path}/{job_id}"
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    @uws_router.get("/async/{job_id}")
    async def get_job(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ) -> JobSummary:
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

    @uws_router.post("/async/{job_id}/phase")
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

    @uws_router.post("/async/{job_id}/destruction")
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
        results = Results(
            results=[
                ResultReference(
                    id="summary",
                    type=XlinkType.SIMPLE,
                    href=f"/api/v0/cutouts/async/{job_id}/results/summary",
                    size=1,
                    mime_type="application/json",
                ),
                ResultReference(
                    id="cutouts",
                    type=XlinkType.SIMPLE,
                    href=f"/api/v0/cutouts/async/{job_id}/results/cutouts",
                    size=100,
                    mime_type="application/xml",
                    # any_attrs={
                    #     "output_format": "xml",
                    #     "page": "0",
                    #     "size": "100",
                    # }
                ),
            ]
        )
        return XmlResponse(results.to_xml())

    @uws_router.get("/async/{job_id}/results/summary")
    async def get_job_summary_results(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ):
        """
        Return job summary results in a JSON format
        """
        completed_jobs = await self.uws_redis.get_completed_count(job_id)
        pending_jobs = await self.uws_redis.get_pending_count(job_id)
        failed_jobs = await self.uws_redis.get_failed_count(job_id)
        total_jobs = await self.uws_redis.get_expected_results(job_id)

        return {
            "completed_jobs": completed_jobs,
            "pending_jobs": pending_jobs,
            "failed_jobs": failed_jobs,
            "total_jobs": total_jobs,
        }

    @uws_router.get("/async/{job_id}/results/cutouts")
    async def get_job_json_results(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
        output_format: Annotated[
            str,
            Query(description="Output format to return", choices=["json", "csv", "votable", "xml"]),
        ] = "xml",
        page: Annotated[
            int,
            Query(description="Page number to return", ge=0),
        ] = 0,
        limit: Annotated[
            int,
            Query(description="Number of results per page", ge=1),
        ] = 100,
    ):
        """
        Return job cutout results in a table format
        """
        job_results = self.uws_redis.get_job_cutout_results(job_id)

        if output_format == "json":
            return job_results.to_py(page=page, limit=limit)

        if output_format == "csv":
            return CsvResponse(job_results.to_csv(page=page, limit=limit))

        if output_format in ["votable", "xml"]:
            return XmlResponse(job_results.to_votable(page=page, limit=limit))

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid output format: {output_format}",
        )

    @uws_router.get("/async/{job_id}/parameters")
    async def get_job_parameters(
        self,
        job_id: Annotated[
            str,
            Path(description="Server-assigned job ID for the request"),
        ],
    ) -> Parameters:
        """
        Return job details per UWS spec
        https://www.ivoa.net/documents/UWS/20161024/REC-UWS-1.1-20161024.html#RESTbinding
        """
        job_parameters = await self.uws_redis.get_job_parameters(job_id)
        return XmlResponse(job_parameters.to_xml())

    @uws_router.post("/async/{job_id}/parameters")
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
