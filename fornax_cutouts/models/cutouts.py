from datetime import timedelta

from pydantic import BaseModel
from vo_models.uws.models import NSMAP, JobSummary, MultiValuedParameter, Parameter, Parameters

from fornax_cutouts.models.base import TargetPosition

REQUEST_EXPIRES_IN = timedelta(days=7)


class CutoutParameters(Parameters, tag="parameters", ns="uws", nsmap=NSMAP):
    runid: Parameter = Parameter(value="", id="runid")
    position: MultiValuedParameter


CutoutJobSummary = JobSummary[CutoutParameters]


class FilenameLookupResponse(BaseModel):
    mission: str
    target: TargetPosition
    filenames: list[str]
    size: int | None = None


#######


class FileResponse(BaseModel):
    filename: str
    url: str


class ColorFilter(BaseModel):
    red: str | None
    green: str | None
    blue: str | None


class CutoutResponse(BaseModel):
    position: TargetPosition
    size_px: tuple[int, int]
    fits: FileResponse | None = None
    preview: FileResponse | None = None
    filter: str | ColorFilter | None = None
