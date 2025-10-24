from pydantic import BaseModel

from fornax_cutouts.models.base import TargetPosition


class FilenameLookupResponse(BaseModel):
    mission: str
    target: TargetPosition
    filenames: list[str]
    size: int | None = None


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
