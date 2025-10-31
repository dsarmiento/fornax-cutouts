import json

from pydantic import BaseModel, field_serializer

from fornax_cutouts.models.base import TargetPosition


class FilenameWithMetadata(BaseModel):
    filename: str
    metadata: dict | None = None  # Mission-specific metadata for this file


class FilenameLookupResponse(BaseModel):
    mission: str
    target: TargetPosition
    filenames: list[FilenameWithMetadata]
    size: int | None = None


class ColorFilter(BaseModel):
    red: str | None
    green: str | None
    blue: str | None


class CutoutRequest(BaseModel):
    mission: str
    position: TargetPosition
    size_px: tuple[int, int]
    filter: str | ColorFilter | None = None
    mission_extras: dict | None = None

    @field_serializer("mission_extras")
    def serialize_mission_extras(self, value: dict | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value)


class CutoutResponse(CutoutRequest):
    fits: str | None = None
    preview: str | None = None
