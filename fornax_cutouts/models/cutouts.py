import json
from typing import Any

import numpy as np
from pydantic import BaseModel, field_validator

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

    @field_validator("position", mode="before")
    @classmethod
    def parse_position(cls, value: TargetPosition | np.ndarray) -> TargetPosition:
        if isinstance(value, np.ndarray):
            if value.size != 2:
                raise ValueError("position array must have exactly 2 elements (ra, dec)")
            return TargetPosition(ra=float(value[0]), dec=float(value[1]))
        return value

    @field_validator("size_px", mode="before")
    @classmethod
    def parse_size_px(cls, value: tuple[int, int] | np.ndarray) -> tuple[int, int]:
        if isinstance(value, np.ndarray):
            if value.size != 2:
                raise ValueError("size_px array must have exactly 2 elements")
            return (int(value[0]), int(value[1]))
        return value

    @field_validator("mission_extras", mode="before")
    @classmethod
    def parse_mission_extras(cls, value: dict[str, Any] | str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return value

class CutoutResponse(CutoutRequest):
    fits: str | None = None
    preview: str | None = None
