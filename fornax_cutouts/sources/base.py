from abc import ABC, abstractmethod
from typing import Any, Type, TypeVar, get_origin

from pydantic import BaseModel, ValidationInfo, field_validator

from fornax_cutouts.models.base import Positions, TargetPosition
from fornax_cutouts.models.cutouts import FilenameWithMetadata


class BaseParameters(BaseModel):
    """
    Base class for all mission parameter models.

    Provides a pre-validator that, for any field annotated as a list[...],
    wraps a single scalar value into a one-element list. This lets callers
    pass either a scalar or a list for list-typed parameters.
    """

    @field_validator("*", mode="before")
    @classmethod
    def normalize_single_to_list(cls, value: Any, info: ValidationInfo):
        field_type = cls.model_fields[info.field_name].annotation

        if get_origin(field_type) is list:
            if isinstance(value, list):
                return value
            else:
                return [value]

        return value


class MissionMetadata(BaseModel):
    name: str
    description: str = ""
    pixel_size_arcseconds: float
    max_cutout_size_px: int


MissionParameters = TypeVar("MissionParameters", bound=BaseParameters)


class AbstractMissionSource(ABC):
    metadata: MissionMetadata
    params_model: Type[MissionParameters]

    def __repr__(self):
        return f"MissionSource(mission={self.metadata.name})"

    def validate_mission_params(self, request: dict = {}) -> MissionParameters:
        return self.params_model.model_validate(request)

    @abstractmethod
    def get_filenames(
        self,
        positions: TargetPosition | Positions,
        params: MissionParameters,
        *args,
        include_metadata: bool = False,
        **kwargs,
    ) -> list[FilenameWithMetadata]:
        ...
