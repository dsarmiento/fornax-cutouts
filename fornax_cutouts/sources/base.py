from abc import ABC, abstractmethod

from pydantic import BaseModel

from fornax_cutouts.models.base import Positions, TargetPosition
from fornax_cutouts.models.cutouts import CutoutRequest


class MissionMetadata(BaseModel):
    name: str
    pixel_size: float
    max_cutout_size: int
    filter: list[str]
    survey: list[str]

    class Config:
        extra = "allow"


class AbstractMissionSource(ABC):
    metadata: MissionMetadata

    def __repr__(self):
        return f"MissionSource(mission={self.metadata.name})"

    def _validate_list_parameter(self, parameter: str | list[str], metadata: list[str]) -> bool:
        if isinstance(parameter,list):
            return all(item in metadata for item in parameter)

        if isinstance(parameter,str):
            return parameter in metadata

        return False

    def _cast_list_parameter(self, parameter: str | list[str]) -> list[str]:
        if isinstance(parameter,list):
            return parameter

        if isinstance(parameter,str):
            return [parameter]

        return []

    def validate_request(self, size: int, **extras):
        filter = extras.get("filter", [])
        survey = extras.get("survey", [])

        is_valid = True
        is_valid &= size > 0
        is_valid &= size <= self.metadata.max_cutout_size
        is_valid &= self._validate_list_parameter(filter, self.metadata.filter)
        is_valid &= self._validate_list_parameter(survey, self.metadata.survey)

        return is_valid

    @abstractmethod
    def get_filenames(
        self,
        positions: TargetPosition | Positions,
        filters: str |list[str],
        *args,
        **kwargs,
    ) -> list[str]: ...
