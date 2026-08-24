import logging
from abc import ABC, abstractmethod

from pydantic import BaseModel

from fornax_cutouts.models.base import Positions, TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse
from fornax_cutouts.utils.logging import get_logger


class MissionMetadata(BaseModel):
    name: str
    pixel_size: float
    max_cutout_size: int
    filter: list[str]  # Filters need to be instrument specific so maybe don't hardcode a single filter parameter here
    survey: list[str]

    class Config:
        extra = "allow"


class AbstractMissionSource(ABC):
    metadata: MissionMetadata

    @property
    def logger(self) -> logging.Logger:
        return get_logger()

    def __repr__(self):
        return f"MissionSource(mission={self.metadata.name})"

    def _validate_list_parameter(self, parameter: str | list[str], metadata: list[str]) -> bool:
        if isinstance(parameter, list):
            return all(item in metadata for item in parameter)

        if isinstance(parameter, str):
            return parameter in metadata

        return False

    def _cast_list_parameter(self, parameter: str | list[str]) -> list[str]:
        if isinstance(parameter, list):
            return parameter

        if isinstance(parameter, str):
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
        position: TargetPosition | Positions,
        filter: str | list[str],
        **kwargs,
    ) -> list[FilenameLookupResponse]: ...

    def get_count(self, position: TargetPosition | Positions, **kwargs) -> int:
        """Count matching files. Override when a cheaper query exists.

        ``**kwargs`` are the mission-specific extras from the request body.
        """
        results = self.get_filenames(position, **kwargs)
        return sum(len(result.filenames) for result in results)
