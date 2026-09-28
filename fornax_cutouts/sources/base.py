import logging
from abc import ABC, abstractmethod
from fnmatch import fnmatch

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

    # filename patterns to associate with the source
    source_file_patterns: tuple[str, ...] = ()

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

    def validate_request(self, size: int | tuple[int, int], **extras):
        """Validate mission parameters against the cutout size (in pixels).

        ``size`` may be a scalar (square cutout) or an (x, y) pixel tuple. Each
        axis must be positive and smaller than ``metadata.max_cutout_size``.
        """
        filter = extras.get("filter", [])
        survey = extras.get("survey", [])

        if isinstance(size, int):
            size = (size, size)

        is_valid = True
        is_valid &= all(dim > 0 for dim in size)
        is_valid &= all(dim <= self.metadata.max_cutout_size for dim in size)
        is_valid &= self._validate_list_parameter(filter, self.metadata.filter)
        is_valid &= self._validate_list_parameter(survey, self.metadata.survey)

        return is_valid

    def matches_source_file(self, source_file: str) -> bool:
        """Return True if ``source_file`` matches this source's filename patterns."""
        return any(fnmatch(source_file, pattern) for pattern in self.source_file_patterns)

    @abstractmethod
    def get_filenames(
        self,
        position: TargetPosition | Positions,
        filter: str | list[str],
        **kwargs,
    ) -> list[FilenameLookupResponse]: ...

    def get_count(
        self,
        position: TargetPosition | Positions,
        filter: str | list[str],
        **kwargs,
    ) -> int:
        """Count matching files. Override when a cheaper query exists.

        ``**kwargs`` are the mission-specific extras from the request body.
        """
        results = self.get_filenames(position, filter, **kwargs)
        return sum(len(result.filenames) for result in results)
