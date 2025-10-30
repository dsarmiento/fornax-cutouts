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

    def validate_request(self, size: int, **extras):
        filter = extras.get("filter", [])
        survey = extras.get("survey", [])

        is_valid = True
        is_valid &= size > 0
        is_valid &= size <= self.metadata.max_cutout_size
        is_valid &= all(item in self.metadata.filter for item in filter)
        is_valid &= all(item in self.metadata.survey for item in survey)
        return is_valid

    @abstractmethod
    def get_filenames(
        self,
        positions: TargetPosition | Positions,
        filters: list[str],
        *args,
        **kwargs,
    ) -> list[str]: ...
