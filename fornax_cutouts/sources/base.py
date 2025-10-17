from abc import ABC, abstractmethod

from pydantic import BaseModel


class MissionMeta(BaseModel):
    name: str
    pixel_size: float
    max_cutout_size: int
    filter: list[str]
    survey: list[str]

    class Config:
        extra = "allow"


class AbstractFilenameResolver(ABC):
    metadata: MissionMeta

    def __repr__(self):
        return f"FilenameResolver(mission={self.metadata.name})"

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
        ra: float,
        dec: float,
        filters: list[str],
        *args,
        **kwargs,
    ) -> list[str]: ...
