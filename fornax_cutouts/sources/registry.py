import logging
from dataclasses import dataclass, field
from functools import cached_property
from typing import TypeVar

from fornax_cutouts.models.base import Positions
from fornax_cutouts.models.cutouts import FilenameLookupResponse
from fornax_cutouts.sources.base import AbstractMissionSource, MissionMetadata
from fornax_cutouts.utils.logging import get_logger

_MissionSourceT = TypeVar("_MissionSourceT", bound=AbstractMissionSource)


@dataclass
class CutoutRegistry:
    _SOURCES: dict[str, AbstractMissionSource] = field(default_factory=dict, init=False)
    logger: logging.Logger = field(default_factory=get_logger, init=False)

    @cached_property
    def _VALID_SOURCES(self) -> list[str]:
        return sorted(self._SOURCES.keys())

    def register_source(self, cls: type[_MissionSourceT]) -> type[_MissionSourceT]:
        """
        Register a mission source by decorating the class with @source_registry.register_source.

        Args:
            cls (type[_MissionSourceT]): The mission source class to register.

        Returns:
            type[_MissionSourceT]: The registered mission source class.
        """
        self._SOURCES[cls.metadata.name] = cls()
        self.logger.info(f"Registered {cls.metadata.name} as a mission source")
        return cls

    def get_source_names(self) -> list[str]:
        """
        Get the names of all registered mission sources.

        Returns:
            list[str]: The names of all registered mission sources.
        """
        return self._VALID_SOURCES

    def get_mission(self, mission: str) -> _MissionSourceT:
        """
        Get the mission source for a given mission name.

        Args:
            mission (str): The name of the mission to get the source for.

        Returns:
            _MissionSourceT: The mission source for the given mission name.
        """
        try:
            return self._SOURCES[mission]
        except KeyError as exc:
            raise ValueError(f"Unknown source '{mission}'. Registered: {', '.join(self._SOURCES)}") from exc

    def get_mission_metadata(self) -> dict[str, MissionMetadata]:
        """
        Get the mission metadata for all registered mission sources.

        Returns:
            dict[str, MissionMetadata]: The mission metadata for all registered mission sources.
        """
        return {mission.metadata.name: mission.metadata for mission in self._SOURCES.values()}

    def validate_mission_params(
        self,
        mission_params: dict[str, dict],
        size: int | None = None,
    ) -> dict[str, bool]:
        """
        Validate the mission parameters.

        Args:
            mission_params (dict[str, dict]): The mission parameters to validate by mission name.
            size (int | None): The size to validate the mission parameters for.

        Returns:
            dict[str, bool]: The validation results for the mission parameters by mission name.
        """
        validation_results = dict.fromkeys(mission_params, True)

        for mission, params in mission_params.items():
            if mission not in self._SOURCES:
                validation_results[mission] &= False
                continue

            params_to_validate = dict(params)
            if "size" not in params_to_validate:
                if size is not None:
                    params_to_validate["size"] = size
                else:
                    validation_results[mission] &= False
                    continue

            validation_results[mission] &= self._SOURCES[mission].validate_request(**params_to_validate)

        return validation_results

    def get_target_filenames(
        self,
        position: Positions,
        mission_params: dict[str, dict],
        size: int | None = None,
    ) -> list[FilenameLookupResponse]:
        """
        Get the target filenames for a given position and mission parameters.

        Args:
            position (Positions): The position to get the filenames for.
            mission_params (dict[str, dict]): The mission parameters to get the filenames for.
            size (int | None): The size to get the filenames for.

        Returns:
            list[FilenameLookupResponse]: The target filenames for the given position and mission parameters.
        """
        ret = []

        for mission, params in mission_params.items():
            filenames = self.get_mission(mission).get_filenames(
                position=position,
                **params,
            )

            ret.extend(filenames)

        return ret
