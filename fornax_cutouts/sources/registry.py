import logging
from dataclasses import dataclass, field
from functools import cached_property
from typing import TypeVar

from fornax_cutouts.models.base import Positions
from fornax_cutouts.models.cutouts import FilenameLookupResponse
from fornax_cutouts.sources.base import AbstractMissionSource, MissionMetadata
from fornax_cutouts.utils.logging import get_logger
from fornax_cutouts.utils.units import SizeSpec, size_spec_to_pixels

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

    def resolve_size_px(self, mission: str, size: SizeSpec | int | tuple[int, int]) -> tuple[int, int]:
        """Resolve a requested cutout size to an (x_px, y_px) pixel tuple for ``mission``.

        A bare int/tuple is treated as pixels. A SizeSpec is converted using the mission's
        ``metadata.pixel_size`` (arcsec/pixel).
        """
        if isinstance(size, tuple):
            return size
        if isinstance(size, int):
            return (size, size)
        return size_spec_to_pixels(size, self.get_mission(mission).metadata.pixel_size)

    def validate_mission_params(
        self,
        mission_params: dict[str, dict],
        size: SizeSpec | int | None = None,
    ) -> dict[str, bool]:
        """
        Validate the mission parameters.

        Args:
            mission_params (dict[str, dict]): The mission parameters to validate by mission name.
            size (SizeSpec | int | None): The requested cutout size. A SizeSpec is resolved via each mission's
                plate scale; an int is treated as pixels.

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
                    params_to_validate["size"] = self.resolve_size_px(mission, size)
                else:
                    validation_results[mission] &= False
                    continue

            validation_results[mission] &= self._SOURCES[mission].validate_request(**params_to_validate)

        return validation_results

    def get_target_filenames(
        self,
        position: Positions,
        mission_params: dict[str, dict],
        size: SizeSpec | int | None = None,
    ) -> list[FilenameLookupResponse]:
        """
        Get the target filenames for a given position and mission parameters.

        Args:
            position (Positions): The position to get the filenames for.
            mission_params (dict[str, dict]): The mission parameters to get the filenames for.
            size (SizeSpec | int | None): The requested cutout size.

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

    def infer_mission(self, source_file: str) -> str | None:
        """Return the unique registered mission whose patterns match ``source_file``.

        Returns None when no source matches or more than one source matches.
        """
        matches = [name for name, src in self._SOURCES.items() if src.matches_source_file(source_file)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            self.logger.warning(
                "Ambiguous mission for %s: %s",
                source_file,
                matches,
                extra={
                    "event": "mission_inference_ambiguous",
                    "source_file": source_file,
                    "missions": matches,
                },
            )
        return None
