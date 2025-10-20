from dataclasses import dataclass, field
from typing import Type

from fornax_cutouts.models.base import Positions
from fornax_cutouts.models.cutouts import FilenameLookupResponse
from fornax_cutouts.sources.base import AbstractMissionSource, MissionMetadata


@dataclass
class CutoutRegistry:
    _SOURCES: dict[str, Type[AbstractMissionSource]] = field(default_factory=dict, init=False)

    def __getitem__(self, mission: str) -> Type[AbstractMissionSource]:
        try:
            return self._SOURCES[mission]
        except KeyError as exc:
            raise ValueError(f"Unknown source '{mission}'. Registered: {', '.join(self._SOURCES)}") from exc

    def register_source(self, name: str):
        def _decorator(cls: Type[AbstractMissionSource]) -> Type[AbstractMissionSource]:
            self._SOURCES[name] = cls
            return cls

        return _decorator

    def get_source_names(self) -> list[str]:
        return sorted(self._SOURCES)

    def get_mission_metadata(self) -> dict[str, MissionMetadata]:
        return {mission.metadata.name: mission.metadata for mission in self._SOURCES.values()}

    def validate_mission_params(
        self,
        mission_params: dict[str, dict],
        size: int | None = None,
    ) -> dict[str, bool]:
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

            validation_results[mission] &= self[mission].validate_request(**params_to_validate)

        return validation_results

    def get_target_filenames(
        self,
        position: Positions,
        mission_params: dict[str, dict],
        size: int | None = None,
    ) -> list[FilenameLookupResponse]:
        ret = []

        # TODO: This can be parallelized with async or something, not needed currently with only ps1
        for target in position:
            for mission, params in mission_params.items():
                fnames = self[mission].get_filenames(position=target, **params)

                if fnames:
                    ret.append(
                        FilenameLookupResponse(
                            mission=mission,
                            target=target,
                            filenames=fnames,
                            size=params["size"] if "size" in params else size,
                        )
                    )

        return ret
