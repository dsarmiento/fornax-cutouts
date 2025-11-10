from dataclasses import dataclass, field
from functools import cached_property
from importlib.util import module_from_spec, spec_from_file_location

from fornax_cutouts.config import CONFIG
from fornax_cutouts.models.base import Positions
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata
from fornax_cutouts.sources.base import AbstractMissionSource, MissionMetadata


@dataclass
class CutoutRegistry:
    _SOURCES: dict[str, AbstractMissionSource] = field(default_factory=dict, init=False)

    @cached_property
    def _VALID_SOURCES(self) -> list[str]:
        return sorted(self._SOURCES.keys())

    def register_source(self, mission: str):
        def _decorator(cls: AbstractMissionSource) -> AbstractMissionSource:
            self._SOURCES[mission] = cls()
            return cls

        return _decorator

    def discover_sources(self):
        for source in CONFIG.source_path.glob("**/*.py"):
            spec = spec_from_file_location(f"cutouts_source_{source.name}", source.as_posix())
            module = module_from_spec(spec)
            spec.loader.exec_module(module)

        print(f"Registered sources: {self.get_source_names()}")

    def get_source_names(self) -> list[str]:
        return self._VALID_SOURCES

    def get_mission(self, mission: str) -> AbstractMissionSource:
        try:
            return self._SOURCES[mission]
        except KeyError as exc:
            raise ValueError(f"Unknown source '{mission}'. Registered: {', '.join(self._SOURCES)}") from exc

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

            validation_results[mission] &= self._SOURCES[mission].validate_request(**params_to_validate)

        return validation_results

    def get_target_filenames(
        self,
        position: Positions,
        mission_params: dict[str, dict],
        size: int | None = None,
        include_metadata: bool = True,
    ) -> list[FilenameLookupResponse]:
        ret = []

        # TODO: This can be parallelized with async or something, not needed currently with only ps1
        for target in position:
            for mission, params in mission_params.items():
                fnames_result = self._SOURCES[mission].get_filenames(
                    position=target,
                    include_metadata=include_metadata,
                    **params,
                )

                if include_metadata:
                    filenames = [
                        FilenameWithMetadata(filename=fname, metadata=meta)
                        for fname, meta in fnames_result
                    ]
                else:
                    filenames = [FilenameWithMetadata(filename=fname) for fname in fnames_result]

                ret.append(
                    FilenameLookupResponse(
                        mission=mission,
                        target=target,
                        filenames=filenames,
                        size=params["size"] if "size" in params else size,
                    )
                )

        return ret
