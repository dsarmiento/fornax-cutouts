from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, status
from fastapi_utils.cbv import cbv

from fornax_cutouts.models.metadata import FilenameRequest
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.santa_resolver import resolve_positions

metadata_router = APIRouter()


@cbv(metadata_router)
class MetadataHandler:
    @metadata_router.get("/missions")
    def get_missions(self):
        """
        Get metadata for all registered missions.
        Includes parameter schemas from params_model.
        """
        missions = cutout_registry.get_mission_metadata()
        result = {}
        for name, meta in missions.items():
            meta_dict = meta.model_dump(exclude={"params_model"})
            meta_dict["parameters_schema"] = meta.params_model.model_json_schema()
            result[name] = meta_dict
        return result

    @metadata_router.get("/missions/{mission}")
    def get_mission(
        self,
        mission: str,
    ):
        """
        Get metadata for a specific mission.
        Includes parameter schema from params_model.
        """
        try:
            meta = cutout_registry.get_mission(mission).metadata
            meta_dict = meta.model_dump(exclude={"params_model"})
            meta_dict["parameters_schema"] = meta.params_model.model_json_schema()
            return meta_dict
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Mission does not exist"
            )

    @metadata_router.post("/filenames")
    def get_filenames(
        self,
        position: Annotated[list[str], Body()],
        mission: Annotated[dict[str, FilenameRequest] | list[str], Body()],
    ):
        mission_result = {}
        total_files = 0

        resolved_positions = resolve_positions(position)

        if isinstance(mission, list):
            mission = {mission_name: FilenameRequest() for mission_name in mission}

        # TODO: Make this portion call get_filenames in parallel once serving more than one mission
        for mission_name, fname_request in mission.items():
            request_dict = fname_request.model_dump()
            request_dict["position"] = resolved_positions
            request_dict = {k: v for k, v in request_dict.items() if v is not None}

            mission_filenames = cutout_registry.get_mission(mission_name).get_filenames(**request_dict)

            mission_total_files = len(mission_filenames)
            total_files += mission_total_files

            mission_result[mission_name] = {
                "total_files": mission_total_files,
                "filenames": mission_filenames,
            }

        # TODO: Build a pydantic model of the return
        return {
            "request": {
                "position": position,
                "mission": mission,
            },
            "total_files": total_files,
            "missions": mission_result,
        }

    @metadata_router.post("/filenames/{mission}")
    def get_mission_filenames(
        self,
        mission: str,
        fname_request: Annotated[dict, Body()],
    ):
        if "position" not in fname_request or fname_request["position"] is None:
            raise ValueError("'position' cannot be null")

        position = fname_request["position"]
        mission_params = {k: v for k, v in fname_request.items() if k != "position"}

        mission_source = cutout_registry.get_mission(mission)
        mission_params = mission_source.validate_mission_params(mission_params)

        fnames = mission_source.get_filenames(
            position=resolve_positions(position),
            params=mission_params,
            include_metadata=True,
        )

        return {
            "request": fname_request,
            "total_files": len(fnames),
            "filenames": fnames,
        }
