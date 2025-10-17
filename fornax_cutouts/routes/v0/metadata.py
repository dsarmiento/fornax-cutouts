from typing import Annotated

from fastapi import APIRouter, Body
from fastapi_utils.cbv import cbv

from mast.cutouts.filename_resolver.missions import MISSION_LOOKUP, MISSION_METADATA
from mast.cutouts.filename_resolver.models import FilenameRequest
from mast.cutouts.santa import resolve_positions

metadata_router = APIRouter()


@cbv(metadata_router)
class MetadataHandler:
    @metadata_router.get("/missions")
    def get_missions(self):
        return MISSION_METADATA

    @metadata_router.get("/missions/{mission}")
    def get_mission(
        self,
        mission: str,
    ):
        return MISSION_METADATA[mission]

    @metadata_router.post("/filenames")
    def get_filenames(
        self,
        position: Annotated[list[str], Body()],
        mission: Annotated[dict[str, FilenameRequest] | list[str], Body()],
    ):
        mission_result = {}

        resolved_positions = resolve_positions(position)

        if isinstance(mission, list):
            mission = {mission_name: FilenameRequest() for mission_name in mission}

        # TODO: Make this portion call get_filenames in parallel once serving more than one mission
        for mission_name, fname_request in mission.items():
            request_dict = fname_request.model_dump()
            request_dict["position"] = resolved_positions
            request_dict = {k: v for k, v in request_dict.items() if v is not None}
            mission_result[mission_name] = MISSION_LOOKUP[mission_name].get_filenames(**request_dict)

        # TODO: Build a pydantic model of the return
        return {
            "request": {
                "position": position,
                "mission": mission,
            },
            "result": mission_result,
        }

    @metadata_router.post("/filenames/{mission}")
    def get_mission_filenames(
        self,
        mission: str,
        fname_request: Annotated[FilenameRequest, Body()],
    ):
        if fname_request.position is None:
            raise ValueError("'position' cannot be null")

        request_dict = fname_request.model_dump()
        request_dict["position"] = resolve_positions(fname_request.position)
        request_dict = {k: v for k, v in request_dict.items() if v is not None}

        fnames = MISSION_LOOKUP[mission].get_filenames(**request_dict)

        return {
            "request": fname_request,
            "result": fnames,
        }
