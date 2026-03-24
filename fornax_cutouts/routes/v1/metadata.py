from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, status
from fastapi_utils.cbv import cbv

from fornax_cutouts.models.metadata import FilenameRequest
from fornax_cutouts.sources import cutout_registry
from fornax_cutouts.utils.santa_resolver import resolve_positions

metadata_router = APIRouter(tags=["Metadata"])


@cbv(metadata_router)
class MetadataHandler:
    @metadata_router.get(
        "/missions",
        summary="List available missions",
        description="Returns metadata for all registered cutout missions/surveys.",
    )
    def get_missions(self):
        return cutout_registry.get_mission_metadata()

    @metadata_router.get(
        "/missions/{mission}",
        summary="Get mission metadata",
        description="Returns metadata for a specific mission (filters, bands, etc.).",
    )
    def get_mission(
        self,
        mission: str,
    ):
        try:
            return cutout_registry.get_mission(mission).metadata
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Mission does not exist",
            )

    @metadata_router.post(
        "/filenames",
        summary="Get filenames for multiple missions",
        description="Resolve positions and return matching FITS filenames for one or more missions.",
    )
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

            mission_source = cutout_registry.get_mission(mission_name)
            mission_filenames = mission_source.get_filenames(
                **request_dict,
                include_metadata=True,
            )

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

    @metadata_router.post(
        "/filenames/{mission}",
        summary="Get filenames for a mission",
        description="Resolve positions and return matching FITS filenames for a single mission.",
    )
    def get_mission_filenames(
        self,
        mission: str,
        fname_request: Annotated[FilenameRequest, Body()],
    ):
        if fname_request.position is None:
            raise ValueError("'position' cannot be null")

        mission_params = fname_request.model_dump(exclude={"position"})

        fnames = cutout_registry.get_mission(mission).get_filenames(
            position=resolve_positions(fname_request.position),
            include_metadata=True,
            **mission_params,
        )

        return {
            "request": fname_request,
            "total_files": len(fnames),
            "filenames": fnames,
        }
