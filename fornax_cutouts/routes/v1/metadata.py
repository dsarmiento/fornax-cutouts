from typing import Annotated

from fastapi import APIRouter, Depends, Form, HTTPException, status
from fastapi_utils.cbv import cbv

from fornax_cutouts.models.metadata import (
    FilenameCountResponse,
    FilenameRequest,
    MultiMissionFilenameCountResponse,
    MultiMissionRequest,
)
from fornax_cutouts.sources import AbstractMissionSource, cutout_registry
from fornax_cutouts.utils.form_data import _filename_params, form_parser
from fornax_cutouts.utils.santa_resolver import resolve_positions

metadata_router = APIRouter(tags=["Metadata"])


def _get_mission_or_404(mission: str) -> AbstractMissionSource:
    """Look up a registered mission or raise 404."""
    try:
        return cutout_registry.get_mission(mission)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Mission does not exist",
        )


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
        except (KeyError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Mission does not exist",
            )

    @metadata_router.post(
        "/filenames/{mission}/count",
        summary="Count filenames for a mission",
        description="Resolve positions and return the matching file count for a single mission without a filename list.",
        response_model=FilenameCountResponse,
    )
    def get_mission_filenames_count(
        self,
        mission: str,
        fname_request: Annotated[FilenameRequest, Form()],
    ):
        """Count matching files for a single mission."""
        if fname_request.position is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'position' cannot be null",
            )

        mission_source = _get_mission_or_404(mission)
        mission_params = _filename_params(fname_request)

        total_files = mission_source.get_count(
            position=resolve_positions(fname_request.position),
            **mission_params,
        )

        return {
            "request": fname_request,
            "total_files": total_files,
        }

    @metadata_router.post(
        "/filenames",
        summary="Get filenames for multiple missions",
        description="Resolve positions and return matching FITS filenames for one or more missions.",
    )
    def get_filenames(
        self,
        multimission_request: Annotated[MultiMissionRequest, Depends(form_parser(MultiMissionRequest))],
    ):
        mission_result = {}
        total_files = 0
        position = multimission_request.position
        missions = multimission_request.missions

        resolved_positions = resolve_positions(position)
        # TODO: Make this portion call get_filenames in parallel once serving more than one mission
        for mission_name, fname_request in missions.items():
            request_dict = _filename_params(fname_request)

            request_dict["position"] = resolved_positions

            mission_source = cutout_registry.get_mission(mission_name)
            mission_filenames = mission_source.get_filenames(
                **request_dict,
            )

            mission_total_files = sum(len(fname_response.filenames) for fname_response in mission_filenames)
            total_files += mission_total_files

            mission_result[mission_name] = {
                "total_files": mission_total_files,
                "filenames": mission_filenames,
            }

        # TODO: Build a pydantic model of the return
        return {
            "request": {
                "position": position,
                "mission": missions,
            },
            "total_files": total_files,
            "missions": mission_result,
        }

    @metadata_router.post(
        "/filenames/count",
        summary="Count filenames for multiple missions",
        description="Resolve positions and return matching file counts for one or more missions.",
        response_model=MultiMissionFilenameCountResponse,
    )
    def get_filenames_count(
        self,
        multimission_request: Annotated[MultiMissionRequest, Depends(form_parser(MultiMissionRequest))],
    ):
        mission_result = {}
        total_files = 0

        missions = multimission_request.missions
        position = multimission_request.position
        resolved_positions = resolve_positions(position)
        for mission_name, fname_request in missions.items():
            request_dict = _filename_params(fname_request)

            mission_source = _get_mission_or_404(mission_name)
            mission_total_files = mission_source.get_count(
                position=resolved_positions,
                **request_dict,
            )
            total_files += mission_total_files
            mission_result[mission_name] = {"total_files": mission_total_files}

        return {
            "request": {
                "position": position,
                "mission": missions,
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
        fname_request: Annotated[FilenameRequest, Form()],
    ):
        if fname_request.position is None:
            raise ValueError("'position' cannot be null")

        mission_params = _filename_params(fname_request)

        fnames = cutout_registry.get_mission(mission).get_filenames(
            position=resolve_positions(fname_request.position),
            **mission_params,
        )

        return {
            "request": fname_request,
            "total_files": sum(len(fname_response.filenames) for fname_response in fnames),
            "filenames": fnames,
        }
