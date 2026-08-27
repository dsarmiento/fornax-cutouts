import json
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fornax_cutouts.sources import cutout_registry


class FilenameRequest(BaseModel):
    position: list[str] | None = None
    survey: list[str] | None = None
    filter: list[str] | None = None

    model_config = ConfigDict(extra="allow")


class MultiMissionRequest(BaseModel):
    """
    Request across multiple missions. Accepts source data at the top level and in dot notation, e.g.
    {
        "fake_source.survey": ["survey1", "survey2"],
        "fake_source.filter": ["filter1", "filter2"]
    }

    """

    position: list[str]
    missions: dict[str, FilenameRequest]

    # Allow extra fields so that we can pass a cutout request to an endpoint that takes a multimission request without
    # modifying it
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    def normalize_missions(cls, input_dict):  # noqa: C901
        """Normalize the missions dictionary by extracting source-specific parameters from the top-level input.

        This allows the request to include source data either at the top level or in dot notation.
        """
        source_names = cutout_registry.get_source_names()

        input_dict_new = input_dict.copy()  # Make a copy to avoid mutating the original input

        mission_params = {}
        if "missions" in input_dict:
            # If missions is a string try to parse it as JSON
            if isinstance(input_dict["missions"], str):
                try:
                    input_dict_new["missions"] = json.loads(input_dict["missions"])
                except json.JSONDecodeError as e:
                    raise ValueError(f"missions must be valid JSON: {e.msg}") from e
                return input_dict_new

            # If missions is a dict use it as a base for the mission params
            if isinstance(input_dict.get("missions"), dict):
                mission_params = input_dict["missions"].copy()

        for key, value in input_dict.items():
            # Case 1: key is a source name with JSON string value
            if key in source_names:
                if key not in mission_params:
                    mission_params[key] = {}
                mission_params[key].update(json.loads(value))
                # Remove the source key from the top level of the final params dict
                del input_dict_new[key]
            # Case 2: key is in format "source_name.parameter"
            elif "." in key:
                parts = key.split(".", 1)  # Split only on first dot
                source_name = parts[0]
                param_name = parts[1]

                if source_name not in source_names:
                    continue

                if source_name not in mission_params:
                    mission_params[source_name] = {}

                # Remove the source key from the top level of the final params dict
                del input_dict_new[key]

                if param_name not in mission_params[source_name]:
                    mission_params[source_name][param_name] = value
                elif not isinstance(mission_params[source_name][param_name], list):
                    mission_params[source_name][param_name] = [mission_params[source_name][param_name], value]
                else:
                    mission_params[source_name][param_name].append(value)
        input_dict_new["missions"] = mission_params
        return input_dict_new


class MultiMissionCutoutRequest(MultiMissionRequest):
    """Cutout request across multiple missions."""

    size: int
    output_format: list[str] = Field(default_factory=lambda: ["fits"])
    run_id: Annotated[str, Field(description="RUNID for the request", max_length=64, alias="RUNID")] = ""


class FilenameCountResponse(BaseModel):
    """File count for a single-mission request."""

    request: FilenameRequest
    total_files: int


class MissionCountResult(BaseModel):
    total_files: int


class MultiMissionFilenameCountResponse(BaseModel):
    request: dict
    total_files: int
    missions: dict[str, MissionCountResult]
