import json
from typing import Annotated, TypeVar

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator, model_validator

from fornax_cutouts.sources import cutout_registry


def ensure_list(v) -> list:
    """Ensure the input is a list. If not, wrap it in a list."""
    return v if isinstance(v, list) else [v]


T = TypeVar("T")
EnsureList = Annotated[list[T], BeforeValidator(ensure_list)]

StringList = EnsureList[str]


class FilenameRequest(BaseModel):
    position: StringList | None = None
    survey: StringList | None = None
    filter: StringList | None = None

    model_config = ConfigDict(extra="allow")


def check_json_string(v):
    """Check if the input is a valid JSON string and parse it."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e.msg}") from e
    return v


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

    @field_validator("missions", mode="before")
    @classmethod
    def parse_missions(cls, value):
        if isinstance(value, str):
            return json.loads(value)
        return value

    @model_validator(mode="before")
    def normalize_missions(cls, input_dict):  # noqa: C901
        """Normalize the missions dictionary by extracting source-specific parameters from the top-level input.

        This allows the request to include source data either at the top level or in dot notation.

        Since the input is form-encoded, some values may be JSON strings. As we normalize we check for valid JSON
        strings and convert them to Python objects when necessary.
        """
        source_names = cutout_registry.get_source_names()

        input_dict_new = input_dict.copy()  # Make a copy to avoid mutating the original input

        mission_params = {}
        # If missions is already present, use it as a base to build the rest of the missions parameters
        if "missions" in input_dict:
            mission_params = input_dict["missions"]
            mission_params = check_json_string(mission_params)

            if not isinstance(mission_params, dict):
                raise ValueError("missions must be a JSON object")

        for key, value in input_dict.items():
            # Case 1: key is a source name
            if key in source_names:
                if key not in mission_params:
                    mission_params[key] = {}
                value = check_json_string(value)
                mission_params[key].update(value)
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
