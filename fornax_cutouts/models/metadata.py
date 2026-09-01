import copy
from typing import Annotated, TypeVar

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

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


class MultiMissionRequest(BaseModel):
    """
    Request across multiple missions. Accepts source data at the top level and in dot notation, e.g.
    {
        "fake_source.survey": ["survey1", "survey2"],
        "fake_source.filter": ["filter1", "filter2"]
    }

    """

    position: StringList | None = None
    missions: dict[str, FilenameRequest]
    # Allow extra fields so that we can pass a cutout request to an endpoint that takes a multimission request without
    # modifying it
    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    def normalize_missions(cls, input_dict):  # noqa: C901
        """Normalize the missions dictionary by extracting source-specific parameters from the top-level input.

        This allows the request to include source data either at the top level or in dot notation.

        Since the input is form-encoded, some values may be JSON strings. As we normalize we check for valid JSON
        strings and convert them to Python objects when necessary.
        """
        source_names = cutout_registry.get_source_names()

        input_dict_new = copy.deepcopy(input_dict)  # Make a copy to avoid mutating the original input

        # If missions is already present, use it as a base to build the rest of the missions parameters
        mission_params = input_dict_new.get("missions", {})

        for key, value in input_dict.items():
            # Case 1: key is a source name
            if key in source_names:
                if not isinstance(value, dict):
                    raise ValueError(f"Expected a dictionary for source '{key}', got {type(value).__name__}")
                if key not in mission_params:
                    mission_params[key] = {}
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
                else:
                    # If the parameter already exists, ensure it is a list and append the new value(s)
                    if not isinstance(mission_params[source_name][param_name], list):
                        mission_params[source_name][param_name] = [mission_params[source_name][param_name]]
                    if isinstance(value, list):
                        mission_params[source_name][param_name].extend(value)
                    else:
                        mission_params[source_name][param_name].append(value)
        input_dict_new["missions"] = mission_params
        return input_dict_new


class MultiMissionCutoutRequest(MultiMissionRequest):
    """Cutout request across multiple missions."""

    size: int
    generate_science: bool = Field(True)
    generate_preview: bool = Field(False)
    run_id: Annotated[str, Field(description="RUNID for the request", max_length=64, alias="RUNID")] = ""

    model_config = ConfigDict(extra="allow")


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
