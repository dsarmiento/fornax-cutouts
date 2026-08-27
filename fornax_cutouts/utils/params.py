import json
from collections import defaultdict
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from fornax_cutouts.models.metadata import FilenameRequest
from fornax_cutouts.sources import cutout_registry


async def parse_mission_params_form(request: Request):
    """Parse mission parameters from a form request.

    Expects the form data to contain mission parameters for various sources, either as JSON strings under the source
    name keys or as individual parameters in the format "source_name.parameter".

    Returns:
        dict[str, dict[str, Any]]: A dictionary mapping source names to their respective parameters.

    Raises:
        RequestValidationError: If the mission parameters fail validation using FilenameRequest.
    """
    form = await request.form()

    mission_params: dict[str, dict[str, Any]] = defaultdict(dict)
    source_names = cutout_registry.get_source_names()
    for key, value in form.multi_items():
        # Case 1: key is a source name with JSON string value
        if key in source_names:
            mission_params[key].update(json.loads(value))
        # Case 2: key is in format "source_name.parameter"
        elif "." in key:
            parts = key.split(".", 1)  # Split only on first dot
            source_name = parts[0]
            param_name = parts[1]

            if source_name not in source_names:
                continue

            if param_name not in mission_params[source_name]:
                mission_params[source_name][param_name] = value
            elif not isinstance(mission_params[source_name][param_name], list):
                mission_params[source_name][param_name] = [mission_params[source_name][param_name], value]
            else:
                mission_params[source_name][param_name].append(value)

    # Validate mission parameters using FilenameRequest
    try:
        for source_name, params in mission_params.items():
            if source_name in cutout_registry.get_source_names():
                # Note: FilenameRequest has no required fields and by default allows extra fields
                # so this check is limited
                FilenameRequest.model_validate(params)
    except ValidationError as e:
        raise RequestValidationError(e.errors())

    return mission_params
