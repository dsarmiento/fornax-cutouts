import json
from typing import Any


def parse_form_data(form_data: list[tuple[str, str]], source_names: list[str]) -> dict[str, dict]:
    mission_params: dict[str, dict[str, Any | list[Any]]] = {}
    for key, value in form_data:
        # Case 1: key is a source name with JSON string value
        if key in source_names:
            mission_params[key] = json.loads(value)

        # Case 2: key is in format "source_name.parameter"
        elif "." in key:
            parts = key.split(".", 1)  # Split only on first dot
            source_name = parts[0]
            param_name = parts[1]

            if source_name in source_names:
                if source_name not in mission_params:
                    mission_params[source_name] = {}

                if param_name not in mission_params[source_name]:
                    mission_params[source_name][param_name] = value
                elif not isinstance(mission_params[source_name][param_name], list):
                    mission_params[source_name][param_name] = [mission_params[source_name][param_name], value]
                else:
                    mission_params[source_name][param_name].append(value)

    return mission_params



def normalize_request_input(request: dict) -> dict[str, dict]:
    """
    Normalize multi-mission request input mapping:

    1. {"ps1": {"filters": "g", "surveys": "3pi"}, ...}
    2. {"ps1.filters": "g", "ps1": {"surveys": "3pi"}, ...}
    3. Or a mix of both.

    Returns:
        {"ps1": {"filters": <value>, "surveys": <value>, ...}, ...}
    Order of precedence:
      - If both 'mission.param' and 'mission':{param} present, the dict under 'mission' wins for its fields.
    """
    result: dict[str, dict] = {}

    # First, handle all nested objects ({"ps1": {...}})
    for k, v in request.items():
        if isinstance(v, dict):
            # This is a block of params for the mission
            mission = k
            result.setdefault(mission, {}).update(v)

    # Next, handle dot-keyed params, without clobbering dict values above
    for k, v in request.items():
        if isinstance(k, str) and "." in k:
            mission, param = k.split(".", 1)
            if mission not in result or param not in result[mission]:
                result.setdefault(mission, {})[param] = v

    return result
