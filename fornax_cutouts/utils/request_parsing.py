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
