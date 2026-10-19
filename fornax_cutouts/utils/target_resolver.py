from fornax_cutouts.models.base import Positions, TargetPosition


def resolve_positions(position: list[str]) -> tuple[Positions, set[str]]:
    """
    Resolve a list of object names or RA/Dec strings into TargetPosition objects.

    - Accepts a list of object names OR string RA/Dec positions.
    - RA/Dec positions can be comma-separated or space-separated strings (e.g. "210.8023,54.3488" or "210.8023 54.3488").
    - Object names are unresolved. TODO: Add instance specific implementation of a name resolver class to be registered.
    - Any object that cannot be resolved (by parsing for RA/Dec) is added to the unresolved_objects set
      and omitted from the resolved results.

    Returns:
        tuple[Positions, set[str]]: (list of resolved TargetPosition objects, set of unresolved object name strings)
    """
    resolved_positions = []
    unresolved_objects = set()

    for item in position:
        try:
            delimiter = "," if "," in item else None
            new_item = TargetPosition(*[float(i) for i in item.split(delimiter)])
            resolved_positions.append(new_item)

        except Exception:
            unresolved_objects.add(item)

    return resolved_positions, unresolved_objects
