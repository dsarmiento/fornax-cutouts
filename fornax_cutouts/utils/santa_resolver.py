from dataclasses import dataclass

import httpx

from mast.cutouts.constants import ENVIRONMENT_NAME
from mast.cutouts.models.base import Positions, TargetPosition

ENVIRONMENT_PREFIX = ENVIRONMENT_NAME if ENVIRONMENT_NAME not in ["int", "prod"] else ""
SANTA_QUERY_URI = f"https://{ENVIRONMENT_PREFIX}mastresolver.stsci.edu/Santa-war"


class SantaResolver:
    def __init__(self):
        self.client = httpx.Client(
            base_url=SANTA_QUERY_URI,
            params={
                "source": "Cloud Cutouts",
                "outputFormat": "json",
            },
        )

    def resolve_targets(self, names: list[str]) -> dict[str, TargetPosition]:
        try:
            santa_resp = self.client.get(
                "query",
                params={"name": names},
                # timeout=5,
            )

            santa_resp.raise_for_status()

        except Exception:
            return {}

        try:
            resolved_points_json = santa_resp.json()["resolvedCoordinate"]
            resolved_points = [SantaResolvedCoordinate.from_dict(d) for d in resolved_points_json]

        except Exception:
            return {}

        return {p.searchString: TargetPosition(p.ra, p.dec) for p in resolved_points}

    def close(self):
        self.client.close()


@dataclass
class SantaResolvedCoordinate:
    searchString: str
    canonicalName: str
    ra: float
    decl: float
    objectType: str
    resolver: str

    @property
    def dec(self) -> float:
        return self.decl

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            searchString=data["searchString"],
            canonicalName=data["canonicalName"],
            ra=data["ra"],
            decl=data["decl"],
            objectType=data["objectType"],
            resolver=data["resolver"],
        )


def resolve_positions(position: list[str]) -> Positions:
    """
    Takes the given list of object names or string ra/dec positions and resolves to TargetPositions

    String ra/dec positions can be either comma or space seperated
    Object names are resolved by SANTA

    If an object isn't found by SANTA it's removed from the resolved position
    """
    resolved_positions = []
    unresolved_objects = {}

    for idx, item in enumerate(position):
        try:
            delimiter = "," if "," in item else None
            new_item = TargetPosition(*[float(i) for i in item.split(delimiter)])
            resolved_positions.append(new_item)

        except Exception:
            resolved_positions.append(item)
            unresolved_objects[item] = idx

    if unresolved_objects:
        santa = SantaResolver()

        santa_resp = santa.resolve_targets(list(unresolved_objects.keys()))
        found_objects = []

        for object_name, idx in unresolved_objects.items():
            if object_name in santa_resp:
                resolved_positions[idx] = santa_resp[object_name]
                found_objects.append(object_name)

        for object_name, idx in reversed(unresolved_objects.items()):
            if object_name not in found_objects:
                print(f"SANTA couldn't find '{object_name}'")
                del resolved_positions[idx]

        santa.close()

    return resolved_positions
