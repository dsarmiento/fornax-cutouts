from typing import NamedTuple


class TargetPosition(NamedTuple):
    ra: float
    dec: float


Positions = list[TargetPosition]
