"""Cutout size units and conversion to pixels."""

from typing import Literal

from astropy import units as u
from pydantic import BaseModel, Field, model_validator

SizeUnit = Literal["px", "s", "m", "d"]

_UNIT_MAP: dict[str, u.UnitBase] = {
    "s": u.arcsec,
    "m": u.arcmin,
    "d": u.deg,
    "px": u.pix,
}


class SizeSpec(BaseModel):
    """Cutout size in an arbitrary supported unit.

    ``y`` defaults to ``x`` (square cutout in the same units).
    """

    x: float = Field(gt=0)
    y: float | None = Field(default=None, gt=0)
    units: SizeUnit = "px"

    @model_validator(mode="after")
    def _default_y_to_x(self) -> "SizeSpec":
        if self.y is None:
            self.y = self.x
        return self

    def to_cutout_size(self) -> tuple[int, int] | u.Quantity:
        """Return the size in a form astrocut accepts as ``cutout_size``.

        Angular sizes are passed through as a Quantity so astrocut converts them with each
        file's own WCS rather than a fixed per-mission plate scale.
        """
        if self.units == "px":
            return (round(self.x), round(self.y))
        return u.Quantity([self.x, self.y], _UNIT_MAP[self.units])


def to_pixels(value: float, units: str, plate_scale_arcsec: float) -> int:
    """Convert a size in the given units to an integer pixel count.

    Args:
        value: The magnitude of the size.
        units: One of "px", "s" (arcsec), "m" (arcmin), "d" (deg).
        plate_scale_arcsec: Mission plate scale in arcsec/pixel.

    Returns:
        int: Size in pixels, rounded to the nearest integer.
    """
    try:
        astropy_unit = _UNIT_MAP[units]
    except KeyError as exc:
        raise ValueError(f"Unsupported size unit {units!r}; expected one of {list(_UNIT_MAP)}") from exc

    quantity = value * astropy_unit
    if astropy_unit is u.pix:
        return round(quantity.value)

    arcsec = quantity.to(u.arcsec).value
    return round(arcsec / plate_scale_arcsec)


def size_spec_to_pixels(spec: SizeSpec, plate_scale_arcsec: float) -> tuple[int, int]:
    """Resolve a SizeSpec to an (x_px, y_px) tuple for a given mission plate scale."""
    return (
        to_pixels(spec.x, spec.units, plate_scale_arcsec),
        to_pixels(spec.y, spec.units, plate_scale_arcsec),
    )
