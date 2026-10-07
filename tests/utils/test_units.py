"""Tests for size specs and pixel conversion."""

import pytest
from astropy import units as u

from fornax_cutouts.utils.units import SizeSpec, size_spec_to_pixels, to_pixels


class TestSizeSpec:
    def test_defaults_to_square_pixels(self):
        assert SizeSpec(x=100) == SizeSpec(x=100, y=100, units="px")

    def test_explicit_y_is_kept(self):
        assert SizeSpec(x=100, y=50).y == 50

    def test_pixel_cutout_size_is_int_tuple(self):
        assert SizeSpec(x=100.4, y=50.6).to_cutout_size() == (100, 51)

    def test_angular_cutout_size_is_quantity(self):
        size = SizeSpec(x=60, y=30, units="s").to_cutout_size()
        assert size.unit == u.arcsec
        assert size.value.tolist() == [60, 30]


class TestToPixels:
    def test_px_passthrough(self):
        assert to_pixels(42, "px", plate_scale_arcsec=0.25) == 42

    def test_arcsec_divides_by_plate_scale(self):
        assert to_pixels(60, "s", plate_scale_arcsec=0.25) == 240

    def test_arcmin_converts_through_arcsec(self):
        assert to_pixels(1, "m", plate_scale_arcsec=0.5) == 120

    def test_degree_converts_through_arcsec(self):
        assert to_pixels(1, "d", plate_scale_arcsec=1.0) == 3600

    def test_rounds_to_nearest_int(self):
        assert to_pixels(60, "s", plate_scale_arcsec=0.55) == 109
        assert to_pixels(60, "s", plate_scale_arcsec=0.4) == 150

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError):
            to_pixels(1, "parsec", plate_scale_arcsec=1.0)


class TestSizeSpecToPixels:
    def test_square_spec_resolves_both_axes(self):
        spec = SizeSpec(x=60, units="s")
        assert size_spec_to_pixels(spec, plate_scale_arcsec=0.25) == (240, 240)

    def test_rectangular_spec(self):
        spec = SizeSpec(x=60, y=30, units="s")
        assert size_spec_to_pixels(spec, plate_scale_arcsec=0.5) == (120, 60)

    def test_px_spec_ignores_plate_scale(self):
        spec = SizeSpec(x=100, y=50, units="px")
        assert size_spec_to_pixels(spec, plate_scale_arcsec=99.9) == (100, 50)
