"""Tests for metadata models and data normalization"""

import pytest

from fornax_cutouts.models.metadata import MultiMissionCutoutRequest, MultiMissionRequest
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry
from fornax_cutouts.utils.units import SizeSpec

_FAKE_METADATA = MissionMetadata(
    name="fake_source",
    pixel_size=0.5,
    max_cutout_size=100,
    filter=["g"],
    survey=["s"],
)

_OTHER_METADATA = MissionMetadata(
    name="other_source",
    pixel_size=0.25,
    max_cutout_size=100,
    filter=["g"],
    survey=["s"],
)


class FakeSource(AbstractMissionSource):
    metadata = _FAKE_METADATA

    def get_filenames(self, position, filter, survey=None, **kwargs):
        raise NotImplementedError


class OtherSource(AbstractMissionSource):
    metadata = _OTHER_METADATA

    def get_filenames(self, position, filter, survey=None, **kwargs):
        raise NotImplementedError


def _setup_registry():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES
    cutout_registry._SOURCES["fake_source"] = FakeSource()
    cutout_registry._SOURCES["other_source"] = OtherSource()


def _teardown_registry():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


@pytest.fixture(autouse=True)
def _registry_fixture():
    _setup_registry()
    yield
    _teardown_registry()


def setup_function():
    _setup_registry()


def teardown_function():
    _teardown_registry()


def _missions_dump(request):
    return {name: req.model_dump(exclude_none=True) for name, req in request.missions.items()}


def test_missions_dict_passed_through():
    request = MultiMissionRequest.model_validate(
        {"position": ["10, 20"], "missions": {"fake_source": {"survey": ["survey1"]}}},
    )
    assert _missions_dump(request) == {"fake_source": {"survey": ["survey1"]}}


def test_dot_notation_single_value_becomes_list_field():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "fake_source.survey": ["survey1"],
        }
    )
    assert _missions_dump(request) == {"fake_source": {"survey": ["survey1"]}}


def test_dot_notation_unknown_source_is_ignored():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "unknown_source.survey": "survey1",
        }
    )
    assert request.missions == {}
    # Unknown "source.param" keys are left untouched at the top level of the model
    assert getattr(request, "unknown_source.survey") == "survey1"


def test_dot_notation_merges_with_existing_missions_dict():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "missions": {"fake_source": {"survey": ["survey1"]}},
            "fake_source.filter": ["filter1"],
        }
    )
    assert _missions_dump(request) == {
        "fake_source": {"survey": ["survey1"], "filter": ["filter1"]},
    }


def test_dot_notation_merges_with_existing_missions_dict_multi():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "missions": {"fake_source": {"survey": ["survey1"]}},
            "fake_source.filter": ["filter1"],
            "fake_source.survey": ["survey2"],
        }
    )
    assert _missions_dump(request) == {
        "fake_source": {"survey": ["survey1", "survey2"], "filter": ["filter1"]},
    }


def test_multiple_sources_combined():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "fake_source.survey": ["survey1"],
            "other_source.survey": ["survey2"],
        }
    )
    assert _missions_dump(request) == {
        "fake_source": {"survey": ["survey1"]},
        "other_source": {"survey": ["survey2"]},
    }


def test_dot_notation_key_removed_from_top_level():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "fake_source.survey": ["survey1"],
        }
    )
    assert not hasattr(request, "fake_source.survey")


def test_string_field_becomes_list():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "fake_source": {"survey": "survey1"},
        }
    )
    assert _missions_dump(request) == {"fake_source": {"survey": ["survey1"]}}


def test_dot_string_field_becomes_list():
    request = MultiMissionRequest.model_validate(
        {
            "position": ["10, 20"],
            "fake_source.survey": "survey1",
        }
    )
    assert _missions_dump(request) == {"fake_source": {"survey": ["survey1"]}}


class TestMultiMissionCutoutRequestSize:
    def test_size_alone_is_square_pixels(self):
        request = MultiMissionCutoutRequest.model_validate({"position": ["m101"], "size": "256"})
        assert request.size_spec == SizeSpec(x=256, y=256, units="px")

    def test_size_y_and_units(self):
        request = MultiMissionCutoutRequest.model_validate(
            {"position": ["m101"], "size": "60", "y": "30", "units": "s"}
        )
        assert request.size_spec == SizeSpec(x=60, y=30, units="s")


class TestResolveSizePx:
    def test_size_per_mission(self):
        spec = SizeSpec(x=1, units="m")
        assert cutout_registry.resolve_size_px("fake_source", spec) == (120, 120)
        assert cutout_registry.resolve_size_px("other_source", spec) == (240, 240)

    def test_pixel_sizespec_ignores_plate_scale(self):
        assert cutout_registry.resolve_size_px("fake_source", SizeSpec(x=200, y=100, units="px")) == (200, 100)

    def test_single_int_as_pixels(self):
        assert cutout_registry.resolve_size_px("fake_source", 128) == (128, 128)

    def test_tuple_passthrough(self):
        assert cutout_registry.resolve_size_px("fake_source", (200, 100)) == (200, 100)


class TestValidateMissionParams:
    def test_sizespec_lt_mission_limit_passes(self):
        result = cutout_registry.validate_mission_params(
            mission_params={"fake_source": {"filter": ["g"]}},
            size=SizeSpec(x=10, units="s"),
        )
        assert result == {"fake_source": True}

    def test_sizespec_gt_mission_limit_fails(self):
        result = cutout_registry.validate_mission_params(
            mission_params={"fake_source": {"filter": ["g"]}},
            size=SizeSpec(x=100, units="s"),
        )
        assert result == {"fake_source": False}

    def test_mixed_mission_limits(self):
        result = cutout_registry.validate_mission_params(
            mission_params={"fake_source": {"filter": ["g"]}, "other_source": {"filter": ["g"]}},
            size=SizeSpec(x=30, units="s"),
        )
        assert result == {"fake_source": True, "other_source": False}


def test_normalization_does_not_mutate_input():
    input_dict = {
        "position": ["10, 20"],
        "fake_source.survey": ["survey1"],
        "fake_source": {"filter": "filter1"},
    }
    input_dict_copy = input_dict.copy()
    request = MultiMissionRequest.model_validate(input_dict)
    assert input_dict == input_dict_copy
    # Also check that the request was correctly normalized
    assert _missions_dump(request) == {
        "fake_source": {
            "survey": ["survey1"],
            "filter": ["filter1"],
        }
    }
