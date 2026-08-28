"""Tests for metadata models and data normalization"""

from fornax_cutouts.models.metadata import MultiMissionRequest
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry

_FAKE_METADATA = MissionMetadata(
    name="fake_source",
    pixel_size=1.0,
    max_cutout_size=100,
    filter=["g"],
    survey=["s"],
)

_OTHER_METADATA = MissionMetadata(
    name="other_source",
    pixel_size=1.0,
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


def setup_function():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES
    cutout_registry._SOURCES["fake_source"] = FakeSource()
    cutout_registry._SOURCES["other_source"] = OtherSource()


def teardown_function():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


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
