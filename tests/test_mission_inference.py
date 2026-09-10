from fornax_cutouts.models.cutouts import FilenameLookupResponse
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry


class PatternSource(AbstractMissionSource):
    def __init__(self, name: str, patterns: tuple[str, ...]):
        self.metadata = MissionMetadata(
            name=name,
            pixel_size=1.0,
            max_cutout_size=100,
            filter=["g"],
            survey=["s"],
        )
        self.source_file_patterns = patterns

    def get_filenames(self, position, filter, **kwargs) -> list[FilenameLookupResponse]:
        return []


def setup_function():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


def teardown_function():
    cutout_registry._SOURCES.clear()
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


def test_matches_source_file_uses_fnmatch_on_full_uri():
    source = PatternSource("ps1", ("*/panstarrs/ps1/*",))
    assert source.matches_source_file("s3://stpubdata/panstarrs/ps1/public/rings.v3.skycell.2386.085.stk.g.unconv.fits")
    assert not source.matches_source_file("s3://stpubdata/roman/public/file.asdf")


def test_infer_mission_one_match():
    cutout_registry._SOURCES["ps1"] = PatternSource("ps1", ("*/panstarrs/ps1/*",))
    cutout_registry._SOURCES["roman"] = PatternSource("roman", ("*/roman/*",))
    assert cutout_registry.infer_mission("s3://stpubdata/panstarrs/ps1/public/file.fits") == "ps1"


def test_infer_mission_no_match(caplog):
    cutout_registry._SOURCES["ps1"] = PatternSource("ps1", ("*/panstarrs/ps1/*",))
    assert cutout_registry.infer_mission("s3://bucket/other/file.fits") is None

    # mission is not ambiguous, just doesn't match any pattern
    assert "Ambiguous mission" not in caplog.text


def test_infer_mission_collision_returns_none(caplog):
    cutout_registry._SOURCES["one"] = PatternSource("one", ("*/shared/*",))
    cutout_registry._SOURCES["two"] = PatternSource("two", ("*/shared/*",))
    assert cutout_registry.infer_mission("s3://bucket/shared/file.fits") is None
    assert "Ambiguous mission" in caplog.text
