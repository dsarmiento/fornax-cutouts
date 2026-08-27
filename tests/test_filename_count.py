import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata
from fornax_cutouts.routes.v1.metadata import metadata_router
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry

_FAKE_METADATA = MissionMetadata(
    name="fake",
    pixel_size=1.0,
    max_cutout_size=100,
    filter=["g"],
    survey=["s"],
)


class FakeSource(AbstractMissionSource):
    """Mission source that only implements get_filenames."""

    metadata = _FAKE_METADATA

    def __init__(self, file_count: int = 2):
        self.file_count = file_count
        self.filenames_calls = 0

    def get_filenames(self, position, filter, survey=None, **kwargs):
        self.filenames_calls += 1
        filenames = [FilenameWithMetadata(filename=f"file-{i}.fits") for i in range(self.file_count)]
        target = position[0] if isinstance(position, list) else position
        return [FilenameLookupResponse(mission="fake", target=target, filenames=filenames)]


class FastCountFakeSource(FakeSource):
    """Mission source with a cheaper count query, like PS1."""

    def __init__(self, file_count: int = 2, count: int = 9):
        super().__init__(file_count=file_count)
        self.count = count
        self.count_calls = 0
        self.count_requests = []

    def get_count(self, position, filter=None, **kwargs):
        self.count_calls += 1
        self.count_requests.append((filter, kwargs))
        return self.count


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(metadata_router, prefix="/api/v0")
    return TestClient(app)


def setup_function():
    cutout_registry._SOURCES.clear()
    # Ensure cached_property _VALID_SOURCES is cleared
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


def teardown_function():
    cutout_registry._SOURCES.clear()
    # Ensure cached_property _VALID_SOURCES is cleared
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


def test_default_get_count_uses_get_filenames():
    source = FakeSource(file_count=3)
    count = source.get_count(position=[TargetPosition(10.0, 20.0)], filter=["g"])
    assert count == 3
    assert source.filenames_calls == 1


def test_mission_count_default_uses_get_filenames():
    source = FakeSource(file_count=3)
    cutout_registry._SOURCES["fake"] = source

    response = _client().post(
        "/api/v0/filenames/fake/count",
        json={"position": ["10.0 20.0"], "filter": ["g"]},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total_files"] == 3
    assert source.filenames_calls == 1


def test_mission_count_uses_get_count():
    source = FastCountFakeSource(file_count=2, count=9)
    cutout_registry._SOURCES["fake"] = source

    response = _client().post(
        "/api/v0/filenames/fake/count",
        json={"position": ["10.0 20.0"], "filter": ["g"]},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total_files"] == 9
    assert source.count_calls == 1
    assert source.filenames_calls == 0


def test_count_unknown_mission_is_404():
    client = _client()
    response = client.post("/api/v0/filenames/missing/count", json={"position": ["10.0 20.0"]})
    assert response.status_code == 404


def test_multi_mission_count_sums_counts_without_filename_lists():
    fast_source = FastCountFakeSource(count=9)
    default_source = FakeSource(file_count=3)
    cutout_registry._SOURCES.update(fast=fast_source, default=default_source)

    response = _client().post(
        "/api/v0/filenames/count",
        data={
            "position": ["10.0 20.0"],
            "fast": json.dumps({"filter": ["g"]}),
            "default": json.dumps({"filter": ["g"]}),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total_files"] == 12
    assert body["missions"] == {
        "fast": {"total_files": 9},
        "default": {"total_files": 3},
    }
    assert "filenames" not in body
    assert fast_source.count_calls == 1
    assert fast_source.filenames_calls == 0
    assert default_source.filenames_calls == 1


def test_multi_mission_count_passes_each_mission_parameters():
    first_source = FastCountFakeSource(count=4)
    second_source = FastCountFakeSource(count=6)
    cutout_registry._SOURCES.update(first=first_source, second=second_source)
    response = _client().post(
        "/api/v0/filenames/count",
        data={
            "position": ["10.0 20.0"],
            "first": json.dumps({"filter": ["g"], "survey": ["s"]}),
            "second": json.dumps({"filter": ["r"], "survey": ["wide"]}),
        },
    )

    assert response.status_code == 200
    assert response.json()["total_files"] == 10
    assert first_source.count_requests[0][0] == ["g"]
    assert first_source.count_requests[0][1] == {"survey": ["s"]}
    assert second_source.count_requests[0][0] == ["r"]
    assert second_source.count_requests[0][1] == {"survey": ["wide"]}


def test_multi_mission_count_unknown_mission():
    cutout_registry._SOURCES["fake"] = FakeSource()

    response = _client().post(
        "/api/v0/filenames/count",
        data={"position": ["10.0 20.0"], "missing": json.dumps({})},
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["total_files"] == 0
    assert response_data["missions"] == {}


def test_single_mission_filenames_route():
    source = FakeSource(file_count=3)
    cutout_registry._SOURCES["fake"] = source

    response = _client().post(
        "/api/v0/filenames/fake",
        json={"position": ["10.0 20.0"], "filter": ["g"]},
    )

    assert response.status_code == 200
    assert response.json()["total_files"] == 3
