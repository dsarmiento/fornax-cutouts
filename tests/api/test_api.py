"""Tests for the API"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from xml.etree import ElementTree as ET

import fakeredis
import pytest
from fastapi.testclient import TestClient
from vo_models.uws.types import ExecutionPhase

from fornax_cutouts.app.api import main_app
from fornax_cutouts.config import CONFIG
from fornax_cutouts.jobs.redis import RedisKeys, async_redis_client_factory
from fornax_cutouts.jobs.results import CutoutResults
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import CutoutResponse, FilenameLookupResponse, FilenameWithMetadata
from fornax_cutouts.models.metadata import FilenameRequest, MultiMissionCutoutRequest
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry

_SYNC_RA = 188.27856215089
_SYNC_DEC = 82.56394517878
_SYNC_SIZE = 300
_SYNC_FILENAME = "s3://example/source/file.fits"
_SYNC_RED = "s3://example/source/red.fits"
_SYNC_GREEN = "s3://example/source/green.fits"
_SYNC_BLUE = "s3://example/source/blue.fits"

_OBJECT_NAME = "tadpole galaxy"
_OBJECT_POS = TargetPosition(123.0, 45.0)
_POSITIONS = ["10.0, 20.0", "30.0, 40.0", "50.0, 60.0"]
_SANTA_POSITIONS = ["10.0, 20.0", _OBJECT_NAME, "30.0, 40.0"]

_ASYNC_JOB_FORM = {
    "RUNID": "test-run-id",
    "generate_preview": "true",
    "position": _POSITIONS,
    "size": "256",
    "fake_source.stack_file": "data",
    "fake_source.survey": "s",
    "fake_source.filter": ["a", "b"],
}

_FORMAT_FORM = {
    "position": ["m101"],
    "size": "4",
    "fake_source.filter": ["a", "b"],
}

_SANTA_COUNT_FORM = {
    "position": _SANTA_POSITIONS,
    "fake_source": '{"filter": ["g"], "survey": ["s"], "stack_file": ["data"]}',
}

_SINGLE_CUTOUT_RESULT = {
    "mission": "sync",
    "position": {"ra": _SYNC_RA, "dec": _SYNC_DEC},
    "size_px": [_SYNC_SIZE, _SYNC_SIZE],
    "science": "cutouts/sync/job/file.fits",
    "preview": "cutouts/sync/job/file.jpg",
}

_COLOR_CUTOUT_RESULT = {
    "mission": "sync",
    "position": {"ra": _SYNC_RA, "dec": _SYNC_DEC},
    "size_px": [_SYNC_SIZE, _SYNC_SIZE],
    "preview": "cutouts/sync/job/color.jpg",
}

_ZERO_RESULT_STATUS = {
    "pending_jobs": 0,
    "queued_jobs": 0,
    "executing_jobs": 0,
    "completed_jobs": 0,
    "skipped_jobs": 0,
    "failed_jobs": 0,
    "total_jobs": 0,
}

_XLINK_HREF = "{http://www.w3.org/1999/xlink}href"


def _xml(text):
    return ET.fromstring(text)


def _xml_text(root, tag):
    element = root.find(f".//{{*}}{tag}")
    assert element is not None
    return element.text


def _xml_local(tag):
    return tag.split("}")[-1]


def _result_hrefs(root):
    return {
        element.get("id"): element.get(_XLINK_HREF) or element.get("href")
        for element in root.iter()
        if _xml_local(element.tag) == "result"
    }


def _params_by_id(root):
    params = {}
    for element in root.iter():
        if _xml_local(element.tag) == "parameter" and element.get("id"):
            params.setdefault(element.get("id"), []).append(element.text)
    return params


def _bust_valid_sources():
    if hasattr(cutout_registry, "_VALID_SOURCES"):
        del cutout_registry._VALID_SOURCES


class FakeSource(AbstractMissionSource):
    """Fake mission to exercise route behavior."""

    metadata: MissionMetadata = MissionMetadata(
        name="fake_source",
        pixel_size=0.55,
        max_cutout_size=4,
        filter=["a", "b"],
        survey=["c", "d"],
    )

    def __init__(self):
        self.count_calls = []
        self.filename_calls = []

    def get_filenames(self, position, filter=None, **kwargs):
        positions = position if isinstance(position, list) else [position]
        self.filename_calls.append({"position": list(positions), "filter": filter, **kwargs})
        target = positions[0] if positions else TargetPosition(ra=210.8023, dec=54.34875)
        return [
            FilenameLookupResponse(
                mission="fake_source",
                target=target,
                filenames=[FilenameWithMetadata(filename="example.fits")],
            )
        ]

    def get_count(self, position, filter=None, **kwargs):
        positions = position if isinstance(position, list) else [position]
        self.count_calls.append({"position": list(positions), "filter": filter, **kwargs})
        return 42


def _celery_result(payload):
    result = MagicMock()
    result.ready.return_value = True
    result.get.return_value = payload
    return result


def _created_job_id(response):
    return response.headers["location"].rstrip("/").split("/")[-1]


def _job_uws(redis, job_id):
    return redis.json().get(RedisKeys(job_id).uws)


def _set_job_result_counts(
    redis,
    job_id,
    *,
    pending=0,
    queued=0,
    executing=0,
    completed=0,
    skipped=0,
    failed=0,
    total=0,
):
    keys = RedisKeys(job_id)
    redis.delete(keys.pending_tasks, keys.failed_tasks)
    for _ in range(pending):
        redis.rpush(keys.pending_tasks, "pending")
    for _ in range(failed):
        redis.rpush(keys.failed_tasks, "failed")
    redis.set(keys.queued_task_count, queued)
    redis.set(keys.executing_task_count, executing)
    redis.set(keys.completed_task_count, completed)
    redis.set(keys.skipped_task_count, skipped)
    redis.set(keys.total_task_count, total)


@pytest.fixture
def api():
    sources_snapshot = dict(cutout_registry._SOURCES)
    overrides_snapshot = dict(main_app.dependency_overrides)

    source = FakeSource()
    cutout_registry._SOURCES.clear()
    _bust_valid_sources()
    cutout_registry._SOURCES["fake_source"] = source
    _bust_valid_sources()

    server = fakeredis.FakeServer()
    sync_redis = fakeredis.FakeRedis(server=server, decode_responses=True)
    async_redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    main_app.dependency_overrides[async_redis_client_factory] = lambda: async_redis

    try:
        with (
            patch("fornax_cutouts.app.api.discover_sources"),
            patch("fornax_cutouts.routes.v1.cutouts.async_uws.schedule_job") as mock_schedule,
            patch("fornax_cutouts.routes.v1.cutouts.sync.execute_cutout") as mock_cutout,
            patch("fornax_cutouts.routes.v1.cutouts.sync.execute_color_preview") as mock_color,
            patch(
                "fornax_cutouts.utils.santa_resolver.SantaResolver.resolve_targets",
                return_value={_OBJECT_NAME: _OBJECT_POS},
            ) as mock_santa,
            TestClient(main_app) as test_client,
        ):
            mock_cutout.apply_async.return_value = _celery_result(_SINGLE_CUTOUT_RESULT)
            mock_color.apply_async.return_value = _celery_result(_COLOR_CUTOUT_RESULT)
            yield SimpleNamespace(
                client=test_client,
                schedule_job=mock_schedule,
                execute_cutout=mock_cutout,
                execute_color_preview=mock_color,
                santa=mock_santa,
                source=source,
                redis=sync_redis,
            )
    finally:
        sync_redis.flushall()
        cutout_registry._SOURCES.clear()
        cutout_registry._SOURCES.update(sources_snapshot)
        _bust_valid_sources()
        main_app.dependency_overrides.clear()
        main_app.dependency_overrides.update(overrides_snapshot)


@pytest.fixture
def client(api):
    return api.client


@pytest.fixture
def job_id(client):
    response = client.post("/api/v0/cutouts/async", data=_ASYNC_JOB_FORM, follow_redirects=False)
    assert response.status_code == 303
    return _created_job_id(response)


class TestHealth:
    def test_health(self, client):
        response = client.get("/api/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert "timestamp" in body


class TestSync:
    def test_single_cutout(self, api):
        response = api.client.get(
            "/api/v0/cutouts/sync/single",
            params={"filename": _SYNC_FILENAME, "ra": _SYNC_RA, "dec": _SYNC_DEC, "size": _SYNC_SIZE},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["science"] == _SINGLE_CUTOUT_RESULT["science"]
        assert body["preview"] == _SINGLE_CUTOUT_RESULT["preview"]

        call = api.execute_cutout.apply_async.call_args
        kwargs = call.kwargs["kwargs"]
        assert kwargs["source_file"] == _SYNC_FILENAME
        assert kwargs["target"] == TargetPosition(_SYNC_RA, _SYNC_DEC)
        assert kwargs["size"] == _SYNC_SIZE
        assert kwargs["generate_science"] is True
        assert kwargs["generate_preview"] is True
        assert kwargs["mission"] == "sync"
        assert kwargs["output_dir"].endswith(f"cutouts/sync/{kwargs['job_id']}")
        assert call.kwargs["priority"] == 0
        assert call.kwargs["task_id"].startswith(f"sync-single-{kwargs['job_id']}-")

    def test_color_preview(self, api):
        response = api.client.get(
            "/api/v0/cutouts/sync/color",
            params={
                "red": _SYNC_RED,
                "green": _SYNC_GREEN,
                "blue": _SYNC_BLUE,
                "ra": _SYNC_RA,
                "dec": _SYNC_DEC,
                "size": _SYNC_SIZE,
            },
        )
        assert response.status_code == 200
        assert response.json()["preview"] == _COLOR_CUTOUT_RESULT["preview"]

        call = api.execute_color_preview.apply_async.call_args
        kwargs = call.kwargs["kwargs"]
        assert kwargs["red"] == _SYNC_RED
        assert kwargs["green"] == _SYNC_GREEN
        assert kwargs["blue"] == _SYNC_BLUE
        assert kwargs["target"] == TargetPosition(_SYNC_RA, _SYNC_DEC)
        assert kwargs["size"] == _SYNC_SIZE
        assert call.kwargs["priority"] == 0
        assert call.kwargs["task_id"].startswith("sync-color-")

    def test_single_cutout_timeout(self, api, monkeypatch):
        monkeypatch.setattr(CONFIG.redis, "timeout", 0)
        result = MagicMock()
        result.ready.return_value = False
        api.execute_cutout.apply_async.return_value = result
        with pytest.raises(TimeoutError, match="did not complete"):
            api.client.get(
                "/api/v0/cutouts/sync/single",
                params={"filename": _SYNC_FILENAME, "ra": _SYNC_RA, "dec": _SYNC_DEC, "size": _SYNC_SIZE},
            )

    def test_single_cutout_task_failure(self, api):
        result = MagicMock()
        result.ready.return_value = True
        result.get.side_effect = RuntimeError("celery failed")
        api.execute_cutout.apply_async.return_value = result
        with pytest.raises(RuntimeError, match="celery failed"):
            api.client.get(
                "/api/v0/cutouts/sync/single",
                params={"filename": _SYNC_FILENAME, "ra": _SYNC_RA, "dec": _SYNC_DEC, "size": _SYNC_SIZE},
            )


class TestMetadata:
    def test_all_missions(self, client):
        response = client.get("/api/v0/missions")
        assert response.status_code == 200
        body = response.json()
        assert body["fake_source"]["name"] == "fake_source"
        assert body["fake_source"]["filter"] == ["a", "b"]

    def test_single_mission(self, client):
        response = client.get("/api/v0/missions/fake_source")
        assert response.status_code == 200
        body = response.json()
        assert body["name"] == "fake_source"
        assert body["survey"] == ["c", "d"]

    def test_unknown_mission(self, client):
        response = client.get("/api/v0/missions/not-a-mission")
        assert response.status_code == 404

    def test_count_resolves_names(self, api):
        response = api.client.post("/api/v0/filenames/count", data=_SANTA_COUNT_FORM)
        assert response.status_code == 200
        assert response.json()["total_files"] == 42
        api.santa.assert_called_once_with([_OBJECT_NAME])

        call = cutout_registry.get_mission("fake_source").count_calls[-1]
        assert call["position"] == [TargetPosition(10.0, 20.0), _OBJECT_POS, TargetPosition(30.0, 40.0)]
        assert call["filter"] == ["g"]
        assert call["survey"] == ["s"]
        assert call["stack_file"] == ["data"]

    def test_count_drops_unresolved_names(self, api):
        api.santa.return_value = {}
        response = api.client.post("/api/v0/filenames/count", data=_SANTA_COUNT_FORM)
        assert response.status_code == 200
        call = cutout_registry.get_mission("fake_source").count_calls[-1]
        assert call["position"] == [TargetPosition(10.0, 20.0), TargetPosition(30.0, 40.0)]


class TestRequestFormats:
    def test_dot_notation_request(self, api):
        response = api.client.post("/api/v0/cutouts/async", data=_FORMAT_FORM, follow_redirects=False)
        assert response.status_code == 303
        job_id = _created_job_id(response)
        assert response.headers["location"] == f"/api/v0/cutouts/async/{job_id}"
        job = _job_uws(api.redis, job_id)
        assert job["parameters"]["generate_science"] is True
        assert job["parameters"]["generate_preview"] is False
        assert job["parameters"]["fake_source"] == {"filter": ["a", "b"]}

    def test_same_form_works_for_async_and_filenames(self, api):
        filename_request = FilenameRequest(survey=["c"], filter=["a"])
        form_data = {
            k: json.dumps(v) if isinstance(v, dict) else v
            for k, v in MultiMissionCutoutRequest(missions={"fake_source": filename_request}, position=["m101"], size=4)
            .model_dump()
            .items()
        }

        async_response = api.client.post("/api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert async_response.status_code == 303
        job_id = _created_job_id(async_response)
        assert async_response.headers["location"] == f"/api/v0/cutouts/async/{job_id}"
        api.schedule_job.apply_async.assert_called_once_with(
            task_id=f"schedule_job-{job_id}",
            kwargs={"job_id": job_id},
        )

        filenames_response = api.client.post("/api/v0/filenames", data=form_data)
        assert filenames_response.status_code == 200
        assert filenames_response.json()["total_files"] == 1
        assert api.source.filename_calls

    def test_invalid_mission_filter_type_is_rejected(self, api):
        response = api.client.post(
            "/api/v0/cutouts/async",
            data={
                "position": ["m101"],
                "size": "4",
                "fake_source": '{"filter": {"my filter": "val"}}',
            },
            follow_redirects=False,
        )
        assert response.status_code == 422
        body = response.json()
        assert "detail" in body
        assert len(body["detail"]) == 1
        detail = body["detail"][0]
        assert detail["type"] == "string_type"
        assert detail["loc"] == ["missions", "fake_source", "filter", 0]
        assert detail["msg"] == "Input should be a valid string"

    def test_source_name_json_string_is_parsed(self, api):
        response = api.client.post(
            "/api/v0/cutouts/async",
            data={
                "position": ["m101"],
                "size": "4",
                "fake_source": '{"survey": ["c"], "filter": ["a"]}',
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        job = _job_uws(api.redis, _created_job_id(response))
        assert job["parameters"]["fake_source"] == {"survey": ["c"], "filter": ["a"]}

    def test_missions_json_string_is_parsed(self, api):
        response = api.client.post(
            "/api/v0/cutouts/async",
            data={
                "position": ["m101"],
                "size": "4",
                "missions": '{"fake_source": {"survey": ["c"]}}',
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        job = _job_uws(api.redis, _created_job_id(response))
        assert job["parameters"]["fake_source"] == {"survey": ["c"]}

    def test_invalid_missions_json_is_rejected(self, api):
        response = api.client.post(
            "/api/v0/cutouts/async",
            data={"position": ["m101"], "size": "4", "missions": "not-json"},
            follow_redirects=False,
        )
        assert response.status_code == 422
        assert "invalid_json" in response.text

    def test_empty_source_json_string_creates_empty_params(self, api):
        response = api.client.post(
            "/api/v0/cutouts/async",
            data={"position": ["m101"], "size": "4", "fake_source": ""},
            follow_redirects=False,
        )
        assert response.status_code == 303
        job = _job_uws(api.redis, _created_job_id(response))
        assert job["parameters"]["fake_source"] == {}

    def test_extra_filename_params_are_forwarded(self, api):
        response = api.client.post(
            "/api/v0/filenames/fake_source",
            data={
                "position": ["m101"],
                "size": "4",
                "filters": "a",
                "extra_param": "value",
            },
        )
        assert response.status_code == 200
        call = api.source.filename_calls[-1]
        assert call["size"] == "4"
        assert call["filters"] == "a"
        assert call["extra_param"] == "value"


class TestAsyncUWS:
    def test_request_job(self, api):
        response = api.client.post("/api/v0/cutouts/async", data=_ASYNC_JOB_FORM, follow_redirects=False)
        assert response.status_code == 303
        job_id = _created_job_id(response)

        api.schedule_job.apply_async.assert_called_once_with(
            task_id=f"schedule_job-{job_id}",
            kwargs={"job_id": job_id},
        )
        job = _job_uws(api.redis, job_id)
        assert job["run_id"] == "test-run-id"
        assert job["parameters"]["size"] == 256
        assert job["parameters"]["generate_science"] is True
        assert job["parameters"]["generate_preview"] is True
        assert job["parameters"]["fake_source"]["stack_file"] == "data"
        assert job["parameters"]["fake_source"]["survey"] == ["s"]
        assert job["parameters"]["fake_source"]["filter"] == ["a", "b"]
        assert job["parameters"]["position_count"] == len(_POSITIONS)
        assert api.redis.lrange(RedisKeys(job_id).positions, 0, -1) == _POSITIONS

    def test_job_list_redirects_without_last(self, client):
        response = client.get("/api/v0/cutouts/async", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].endswith("/api/v0/cutouts/async?last=100")

    def test_job_list_empty(self, client):
        response = client.get("/api/v0/cutouts/async", params={"last": 100})
        assert response.status_code == 200
        assert "xml" in response.headers["content-type"]
        root = _xml(response.text)
        assert _xml_local(root.tag) == "jobs"
        assert [element.get("id") for element in root.iter() if _xml_local(element.tag) == "jobref"] == []

    def test_job_list_includes_created_job(self, client):
        created = client.post("/api/v0/cutouts/async", data=_ASYNC_JOB_FORM, follow_redirects=False)
        job_id = _created_job_id(created)
        response = client.get("/api/v0/cutouts/async", params={"last": 100})
        assert response.status_code == 200
        root = _xml(response.text)
        job_ids = [element.get("id") for element in root.iter() if _xml_local(element.tag) == "jobref"]
        assert job_id in job_ids

    def test_invalid_form(self, client):
        response = client.post("/api/v0/cutouts/async", data={"RUNID": "x"}, follow_redirects=False)
        assert response.status_code == 422


class TestJobSpecific:
    def test_status(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}")
        assert response.status_code == 200
        assert "xml" in response.headers["content-type"]
        root = _xml(response.text)
        assert _xml_text(root, "jobId") == job_id
        assert _xml_text(root, "phase") == "PENDING"
        assert _xml_text(root, "runId") == "test-run-id"

    def test_unknown_job(self, client):
        response = client.get("/api/v0/cutouts/async/doesnotexist")
        assert response.status_code == 404

    def test_results_summary(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/results/summary")
        assert response.status_code == 200
        assert response.json() == _ZERO_RESULT_STATUS

    def test_results_summary_counts(self, api, client, job_id):
        _set_job_result_counts(
            api.redis,
            job_id,
            pending=1,
            executing=1,
            completed=3,
            failed=1,
            total=5,
        )
        response = client.get(f"/api/v0/cutouts/async/{job_id}/results/summary")
        assert response.status_code == 200
        assert response.json() == {
            "pending_jobs": 1,
            "queued_jobs": 0,
            "executing_jobs": 1,
            "completed_jobs": 3,
            "skipped_jobs": 0,
            "failed_jobs": 1,
            "total_jobs": 5,
        }

    def test_phase(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/phase")
        assert response.status_code == 200
        assert "PENDING" in response.text

    def test_phase_executing(self, api, client, job_id):
        api.redis.json().set(RedisKeys(job_id).uws, "$.phase", ExecutionPhase.EXECUTING)
        response = client.get(f"/api/v0/cutouts/async/{job_id}/phase")
        assert response.status_code == 200
        assert "EXECUTING" in response.text

    def test_execution_duration(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/executionduration")
        assert response.status_code == 200
        assert response.json() == 0

    def test_destruction(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/destruction")
        assert response.status_code == 501

    def test_error(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/error")
        assert response.status_code == 200
        assert response.json() is None

    def test_quote(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/quote")
        assert response.status_code == 200
        assert response.json() == ""

    def test_results(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/results")
        assert response.status_code == 200
        assert "xml" in response.headers["content-type"]
        hrefs = _result_hrefs(_xml(response.text))
        assert hrefs["summary"] == f"/api/v0/cutouts/async/{job_id}/results/summary"
        assert hrefs["cutouts"] == f"/api/v0/cutouts/async/{job_id}/results/cutouts"

    def test_parameters(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/parameters")
        assert response.status_code == 200
        assert "xml" in response.headers["content-type"]
        params = _params_by_id(_xml(response.text))
        assert params["size"] == ["256"]
        assert params["fake_source.filter"] == ["a", "b"]
        assert params["fake_source.survey"] == ["s"]
        assert params["fake_source.stack_file"] == ["data"]
        assert params["position"][0].endswith(f"/api/v0/cutouts/async/{job_id}/parameters/position")

    def test_owner(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/owner")
        assert response.status_code == 200
        assert response.json() is None

    def test_results_cutouts(self, client, job_id, tmp_path, monkeypatch):
        monkeypatch.setattr(CONFIG.storage, "prefix", str(tmp_path))
        rows = [
            CutoutResponse(
                mission="fake_source",
                position=TargetPosition(float(i), float(i + 1)),
                size_px=(256, 256),
                science=f"cutouts/a{i}.fits",
                preview=f"cutouts/a{i}.jpg",
            )
            for i in range(3)
        ]
        CutoutResults(job_id).add_results(rows, batch_num=0)

        response = client.get(
            f"/api/v0/cutouts/async/{job_id}/results/cutouts",
            params={"output_format": "json", "page": 0, "limit": 2},
        )
        assert response.status_code == 200
        body = response.json()
        assert len(body["results"]) == 2
        assert body["results"][0]["science"] == "cutouts/a0.fits"
        assert body["results"][0]["preview"] == "cutouts/a0.jpg"
        assert body["results"][0]["mission"] == "fake_source"
        assert body["metadata"]["totalItems"] == 3
        assert body["metadata"]["totalPages"] == 2
        assert body["metadata"]["page"] == 0
        assert body["metadata"]["limit"] == 2
        assert "next" in body["links"]

    def test_invalid_output_format(self, client, job_id):
        response = client.get(
            f"/api/v0/cutouts/async/{job_id}/results/cutouts",
            params={"output_format": "nope"},
        )
        assert response.status_code == 400

    def test_parameters_positions(self, client, job_id):
        response = client.get(f"/api/v0/cutouts/async/{job_id}/parameters/position")
        assert response.status_code == 200
        body = response.json()
        assert body["positions"] == _POSITIONS
        assert body["metadata"]["totalItems"] == len(_POSITIONS)
