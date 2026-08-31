"""Tests for the API"""

import json
from unittest import mock
from unittest.mock import patch

from fastapi.testclient import TestClient

from fornax_cutouts.app.api import main_app
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata
from fornax_cutouts.models.metadata import FilenameRequest, MultiMissionCutoutRequest
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry


class FakeSource(AbstractMissionSource):
    metadata: MissionMetadata = MissionMetadata(
        name="fake_source",
        pixel_size=0.55,
        max_cutout_size=4,
        filter=["a", "b"],
        survey=["c", "d"],
    )

    def get_filenames(self, **kwargs) -> list[FilenameLookupResponse]:
        print("get_filenames called with kwargs:", kwargs)
        cutout_position = TargetPosition(ra=210.8023, dec=54.34875)
        file_with_metadata = FilenameWithMetadata(filename="example.fits", metadata={})
        response = FilenameLookupResponse(mission="fake", target=cutout_position, filenames=[file_with_metadata])
        return [response]


class TestAPIInputFormats:
    """Test API input formats for the /filenames and /async endpoints"""

    client = TestClient(main_app)

    def setup_class(self):
        # Register the fake source for testing (once for this test class at start of tests)
        cutout_registry.register_source(FakeSource)

    def teardown_class(self):
        # Clear fake source from registry (once for this test class at end of tests)
        cutout_registry._SOURCES.clear()
        # Ensure cached_property _VALID_SOURCES is cleared
        if hasattr(cutout_registry, "_VALID_SOURCES"):
            del cutout_registry._VALID_SOURCES

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_same_request_filenames_async(self, mock_job):
        """Test that the same request works for the /filenames endpoint and the /async endpoint"""

        # Mock the job creation for the async cutout endpoint
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock(return_value="job123")

        # make a MultimissionCutoutRequest to the /async endpoint
        filename_request = FilenameRequest(survey=["c"], filter=["a"])
        cutout_request = MultiMissionCutoutRequest(
            missions={"fake_source": filename_request}, position=["m101"], size=4
        ).model_dump()
        # convert cutout_request to form data by json encoding any dict values
        form_data = {k: json.dumps(v) if isinstance(v, dict) else v for k, v in cutout_request.items()}

        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert response.status_code == 303

        # Get the job id from the mock and check that we redirected to that location
        job_id = mock_job.call_args.kwargs["job_id"]
        assert response.headers["location"] == f"/api/v0/cutouts/async/{job_id}"

        # Check that we can send the same request to the /filenames endpoint
        response = self.client.post("/api/v0/filenames", data=form_data)

        assert response.status_code == 200

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_file_name_request_invalid_rejected(self, mock_job):
        """Test that we reject invalid mission parameters for the /async endpoint"""

        # Mock the job creation for the async cutout endpoint
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()

        # Send an invalid request
        file_name_request = {"filter": {"my filter": "val"}}
        async_form_data = {"fake_source": json.dumps(file_name_request), "position": ["m101"], "size": 4}
        response = self.client.post("api/v0/cutouts/async", data=async_form_data, follow_redirects=False)
        assert response.status_code == 422

        response_body = response.json()
        assert "detail" in response_body

        assert len(response_body["detail"]) == 1
        assert response_body["detail"][0]["type"] == "string_type"
        assert response_body["detail"][0]["loc"] == ["missions", "fake_source", "filter", 0]
        assert response_body["detail"][0]["msg"] == "Input should be a valid string"

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_dot_notation_request(self, mock_job):
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()
        form_data = {
            "position": ["m101"],
            "size": 4,
            "fake_source.filter": ["a", "b"],
        }
        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert response.status_code == 303
        job_id = mock_job.call_args.kwargs["job_id"]

        mock_instance.create_job.assert_awaited_once()
        # check that the parameters passed to create_job are correct
        assert mock_instance.create_job.await_args_list[0].kwargs["parameters"] == {
            "position": ["m101"],
            "size": 4,
            "output_format": ["fits"],
            "fake_source": {"filter": ["a", "b"]},
        }
        assert response.headers["location"] == f"/api/v0/cutouts/async/{job_id}"

    def test_extra_params_filenames_request_okay(self):
        form_data = {
            "position": ["m101"],
            "size": 4,
            "filters": "a",
            "extra_param": "value",
        }
        with patch.object(FakeSource, "get_filenames") as mock_get_filenames:
            self.client.post("api/v0/filenames/fake_source", data=form_data, follow_redirects=False)
            mock_get_filenames.assert_called()
            # check that the size, filters, extra_param call args are there
            assert mock_get_filenames.call_args.kwargs["size"] == "4"
            assert mock_get_filenames.call_args.kwargs["filters"] == "a"
            assert mock_get_filenames.call_args.kwargs["extra_param"] == "value"

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_source_name_key_with_json_string_value(self, mock_job):
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()
        form_data = {
            "position": ["10, 20"],
            "size": 4,
            "fake_source": '{"survey": ["c"], "filter": ["a"]}',
        }
        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert response.status_code == 303
        job_id = response.headers["location"].split("/")[-1]
        assert job_id is not None

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_missions_as_json_string_is_parsed_api(self, mock_job):
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()
        form_data = {
            "position": ["10, 20"],
            "size": 4,
            "missions": '{"fake_source": {"survey": ["survey1"]}}',
        }
        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert response.status_code == 303
        job_id = response.headers["location"].split("/")[-1]
        assert job_id is not None

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_missions_as_invalid_json_string_raises_api(self, mock_job):
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()
        form_data = {
            "position": ["10, 20"],
            "size": 4,
            "missions": "not-json",
        }
        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)
        assert response.status_code == 422
        assert "invalid_json" in response.text

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_empty_str_mission_value_treated_as_empty_dict_top_level(self, mock_job):
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()
        form_data = {"position": ["10, 20"], "size": 4, "fake_source": ""}
        response = self.client.post("api/v0/cutouts/async", data=form_data, follow_redirects=False)

        assert response.status_code == 303
        job_id = response.headers["location"].split("/")[-1]
        assert job_id is not None
