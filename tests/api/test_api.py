"""Tests for the API"""

import json
from unittest import mock
from unittest.mock import patch

from fastapi.testclient import TestClient

from fornax_cutouts.app.api import main_app
from fornax_cutouts.models.base import TargetPosition
from fornax_cutouts.models.cutouts import FilenameLookupResponse, FilenameWithMetadata
from fornax_cutouts.models.metadata import FilenameRequest
from fornax_cutouts.sources import AbstractMissionSource, MissionMetadata, cutout_registry


# Register a fake source for testing
@cutout_registry.register_source()
class FakeSource(AbstractMissionSource):
    metadata: MissionMetadata = MissionMetadata(
        name="fake_source",
        pixel_size=0.55,
        max_cutout_size=4,
        filter=["a", "b"],
        survey=["c", "d"],
    )

    def get_filenames(self, **kwargs) -> list[FilenameLookupResponse]:
        cutout_position = TargetPosition(ra=210.8023, dec=54.34875)
        file_with_metadata = FilenameWithMetadata(filename="example.fits", metadata={})
        response = FilenameLookupResponse(mission="fake", target=cutout_position, filenames=[file_with_metadata])
        return [response]


class TestAPIInputFormats:
    """Test API input formats for the /filenames and /async endpoints"""

    client = TestClient(main_app)

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_file_name_request_valid_across_apis(self, mock_job):
        """Test that the mission parameters passed to the /filenames endpoint is valid when passed as a JSON string
        to the /async endpoint"""

        # Mock the job creation for the async cutout endpoint
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock(return_value="job123")

        # Send a request to the /filenames endpoint
        file_name_request = FilenameRequest(survey=["c"], filter=["a"]).model_dump()
        request_data = {"position": ["m101"], "mission": {"fake_source": file_name_request}}
        response = self.client.post("/api/v0/filenames", json=request_data)
        assert response.status_code == 200

        # Check that we can send the same FilenameRequest to the async cutout endpoint
        async_form_data = {"fake_source": json.dumps(file_name_request), "position": ["m101"], "size": 4}
        response = self.client.post("api/v0/cutouts/async", data=async_form_data, follow_redirects=False)
        assert response.status_code == 303

        # Get the job id from the mock and check that we redirected to that location
        job_id = mock_job.call_args.kwargs["job_id"]
        assert response.headers["location"] == f"/api/v0/cutouts/async/{job_id}"

    @patch("fornax_cutouts.routes.v1.cutouts.async_uws.AsyncRedisCutoutJob")
    def test_file_name_request_invalid_rejected(self, mock_job):
        """Test that we reject invalid mission parameters for the /async endpoint"""

        # Mock the job creation for the async cutout endpoint
        mock_instance = mock_job.return_value
        mock_instance.create_job = mock.AsyncMock()

        # Send an invalid request
        file_name_request = {"survey": "bad survey type", "filter": {"my filter": "val"}}
        async_form_data = {"fake_source": json.dumps(file_name_request), "position": ["m101"], "size": 4}
        response = self.client.post("api/v0/cutouts/async", data=async_form_data, follow_redirects=False)
        assert response.status_code == 422

        response_body = response.json()
        assert "detail" in response_body
        assert len(response_body["detail"]) == 2
        assert response_body["detail"][0]["type"] == "list_type"
        assert response_body["detail"][0]["loc"] == ["survey"]
        assert response_body["detail"][0]["msg"] == "Input should be a valid list"
