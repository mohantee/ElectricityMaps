from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from electricity_maps.api.client import APIError, ElectricityMapsClient


def test_api_client_get_mix_success(test_settings, sample_mix_response):
    """Positive scenario: Successful API call for electricity mix."""
    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: sample_mix_response
        )

        with ElectricityMapsClient(test_settings) as client:
            result = client.get_raw_mix_range("FR", datetime.now(UTC), datetime.now(UTC))
            assert result == sample_mix_response
            assert "data" in result
            assert len(result["data"]) == 1

def test_api_client_get_mix_retry_on_5xx(test_settings, sample_mix_response):
    """Positive scenario: Retry logic works on transient 5xx errors."""
    with patch("httpx.Client.get") as mock_get:
        # Mock 500 error followed by 200 success
        mock_get.side_effect = [
            httpx.Response(500, content=b"Server Error", request=httpx.Request("GET", "http://test")),
            httpx.Response(200, json=sample_mix_response, request=httpx.Request("GET", "http://test"))
        ]

        with ElectricityMapsClient(test_settings) as client:
            result = client.get_raw_mix_range("FR", datetime.now(UTC), datetime.now(UTC))
            assert result == sample_mix_response
            assert mock_get.call_count == 2

def test_api_client_auth_error(test_settings):
    """Negative scenario: API returns 401 Unauthorized."""
    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=401, text="Unauthorized")

        with ElectricityMapsClient(test_settings) as client:
            with pytest.raises(APIError) as exc:
                client.get_raw_mix_range("FR", datetime.now(UTC), datetime.now(UTC))
            assert "401" in str(exc.value)

def test_api_client_timeout(test_settings):
    """Negative scenario: API call times out."""
    with patch("httpx.Client.get", side_effect=httpx.TimeoutException("Timeout")), \
         ElectricityMapsClient(test_settings) as client:
        with pytest.raises(httpx.TimeoutException):
            client.get_raw_mix_range("FR", datetime.now(UTC), datetime.now(UTC))
