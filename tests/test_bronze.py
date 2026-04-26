from unittest.mock import patch

import polars as pl
import pytest

from electricity_maps.layers.bronze import ingest_bronze
from electricity_maps.utils.state import PipelineState


def test_ingest_bronze_full_cycle(test_settings, sample_mix_response, sample_flows_response):
    """Positive scenario: Full bronze ingestion cycle works."""
    process_ts = 1745489400

    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.return_value = sample_mix_response
        mock_client.get_raw_flows_range.return_value = sample_flows_response

        result = ingest_bronze(test_settings, process_ts)

        assert result["process_ts"] == process_ts
        assert result["mix_records"] == 1
        assert result["flows_records"] == 1

        # Check el_state
        state = PipelineState(test_settings)
        summary = state.get_state_summary()
        assert summary.filter(pl.col("process") == "bronze")["status"][0] == "R"

        # Check files
        from electricity_maps.utils.helpers import get_s3fs
        fs = get_s3fs(test_settings)
        assert fs.exists(result["mix_key"])
        assert fs.exists(result["flows_key"])

def test_ingest_bronze_api_failure(test_settings):
    """Negative scenario: Bronze ingestion fails when API fails."""
    process_ts = 1745489401

    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.side_effect = Exception("API Down")

        with pytest.raises(Exception, match="API Down"):
            ingest_bronze(test_settings, process_ts)

        # Check el_state recorded the error
        state = PipelineState(test_settings)
        summary = state.get_state_summary()
        bronze_row = summary.filter((pl.col("process") == "bronze") & (pl.col("process_ts") == process_ts))
        assert bronze_row["status"][0] == "I"
        assert "API Down" in bronze_row["error_message"][0]
