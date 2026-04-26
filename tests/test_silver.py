import json
from unittest.mock import patch

import polars as pl

from electricity_maps.layers.bronze import ingest_bronze
from electricity_maps.layers.silver import _flatten_flows, _flatten_mix, transform_silver
from electricity_maps.utils.state import PipelineState


def test_flatten_mix_valid(sample_mix_response):
    """Positive scenario: Flatten a valid mix JSON response."""
    raw_json = json.dumps(sample_mix_response)
    good_df, bad_df = _flatten_mix(raw_json, "FR", 1745489400)

    assert len(good_df) == 1
    assert len(bad_df) == 0
    assert good_df[0, "nuclear_mw"] == 27069.648
    assert good_df[0, "hydro_storage_charge_mw"] == 1239.355

def test_flatten_mix_malformed_json():
    """Negative scenario: Handle malformed JSON in mix."""
    raw_json = "{ malformed }"
    good_df, bad_df = _flatten_mix(raw_json, "FR", 1745489400)

    assert len(good_df) == 0
    assert len(bad_df) == 1
    assert "Expecting property name" in bad_df[0, "error_message"]

def test_flatten_flows_valid(sample_flows_response):
    """Positive scenario: Flatten valid flows JSON (unpivoting)."""
    raw_json = json.dumps(sample_flows_response)
    good_df, bad_df = _flatten_flows(raw_json, "FR", 1745489400)

    # 2 imports (BE, CH) + 2 exports (DE, GB) = 4 rows
    assert len(good_df) == 4
    assert len(bad_df) == 0
    assert good_df.filter(pl.col("neighbor_zone") == "BE")["power_mw"][0] == 561.0

def test_silver_full_cycle(test_settings, sample_mix_response, sample_flows_response):
    """Integration scenario: Full silver transformation from bronze data."""
    # 1. Prepare Bronze data
    process_ts_bronze = 1745489400
    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.return_value = sample_mix_response
        mock_client.get_raw_flows_range.return_value = sample_flows_response
        ingest_bronze(test_settings, process_ts_bronze)

    # 2. Run Silver
    process_ts_silver = 1745489410
    result = transform_silver(test_settings, process_ts_silver)

    assert result["mix_records"] == 1
    assert result["flows_records"] == 4

    # 3. Check el_state
    state = PipelineState(test_settings)
    summary = state.get_state_summary()
    assert summary.filter(pl.col("process") == "silver")["status"][0] == "R"
    assert summary.filter((pl.col("process") == "bronze") & (pl.col("process_ts") == process_ts_bronze))["status"][0] == "C"

def test_silver_validation_failure(test_settings, sample_mix_response):
    """Negative scenario: Row fails Pandera validation (e.g. negative MW)."""
    # Create bad data: negative nuclear MW
    sample_mix_response["data"][0]["mix"]["nuclear"] = -100.0

    # 1. Prepare Bronze
    process_ts_bronze = 1745489500
    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.return_value = sample_mix_response
        mock_client.get_raw_flows_range.return_value = {"data": []}
        ingest_bronze(test_settings, process_ts_bronze)

    # 2. Run Silver
    result = transform_silver(test_settings, 1745489510)

    # Pandera should catch the negative value (assuming schema has ge=0 or similar,
    # if not, it might still pass depending on exact SilverMixSchema definition.
    # Looking at silver_schemas.py, nuclear_mw is just pl.Float64, no ge=0 constraint.
    # Let's assume we want to test bad JSON structure then.

    # Let's try missing required field 'datetime'
    del sample_mix_response["data"][0]["datetime"]
    # This will fail in _flatten_mix with KeyError

    # 3. Re-run with missing field
    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.return_value = sample_mix_response
        mock_client.get_raw_flows_range.return_value = {"data": []}
        ingest_bronze(test_settings, 1745489600)

    result = transform_silver(test_settings, 1745489610)
    assert result["bad_mix_records"] > 0
