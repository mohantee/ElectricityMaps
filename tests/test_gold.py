from datetime import UTC, datetime
from unittest.mock import patch

import polars as pl

from electricity_maps.layers.bronze import ingest_bronze
from electricity_maps.layers.gold import _aggregate_mix, transform_gold
from electricity_maps.layers.silver import transform_silver
from electricity_maps.utils.state import PipelineState


def test_aggregate_mix_calculation():
    """Positive scenario: Test MW to MWh aggregation and % calculation."""
    # Create sample silver data: 2 hours for 1 day
    silver_data = pl.DataFrame({
        "process_ts": [1745489400, 1745489400],
        "zone": ["FR", "FR"],
        "datetime": [datetime(2026, 4, 23, 9, tzinfo=UTC), datetime(2026, 4, 23, 10, tzinfo=UTC)],
        "updated_at": [datetime.now(UTC), datetime.now(UTC)],
        "is_estimated": [False, False],
        "estimation_method": [None, None],
        "nuclear_mw": [100.0, 200.0],  # Sum = 300 MWh
        "solar_mw": [50.0, 50.0],      # Sum = 100 MWh
        "wind_mw": [0.0, 0.0],
        "coal_mw": [0.0, 0.0],
        "biomass_mw": [0.0, 0.0],
        "hydro_mw": [0.0, 0.0],
        "gas_mw": [0.0, 0.0],
        "oil_mw": [0.0, 0.0],
        "geothermal_mw": [0.0, 0.0],
        "unknown_mw": [0.0, 0.0],
        "year": [2026, 2026],
        "month": [4, 4],
        "day": [23, 23]
    })

    result = _aggregate_mix(silver_data, 1745489420)

    assert len(result) == 1
    assert result[0, "total_production_mwh"] == 400.0
    assert result[0, "nuclear_pct"] == 75.0  # 300 / 400 * 100
    assert result[0, "solar_pct"] == 25.0    # 100 / 400 * 100
    assert result[0, "hours_covered"] == 2

def test_gold_full_pipeline(test_settings, sample_mix_response, sample_flows_response):
    """End-to-end integration: Bronze -> Silver -> Gold."""
    # 1. Bronze
    with patch("electricity_maps.layers.bronze.ElectricityMapsClient") as mock_client_class:
        mock_client = mock_client_class.return_value.__enter__.return_value
        mock_client.get_raw_mix_range.return_value = sample_mix_response
        mock_client.get_raw_flows_range.return_value = sample_flows_response
        ingest_bronze(test_settings, 1745489400)

    # 2. Silver
    transform_silver(test_settings, 1745489410)

    # 3. Gold
    result = transform_gold(test_settings, 1745489420)

    assert result["mix_records"] == 1
    assert result["imports_records"] > 0

    # 4. Check state
    state = PipelineState(test_settings)
    summary = state.get_state_summary()
    assert summary.filter(pl.col("process") == "gold")["status"][0] == "R"
    assert summary.filter(pl.col("process") == "silver")["status"][0] == "C"

def test_gold_no_silver_ready(test_settings):
    """Positive scenario: Gold handles no pending silver gracefully."""
    result = transform_gold(test_settings, 1745489420)
    assert result["status"] == "no_pending"
