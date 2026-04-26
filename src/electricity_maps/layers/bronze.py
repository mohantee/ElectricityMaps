"""Bronze Layer — Raw immutable data ingestion.

Fetches electricity mix and flows from the Electricity Maps API and
stores them as Parquet files on S3 with minimal metadata.

Partitioning: ``year=YYYY/month=MM/day=DD`` using ingestion timestamp.
Catch-up: automatically ingests all missing data since the last run.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime, timedelta

import polars as pl
from deltalake import write_deltalake

from electricity_maps.api.client import ElectricityMapsClient
from electricity_maps.config import Settings, get_settings
from electricity_maps.utils.helpers import floor_to_hour
from electricity_maps.utils.state import PipelineState

logger = logging.getLogger(__name__)


def _calculate_time_range(
    state: PipelineState,
    settings: Settings,
) -> tuple[datetime, datetime]:
    """Determine the ingestion time range for this run.

    - ``start`` = last bronze ``process_ts`` (floored to hour),
      or 24h ago if no prior run.
    - ``end`` = current hour start.

    This ensures automatic catch-up if the pipeline was down.
    """
    last_end = state.get_last_process_ts("bronze")

    if last_end is not None:
        start = floor_to_hour(last_end)
    else:
        start = floor_to_hour(datetime.now(UTC) - timedelta(hours=24))
        logger.info(f"no_prior_run: defaulting_start_to={start}")

    end = floor_to_hour(datetime.now(UTC))

    # Ensure we have at least 1 hour to fetch
    if start >= end:
        start = end - timedelta(hours=1)
        

    return start, end


def _write_bronze_delta(
    data: dict,
    table_uri: str,
    storage_options: dict[str, str],
    *,
    process_ts: int,
    ingestion_ts: datetime,
    source_url: str,
    zone: str,
    start: datetime,
    end: datetime,
) -> None:
    """Append one raw Bronze row to a Delta table."""
    row_df = pl.DataFrame(
        [{
            "process_ts": process_ts,
            "ingestion_timestamp": ingestion_ts,
            "source_url": source_url,
            "zone": zone,
            "range_start": start,
            "range_end": end,
            "raw_json": json.dumps(data, default=str),
            "record_count": len(data.get("data", [])),
            "year": ingestion_ts.year,
            "month": ingestion_ts.month,
            "day": ingestion_ts.day,
        }],
        strict=False,
    )
    write_deltalake(
        table_uri,
        row_df.to_arrow(),
        mode="append",
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
    )


def ingest_bronze(
    settings: Settings | None = None,
    process_ts: int | None = None,
) -> dict:
    """Run a full Bronze ingestion cycle.

    1. Write ``el_state`` entry with ``status=I``
    2. Calculate time range (catch-up aware)
    3. Fetch mix + flows from API
    4. Build metadata columns
    5. Write Parquet files to S3
    6. Update ``el_state`` to ``R``

    Args:
        settings: Pipeline settings (defaults to cached singleton).
        process_ts: Epoch-ms batch identifier (defaults to now).

    Returns:
        Summary dict with counts and paths.
    """
    settings = settings or get_settings()
    state = PipelineState(settings)
    process_ts = process_ts or int(time.time() * 1000)
    ingestion_ts = datetime.now(UTC)

    # Step 1: Init state
    state.init_layer("bronze", process_ts, ingestion_ts)
    logger.info(f"bronze_started: process_ts={process_ts}")

    try:
        # Step 2: Calculate time range
        start, end = _calculate_time_range(state, settings)
        logger.info(f"bronze_time_range: start={start}, end={end}")

        # Step 3: Fetch from API
        with ElectricityMapsClient(settings) as client:
            raw_mix = client.get_raw_mix_range(settings.zone, start, end)
            raw_flows = client.get_raw_flows_range(settings.zone, start, end)

        mix_count = len(raw_mix.get("data", []))
        flows_count = len(raw_flows.get("data", []))
        logger.info(
            f"bronze_api_fetched: mix_records={mix_count}, flows_records={flows_count}"
        )

        # Step 4: Build metadata values stored outside raw_json
        mix_url = (
            f"{settings.api_base_url}/electricity-mix/past-range"
            f"?zone={settings.zone}&start={start.isoformat()}&end={end.isoformat()}"
        )
        flows_url = (
            f"{settings.api_base_url}/electricity-flows/past-range"
            f"?zone={settings.zone}&start={start.isoformat()}&end={end.isoformat()}"
        )

        # Step 5: Write to Bronze Delta tables
        so = settings.storage_options
        mix_table = f"{settings.bronze_dir}/electricity_mix"
        flows_table = f"{settings.bronze_dir}/electricity_flows"
        _write_bronze_delta(
            raw_mix,
            mix_table,
            so,
            process_ts=process_ts,
            ingestion_ts=ingestion_ts,
            source_url=mix_url,
            zone=settings.zone,
            start=start,
            end=end,
        )
        _write_bronze_delta(
            raw_flows,
            flows_table,
            so,
            process_ts=process_ts,
            ingestion_ts=ingestion_ts,
            source_url=flows_url,
            zone=settings.zone,
            start=start,
            end=end,
        )
        logger.info(f"bronze_written_to_delta: mix_table={mix_table}, flows_table={flows_table}")

        # Step 6: Mark ready
        total_records = mix_count + flows_count
        state.mark_ready("bronze", process_ts, total_records)
        logger.info(f"bronze_completed: total_records={total_records}")

        return {
            "process_ts": process_ts,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "mix_records": mix_count,
            "flows_records": flows_count,
            "mix_table": mix_table,
            "flows_table": flows_table,
        }

    except Exception as e:
        logger.error(f"bronze_failed: error={e}, process_ts={process_ts}")
        state.mark_error("bronze", process_ts, str(e))
        raise
