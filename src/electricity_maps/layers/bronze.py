"""Bronze Layer — Raw immutable data ingestion.

Fetches electricity mix and flows from the Electricity Maps API and
stores them as Parquet files on S3 with minimal metadata.

Partitioning: ``year=YYYY/month=MM/day=DD`` using ingestion timestamp.
Catch-up: automatically ingests all missing data since the last run.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from electricity_maps.api.client import ElectricityMapsClient
from electricity_maps.config import Settings, get_settings
from electricity_maps.utils.helpers import floor_to_hour, get_s3fs
from electricity_maps.utils.logging import get_logger
from electricity_maps.utils.partitioning import build_bronze_key
from electricity_maps.utils.state import PipelineState

logger = get_logger(__name__)


def _calculate_time_range(
    state: PipelineState,
    settings: Settings,
) -> tuple[datetime, datetime]:
    """Determine the ingestion time range for this run.

    - ``start`` = last bronze ``end_timestamp`` (floored to hour),
      or 24h ago if no prior run.
    - ``end`` = current hour start.

    This ensures automatic catch-up if the pipeline was down.
    """
    last_end = state.get_last_end_timestamp("bronze")

    if last_end is not None:
        start = floor_to_hour(last_end)
    else:
        start = floor_to_hour(datetime.now(timezone.utc) - timedelta(hours=24))
        logger.info("no_prior_run", defaulting_start_to=str(start))

    end = floor_to_hour(datetime.now(timezone.utc))

    # Ensure we have at least 1 hour to fetch
    if start >= end:
        start = end - timedelta(hours=1)

    return start, end


def _write_parquet_to_s3(
    data: dict,
    s3_key: str,
    fs: s3fs.S3FileSystem,
    *,
    process_ts: int,
    ingestion_ts: datetime,
    source_url: str,
    zone: str,
    start: datetime,
    end: datetime,
) -> None:
    """Serialize one raw API response plus metadata columns to S3."""
    json_bytes = json.dumps(data, default=str).encode("utf-8")
    table = pa.table({
        "process_ts": [process_ts],
        "ingestion_timestamp": [ingestion_ts],
        "source_url": [source_url],
        "zone": [zone],
        "range_start": [start],
        "range_end": [end],
        "raw_json": [json_bytes.decode("utf-8")],
        "record_count": [len(data.get("data", []))],
    })

    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    with fs.open(s3_key, "wb") as f:
        f.write(buf.read())


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
    ingestion_ts = datetime.now(timezone.utc)

    # Step 1: Init state
    state.init_layer("bronze", process_ts, ingestion_ts)
    logger.info("bronze_started", process_ts=process_ts)

    try:
        # Step 2: Calculate time range
        start, end = _calculate_time_range(state, settings)
        logger.info("bronze_time_range", start=str(start), end=str(end))

        # Step 3: Fetch from API
        with ElectricityMapsClient(settings) as client:
            raw_mix = client.get_raw_mix_range(settings.zone, start, end)
            raw_flows = client.get_raw_flows_range(settings.zone, start, end)

        mix_count = len(raw_mix.get("data", []))
        flows_count = len(raw_flows.get("data", []))
        logger.info(
            "bronze_api_fetched",
            mix_records=mix_count,
            flows_records=flows_count,
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

        # Step 5: Write to S3
        fs = get_s3fs(settings)
        mix_key = build_bronze_key(
            settings.bronze_dir, "electricity_mix",
            settings.zone, process_ts, ingestion_ts,
        )
        flows_key = build_bronze_key(
            settings.bronze_dir, "electricity_flows",
            settings.zone, process_ts, ingestion_ts,
        )

        _write_parquet_to_s3(
            raw_mix,
            mix_key,
            fs,
            process_ts=process_ts,
            ingestion_ts=ingestion_ts,
            source_url=mix_url,
            zone=settings.zone,
            start=start,
            end=end,
        )
        _write_parquet_to_s3(
            raw_flows,
            flows_key,
            fs,
            process_ts=process_ts,
            ingestion_ts=ingestion_ts,
            source_url=flows_url,
            zone=settings.zone,
            start=start,
            end=end,
        )
        logger.info("bronze_written_to_s3", mix_key=mix_key, flows_key=flows_key)

        # Step 6: Mark ready
        total_records = mix_count + flows_count
        state.mark_ready("bronze", process_ts, total_records)
        logger.info("bronze_completed", total_records=total_records)

        return {
            "process_ts": process_ts,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "mix_records": mix_count,
            "flows_records": flows_count,
            "mix_key": mix_key,
            "flows_key": flows_key,
        }

    except Exception as e:
        logger.error("bronze_failed", error=str(e), process_ts=process_ts)
        state.mark_error("bronze", process_ts, str(e))
        raise
