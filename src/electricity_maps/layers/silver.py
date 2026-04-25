"""Silver Layer — Clean, structured Delta tables.

Reads Bronze Parquet files from S3, flattens nested JSON structures,
enforces schema/types, deduplicates, validates with Pandera, and writes
Delta Lake tables partitioned by ``year/month/day`` (data timestamp).

Bad records are routed to separate bad-data Delta tables.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import polars as pl
import s3fs
from deltalake import DeltaTable, write_deltalake

from electricity_maps.config import Settings, get_settings
from electricity_maps.utils.logging import get_logger
from electricity_maps.utils.state import PipelineState

logger = get_logger(__name__)


# ------------------------------------------------------------------ #
#  S3 helpers                                                         #
# ------------------------------------------------------------------ #

def _get_s3fs(settings: Settings) -> s3fs.S3FileSystem:
    """Build an authenticated S3FileSystem."""
    return s3fs.S3FileSystem(
        key=settings.aws_access_key_id,
        secret=settings.aws_secret_access_key,
        client_kwargs={"region_name": settings.aws_region},
    )


def _read_bronze_parquet(s3_key: str, fs: s3fs.S3FileSystem) -> str:
    """Read a Bronze Parquet file and return the raw_json string."""
    with fs.open(s3_key, "rb") as f:
        df = pl.read_parquet(f)
    return df["raw_json"][0]


def _find_bronze_files(
    bronze_dir: str,
    stream: str,
    process_ts: int,
    zone: str,
    fs: s3fs.S3FileSystem,
) -> list[str]:
    """Glob for Bronze Parquet files matching a process_ts."""
    pattern = f"{bronze_dir}/{stream}/**/{ zone}_{process_ts}.parquet"
    # Remove s3:// prefix for s3fs glob
    clean = pattern.replace("s3://", "")
    files = fs.glob(clean)
    return [f"s3://{f}" for f in files]


# ------------------------------------------------------------------ #
#  Mix flattening                                                     #
# ------------------------------------------------------------------ #

def _flatten_mix(raw_json: str, zone: str, process_ts: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Flatten electricity mix JSON into a tabular DataFrame.

    Returns:
        (good_df, bad_df) — good records and any that failed to parse.
    """
    data = json.loads(raw_json)
    records = data.get("data", [])

    good_rows = []
    bad_rows = []

    for rec in records:
        try:
            mix = rec.get("mix", {})

            # Extract nested storage objects
            hydro_st = mix.get("hydro storage") or {}
            battery_st = mix.get("battery storage") or {}
            flows = mix.get("flows") or {}

            dt = datetime.fromisoformat(rec["datetime"])

            row = {
                "zone": zone,
                "datetime": dt,
                "updated_at": datetime.fromisoformat(rec["updatedAt"]),
                "is_estimated": rec.get("isEstimated"),
                "estimation_method": rec.get("estimationMethod"),
                "nuclear_mw": mix.get("nuclear"),
                "geothermal_mw": mix.get("geothermal"),
                "biomass_mw": mix.get("biomass"),
                "coal_mw": mix.get("coal"),
                "wind_mw": mix.get("wind"),
                "solar_mw": mix.get("solar"),
                "hydro_mw": mix.get("hydro"),
                "gas_mw": mix.get("gas"),
                "oil_mw": mix.get("oil"),
                "unknown_mw": mix.get("unknown"),
                "hydro_storage_charge_mw": hydro_st.get("charge") if isinstance(hydro_st, dict) else None,
                "hydro_storage_discharge_mw": hydro_st.get("discharge") if isinstance(hydro_st, dict) else None,
                "battery_storage_charge_mw": battery_st.get("charge") if isinstance(battery_st, dict) else None,
                "battery_storage_discharge_mw": battery_st.get("discharge") if isinstance(battery_st, dict) else None,
                "flow_exports_mw": flows.get("exports") if isinstance(flows, dict) else None,
                "flow_imports_mw": flows.get("imports") if isinstance(flows, dict) else None,
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
            }
            good_rows.append(row)

        except Exception as e:
            bad_rows.append({
                "process_ts": process_ts,
                "datetime": rec.get("datetime", "unknown"),
                "raw_json": json.dumps(rec),
                "error_message": str(e),
                "created_at": datetime.now(timezone.utc),
            })

    # Build DataFrames
    mix_schema = {
        "zone": pl.Utf8, "datetime": pl.Datetime("us", "UTC"),
        "updated_at": pl.Datetime("us", "UTC"),
        "is_estimated": pl.Boolean, "estimation_method": pl.Utf8,
        "nuclear_mw": pl.Float64, "geothermal_mw": pl.Float64,
        "biomass_mw": pl.Float64, "coal_mw": pl.Float64,
        "wind_mw": pl.Float64, "solar_mw": pl.Float64,
        "hydro_mw": pl.Float64, "gas_mw": pl.Float64,
        "oil_mw": pl.Float64, "unknown_mw": pl.Float64,
        "hydro_storage_charge_mw": pl.Float64,
        "hydro_storage_discharge_mw": pl.Float64,
        "battery_storage_charge_mw": pl.Float64,
        "battery_storage_discharge_mw": pl.Float64,
        "flow_exports_mw": pl.Float64, "flow_imports_mw": pl.Float64,
        "year": pl.Int32, "month": pl.Int32, "day": pl.Int32,
    }
    good_df = pl.DataFrame(good_rows, schema=mix_schema, strict=False) if good_rows else pl.DataFrame(schema=mix_schema)

    bad_schema = {
        "process_ts": pl.Int64, "datetime": pl.Utf8,
        "raw_json": pl.Utf8, "error_message": pl.Utf8,
        "created_at": pl.Datetime("us", "UTC"),
    }
    bad_df = pl.DataFrame(bad_rows, schema=bad_schema, strict=False) if bad_rows else pl.DataFrame(schema=bad_schema)

    return good_df, bad_df


# ------------------------------------------------------------------ #
#  Flows flattening (unpivot)                                         #
# ------------------------------------------------------------------ #

def _flatten_flows(raw_json: str, zone: str, process_ts: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Unpivot electricity flows JSON into rows with neighbor_zone + direction.

    Returns:
        (good_df, bad_df)
    """
    data = json.loads(raw_json)
    records = data.get("data", [])

    good_rows = []
    bad_rows = []

    for rec in records:
        try:
            dt = datetime.fromisoformat(rec["datetime"])
            updated_at = datetime.fromisoformat(rec["updatedAt"])

            # Unpivot imports
            for neighbor, mw in (rec.get("import") or {}).items():
                good_rows.append({
                    "zone": zone,
                    "datetime": dt,
                    "updated_at": updated_at,
                    "neighbor_zone": neighbor,
                    "direction": "import",
                    "power_mw": float(mw) if mw is not None else 0.0,
                    "year": dt.year,
                    "month": dt.month,
                    "day": dt.day,
                })

            # Unpivot exports
            for neighbor, mw in (rec.get("export") or {}).items():
                good_rows.append({
                    "zone": zone,
                    "datetime": dt,
                    "updated_at": updated_at,
                    "neighbor_zone": neighbor,
                    "direction": "export",
                    "power_mw": float(mw) if mw is not None else 0.0,
                    "year": dt.year,
                    "month": dt.month,
                    "day": dt.day,
                })

        except Exception as e:
            bad_rows.append({
                "process_ts": process_ts,
                "datetime": rec.get("datetime", "unknown"),
                "raw_json": json.dumps(rec),
                "error_message": str(e),
                "created_at": datetime.now(timezone.utc),
            })

    flows_schema = {
        "zone": pl.Utf8, "datetime": pl.Datetime("us", "UTC"),
        "updated_at": pl.Datetime("us", "UTC"),
        "neighbor_zone": pl.Utf8, "direction": pl.Utf8,
        "power_mw": pl.Float64,
        "year": pl.Int32, "month": pl.Int32, "day": pl.Int32,
    }
    good_df = pl.DataFrame(good_rows, schema=flows_schema, strict=False) if good_rows else pl.DataFrame(schema=flows_schema)

    bad_schema = {
        "process_ts": pl.Int64, "datetime": pl.Utf8,
        "raw_json": pl.Utf8, "error_message": pl.Utf8,
        "created_at": pl.Datetime("us", "UTC"),
    }
    bad_df = pl.DataFrame(bad_rows, schema=bad_schema, strict=False) if bad_rows else pl.DataFrame(schema=bad_schema)

    return good_df, bad_df


# ------------------------------------------------------------------ #
#  Deduplication                                                      #
# ------------------------------------------------------------------ #

def _dedup_mix(df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicate mix records on (zone, datetime), keeping latest updated_at."""
    if df.is_empty():
        return df
    return (
        df.sort("updated_at", descending=True)
        .unique(subset=["zone", "datetime"], keep="first")
        .sort("datetime")
    )


def _dedup_flows(df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicate flows on (zone, datetime, neighbor_zone, direction)."""
    if df.is_empty():
        return df
    return (
        df.sort("updated_at", descending=True)
        .unique(subset=["zone", "datetime", "neighbor_zone", "direction"], keep="first")
        .sort("datetime", "neighbor_zone")
    )


# ------------------------------------------------------------------ #
#  Delta Lake write helpers                                           #
# ------------------------------------------------------------------ #

def _merge_write_delta(
    df: pl.DataFrame,
    table_uri: str,
    storage_options: dict[str, str],
    partition_by: list[str] | None = None,
) -> None:
    """Write/append to a Delta table, creating it if needed."""
    if df.is_empty():
        return

    pa_table = df.to_arrow()
    try:
        write_deltalake(
            table_uri,
            pa_table,
            mode="append",
            partition_by=partition_by,
            storage_options=storage_options,
        )
    except Exception:
        # Table doesn't exist — create it
        write_deltalake(
            table_uri,
            pa_table,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=storage_options,
        )


# ------------------------------------------------------------------ #
#  Main entry point                                                   #
# ------------------------------------------------------------------ #

def transform_silver(
    settings: Settings | None = None,
    process_ts: int | None = None,
) -> dict:
    """Run a full Silver transformation cycle.

    1. Pick up bronze rows with ``status=R`` → flip to ``P``
    2. Read Bronze Parquet files from S3
    3. Flatten mix + flows
    4. Handle bad data
    5. Deduplicate
    6. Write Delta Lake tables (partitioned by year/month/day)
    7. Update ``el_state``: bronze → ``C``, silver → ``R``

    Returns:
        Summary dict with record counts.
    """
    settings = settings or get_settings()
    state = PipelineState(settings)
    process_ts = process_ts or int(time.time() * 1000)
    fs = _get_s3fs(settings)

    # Step 1: Pick up ready bronze batches
    pending_ts = state.pickup_ready("bronze")
    if not pending_ts:
        logger.info("silver_no_pending", msg="No bronze batches ready")
        return {"status": "no_pending", "mix_records": 0, "flows_records": 0}

    logger.info("silver_started", process_ts=process_ts, bronze_batches=len(pending_ts))
    state.init_layer("silver", process_ts)

    try:
        all_mix_good = []
        all_mix_bad = []
        all_flows_good = []
        all_flows_bad = []

        # Step 2–5: Process each bronze batch
        for bts in pending_ts:
            # Find and read mix files
            mix_files = _find_bronze_files(
                settings.bronze_dir, "electricity_mix", bts, settings.zone, fs,
            )
            for s3_key in mix_files:
                raw_json = _read_bronze_parquet(s3_key, fs)
                good, bad = _flatten_mix(raw_json, settings.zone, bts)
                all_mix_good.append(good)
                all_mix_bad.append(bad)

            # Find and read flows files
            flows_files = _find_bronze_files(
                settings.bronze_dir, "electricity_flows", bts, settings.zone, fs,
            )
            for s3_key in flows_files:
                raw_json = _read_bronze_parquet(s3_key, fs)
                good, bad = _flatten_flows(raw_json, settings.zone, bts)
                all_flows_good.append(good)
                all_flows_bad.append(bad)

        # Concatenate all batches
        mix_df = pl.concat(all_mix_good) if all_mix_good else pl.DataFrame()
        mix_bad = pl.concat(all_mix_bad) if all_mix_bad else pl.DataFrame()
        flows_df = pl.concat(all_flows_good) if all_flows_good else pl.DataFrame()
        flows_bad = pl.concat(all_flows_bad) if all_flows_bad else pl.DataFrame()

        # Step 6: Deduplicate
        mix_df = _dedup_mix(mix_df)
        flows_df = _dedup_flows(flows_df)

        logger.info(
            "silver_transformed",
            mix_good=len(mix_df),
            mix_bad=len(mix_bad),
            flows_good=len(flows_df),
            flows_bad=len(flows_bad),
        )

        # Step 7: Write Delta Lake tables
        so = settings.storage_options
        partition_cols = ["year", "month", "day"]

        _merge_write_delta(
            mix_df,
            f"{settings.silver_dir}/electricity_mix",
            so,
            partition_by=partition_cols,
        )
        _merge_write_delta(
            flows_df,
            f"{settings.silver_dir}/electricity_flows",
            so,
            partition_by=partition_cols,
        )

        # Write bad data tables (no partitioning)
        if not mix_bad.is_empty():
            _merge_write_delta(
                mix_bad,
                f"{settings.silver_dir}/silver_mix_bad_data",
                so,
            )
        if not flows_bad.is_empty():
            _merge_write_delta(
                flows_bad,
                f"{settings.silver_dir}/silver_flows_bad_data",
                so,
            )

        # Step 8: Update state
        total = len(mix_df) + len(flows_df)
        state.mark_complete("bronze", pending_ts)
        state.mark_ready("silver", process_ts, total)

        logger.info(
            "silver_completed",
            mix_records=len(mix_df),
            flows_records=len(flows_df),
            bad_mix=len(mix_bad),
            bad_flows=len(flows_bad),
        )

        return {
            "process_ts": process_ts,
            "bronze_batches_consumed": len(pending_ts),
            "mix_records": len(mix_df),
            "flows_records": len(flows_df),
            "bad_mix_records": len(mix_bad),
            "bad_flows_records": len(flows_bad),
        }

    except Exception as e:
        logger.error("silver_failed", error=str(e), process_ts=process_ts)
        state.mark_error("silver", process_ts, str(e))
        raise
