"""Silver Layer — Clean, structured Delta tables.

Reads Bronze Parquet files from S3, flattens nested JSON structures,
enforces schema/types, deduplicates, validates with Pandera, and writes
Delta Lake tables partitioned by ``year/month/day`` (data timestamp).

Bad records are routed to separate bad-data Delta tables.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime

import pandera.polars as pa
import polars as pl
from deltalake import write_deltalake

from electricity_maps.config import Settings, get_settings
from electricity_maps.schemas.silver_schemas import (
    BAD_DATA_SCHEMA,
    FLOWS_SCHEMA,
    MIX_SCHEMA,
    SilverFlowsSchema,
    SilverMixSchema,
)
from electricity_maps.utils.helpers import find_bronze_files, get_s3fs, read_bronze_parquet
from electricity_maps.utils.state import PipelineState

logger = logging.getLogger(__name__)


# ================================================================
#  Data processing helpers
# ================================================================

def _empty_bad_data() -> pl.DataFrame:
    return pl.DataFrame(schema=BAD_DATA_SCHEMA)


def _concat_or_empty(frames: list[pl.DataFrame], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    non_empty = [df for df in frames if not df.is_empty()]
    return pl.concat(non_empty) if non_empty else pl.DataFrame(schema=schema)


def _bad_row(
    *,
    process_ts: int,
    row: object,
    error_message: str,
    row_datetime: object = "unknown",
) -> dict:
    return {
        "process_ts": process_ts,
        "datetime": str(row_datetime or "unknown"),
        "raw_json": row if isinstance(row, str) else json.dumps(row, default=str),
        "error_message": error_message,
        "created_at": datetime.now(UTC),
    }


def _validate_rows(
    df: pl.DataFrame,
    schema_model: type[pa.DataFrameModel],
    process_ts: int,
    empty_schema: dict[str, pl.DataType],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Validate with Pandera and route row-level schema failures to bad data."""
    if df.is_empty():
        return pl.DataFrame(schema=empty_schema), _empty_bad_data()

    try:
        return schema_model.validate(df), _empty_bad_data()
    except Exception:
        valid_rows: list[pl.DataFrame] = []
        bad_rows: list[dict] = []

        for row in df.iter_rows(named=True):
            row_df = pl.DataFrame([row], schema=df.schema, strict=False)
            try:
                valid_rows.append(schema_model.validate(row_df))
            except Exception as row_error:
                bad_rows.append(
                    _bad_row(
                        process_ts=int(row.get("process_ts") or process_ts),
                        row=row,
                        row_datetime=row.get("datetime", "unknown"),
                        error_message=str(row_error),
                    )
                )

        good_df = _concat_or_empty(valid_rows, empty_schema)
        bad_df = (
            pl.DataFrame(bad_rows, schema=BAD_DATA_SCHEMA, strict=False)
            if bad_rows
            else _empty_bad_data()
        )
        return good_df, bad_df


# ================================================================
#  Mix flattening
# ================================================================

def _flatten_mix(raw_json: str, zone: str, process_ts: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Flatten electricity mix JSON into a tabular DataFrame.

    Returns:
        (good_df, bad_df) — good records and any that failed to parse.
    """
    try:
        data = json.loads(raw_json)
        records = data.get("data", [])
    except Exception as e:
        return (
            pl.DataFrame(schema=MIX_SCHEMA),
            pl.DataFrame(
                [_bad_row(process_ts=process_ts, row=raw_json, error_message=str(e))],
                schema=BAD_DATA_SCHEMA,
                strict=False,
            ),
        )

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
                "process_ts": process_ts,
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
            bad_rows.append(
                _bad_row(
                    process_ts=process_ts,
                    row=rec,
                    row_datetime=rec.get("datetime", "unknown") if isinstance(rec, dict) else "unknown",
                    error_message=str(e),
                )
            )

    # Build DataFrames
    good_df = pl.DataFrame(good_rows, schema=MIX_SCHEMA, strict=False) if good_rows else pl.DataFrame(schema=MIX_SCHEMA)
    bad_df = pl.DataFrame(bad_rows, schema=BAD_DATA_SCHEMA, strict=False) if bad_rows else _empty_bad_data()

    return good_df, bad_df


# ------------------------------------------------------------------ #
#  Flows flattening (unpivot)                                         #
# ------------------------------------------------------------------ #

def _flatten_flows(raw_json: str, zone: str, process_ts: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Unpivot electricity flows JSON into rows with neighbor_zone + direction.

    Returns:
        (good_df, bad_df)
    """
    try:
        data = json.loads(raw_json)
        records = data.get("data", [])
    except Exception as e:
        return (
            pl.DataFrame(schema=FLOWS_SCHEMA),
            pl.DataFrame(
                [_bad_row(process_ts=process_ts, row=raw_json, error_message=str(e))],
                schema=BAD_DATA_SCHEMA,
                strict=False,
            ),
        )

    good_rows = []
    bad_rows = []

    for rec in records:
        try:
            dt = datetime.fromisoformat(rec["datetime"])
            updated_at = datetime.fromisoformat(rec["updatedAt"])

            # Unpivot imports
            for neighbor, mw in (rec.get("import") or {}).items():
                good_rows.append({
                    "process_ts": process_ts,
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
                    "process_ts": process_ts,
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
            bad_rows.append(
                _bad_row(
                    process_ts=process_ts,
                    row=rec,
                    row_datetime=rec.get("datetime", "unknown") if isinstance(rec, dict) else "unknown",
                    error_message=str(e),
                )
            )

    good_df = pl.DataFrame(good_rows, schema=FLOWS_SCHEMA, strict=False) if good_rows else pl.DataFrame(schema=FLOWS_SCHEMA)
    bad_df = pl.DataFrame(bad_rows, schema=BAD_DATA_SCHEMA, strict=False) if bad_rows else _empty_bad_data()

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
    fs = get_s3fs(settings)

    # Step 1: Pick up ready bronze batches
    pending_ts = state.pickup_ready("bronze")
    if not pending_ts:
        logger.info("silver_no_pending: No bronze batches ready")
        return {"status": "no_pending", "mix_records": 0, "flows_records": 0}

    logger.info(f"silver_started: process_ts={process_ts}, bronze_batches={len(pending_ts)}")
    state.init_layer("silver", process_ts)

    try:
        all_mix_good = []
        all_mix_bad = []
        all_flows_good = []
        all_flows_bad = []

        # Step 2-5: Process each bronze batch
        for bts in pending_ts:
            # Find and read mix files
            mix_files = find_bronze_files(
                settings.bronze_dir, "electricity_mix", bts, settings.zone, fs,
            )
            for s3_key in mix_files:
                raw_json = read_bronze_parquet(s3_key, fs)
                good, bad = _flatten_mix(raw_json, settings.zone, process_ts)
                all_mix_good.append(good)
                all_mix_bad.append(bad)

            # Find and read flows files
            flows_files = find_bronze_files(
                settings.bronze_dir, "electricity_flows", bts, settings.zone, fs,
            )
            for s3_key in flows_files:
                raw_json = read_bronze_parquet(s3_key, fs)
                good, bad = _flatten_flows(raw_json, settings.zone, process_ts)
                all_flows_good.append(good)
                all_flows_bad.append(bad)

        # Concatenate all batches
        mix_df = _concat_or_empty(all_mix_good, MIX_SCHEMA)
        mix_bad = _concat_or_empty(all_mix_bad, BAD_DATA_SCHEMA)
        flows_df = _concat_or_empty(all_flows_good, FLOWS_SCHEMA)
        flows_bad = _concat_or_empty(all_flows_bad, BAD_DATA_SCHEMA)

        # Step 6: Deduplicate
        mix_df = _dedup_mix(mix_df)
        flows_df = _dedup_flows(flows_df)

        # Step 7: Enforce schema and route validation failures to bad data
        mix_df, mix_schema_bad = _validate_rows(mix_df, SilverMixSchema, process_ts, MIX_SCHEMA)
        flows_df, flows_schema_bad = _validate_rows(flows_df, SilverFlowsSchema, process_ts, FLOWS_SCHEMA)
        mix_bad = _concat_or_empty([mix_bad, mix_schema_bad], BAD_DATA_SCHEMA)
        flows_bad = _concat_or_empty([flows_bad, flows_schema_bad], BAD_DATA_SCHEMA)

        logger.info(
            f"silver_transformed: mix_good={len(mix_df)}, mix_bad={len(mix_bad)}, "
            f"flows_good={len(flows_df)}, flows_bad={len(flows_bad)}"
        )

        # Step 8: Write Delta Lake tables
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

        # Step 9: Update state
        total = len(mix_df) + len(flows_df)
        state.mark_complete("bronze", pending_ts)
        state.mark_ready("silver", process_ts, total)

        logger.info(
            f"silver_completed: mix_records={len(mix_df)}, flows_records={len(flows_df)}, "
            f"bad_mix={len(mix_bad)}, bad_flows={len(flows_bad)}"
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
        logger.error(f"silver_failed: error={e}, process_ts={process_ts}")
        state.mark_error("silver", process_ts, str(e))
        raise
