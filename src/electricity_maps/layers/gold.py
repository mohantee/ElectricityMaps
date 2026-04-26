"""Gold Layer — Business-Ready Data Products.

Reads Silver Delta tables, performs aggregations (converting MW to MWh),
calculates percentages, and splits flows into imports and exports.
Results are written as partitioned Delta Lake tables.
"""

from __future__ import annotations

import logging
import time

import polars as pl
from deltalake import DeltaTable, write_deltalake

from electricity_maps.config import Settings, get_settings
from electricity_maps.schemas.gold_schemas import (
    GOLD_EXPORTS_SCHEMA,
    GOLD_IMPORTS_SCHEMA,
    GOLD_MIX_SCHEMA,
    GoldExportsSchema,
    GoldImportsSchema,
    GoldMixSchema,
)
from electricity_maps.utils.helpers import filter_by_process_ts, get_zone_name
from electricity_maps.utils.state import PipelineState

logger = logging.getLogger(__name__)


# ================================================================
#  Daily Relative Mix
# ================================================================

def _aggregate_mix(silver_mix: pl.DataFrame, process_ts: int) -> pl.DataFrame:
    """Aggregate hourly MW to daily MWh and percentages."""
    if silver_mix.is_empty():
        return pl.DataFrame(schema=GOLD_MIX_SCHEMA)

    # With hourly samples, summing MW across hours gives daily energy in MWh.
    # Create calendar date used as the daily aggregation grain.
    df = silver_mix.with_columns(
        pl.col("datetime").dt.date().alias("date")
    )

    # Source-level generation inputs from Silver.
    prod_cols = [
        "nuclear_mw", "geothermal_mw", "biomass_mw", "coal_mw",
        "wind_mw", "solar_mw", "hydro_mw", "gas_mw", "oil_mw", "unknown_mw"
    ]

    # Aggregate to daily source energy totals (MWh) per zone.
    agg_exprs = [
        pl.col(c).fill_null(0.0).sum().alias(c.replace("_mw", "_mwh")) for c in prod_cols
    ]
    agg_exprs.append(pl.col("datetime").n_unique().cast(pl.Int32).alias("hours_covered"))
    daily = df.group_by(["zone", "date", "year", "month"]).agg(agg_exprs)

    # Build denominator for relative mix (% contribution by source).
    mwh_cols = [c.replace("_mw", "_mwh") for c in prod_cols]
    daily = daily.with_columns(
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in mwh_cols]).alias("total_production_mwh")
    )

    # Relative mix formula: source_pct = source_mwh / total_mwh * 100.
    for c in mwh_cols:
        pct_col = c.replace("_mwh", "_pct")
        daily = daily.with_columns(
            pl.when(pl.col("total_production_mwh") > 0)
            .then(pl.col(c) / pl.col("total_production_mwh") * 100)
            .otherwise(0.0)
            .fill_nan(0.0)
            .alias(pct_col)
        )

    # Derived portfolio-level shares computed as sum of source percentages.
    # Note: column names use `_avg_pct`, but values are summed share contributions.
    fossil_free_cols = ["nuclear_pct", "geothermal_pct", "biomass_pct", "wind_pct", "solar_pct", "hydro_pct"]
    renewable_cols = ["geothermal_pct", "biomass_pct", "wind_pct", "solar_pct", "hydro_pct"]

    daily = daily.with_columns(
        # `process_ts` is the transformation reference timestamp for this Gold batch.
        pl.lit(process_ts).cast(pl.Int64).alias("process_ts"),
        pl.col("zone").map_elements(get_zone_name, return_dtype=pl.Utf8).alias("zone_name"),
        pl.col("date").dt.day().alias("day"),
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in fossil_free_cols]).alias("fossil_free_avg_pct"),
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in renewable_cols]).alias("renewable_avg_pct"),
    )

    select_cols = [
        "process_ts", "zone", "zone_name", "date", "nuclear_pct", "biomass_pct", "wind_pct",
        "solar_pct", "hydro_pct", "gas_pct", "oil_pct", "coal_pct", "geothermal_pct",
        "unknown_pct", "total_production_mwh", "fossil_free_avg_pct", "renewable_avg_pct",
        "hours_covered", "year", "month", "day"
    ]

    return daily.select(select_cols)


# ------------------------------------------------------------------ #
#  Daily Net Flows                                                #
# ------------------------------------------------------------------ #

def _aggregate_flows(silver_flows: pl.DataFrame, process_ts: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Aggregate hourly MW to daily Net MWh and split into imports and exports."""
    if silver_flows.is_empty():
        return pl.DataFrame(schema=GOLD_IMPORTS_SCHEMA), pl.DataFrame(schema=GOLD_EXPORTS_SCHEMA)

    df = silver_flows.with_columns(
        pl.col("datetime").dt.date().alias("date")
    )

    # Sum hourly power (MW) by direction to obtain daily gross energy (MWh).
    agg_df = df.group_by(["zone", "neighbor_zone", "date", "year", "month"]).agg([
        pl.col("power_mw").filter(pl.col("direction") == "import").sum().alias("gross_import_mwh"),
        pl.col("power_mw").filter(pl.col("direction") == "export").sum().alias("gross_export_mwh"),
        pl.col("datetime").n_unique().cast(pl.Int32).alias("hours_covered"),
    ])

    agg_df = agg_df.with_columns([
        pl.col("gross_import_mwh").fill_null(0.0),
        pl.col("gross_export_mwh").fill_null(0.0),
        pl.lit(process_ts).cast(pl.Int64).alias("process_ts"),
        pl.col("zone").map_elements(get_zone_name, return_dtype=pl.Utf8).alias("zone_name"),
        pl.col("neighbor_zone").map_elements(get_zone_name, return_dtype=pl.Utf8).alias("neighbor_zone_name"),
        pl.col("date").dt.day().alias("day"),
    ])

    # Compute net daily flow: positive indicates net import, negative indicates net export.
    agg_df = agg_df.with_columns(
        (pl.col("gross_import_mwh") - pl.col("gross_export_mwh")).alias("net_flow")
    )

    # Isolate net imports: label neighbor as source_zone and keep positive flow.
    imports_agg = agg_df.filter(pl.col("net_flow") > 0).with_columns([
        pl.col("net_flow").alias("import_mwh")
    ]).rename({"neighbor_zone": "source_zone", "neighbor_zone_name": "source_zone_name"}).select([
        "process_ts", "zone", "zone_name", "source_zone", "source_zone_name",
        "date", "import_mwh", "hours_covered", "year", "month", "day"
    ])

    # Isolate net exports: label neighbor as destination_zone and flip sign to positive.
    exports_agg = agg_df.filter(pl.col("net_flow") < 0).with_columns([
        (pl.col("net_flow") * -1.0).alias("export_mwh")
    ]).rename({"neighbor_zone": "destination_zone", "neighbor_zone_name": "destination_zone_name"}).select([
        "process_ts", "zone", "zone_name", "destination_zone", "destination_zone_name",
        "date", "export_mwh", "hours_covered", "year", "month", "day"
    ])

    return imports_agg, exports_agg


# ================================================================
#  Incremental helpers
# ================================================================

def _affected_days(*frames: pl.DataFrame) -> list[tuple[int, int, int]]:
    """Identify all (year, month, day) tuples present in a set of DataFrames."""
    days: set[tuple[int, int, int]] = set()
    for df in frames:
        if df.is_empty() or not {"year", "month", "day"}.issubset(set(df.columns)):
            continue
        for row in df.select(["year", "month", "day"]).unique().iter_rows(named=True):
            days.add((int(row["year"]), int(row["month"]), int(row["day"])))
    return sorted(days)


def _filter_to_days(df: pl.DataFrame, days: list[tuple[int, int, int]]) -> pl.DataFrame:
    """Filter DataFrame to only rows with specified (year, month, day) combinations."""
    if not days:
        return df.head(0)

    predicate: pl.Expr | None = None
    for year, month, day in days:
        day_expr = (pl.col("year") == year) & (pl.col("month") == month) & (pl.col("day") == day)
        predicate = day_expr if predicate is None else predicate | day_expr

    if predicate is None:
        return df.head(0)
    return df.filter(predicate)


def _delta_exists(table_uri: str, storage_options: dict[str, str]) -> bool:
    """Check if a Delta table exists."""
    try:
        DeltaTable(table_uri, storage_options=storage_options)
        return True
    except Exception:
        return False


def _read_delta_columns(
    table_uri: str,
    storage_options: dict[str, str],
    columns: list[str],
) -> pl.DataFrame:
    """Read selected columns from a Delta table."""
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = pl.from_arrow(dt.to_pyarrow_table(columns=columns))
        if isinstance(df, pl.Series):
            return df.to_frame()
        return df
    except Exception:
        return pl.DataFrame()


def _read_delta_for_days(
    table_uri: str,
    storage_options: dict[str, str],
    days: list[tuple[int, int, int]],
) -> pl.DataFrame:
    """Read only requested (year, month, day) partitions from Delta table."""
    if not days:
        return pl.DataFrame()
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for year, month, day in days:
        part_rows = dt.to_pyarrow_table(
            partitions=[
                ("year", "=", year),
                ("month", "=", month),
                ("day", "=", day),
            ]
        )
        part_df = pl.from_arrow(part_rows)
        if isinstance(part_df, pl.Series):
            part_df = part_df.to_frame()
        if not part_df.is_empty():
            frames.append(part_df)

    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _assert_unique_keys(df: pl.DataFrame, keys: list[str], label: str) -> None:
    """Fail fast if duplicate business keys are present."""
    if df.is_empty():
        return
    dupes = (
        df.group_by(keys)
        .len()
        .filter(pl.col("len") > 1)
    )
    if not dupes.is_empty():
        raise ValueError(f"{label} contains duplicate keys for {keys}")


def _overwrite_affected_days(
    df: pl.DataFrame,
    table_uri: str,
    storage_options: dict[str, str],
    days: list[tuple[int, int, int]],
    key_cols: list[str],
    partition_by: list[str] | None = None,
) -> None:
    """Overwrite only affected days by read-filter-union-overwrite."""
    if not days:
        return

    if not df.is_empty():
        _assert_unique_keys(df, key_cols, label="incoming_df")

    day_filter = pl.DataFrame(
        [{"year": y, "month": m, "day": d} for y, m, d in days],
        schema={"year": pl.Int32, "month": pl.Int32, "day": pl.Int32},
    )

    if _delta_exists(table_uri, storage_options):
        existing = pl.from_arrow(DeltaTable(table_uri, storage_options=storage_options).to_pyarrow_table())
        if isinstance(existing, pl.Series):
            existing = existing.to_frame()
        remaining = existing.join(day_filter, on=["year", "month", "day"], how="anti")
        merged = pl.concat([remaining, df], how="diagonal_relaxed") if not df.is_empty() else remaining
    else:
        merged = df

    if merged.is_empty():
        return
    _assert_unique_keys(merged, key_cols, label="final_df")
    write_deltalake(
        table_uri,
        merged.to_arrow(),
        mode="overwrite",
        partition_by=partition_by,
        storage_options=storage_options,
    )


# ================================================================
#  Main entry point
# ================================================================

def transform_gold(
    settings: Settings | None = None,
    process_ts: int | None = None,
) -> dict:
    """Run a full Gold transformation cycle.

    1. Pick up silver rows with ``status=R`` → flip to ``P``
    2. Read Silver Delta tables
    3. Aggregate daily statistics (mix, imports, exports)
    4. Write Delta Lake tables (partitioned by year/month)
    5. Update ``el_state``: silver → ``C``, gold → ``R``

    Returns:
        Summary dict with record counts.
    """
    settings = settings or get_settings()
    state = PipelineState(settings)
    process_ts = process_ts or int(time.time() * 1000)
    so = settings.storage_options

    # Step 1: Identify all Silver batches ready for promotion.
    pending_ts = state.pickup_ready("silver")
    if not pending_ts:
        logger.info("gold_no_pending: No silver batches ready")
        return {"status": "no_pending", "mix_records": 0, "imports_records": 0, "exports_records": 0}

    logger.info(f"gold_started: process_ts={process_ts}, silver_batches={len(pending_ts)}")
    state.init_layer("gold", process_ts)

    try:
        # Step 2: Load only the physical partitions affected by the new data.
        silver_mix_ts = _read_delta_columns(
            f"{settings.silver_dir}/electricity_mix",
            so,
            ["process_ts", "year", "month", "day"],
        )
        silver_flows_ts = _read_delta_columns(
            f"{settings.silver_dir}/electricity_flows",
            so,
            ["process_ts", "year", "month", "day"],
        )

        pending_mix = filter_by_process_ts(silver_mix_ts, pending_ts)
        pending_flows = filter_by_process_ts(silver_flows_ts, pending_ts)
        affected_days = _affected_days(pending_mix, pending_flows)

        silver_mix = _read_delta_for_days(
            f"{settings.silver_dir}/electricity_mix",
            so,
            affected_days,
        )
        silver_flows = _read_delta_for_days(
            f"{settings.silver_dir}/electricity_flows",
            so,
            affected_days,
        )

        # Step 3: Compute daily business products (Mix, Imports, Exports).
        gold_mix = GoldMixSchema.validate(_aggregate_mix(silver_mix, process_ts))
        gold_imports, gold_exports = _aggregate_flows(silver_flows, process_ts)
        gold_imports = GoldImportsSchema.validate(gold_imports)
        gold_exports = GoldExportsSchema.validate(gold_exports)

        logger.info(
            f"gold_aggregated: mix_records={len(gold_mix)}, "
            f"imports_records={len(gold_imports)}, exports_records={len(gold_exports)}"
        )

        # Step 4: Perform atomic overwrite of affected days in Gold Delta tables.
        partition_cols = ["year", "month", "day"]

        _overwrite_affected_days(
            gold_mix,
            f"{settings.gold_dir}/daily_electricity_mix",
            so,
            affected_days,
            key_cols=["zone", "date"],
            partition_by=partition_cols,
        )
        _overwrite_affected_days(
            gold_imports,
            f"{settings.gold_dir}/daily_imports",
            so,
            affected_days,
            key_cols=["zone", "source_zone", "date"],
            partition_by=partition_cols,
        )
        _overwrite_affected_days(
            gold_exports,
            f"{settings.gold_dir}/daily_exports",
            so,
            affected_days,
            key_cols=["zone", "destination_zone", "date"],
            partition_by=partition_cols,
        )

        # Step 5: Advance the pipeline state for both Silver (consumed) and Gold (ready).
        total = len(gold_mix) + len(gold_imports) + len(gold_exports)
        state.mark_complete("silver", pending_ts)
        state.mark_ready("gold", process_ts, total)

        logger.info(
            f"gold_completed: mix_records={len(gold_mix)}, "
            f"imports_records={len(gold_imports)}, exports_records={len(gold_exports)}"
        )

        return {
            "process_ts": process_ts,
            "silver_batches_consumed": len(pending_ts),
            "mix_records": len(gold_mix),
            "imports_records": len(gold_imports),
            "exports_records": len(gold_exports),
        }

    except Exception as e:
        logger.error(f"gold_failed: error={e}, process_ts={process_ts}")
        state.mark_error("gold", process_ts, str(e))
        raise
