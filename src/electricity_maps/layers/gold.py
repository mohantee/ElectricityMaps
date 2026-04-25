"""Gold Layer — Business-Ready Data Products.

Reads Silver Delta tables, performs daily aggregations (converting MW to MWh),
calculates percentages, and splits flows into imports and exports.
Results are written as partitioned Delta Lake tables.
"""

from __future__ import annotations

import time

import polars as pl
from deltalake import DeltaTable, write_deltalake

from electricity_maps.config import Settings, get_settings
from electricity_maps.utils.logging import get_logger
from electricity_maps.utils.state import PipelineState

logger = get_logger(__name__)


# ------------------------------------------------------------------ #
#  Metadata mapping                                                   #
# ------------------------------------------------------------------ #

# A real pipeline might load this from a reference table
_ZONE_NAMES = {
    "FR": "France",
    "DE": "Germany",
    "ES": "Spain",
    "IT-NO": "Italy North",
    "CH": "Switzerland",
    "BE": "Belgium",
    "GB": "Great Britain",
}


def _get_zone_name(zone: str) -> str:
    return _ZONE_NAMES.get(zone, zone)


# ------------------------------------------------------------------ #
#  Mix Aggregation                                                    #
# ------------------------------------------------------------------ #

def _aggregate_mix(silver_mix: pl.DataFrame) -> pl.DataFrame:
    """Aggregate hourly MW to daily MWh and percentages."""
    if silver_mix.is_empty():
        return silver_mix
        
    # Since granularity is hourly, 1 MW for 1 hour = 1 MWh.
    # Group by zone and date (extract date from datetime)
    df = silver_mix.with_columns(
        pl.col("datetime").dt.date().alias("reference_timestamp")
    )
    
    # Sum the production columns
    prod_cols = [
        "nuclear_mw", "geothermal_mw", "biomass_mw", "coal_mw",
        "wind_mw", "solar_mw", "hydro_mw", "gas_mw", "oil_mw", "unknown_mw"
    ]
    
    # Perform aggregation
    agg_exprs = [
        pl.col(c).sum().alias(c.replace("_mw", "_mwh")) for c in prod_cols
    ]
    agg_exprs.append(pl.count("datetime").alias("hours_covered"))
    
    daily = df.group_by(["zone", "reference_timestamp", "year", "month"]).agg(agg_exprs)
    
    # Calculate total production
    mwh_cols = [c.replace("_mw", "_mwh") for c in prod_cols]
    
    # Sum all sources, treating nulls as 0
    daily = daily.with_columns(
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in mwh_cols]).alias("total_production_mwh")
    )
    
    # Calculate percentages
    for c in mwh_cols:
        pct_col = c.replace("_mwh", "_pct")
        daily = daily.with_columns(
            (pl.col(c) / pl.col("total_production_mwh") * 100).fill_nan(0.0).alias(pct_col)
        )
        
    # Simple proxies for fossil-free and renewable (a full implementation would be more robust)
    fossil_free_cols = ["nuclear_pct", "geothermal_pct", "biomass_pct", "wind_pct", "solar_pct", "hydro_pct"]
    renewable_cols = ["geothermal_pct", "biomass_pct", "wind_pct", "solar_pct", "hydro_pct"]
    
    daily = daily.with_columns(
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in fossil_free_cols]).alias("fossil_free_avg_pct"),
        pl.sum_horizontal([pl.col(c).fill_null(0.0) for c in renewable_cols]).alias("renewable_avg_pct"),
    )
    
    # Add zone metadata
    daily = daily.with_columns(
        pl.col("zone").map_elements(_get_zone_name, return_dtype=pl.Utf8).alias("zone_metadata")
    )
    
    # Keep only the required columns (drop the raw MWh sums to match schema)
    select_cols = [
        "zone", "zone_metadata", "reference_timestamp", "nuclear_pct", "biomass_pct", "wind_pct",
        "solar_pct", "hydro_pct", "gas_pct", "oil_pct", "coal_pct", "geothermal_pct",
        "unknown_pct", "total_production_mwh", "fossil_free_avg_pct", "renewable_avg_pct",
        "hours_covered", "year", "month"
    ]
    
    return daily.select(select_cols)


# ------------------------------------------------------------------ #
#  Flows Aggregation                                                  #
# ------------------------------------------------------------------ #

def _aggregate_flows(silver_flows: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Aggregate hourly MW to daily MWh and split into imports and exports."""
    if silver_flows.is_empty():
        return pl.DataFrame(), pl.DataFrame()
        
    df = silver_flows.with_columns(
        pl.col("datetime").dt.date().alias("date")
    )
    
    # Imports
    imports_df = df.filter(pl.col("direction") == "import")
    if not imports_df.is_empty():
        imports_agg = imports_df.group_by(["zone", "neighbor_zone", "date", "year", "month"]).agg([
            pl.col("power_mw").sum().alias("import_mwh"),
            pl.count("datetime").alias("hours_covered")
        ]).rename({"neighbor_zone": "source_zone"})
        
        imports_agg = imports_agg.with_columns(
            pl.col("zone").map_elements(_get_zone_name, return_dtype=pl.Utf8).alias("zone_metadata")
        )
        # Select target columns
        imports_agg = imports_agg.select([
            "zone", "zone_name", "source_zone", "date", "import_mwh", "hours_covered", "year", "month"
        ])
    else:
        imports_agg = pl.DataFrame()
        
    # Exports
    exports_df = df.filter(pl.col("direction") == "export")
    if not exports_df.is_empty():
        exports_agg = exports_df.group_by(["zone", "neighbor_zone", "date", "year", "month"]).agg([
            pl.col("power_mw").sum().alias("export_mwh"),
            pl.count("datetime").alias("hours_covered")
        ]).rename({"neighbor_zone": "destination_zone"})
        
        exports_agg = exports_agg.with_columns(
            pl.col("zone").map_elements(_get_zone_name, return_dtype=pl.Utf8).alias("zone_metadata")
        )
        # Select target columns
        exports_agg = exports_agg.select([
            "zone", "zone_name", "destination_zone", "date", "export_mwh", "hours_covered", "year", "month"
        ])
    else:
        exports_agg = pl.DataFrame()
        
    return imports_agg, exports_agg


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

    # Step 1: Pick up ready silver batches
    pending_ts = state.pickup_ready("silver")
    if not pending_ts:
        logger.info("gold_no_pending", msg="No silver batches ready")
        return {"status": "no_pending", "mix_records": 0, "imports_records": 0, "exports_records": 0}

    logger.info("gold_started", process_ts=process_ts, silver_batches=len(pending_ts))
    state.init_layer("gold", process_ts)

    try:
        # We need to process all pending batches. For Gold layer (aggregation), 
        # reading the entire silver tables and doing a full overwrite or merge might be
        # needed in a real data warehouse. For this exercise, we will just read 
        # the relevant partitions or the whole table, aggregate it, and overwrite 
        # the gold table. In a real-world scenario, you'd want incremental aggregation.
        
        # Here we just read the whole silver tables to simplify the aggregation over dates
        try:
            mix_dt = DeltaTable(f"{settings.silver_dir}/electricity_mix", storage_options=so)
            silver_mix = pl.from_arrow(mix_dt.to_pyarrow_table())
        except Exception:
            silver_mix = pl.DataFrame()
            
        try:
            flows_dt = DeltaTable(f"{settings.silver_dir}/electricity_flows", storage_options=so)
            silver_flows = pl.from_arrow(flows_dt.to_pyarrow_table())
        except Exception:
            silver_flows = pl.DataFrame()

        # Step 3: Aggregate
        gold_mix = _aggregate_mix(silver_mix)
        gold_imports, gold_exports = _aggregate_flows(silver_flows)

        logger.info(
            "gold_aggregated",
            mix_records=len(gold_mix),
            imports_records=len(gold_imports),
            exports_records=len(gold_exports),
        )

        # Step 4: Write Delta Lake tables
        # For simplicity, we overwrite the gold tables since we did a full aggregation.
        # In a real system, you'd use write_deltalake with merge strategy.
        partition_cols = ["year", "month"]

        if not gold_mix.is_empty():
            write_deltalake(
                f"{settings.gold_dir}/daily_electricity_mix",
                gold_mix.to_arrow(),
                mode="overwrite",
                partition_by=partition_cols,
                storage_options=so,
            )
            
        if not gold_imports.is_empty():
            write_deltalake(
                f"{settings.gold_dir}/daily_imports",
                gold_imports.to_arrow(),
                mode="overwrite",
                partition_by=partition_cols,
                storage_options=so,
            )
            
        if not gold_exports.is_empty():
            write_deltalake(
                f"{settings.gold_dir}/daily_exports",
                gold_exports.to_arrow(),
                mode="overwrite",
                partition_by=partition_cols,
                storage_options=so,
            )

        # Step 5: Update state
        total = len(gold_mix) + len(gold_imports) + len(gold_exports)
        state.mark_complete("silver", pending_ts)
        state.mark_ready("gold", process_ts, total)

        logger.info(
            "gold_completed",
            mix_records=len(gold_mix),
            imports_records=len(gold_imports),
            exports_records=len(gold_exports),
        )

        return {
            "process_ts": process_ts,
            "silver_batches_consumed": len(pending_ts),
            "mix_records": len(gold_mix),
            "imports_records": len(gold_imports),
            "exports_records": len(gold_exports),
        }

    except Exception as e:
        logger.error("gold_failed", error=str(e), process_ts=process_ts)
        state.mark_error("gold", process_ts, str(e))
        raise
