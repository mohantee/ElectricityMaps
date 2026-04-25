"""Pandera schema contracts for Gold layer Delta tables.

These schemas enforce data types and constraints on the aggregated,
business-ready DataFrames before they are written to Delta Lake.
"""

from __future__ import annotations

import pandera.polars as pa
import polars as pl


# ================================================================
# Polars Schema Dictionaries
# ================================================================

GOLD_MIX_SCHEMA = {
    "process_ts": pl.Int64,
    "zone": pl.Utf8,
    "zone_name": pl.Utf8,
    "date": pl.Date,
    "nuclear_pct": pl.Float64,
    "biomass_pct": pl.Float64,
    "wind_pct": pl.Float64,
    "solar_pct": pl.Float64,
    "hydro_pct": pl.Float64,
    "gas_pct": pl.Float64,
    "oil_pct": pl.Float64,
    "coal_pct": pl.Float64,
    "geothermal_pct": pl.Float64,
    "unknown_pct": pl.Float64,
    "total_production_mwh": pl.Float64,
    "fossil_free_avg_pct": pl.Float64,
    "renewable_avg_pct": pl.Float64,
    "hours_covered": pl.Int32,
    "year": pl.Int32,
    "month": pl.Int32,
}


GOLD_IMPORTS_SCHEMA = {
    "process_ts": pl.Int64,
    "zone": pl.Utf8,
    "zone_name": pl.Utf8,
    "source_zone": pl.Utf8,
    "source_zone_name": pl.Utf8,
    "date": pl.Date,
    "import_mwh": pl.Float64,
    "hours_covered": pl.Int32,
    "year": pl.Int32,
    "month": pl.Int32,
}


GOLD_EXPORTS_SCHEMA = {
    "process_ts": pl.Int64,
    "zone": pl.Utf8,
    "zone_name": pl.Utf8,
    "destination_zone": pl.Utf8,
    "destination_zone_name": pl.Utf8,
    "date": pl.Date,
    "export_mwh": pl.Float64,
    "hours_covered": pl.Int32,
    "year": pl.Int32,
    "month": pl.Int32,
}


# ================================================================
# Pandera Schema Contracts
# ================================================================


class GoldMixSchema(pa.DataFrameModel):
    """Schema contract for the ``gold.daily_electricity_mix`` Delta table."""

    process_ts: pl.Int64
    zone: pl.Utf8
    zone_name: pl.Utf8
    date: pl.Date
    
    # Percentages
    nuclear_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    biomass_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    wind_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    solar_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    hydro_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    gas_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    oil_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    coal_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    geothermal_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    unknown_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    
    total_production_mwh: pl.Float64 = pa.Field(ge=0.0)
    fossil_free_avg_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    renewable_avg_pct: pl.Float64 = pa.Field(nullable=True, ge=0.0, le=100.0)
    hours_covered: pl.Int32 = pa.Field(ge=1, le=24)
    
    # Partition keys
    year: pl.Int32
    month: pl.Int32

    class Config:
        coerce = True
        strict = False


class GoldImportsSchema(pa.DataFrameModel):
    """Schema contract for the ``gold.daily_imports`` Delta table."""

    process_ts: pl.Int64
    zone: pl.Utf8
    zone_name: pl.Utf8
    source_zone: pl.Utf8
    source_zone_name: pl.Utf8
    date: pl.Date
    import_mwh: pl.Float64 = pa.Field(ge=0.0)
    hours_covered: pl.Int32 = pa.Field(ge=1, le=24)
    
    # Partition keys
    year: pl.Int32
    month: pl.Int32

    class Config:
        coerce = True
        strict = False


class GoldExportsSchema(pa.DataFrameModel):
    """Schema contract for the ``gold.daily_exports`` Delta table."""

    process_ts: pl.Int64
    zone: pl.Utf8
    zone_name: pl.Utf8
    destination_zone: pl.Utf8
    destination_zone_name: pl.Utf8
    date: pl.Date
    export_mwh: pl.Float64 = pa.Field(ge=0.0)
    hours_covered: pl.Int32 = pa.Field(ge=1, le=24)
    
    # Partition keys
    year: pl.Int32
    month: pl.Int32

    class Config:
        coerce = True
        strict = False
