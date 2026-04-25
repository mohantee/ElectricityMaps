"""Pandera schema contracts for Silver layer Delta tables.

These schemas enforce data types and constraints on the cleaned,
flattened DataFrames before they are written to Delta Lake.
"""

from __future__ import annotations

import pandera.polars as pa
import polars as pl


class SilverMixSchema(pa.DataFrameModel):
    """Schema contract for the ``silver.electricity_mix`` Delta table."""

    zone: pl.Utf8
    datetime: pl.Datetime = pa.Field(dtype=pl.Datetime("us", "UTC"))
    updated_at: pl.Datetime = pa.Field(dtype=pl.Datetime("us", "UTC"))
    is_estimated: pl.Boolean = pa.Field(nullable=True)
    estimation_method: pl.Utf8 = pa.Field(nullable=True)

    # Energy sources (MW)
    nuclear_mw: pl.Float64 = pa.Field(nullable=True)
    geothermal_mw: pl.Float64 = pa.Field(nullable=True)
    biomass_mw: pl.Float64 = pa.Field(nullable=True)
    coal_mw: pl.Float64 = pa.Field(nullable=True)
    wind_mw: pl.Float64 = pa.Field(nullable=True)
    solar_mw: pl.Float64 = pa.Field(nullable=True)
    hydro_mw: pl.Float64 = pa.Field(nullable=True)
    gas_mw: pl.Float64 = pa.Field(nullable=True)
    oil_mw: pl.Float64 = pa.Field(nullable=True)
    unknown_mw: pl.Float64 = pa.Field(nullable=True)

    # Storage (MW)
    hydro_storage_charge_mw: pl.Float64 = pa.Field(nullable=True)
    hydro_storage_discharge_mw: pl.Float64 = pa.Field(nullable=True)
    battery_storage_charge_mw: pl.Float64 = pa.Field(nullable=True)
    battery_storage_discharge_mw: pl.Float64 = pa.Field(nullable=True)

    # Aggregate flows (MW)
    flow_exports_mw: pl.Float64 = pa.Field(nullable=True)
    flow_imports_mw: pl.Float64 = pa.Field(nullable=True)

    # Partition keys
    year: pl.Int32
    month: pl.Int32
    day: pl.Int32

    class Config:
        coerce = True
        strict = False


class SilverFlowsSchema(pa.DataFrameModel):
    """Schema contract for the ``silver.electricity_flows`` Delta table."""

    zone: pl.Utf8
    datetime: pl.Datetime = pa.Field(dtype=pl.Datetime("us", "UTC"))
    updated_at: pl.Datetime = pa.Field(dtype=pl.Datetime("us", "UTC"))
    neighbor_zone: pl.Utf8
    direction: pl.Utf8 = pa.Field(isin=["import", "export"])
    power_mw: pl.Float64

    # Partition keys
    year: pl.Int32
    month: pl.Int32
    day: pl.Int32

    class Config:
        coerce = True
        strict = False
