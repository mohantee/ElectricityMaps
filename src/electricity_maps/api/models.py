"""Pydantic v2 models for Electricity Maps API v4 responses.

These models match the *actual* sandbox API response shapes (verified).
Key quirks handled:
- ``hydro storage`` and ``battery storage`` are space-separated keys
- Several numeric fields are nullable (geothermal, unknown, etc.)
- ``import`` / ``export`` in flows are Python reserved words → aliased
"""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, Field

# ------------------------------------------------------------------ #
#  Electricity Mix models                                             #
# ------------------------------------------------------------------ #

class StorageData(BaseModel):
    """Nested charge/discharge values for hydro or battery storage."""
    charge: float | None = None
    discharge: float | None = None


class FlowsAggregate(BaseModel):
    """Aggregate import/export totals inside the mix response."""
    exports: float | None = None
    imports: float | None = None


class MixData(BaseModel):
    """Energy source breakdown for a single hourly record."""
    nuclear: float | None = None
    geothermal: float | None = None
    biomass: float | None = None
    coal: float | None = None
    wind: float | None = None
    solar: float | None = None
    hydro: float | None = None
    gas: float | None = None
    oil: float | None = None
    unknown: float | None = None
    hydro_storage: StorageData | None = Field(None, alias="hydro storage")
    battery_storage: StorageData | None = Field(None, alias="battery storage")
    flows: FlowsAggregate | None = None

    model_config = {"populate_by_name": True}


class MixRecord(BaseModel):
    """A single hourly record in the electricity mix response."""
    datetime: dt.datetime
    updated_at: dt.datetime = Field(alias="updatedAt")
    is_estimated: bool | None = Field(None, alias="isEstimated")
    estimation_method: str | None = Field(None, alias="estimationMethod")
    mix: MixData

    model_config = {"populate_by_name": True}


class ElectricityMixResponse(BaseModel):
    """Top-level response from ``GET /v4/electricity-mix/past-range``."""
    zone: str
    temporal_granularity: str = Field(alias="temporalGranularity")
    unit: str
    data: list[MixRecord]

    model_config = {"populate_by_name": True}


# ------------------------------------------------------------------ #
#  Electricity Flows models                                           #
# ------------------------------------------------------------------ #

class FlowsRecord(BaseModel):
    """A single hourly record in the electricity flows response."""
    datetime: dt.datetime
    updated_at: dt.datetime = Field(alias="updatedAt")
    import_flows: dict[str, float] | None = Field(None, alias="import")
    export_flows: dict[str, float] | None = Field(None, alias="export")

    model_config = {"populate_by_name": True}


class ElectricityFlowsResponse(BaseModel):
    """Top-level response from ``GET /v4/electricity-flows/past-range``."""
    zone: str
    temporal_granularity: str = Field(alias="temporalGranularity")
    unit: str
    data: list[FlowsRecord]

    model_config = {"populate_by_name": True}
