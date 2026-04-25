"""Pydantic v2 models for Electricity Maps API v4 responses.

These models match the *actual* sandbox API response shapes (verified).
Key quirks handled:
- ``hydro storage`` and ``battery storage`` are space-separated keys
- Several numeric fields are nullable (geothermal, unknown, etc.)
- ``import`` / ``export`` in flows are Python reserved words → aliased
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ------------------------------------------------------------------ #
#  Electricity Mix models                                             #
# ------------------------------------------------------------------ #

class StorageData(BaseModel):
    """Nested charge/discharge values for hydro or battery storage."""
    charge: Optional[float] = None
    discharge: Optional[float] = None


class FlowsAggregate(BaseModel):
    """Aggregate import/export totals inside the mix response."""
    exports: Optional[float] = None
    imports: Optional[float] = None


class MixData(BaseModel):
    """Energy source breakdown for a single hourly record."""
    nuclear: Optional[float] = None
    geothermal: Optional[float] = None
    biomass: Optional[float] = None
    coal: Optional[float] = None
    wind: Optional[float] = None
    solar: Optional[float] = None
    hydro: Optional[float] = None
    gas: Optional[float] = None
    oil: Optional[float] = None
    unknown: Optional[float] = None
    hydro_storage: Optional[StorageData] = Field(None, alias="hydro storage")
    battery_storage: Optional[StorageData] = Field(None, alias="battery storage")
    flows: Optional[FlowsAggregate] = None

    model_config = {"populate_by_name": True}


class MixRecord(BaseModel):
    """A single hourly record in the electricity mix response."""
    datetime: datetime
    updated_at: datetime = Field(alias="updatedAt")
    is_estimated: Optional[bool] = Field(None, alias="isEstimated")
    estimation_method: Optional[str] = Field(None, alias="estimationMethod")
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
    datetime: datetime
    updated_at: datetime = Field(alias="updatedAt")
    import_flows: Optional[dict[str, float]] = Field(None, alias="import")
    export_flows: Optional[dict[str, float]] = Field(None, alias="export")

    model_config = {"populate_by_name": True}


class ElectricityFlowsResponse(BaseModel):
    """Top-level response from ``GET /v4/electricity-flows/past-range``."""
    zone: str
    temporal_granularity: str = Field(alias="temporalGranularity")
    unit: str
    data: list[FlowsRecord]

    model_config = {"populate_by_name": True}
