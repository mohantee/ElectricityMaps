"""General utility helpers for ETL pipeline layers.

Includes datetime utilities, S3 filesystem helpers, and Delta Lake operations.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl
import s3fs
from deltalake import DeltaTable

from electricity_maps.config import Settings

# ================================================================
# Datetime helpers
# ================================================================

def floor_to_hour(dt: datetime) -> datetime:
    """Floor a datetime to the start of its hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


# ================================================================
# S3 FileSystem helpers
# ================================================================

def get_s3fs(settings: Settings) -> s3fs.S3FileSystem:
    """Build an authenticated S3FileSystem or LocalFileSystem based on settings.

    If settings.emaps_data_dir is a local path (doesn't start with 's3://'),
    returns a LocalFileSystem for local storage/testing.

    Args:
        settings: Pipeline settings containing AWS credentials or local path.

    Returns:
        fsspec filesystem instance (S3FileSystem or LocalFileSystem).
    """
    if not settings.data_dir.startswith("s3://"):
        import fsspec
        return fsspec.filesystem("file")

    return s3fs.S3FileSystem(
        key=settings.aws_access_key_id,
        secret=settings.aws_secret_access_key,
        client_kwargs={"region_name": settings.aws_region},
    )


def read_bronze_parquet(s3_key: str, fs: s3fs.S3FileSystem) -> str:
    """Read a Bronze Parquet file and return the raw_json string.

    Args:
        s3_key: Full S3 path to parquet file.
        fs: Authenticated S3FileSystem.

    Returns:
        Raw JSON string from the first row's raw_json column.
    """
    with fs.open(s3_key, "rb") as f:
        df = pl.read_parquet(f)
    return str(df["raw_json"][0])


def find_bronze_files(
    bronze_dir: str,
    stream: str,
    process_ts: int,
    zone: str,
    fs: s3fs.S3FileSystem,
) -> list[str]:
    """Glob for Bronze Parquet files matching a process_ts.

    Args:
        bronze_dir: Base S3 directory for bronze data.
        stream: Stream type (electricity_mix or electricity_flows).
        process_ts: Batch identifier to match.
        zone: Zone code to match.
        fs: Authenticated S3FileSystem.

    Returns:
        List of full S3 paths matching the pattern.
    """
    is_s3 = bronze_dir.startswith("s3://")
    pattern = f"{bronze_dir}/{stream}/**/{zone}_{process_ts}.parquet"

    # Remove s3:// prefix for fsspec glob if present
    clean = pattern.replace("s3://", "")
    files = fs.glob(clean)

    if is_s3:
        return [f"s3://{f}" for f in files]
    return [str(f) for f in files]


# ================================================================
# Delta Lake helpers
# ================================================================

def read_delta_table(table_uri: str, storage_options: dict[str, str]) -> pl.DataFrame:
    """Read a Delta Lake table into a Polars DataFrame.

    Args:
        table_uri: URI to the Delta table.
        storage_options: Storage connection options (e.g., S3 credentials).

    Returns:
        Polars DataFrame with table contents, or empty DataFrame if table doesn't exist.
    """
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = pl.from_arrow(dt.to_pyarrow_table())
        if isinstance(df, pl.Series):
            return df.to_frame()
        return df
    except Exception:
        return pl.DataFrame()


def filter_by_process_ts(df: pl.DataFrame, process_ts_values: list[int]) -> pl.DataFrame:
    """Filter a DataFrame to only rows matching specific process_ts values.

    Args:
        df: Polars DataFrame with process_ts column.
        process_ts_values: List of process_ts values to include.

    Returns:
        Filtered DataFrame.
    """
    if df.is_empty():
        return df
    if "process_ts" not in df.columns:
        return df
    return df.filter(pl.col("process_ts").is_in(process_ts_values))


# ================================================================
# Zone metadata helpers
# ================================================================

_ZONE_NAMES = {
    "FR": "France",
    "DE": "Germany",
    "ES": "Spain",
    "IT-NO": "Italy North",
    "CH": "Switzerland",
    "BE": "Belgium",
    "GB": "Great Britain",
}


def get_zone_name(zone: str) -> str:
    """Get the human-readable name for a zone code.

    Args:
        zone: Zone code (e.g., 'FR').

    Returns:
        Zone name or the zone code if not found in mapping.
    """
    return _ZONE_NAMES.get(zone, zone)
