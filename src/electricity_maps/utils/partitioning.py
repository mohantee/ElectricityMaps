"""Partitioning helpers for S3 path construction.

All data is stored on S3 using Hive-style partitioning:
    ``year=YYYY/month=MM/day=DD``
"""

from __future__ import annotations

from datetime import datetime


def partition_path(base_dir: str, dt: datetime) -> str:
    """Build a Hive-style partition path from a datetime.

    Args:
        base_dir: S3 prefix (e.g. ``s3://bucket/data/bronze/electricity_mix``).
        dt: Timestamp to partition by.

    Returns:
        Full S3 path like ``s3://bucket/.../year=2026/month=04/day=25``.
    """
    return (
        f"{base_dir.rstrip('/')}"
        f"/year={dt.year}"
        f"/month={dt.month:02d}"
        f"/day={dt.day:02d}"
    )


def build_bronze_key(
    base_dir: str,
    stream: str,
    zone: str,
    process_ts: int,
    dt: datetime,
) -> str:
    """Build the full S3 key for a Bronze parquet file.

    Args:
        base_dir: Bronze root (e.g. ``s3://bucket/data/bronze``).
        stream: ``electricity_mix`` or ``electricity_flows``.
        zone: Zone code (e.g. ``FR``).
        process_ts: Epoch-ms batch identifier.
        dt: Ingestion timestamp (used for partitioning).

    Returns:
        Full key like ``s3://…/bronze/electricity_mix/year=…/FR_174…parquet``.
    """
    part = partition_path(f"{base_dir.rstrip('/')}/{stream}", dt)
    return f"{part}/{zone}_{process_ts}.parquet"
