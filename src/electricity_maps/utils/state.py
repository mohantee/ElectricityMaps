"""Pipeline state tracking via the ``el_state`` Delta table.

State machine per row: **I → R → P → C**

- ``I`` (Init)       — layer has started processing
- ``R`` (Ready)      — layer completed, data ready for downstream
- ``P`` (Processing) — downstream layer picked it up
- ``C`` (Complete)   — downstream layer finished processing it
"""

from __future__ import annotations

import typing as tp
from datetime import UTC, datetime

import polars as pl
from deltalake import DeltaTable, write_deltalake

from electricity_maps.config import Settings, get_settings

# Schema for the el_state table
_STATE_SCHEMA = {
    "process": pl.Utf8,
    "process_ts": pl.Int64,
    "start_timestamp": pl.Datetime("us", "UTC"),
    "end_timestamp": pl.Datetime("us", "UTC"),
    "status": pl.Utf8,
    "record_count": pl.Int64,
    "error_message": pl.Utf8,
}


class PipelineState:
    """Manages the ``el_state`` Delta table (I → R → P → C).

    Args:
        settings: Pipeline settings (defaults to cached singleton).
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._table_uri = f"{self._settings.state_dir}/el_state"
        self._storage_options = self._settings.storage_options

    # ------------------------------------------------------------------ #
    #  State transitions                                                  #
    # ------------------------------------------------------------------ #

    def init_layer(
        self,
        process: str,
        process_ts: int,
        start_ts: datetime | None = None,
    ) -> None:
        """INSERT a row with ``status='I'`` when a layer starts.

        Args:
            process: Layer name (``bronze``, ``silver``, ``gold``).
            process_ts: Epoch-ms batch identifier.
            start_ts: When processing started (defaults to now UTC).
        """
        start_ts = start_ts or datetime.now(UTC)
        row = pl.DataFrame(
            {
                "process": [process],
                "process_ts": [process_ts],
                "start_timestamp": [start_ts],
                "end_timestamp": [None],
                "status": ["I"],
                "record_count": [None],
                "error_message": [None],
            },
            schema=tp.cast(tp.Any, _STATE_SCHEMA),
        )
        self._append(row)

    def mark_ready(
        self,
        process: str,
        process_ts: int,
        record_count: int,
        end_ts: datetime | None = None,
    ) -> None:
        """UPDATE ``status='R'`` when a layer completes successfully.

        Args:
            process: Layer name.
            process_ts: Epoch-ms batch identifier.
            record_count: Number of records produced.
            end_ts: When processing finished (defaults to now UTC).
        """
        end_ts = end_ts or datetime.now(UTC)
        self._update_status(
            process=process,
            process_ts=process_ts,
            new_status="R",
            end_ts=end_ts,
            record_count=record_count,
        )

    def pickup_ready(self, process: str) -> list[int]:
        """SELECT rows with ``status='R'`` and flip them to ``'P'``.

        Args:
            process: Upstream layer to pick up (e.g. ``bronze``).

        Returns:
            List of ``process_ts`` values that were picked up.
        """
        df = self._read_all()
        if df.is_empty():
            return []

        ready = df.filter(
            (pl.col("process") == process) & (pl.col("status") == "R")
        )
        if ready.is_empty():
            return []

        ts_list = ready["process_ts"].to_list()

        # Flip each to "P"
        for ts in ts_list:
            self._update_status(process=process, process_ts=ts, new_status="P")

        return ts_list

    def mark_complete(self, process: str, process_ts_list: list[int]) -> None:
        """UPDATE ``status='C'`` for rows consumed by downstream.

        Args:
            process: Layer whose rows to mark complete.
            process_ts_list: Batch identifiers to mark.
        """
        for ts in process_ts_list:
            self._update_status(process=process, process_ts=ts, new_status="C")

    def mark_error(
        self,
        process: str,
        process_ts: int,
        error_message: str,
    ) -> None:
        """Record an error on a state row (status stays at ``I``).

        Args:
            process: Layer name.
            process_ts: Epoch-ms batch identifier.
            error_message: Error details.
        """
        self._update_status(
            process=process,
            process_ts=process_ts,
            new_status="I",
            error_message=error_message,
        )

    # ------------------------------------------------------------------ #
    #  Queries                                                            #
    # ------------------------------------------------------------------ #

    def get_last_process_ts(self, process: str) -> datetime | None:
        """Return the latest ``process_ts`` for a process layer.

        Used by Bronze to determine the catch-up start time.
        """
        df = self._read_all()
        if df.is_empty():
            return None

        # Filter by process and ensure status is not null (as per user request: null indicates didn't complete)
        layer = (
            df.filter(
                (pl.col("process") == process)
                & (pl.col("status").is_not_null())
            )
            .sort("process_ts", descending=True)
        )

        if layer.is_empty():
            return None

        # Get the process_ts from the most recent run and convert to datetime
        ts_ms = layer.item(0, "process_ts")
        return datetime.fromtimestamp(ts_ms / 1000.0, UTC)

    def get_state_summary(self) -> pl.DataFrame:
        """Return the full ``el_state`` table for monitoring / debugging."""
        return self._read_all()

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                   #
    # ------------------------------------------------------------------ #

    def _read_all(self) -> pl.DataFrame:
        """Read the entire el_state Delta table."""
        try:
            dt = DeltaTable(
                self._table_uri,
                storage_options=self._storage_options,
            )
            df = pl.from_arrow(dt.to_pyarrow_table())
            if isinstance(df, pl.Series):
                return df.to_frame()
            return df
        except Exception:
            # Table doesn't exist yet
            return pl.DataFrame(schema=tp.cast(tp.Any, _STATE_SCHEMA))

    def _append(self, df: pl.DataFrame) -> None:
        """Append rows to the el_state Delta table (create if needed)."""
        pa_table = df.to_arrow()
        try:
            write_deltalake(
                self._table_uri,
                pa_table,
                mode="append",
                storage_options=self._storage_options,
            )
        except Exception:
            # First write — table doesn't exist yet
            write_deltalake(
                self._table_uri,
                pa_table,
                mode="overwrite",
                storage_options=self._storage_options,
            )

    def _update_status(
        self,
        process: str,
        process_ts: int,
        new_status: str,
        end_ts: datetime | None = None,
        record_count: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """Overwrite-based update (read → mutate → write).

        Delta Lake on S3 doesn't support merge/update in deltalake-rs,
        so we do a full read-mutate-overwrite.
        """
        df = self._read_all()
        if df.is_empty():
            return

        mask = (pl.col("process") == process) & (pl.col("process_ts") == process_ts)

        updates: dict = {"status": pl.lit(new_status)}
        if end_ts is not None:
            updates["end_timestamp"] = pl.lit(end_ts).cast(pl.Datetime("us", "UTC"))
        if record_count is not None:
            updates["record_count"] = pl.lit(record_count)
        if error_message is not None:
            updates["error_message"] = pl.lit(error_message)

        df = df.with_columns([
            pl.when(mask).then(v).otherwise(pl.col(k)).alias(k)
            for k, v in updates.items()
        ])

        pa_table = df.to_arrow()
        write_deltalake(
            self._table_uri,
            pa_table,
            mode="overwrite",
            storage_options=self._storage_options,
        )
