"""Parquet output sink with optional date partitioning for RCD Corp."""
from __future__ import annotations

import time as _time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

log = structlog.get_logger()


class ParquetSink:
    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
        partition_col: str | None = None,
        append: bool = False,
    ) -> None:
        if df is None or df.empty:
            log.warning("parquet_skip_empty", table=table_name)
            return
        out_dir = self.base_path / table_name
        out_dir.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(df, preserve_index=False)

        if partition_col and partition_col in df.columns:
            pq.write_to_dataset(
                table,
                root_path=str(out_dir),
                partition_cols=[partition_col],
                existing_data_behavior="overwrite_or_ignore",
            )
        elif append:
            ts_ms = int(_time.time() * 1000)
            pq.write_table(table, str(out_dir / f"stream_{ts_ms}.parquet"))
        else:
            pq.write_table(table, str(out_dir / "data.parquet"))

        log.info("parquet_written", table=table_name, rows=len(df), path=str(out_dir), append=append)
