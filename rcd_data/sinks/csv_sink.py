"""CSV output sink for RCD Corp data generator."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger()


class CSVSink:
    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def write(self, table_name: str, df: pd.DataFrame, partition_col: str | None = None) -> None:
        if df is None or df.empty:
            log.warning("csv_skip_empty", table=table_name)
            return
        out_path = self.base_path / f"{table_name}.csv"
        df.to_csv(out_path, index=False)
        log.info("csv_written", table=table_name, rows=len(df), path=str(out_path))
