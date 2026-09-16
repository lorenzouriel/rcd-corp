"""MongoDB output sink for RCD Corp data generator.

One collection per table. `replace` (the default from `generate`) drops the
collection before inserting; `append` (used by `stream`) just inserts.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import structlog
from pymongo import MongoClient
from pymongo.database import Database

log = structlog.get_logger()

DEFAULT_URL = "mongodb://rcd:rcd@localhost:27017/rcd_corp?authSource=admin"
INSERT_CHUNK_SIZE = 5_000


class MongoDBSink:
    def __init__(self, connection_url: str | None = None) -> None:
        url = connection_url or os.environ.get("RCD_MONGODB_URL", DEFAULT_URL)
        self.client: MongoClient = MongoClient(url)
        self.db: Database = self.client.get_default_database()

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
        partition_col: str | None = None,
        append: bool = False,
    ) -> None:
        if df is None or df.empty:
            log.warning("mongodb_skip_empty", table=table_name)
            return

        collection = self.db[table_name]
        if not append:
            collection.drop()

        records = json.loads(df.to_json(orient="records", date_format="iso"))
        for i in range(0, len(records), INSERT_CHUNK_SIZE):
            chunk = records[i : i + INSERT_CHUNK_SIZE]
            if chunk:
                collection.insert_many(chunk, ordered=False)

        log.info("mongodb_written", table=table_name, rows=len(records), append=append)
