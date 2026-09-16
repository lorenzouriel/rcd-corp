"""Redis output sink for RCD Corp data generator.

Redis is treated as a bounded, recent-window sink, not full historical
storage like the file/DB sinks — each table is written as a Redis Stream
(`XADD`) capped at `RCD_REDIS_STREAM_MAXLEN` entries (approximate trim), one
entry per row, JSON-encoded in a single `data` field. Streams are a natural
fit for `rcd-data stream`'s append-only batches: each tick just XADDs new
rows onto the existing stream for downstream consumers (e.g. a consumer
group) to pick up.

`replace` (the default from `generate`) deletes the stream key before
refilling it; `append` (used by `stream`) just adds.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import redis
import structlog

log = structlog.get_logger()

DEFAULT_URL = "redis://localhost:6379/0"
DEFAULT_STREAM_MAXLEN = 100_000
STREAM_PREFIX = "rcd:"

SOCKET_TIMEOUT_S = 30
PIPELINE_CHUNK_SIZE = 2_000


class RedisSink:
    def __init__(self, connection_url: str | None = None, maxlen: int | None = None) -> None:
        url = connection_url or os.environ.get("RCD_REDIS_URL", DEFAULT_URL)
        self.client: redis.Redis = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=SOCKET_TIMEOUT_S,
            socket_connect_timeout=SOCKET_TIMEOUT_S,
        )
        self.maxlen = maxlen or int(
            os.environ.get("RCD_REDIS_STREAM_MAXLEN", DEFAULT_STREAM_MAXLEN)
        )

    def write(
        self,
        table_name: str,
        df: pd.DataFrame,
        partition_col: str | None = None,
        append: bool = False,
    ) -> None:
        if df is None or df.empty:
            log.warning("redis_skip_empty", table=table_name)
            return

        key = f"{STREAM_PREFIX}{table_name}"
        if not append:
            self.client.delete(key)

        records = json.loads(df.to_json(orient="records", date_format="iso"))
        for i in range(0, len(records), PIPELINE_CHUNK_SIZE):
            chunk = records[i : i + PIPELINE_CHUNK_SIZE]
            pipe = self.client.pipeline(transaction=False)
            for record in chunk:
                pipe.xadd(key, {"data": json.dumps(record)}, maxlen=self.maxlen, approximate=True)
            pipe.execute()

        log.info("redis_written", table=table_name, rows=len(records), stream=key, append=append)
