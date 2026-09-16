"""Database engine/session for the RCD Corp query API.

Mirrors the connection convention in `rcd_data/sinks/postgres_sink.py`
(`RCD_POSTGRES_URL`, same default) — the API reads whatever was populated by
`rcd-data generate --sink postgres`.
"""
from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

DEFAULT_URL = "postgresql+psycopg2://rcd:rcd@localhost:5432/rcd_corp"

engine = create_engine(
    os.environ.get("RCD_POSTGRES_URL", DEFAULT_URL),
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
