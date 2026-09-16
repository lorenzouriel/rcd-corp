"""Shared DB-sink connection helpers for the output validation tests.

Not a test module itself (leading underscore keeps pytest from collecting it).
"""
from __future__ import annotations

import os

DB_DEFAULT_URLS = {
    "postgres": "postgresql+psycopg2://rcd:rcd@localhost:5432/rcd_corp",
    "sqlserver": (
        "mssql+pyodbc://rcd:rcd@localhost:1433/rcd_corp"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    ),
}

DB_ENV_VARS = {
    "postgres": "RCD_POSTGRES_URL",
    "sqlserver": "RCD_SQLSERVER_URL",
}


def get_db_engine(sink: str):
    """Build a SQLAlchemy engine for 'postgres' or 'sqlserver' from env vars."""
    from sqlalchemy import create_engine

    url = os.environ.get(DB_ENV_VARS[sink], DB_DEFAULT_URLS[sink])
    return create_engine(url, pool_pre_ping=True)


MONGO_DEFAULT_URL = "mongodb://rcd:rcd@localhost:27017/rcd_corp?authSource=admin"
REDIS_DEFAULT_URL = "redis://localhost:6379/0"
REDIS_STREAM_PREFIX = "rcd:"


def get_mongo_db():
    """Build a pymongo Database handle for the 'mongodb' sink from env vars."""
    from pymongo import MongoClient

    url = os.environ.get("RCD_MONGODB_URL", MONGO_DEFAULT_URL)
    return MongoClient(url).get_default_database()


def get_redis_client():
    """Build a redis-py client for the 'redis' sink from env vars."""
    import redis

    url = os.environ.get("RCD_REDIS_URL", REDIS_DEFAULT_URL)
    return redis.Redis.from_url(url, decode_responses=True)
