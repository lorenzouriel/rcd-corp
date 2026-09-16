"""API-key authentication for the RCD Corp query API.

Shared by both the REST routers and the GraphQL router — one dependency,
applied at router-mount time in `app.py`, so there is exactly one place
that decides who's allowed in. Matches this repo's existing access-control
posture for every other service (Postgres, SQL Server, MongoDB, Redis): a
single shared credential, with the network boundary (bind interface /
Tailscale) doing the actual perimeter enforcement — no user/session model
exists anywhere in this project, so this doesn't invent one just for the
API.
"""
from __future__ import annotations

import os

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER_NAME = "X-API-Key"
# Same "weak-but-documented" dev default as POSTGRES_PASSWORD=rcd elsewhere
# in this repo — override via RCD_API_KEY for anything beyond localhost.
DEFAULT_API_KEY = "rcd-dev-key"

_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def _valid_keys() -> set[str]:
    raw = os.environ.get("RCD_API_KEY", DEFAULT_API_KEY)
    return {k.strip() for k in raw.split(",") if k.strip()}


def verify_api_key(api_key: str | None = Security(_header)) -> None:
    if api_key not in _valid_keys():
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")
