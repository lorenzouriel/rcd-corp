"""FastAPI application: mounts the REST routers and the GraphQL router,
both gated behind the same API-key dependency (see `auth.py`). `/health` is
mounted directly on `app`, unauthenticated, for Docker healthchecks and
DEPLOY.md's verification loop.
"""
from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from .auth import verify_api_key
from .db import engine
from .graphql.schema import graphql_router
from .rest import router as rest_router

app = FastAPI(
    title="RCD Corp Query API",
    description="REST + GraphQL query surface over RCD Corp's generated Postgres data.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rest_router, dependencies=[Depends(verify_api_key)])
app.include_router(graphql_router, prefix="/graphql", dependencies=[Depends(verify_api_key)])


@app.get("/health")
def health() -> dict[str, str]:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok"}
