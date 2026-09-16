"""Pagination helpers shared by every `queries/<domain>.py` module.

Date-range filter convention used throughout `queries/*.py` (documented
once, here, rather than repeated per table): a `*_after` filter is
inclusive (`>=`), a `*_before` filter is exclusive (`<`).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

T = TypeVar("T")

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


@dataclass
class PageResult(Generic[T]):
    items: list[T]
    total: int | None
    limit: int
    offset: int


def clamp_pagination(limit: int, offset: int) -> tuple[int, int]:
    """Validate limit/offset. Raises ValueError on an out-of-bounds value —
    callers map that to a REST 422 or a GraphQL error, never silently clamp.
    """
    if not (1 <= limit <= MAX_LIMIT):
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}, got {limit}")
    if offset < 0:
        raise ValueError(f"offset must be >= 0, got {offset}")
    return limit, offset


def compute_total(session: Session, stmt: Select, include_total: bool) -> int | None:
    """Row count for `stmt` ignoring its limit/offset, or None if skipped
    (`include_total=False`) — see the `total`/`includeTotal` opt-out note
    in the Query API docs: cheap at demo/standard scale, real cost at
    loadtest scale.
    """
    if not include_total:
        return None
    return session.scalar(select(func.count()).select_from(stmt.subquery()))
