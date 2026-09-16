"""Shared queries for tickets — called identically by REST and GraphQL."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Ticket
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_tickets(
    session: Session,
    *,
    status: str | None = None,
    priority: str | None = None,
    category: str | None = None,
    customer_id: str | None = None,
    agent_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Ticket]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Ticket)
    if status:
        stmt = stmt.where(Ticket.status == status)
    if priority:
        stmt = stmt.where(Ticket.priority == priority)
    if category:
        stmt = stmt.where(Ticket.category == category)
    if customer_id:
        stmt = stmt.where(Ticket.customer_id == customer_id)
    if agent_id:
        stmt = stmt.where(Ticket.agent_id == agent_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Ticket.created_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_ticket(session: Session, ticket_id: str) -> Ticket | None:
    return session.get(Ticket, ticket_id)
