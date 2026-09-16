"""Shared queries for invoices — called identically by REST and GraphQL."""
from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Invoice
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_invoices(
    session: Session,
    *,
    status: str | None = None,
    customer_id: str | None = None,
    order_id: str | None = None,
    issued_after: date | None = None,
    issued_before: date | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Invoice]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Invoice)
    if status:
        stmt = stmt.where(Invoice.status == status)
    if customer_id:
        stmt = stmt.where(Invoice.customer_id == customer_id)
    if order_id:
        stmt = stmt.where(Invoice.order_id == order_id)
    if issued_after:
        stmt = stmt.where(Invoice.issued_at >= issued_after)
    if issued_before:
        stmt = stmt.where(Invoice.issued_at < issued_before)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Invoice.issued_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_invoice(session: Session, invoice_id: str) -> Invoice | None:
    return session.get(Invoice, invoice_id)
