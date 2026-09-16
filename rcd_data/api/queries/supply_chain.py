"""Shared queries for returns, shipments, purchase_orders — called
identically by REST and GraphQL.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import PurchaseOrder, Return, Shipment
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_returns(
    session: Session,
    *,
    status: str | None = None,
    reason: str | None = None,
    customer_id: str | None = None,
    order_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Return]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Return)
    if status:
        stmt = stmt.where(Return.status == status)
    if reason:
        stmt = stmt.where(Return.reason == reason)
    if customer_id:
        stmt = stmt.where(Return.customer_id == customer_id)
    if order_id:
        stmt = stmt.where(Return.order_id == order_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Return.created_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_return(session: Session, return_id: str) -> Return | None:
    return session.get(Return, return_id)


def list_shipments(
    session: Session,
    *,
    status: str | None = None,
    carrier: str | None = None,
    warehouse_id: str | None = None,
    order_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Shipment]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Shipment)
    if status:
        stmt = stmt.where(Shipment.status == status)
    if carrier:
        stmt = stmt.where(Shipment.carrier == carrier)
    if warehouse_id:
        stmt = stmt.where(Shipment.warehouse_id == warehouse_id)
    if order_id:
        stmt = stmt.where(Shipment.order_id == order_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Shipment.created_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_shipment(session: Session, shipment_id: str) -> Shipment | None:
    return session.get(Shipment, shipment_id)


def list_purchase_orders(
    session: Session,
    *,
    status: str | None = None,
    supplier_id: str | None = None,
    warehouse_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[PurchaseOrder]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(PurchaseOrder)
    if status:
        stmt = stmt.where(PurchaseOrder.status == status)
    if supplier_id:
        stmt = stmt.where(PurchaseOrder.supplier_id == supplier_id)
    if warehouse_id:
        stmt = stmt.where(PurchaseOrder.warehouse_id == warehouse_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(PurchaseOrder.ordered_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_purchase_order(session: Session, purchase_order_id: str) -> PurchaseOrder | None:
    return session.get(PurchaseOrder, purchase_order_id)
