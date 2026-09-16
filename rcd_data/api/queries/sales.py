"""Shared queries for orders, order_items, payments — called identically by
REST and GraphQL.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Order, OrderItem, Payment
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_orders(
    session: Session,
    *,
    status: str | None = None,
    customer_id: str | None = None,
    store_id: str | None = None,
    channel: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Order]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Order)
    if status:
        stmt = stmt.where(Order.status == status)
    if customer_id:
        stmt = stmt.where(Order.customer_id == customer_id)
    if store_id:
        stmt = stmt.where(Order.store_id == store_id)
    if channel:
        stmt = stmt.where(Order.channel == channel)
    if created_after:
        stmt = stmt.where(Order.created_at >= created_after)
    if created_before:
        stmt = stmt.where(Order.created_at < created_before)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Order.created_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_order(session: Session, order_id: str) -> Order | None:
    return session.get(Order, order_id)


def list_order_items(
    session: Session,
    *,
    order_id: str | None = None,
    product_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[OrderItem]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(OrderItem)
    if order_id:
        stmt = stmt.where(OrderItem.order_id == order_id)
    if product_id:
        stmt = stmt.where(OrderItem.product_id == product_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(OrderItem.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_order_item(session: Session, order_item_id: str) -> OrderItem | None:
    return session.get(OrderItem, order_item_id)


def list_payments(
    session: Session,
    *,
    order_id: str | None = None,
    method: str | None = None,
    status: str | None = None,
    gateway: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Payment]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Payment)
    if order_id:
        stmt = stmt.where(Payment.order_id == order_id)
    if method:
        stmt = stmt.where(Payment.method == method)
    if status:
        stmt = stmt.where(Payment.status == status)
    if gateway:
        stmt = stmt.where(Payment.gateway == gateway)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Payment.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_payment(session: Session, payment_id: str) -> Payment | None:
    return session.get(Payment, payment_id)
