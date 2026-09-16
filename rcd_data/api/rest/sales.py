"""REST endpoints for orders, order_items, payments — thin adapters over
`queries/sales.py`.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import sales as q
from ..schemas import OrderItemOut, OrderOut, Page, PaymentOut

router = APIRouter(tags=["sales"])


@router.get("/orders", response_model=Page[OrderOut])
def list_orders(
    status: str | None = None,
    customer_id: str | None = None,
    store_id: str | None = None,
    channel: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[OrderOut]:
    try:
        result = q.list_orders(
            session, status=status, customer_id=customer_id, store_id=store_id,
            channel=channel, created_after=created_after, created_before=created_before,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[OrderOut](
        items=[OrderOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/orders/{order_id}", response_model=OrderOut)
def get_order(order_id: str, session: Session = Depends(get_session)) -> OrderOut:
    obj = q.get_order(session, order_id)
    if obj is None:
        raise HTTPException(404, "Order not found")
    return OrderOut.model_validate(obj)


@router.get("/order-items", response_model=Page[OrderItemOut])
def list_order_items(
    order_id: str | None = None,
    product_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[OrderItemOut]:
    try:
        result = q.list_order_items(
            session, order_id=order_id, product_id=product_id,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[OrderItemOut](
        items=[OrderItemOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/order-items/{order_item_id}", response_model=OrderItemOut)
def get_order_item(order_item_id: str, session: Session = Depends(get_session)) -> OrderItemOut:
    obj = q.get_order_item(session, order_item_id)
    if obj is None:
        raise HTTPException(404, "Order item not found")
    return OrderItemOut.model_validate(obj)


@router.get("/payments", response_model=Page[PaymentOut])
def list_payments(
    order_id: str | None = None,
    method: str | None = None,
    status: str | None = None,
    gateway: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[PaymentOut]:
    try:
        result = q.list_payments(
            session, order_id=order_id, method=method, status=status, gateway=gateway,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[PaymentOut](
        items=[PaymentOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/payments/{payment_id}", response_model=PaymentOut)
def get_payment(payment_id: str, session: Session = Depends(get_session)) -> PaymentOut:
    obj = q.get_payment(session, payment_id)
    if obj is None:
        raise HTTPException(404, "Payment not found")
    return PaymentOut.model_validate(obj)
