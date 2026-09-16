"""REST endpoints for returns, shipments, purchase_orders — thin adapters
over `queries/supply_chain.py`.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import supply_chain as q
from ..schemas import Page, PurchaseOrderOut, ReturnOut, ShipmentOut

router = APIRouter(tags=["supply-chain"])


@router.get("/returns", response_model=Page[ReturnOut])
def list_returns(
    status: str | None = None,
    reason: str | None = None,
    customer_id: str | None = None,
    order_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[ReturnOut]:
    try:
        result = q.list_returns(
            session, status=status, reason=reason, customer_id=customer_id,
            order_id=order_id, limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[ReturnOut](
        items=[ReturnOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/returns/{return_id}", response_model=ReturnOut)
def get_return(return_id: str, session: Session = Depends(get_session)) -> ReturnOut:
    obj = q.get_return(session, return_id)
    if obj is None:
        raise HTTPException(404, "Return not found")
    return ReturnOut.model_validate(obj)


@router.get("/shipments", response_model=Page[ShipmentOut])
def list_shipments(
    status: str | None = None,
    carrier: str | None = None,
    warehouse_id: str | None = None,
    order_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[ShipmentOut]:
    try:
        result = q.list_shipments(
            session, status=status, carrier=carrier, warehouse_id=warehouse_id,
            order_id=order_id, limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[ShipmentOut](
        items=[ShipmentOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/shipments/{shipment_id}", response_model=ShipmentOut)
def get_shipment(shipment_id: str, session: Session = Depends(get_session)) -> ShipmentOut:
    obj = q.get_shipment(session, shipment_id)
    if obj is None:
        raise HTTPException(404, "Shipment not found")
    return ShipmentOut.model_validate(obj)


@router.get("/purchase-orders", response_model=Page[PurchaseOrderOut])
def list_purchase_orders(
    status: str | None = None,
    supplier_id: str | None = None,
    warehouse_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[PurchaseOrderOut]:
    try:
        result = q.list_purchase_orders(
            session, status=status, supplier_id=supplier_id, warehouse_id=warehouse_id,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[PurchaseOrderOut](
        items=[PurchaseOrderOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/purchase-orders/{purchase_order_id}", response_model=PurchaseOrderOut)
def get_purchase_order(
    purchase_order_id: str, session: Session = Depends(get_session)
) -> PurchaseOrderOut:
    obj = q.get_purchase_order(session, purchase_order_id)
    if obj is None:
        raise HTTPException(404, "Purchase order not found")
    return PurchaseOrderOut.model_validate(obj)
