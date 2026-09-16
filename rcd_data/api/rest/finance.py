"""REST endpoints for invoices — thin adapters over `queries/finance.py`."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import finance as q
from ..schemas import InvoiceOut, Page

router = APIRouter(tags=["finance"])


@router.get("/invoices", response_model=Page[InvoiceOut])
def list_invoices(
    status: str | None = None,
    customer_id: str | None = None,
    order_id: str | None = None,
    issued_after: date | None = None,
    issued_before: date | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[InvoiceOut]:
    try:
        result = q.list_invoices(
            session, status=status, customer_id=customer_id, order_id=order_id,
            issued_after=issued_after, issued_before=issued_before,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[InvoiceOut](
        items=[InvoiceOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/invoices/{invoice_id}", response_model=InvoiceOut)
def get_invoice(invoice_id: str, session: Session = Depends(get_session)) -> InvoiceOut:
    obj = q.get_invoice(session, invoice_id)
    if obj is None:
        raise HTTPException(404, "Invoice not found")
    return InvoiceOut.model_validate(obj)
