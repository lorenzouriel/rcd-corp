"""REST endpoints for tickets — thin adapters over `queries/support.py`."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import support as q
from ..schemas import Page, TicketOut

router = APIRouter(tags=["support"])


@router.get("/tickets", response_model=Page[TicketOut])
def list_tickets(
    status: str | None = None,
    priority: str | None = None,
    category: str | None = None,
    customer_id: str | None = None,
    agent_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[TicketOut]:
    try:
        result = q.list_tickets(
            session, status=status, priority=priority, category=category,
            customer_id=customer_id, agent_id=agent_id,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[TicketOut](
        items=[TicketOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/tickets/{ticket_id}", response_model=TicketOut)
def get_ticket(ticket_id: str, session: Session = Depends(get_session)) -> TicketOut:
    obj = q.get_ticket(session, ticket_id)
    if obj is None:
        raise HTTPException(404, "Ticket not found")
    return TicketOut.model_validate(obj)
