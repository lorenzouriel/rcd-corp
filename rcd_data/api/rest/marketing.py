"""REST endpoints for campaigns, leads — thin adapters over
`queries/marketing.py`.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import marketing as q
from ..schemas import CampaignOut, LeadOut, Page

router = APIRouter(tags=["marketing"])


@router.get("/campaigns", response_model=Page[CampaignOut])
def list_campaigns(
    type: str | None = None,
    channel: str | None = None,
    status: str | None = None,
    target_segment: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[CampaignOut]:
    try:
        result = q.list_campaigns(
            session, type=type, channel=channel, status=status,
            target_segment=target_segment, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[CampaignOut](
        items=[CampaignOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/campaigns/{campaign_id}", response_model=CampaignOut)
def get_campaign(campaign_id: str, session: Session = Depends(get_session)) -> CampaignOut:
    obj = q.get_campaign(session, campaign_id)
    if obj is None:
        raise HTTPException(404, "Campaign not found")
    return CampaignOut.model_validate(obj)


@router.get("/leads", response_model=Page[LeadOut])
def list_leads(
    status: str | None = None,
    source: str | None = None,
    campaign_id: str | None = None,
    owner_employee_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[LeadOut]:
    try:
        result = q.list_leads(
            session, status=status, source=source, campaign_id=campaign_id,
            owner_employee_id=owner_employee_id, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[LeadOut](
        items=[LeadOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/leads/{lead_id}", response_model=LeadOut)
def get_lead(lead_id: str, session: Session = Depends(get_session)) -> LeadOut:
    obj = q.get_lead(session, lead_id)
    if obj is None:
        raise HTTPException(404, "Lead not found")
    return LeadOut.model_validate(obj)
