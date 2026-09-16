"""Shared queries for campaigns, leads — called identically by REST and
GraphQL.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Campaign, Lead
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_campaigns(
    session: Session,
    *,
    type: str | None = None,
    channel: str | None = None,
    status: str | None = None,
    target_segment: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Campaign]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Campaign)
    if type:
        stmt = stmt.where(Campaign.type == type)
    if channel:
        stmt = stmt.where(Campaign.channel == channel)
    if status:
        stmt = stmt.where(Campaign.status == status)
    if target_segment:
        stmt = stmt.where(Campaign.target_segment == target_segment)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Campaign.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_campaign(session: Session, campaign_id: str) -> Campaign | None:
    return session.get(Campaign, campaign_id)


def list_leads(
    session: Session,
    *,
    status: str | None = None,
    source: str | None = None,
    campaign_id: str | None = None,
    owner_employee_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Lead]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Lead)
    if status:
        stmt = stmt.where(Lead.status == status)
    if source:
        stmt = stmt.where(Lead.source == source)
    if campaign_id:
        stmt = stmt.where(Lead.campaign_id == campaign_id)
    if owner_employee_id:
        stmt = stmt.where(Lead.owner_employee_id == owner_employee_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Lead.created_at.desc()).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_lead(session: Session, lead_id: str) -> Lead | None:
    return session.get(Lead, lead_id)
