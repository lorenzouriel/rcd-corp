"""Pydantic response schemas for the REST surface.

REST-only — GraphQL has its own hand-written `strawberry.type` classes in
`graphql/types.py` (see the rationale in `foundation/` planning notes: the
pydantic->strawberry bridge only lifts scalar fields and knows nothing
about SQLAlchemy `relationship()`). Both are thin views over the same
`models.py` + `queries/*.py` — this file is not a second source of truth
for filtering, only for response shape.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int | None
    limit: int
    offset: int


class _ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# ── Master data ──────────────────────────────────────────────────────────


class CustomerOut(_ORMModel):
    id: str
    name: str | None
    email: str | None
    phone: str | None
    cpf_or_cnpj: str | None
    segment: str | None
    country: str | None
    state: str | None
    city: str | None
    signup_date: date | None
    ltv_tier: str | None
    preferred_channel: str | None
    loyalty_points: int | None


class ProductOut(_ORMModel):
    sku: str
    name: str | None
    category: str | None
    subcategory: str | None
    brand: str | None
    cost: float | None
    price: float | None
    currency: str | None
    margin: float | None
    supplier_id: str | None
    weight_kg: float | None
    launch_date: date | None
    is_active: bool | None


class EmployeeOut(_ORMModel):
    id: str
    name: str | None
    email: str | None
    department: str | None
    role: str | None
    manager_id: str | None
    hire_date: date | None
    salary: float | None
    location: str | None
    employment_type: str | None
    level: str | None
    country: str | None


class StoreOut(_ORMModel):
    id: str
    name: str | None
    region: str | None
    country: str | None
    city: str | None
    type: str | None
    opening_date: date | None
    size_sqm: int | None
    manager_employee_id: str | None


class WarehouseOut(_ORMModel):
    id: str
    name: str | None
    location: str | None
    country: str | None
    capacity_m3: int | None
    manager_id: str | None
    type: str | None


class SupplierOut(_ORMModel):
    id: str
    name: str | None
    country: str | None
    rating: float | None
    lead_time_days: int | None
    payment_terms: str | None
    category: str | None


# ── Sales ─────────────────────────────────────────────────────────────────


class OrderOut(_ORMModel):
    id: str
    customer_id: str | None
    store_id: str | None
    channel: str | None
    marketplace: str | None
    status: str | None
    subtotal: float | None
    shipping: float | None
    tax: float | None
    total: float | None
    currency: str | None
    promo_code: str | None
    created_at: datetime | None
    date: date | None


class OrderItemOut(_ORMModel):
    id: str
    order_id: str | None
    product_id: str | None
    quantity: int | None
    unit_price: float | None
    discount_pct: float | None
    line_total: float | None


class PaymentOut(_ORMModel):
    id: str
    order_id: str | None
    method: str | None
    installments: int | None
    status: str | None
    gateway: str | None
    processing_fee: float | None
    authorized_at: datetime | None
    currency: str | None


# ── Finance ───────────────────────────────────────────────────────────────


class InvoiceOut(_ORMModel):
    id: str
    order_id: str | None
    customer_id: str | None
    amount: float | None
    currency: str | None
    issued_at: date | None
    due_at: date | None
    status: str | None
    payment_method: str | None


# ── Support ───────────────────────────────────────────────────────────────


class TicketOut(_ORMModel):
    id: str
    customer_id: str | None
    channel: str | None
    category: str | None
    subject: str | None
    status: str | None
    priority: str | None
    sentiment: str | None
    created_at: datetime | None
    resolved_at: date | None  # generator quirk: date, not timestamp — see models.py::Ticket
    csat_score: float | None
    agent_id: str | None


# ── Marketing ─────────────────────────────────────────────────────────────


class CampaignOut(_ORMModel):
    id: str
    name: str | None
    type: str | None
    channel: str | None
    start_date: date | None
    end_date: date | None
    budget: float | None
    actual_spend: float | None
    currency: str | None
    target_segment: str | None
    status: str | None
    owner_employee_id: str | None


class LeadOut(_ORMModel):
    id: str
    customer_id: str | None
    source: str | None
    campaign_id: str | None
    status: str | None
    score: int | None
    created_at: datetime | None
    updated_at: datetime | None
    owner_employee_id: str | None


# ── Supply chain ──────────────────────────────────────────────────────────


class ReturnOut(_ORMModel):
    id: str
    order_id: str | None
    customer_id: str | None
    product_sku: str | None
    reason: str | None
    quantity: int | None
    status: str | None
    created_at: datetime | None
    refund_amount: float | None
    currency: str | None


class ShipmentOut(_ORMModel):
    id: str
    order_id: str | None
    warehouse_id: str | None
    carrier: str | None
    tracking_number: str | None
    status: str | None
    weight_kg: float | None
    created_at: date | None
    delivered_at: date | None


class PurchaseOrderOut(_ORMModel):
    id: str
    supplier_id: str | None
    warehouse_id: str | None
    status: str | None
    total_amount: float | None
    currency: str | None
    ordered_at: date | None
    expected_at: date | None
    received_at: date | None
    notes: str | None
