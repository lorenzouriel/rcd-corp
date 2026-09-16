"""Hand-written Strawberry GraphQL types.

Not derived from the REST Pydantic schemas (`schemas.py`) or auto-mapped
from the SQLAlchemy models (`models.py`) via
`strawberry.experimental.pydantic.type` — that bridge only lifts scalar
fields and knows nothing about SQLAlchemy `relationship()`, so nested
fields (`order { customer { name } }`) would need hand-written resolvers on
top of it anyway. Each type's `from_model()` reuses the matching REST
schema purely to avoid re-typing the ORM->dict mapping twice; the GraphQL
field *declarations* below are still the single source of truth for what
GraphQL exposes.

Relationship fields are resolved via per-request DataLoaders (see
`dataloaders.py`) to avoid N+1 queries — only the FK pairs actually
exposed as nested fields get a loader; every other FK stays a plain scalar
id field, same as REST.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Generic, TypeVar

import strawberry
from strawberry.types import Info

from .. import schemas

GenericType = TypeVar("GenericType")


@strawberry.type
class Page(Generic[GenericType]):
    items: list[GenericType]
    total: int | None
    limit: int
    offset: int


# ── Master data ──────────────────────────────────────────────────────────


@strawberry.type
class CustomerType:
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

    @classmethod
    def from_model(cls, obj) -> CustomerType:
        return cls(**schemas.CustomerOut.model_validate(obj).model_dump())


@strawberry.type
class SupplierType:
    id: str
    name: str | None
    country: str | None
    rating: float | None
    lead_time_days: int | None
    payment_terms: str | None
    category: str | None

    @classmethod
    def from_model(cls, obj) -> SupplierType:
        return cls(**schemas.SupplierOut.model_validate(obj).model_dump())


@strawberry.type
class ProductType:
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

    @classmethod
    def from_model(cls, obj) -> ProductType:
        return cls(**schemas.ProductOut.model_validate(obj).model_dump())


@strawberry.type
class EmployeeType:
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

    @classmethod
    def from_model(cls, obj) -> EmployeeType:
        return cls(**schemas.EmployeeOut.model_validate(obj).model_dump())


@strawberry.type
class StoreType:
    id: str
    name: str | None
    region: str | None
    country: str | None
    city: str | None
    type: str | None
    opening_date: date | None
    size_sqm: int | None
    manager_employee_id: str | None

    @classmethod
    def from_model(cls, obj) -> StoreType:
        return cls(**schemas.StoreOut.model_validate(obj).model_dump())


@strawberry.type
class WarehouseType:
    id: str
    name: str | None
    location: str | None
    country: str | None
    capacity_m3: int | None
    manager_id: str | None
    type: str | None

    @classmethod
    def from_model(cls, obj) -> WarehouseType:
        return cls(**schemas.WarehouseOut.model_validate(obj).model_dump())


# ── Sales ─────────────────────────────────────────────────────────────────


@strawberry.type
class OrderType:
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

    @classmethod
    def from_model(cls, obj) -> OrderType:
        return cls(**schemas.OrderOut.model_validate(obj).model_dump())

    @strawberry.field
    async def customer(self, info: Info) -> CustomerType | None:
        if not self.customer_id:
            return None
        obj = await info.context["customer_loader"].load(self.customer_id)
        return CustomerType.from_model(obj) if obj else None

    @strawberry.field
    async def store(self, info: Info) -> StoreType | None:
        if not self.store_id:
            return None
        obj = await info.context["store_loader"].load(self.store_id)
        return StoreType.from_model(obj) if obj else None


@strawberry.type
class OrderItemType:
    id: str
    order_id: str | None
    product_id: str | None
    quantity: int | None
    unit_price: float | None
    discount_pct: float | None
    line_total: float | None

    @classmethod
    def from_model(cls, obj) -> OrderItemType:
        return cls(**schemas.OrderItemOut.model_validate(obj).model_dump())


@strawberry.type
class PaymentType:
    id: str
    order_id: str | None
    method: str | None
    installments: int | None
    status: str | None
    gateway: str | None
    processing_fee: float | None
    authorized_at: datetime | None
    currency: str | None

    @classmethod
    def from_model(cls, obj) -> PaymentType:
        return cls(**schemas.PaymentOut.model_validate(obj).model_dump())


# ── Finance ───────────────────────────────────────────────────────────────


@strawberry.type
class InvoiceType:
    id: str
    order_id: str | None
    customer_id: str | None
    amount: float | None
    currency: str | None
    issued_at: date | None
    due_at: date | None
    status: str | None
    payment_method: str | None

    @classmethod
    def from_model(cls, obj) -> InvoiceType:
        return cls(**schemas.InvoiceOut.model_validate(obj).model_dump())

    @strawberry.field
    async def customer(self, info: Info) -> CustomerType | None:
        if not self.customer_id:
            return None
        obj = await info.context["customer_loader"].load(self.customer_id)
        return CustomerType.from_model(obj) if obj else None


# ── Support ───────────────────────────────────────────────────────────────


@strawberry.type
class TicketType:
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

    @classmethod
    def from_model(cls, obj) -> TicketType:
        return cls(**schemas.TicketOut.model_validate(obj).model_dump())

    @strawberry.field
    async def customer(self, info: Info) -> CustomerType | None:
        if not self.customer_id:
            return None
        obj = await info.context["customer_loader"].load(self.customer_id)
        return CustomerType.from_model(obj) if obj else None

    @strawberry.field
    async def agent(self, info: Info) -> EmployeeType | None:
        if not self.agent_id:
            return None
        obj = await info.context["employee_loader"].load(self.agent_id)
        return EmployeeType.from_model(obj) if obj else None


# ── Marketing ─────────────────────────────────────────────────────────────


@strawberry.type
class CampaignType:
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

    @classmethod
    def from_model(cls, obj) -> CampaignType:
        return cls(**schemas.CampaignOut.model_validate(obj).model_dump())


@strawberry.type
class LeadType:
    id: str
    customer_id: str | None
    source: str | None
    campaign_id: str | None
    status: str | None
    score: int | None
    created_at: datetime | None
    updated_at: datetime | None
    owner_employee_id: str | None

    @classmethod
    def from_model(cls, obj) -> LeadType:
        return cls(**schemas.LeadOut.model_validate(obj).model_dump())

    @strawberry.field
    async def customer(self, info: Info) -> CustomerType | None:
        if not self.customer_id:
            return None
        obj = await info.context["customer_loader"].load(self.customer_id)
        return CustomerType.from_model(obj) if obj else None

    @strawberry.field
    async def campaign(self, info: Info) -> CampaignType | None:
        if not self.campaign_id:
            return None
        obj = await info.context["campaign_loader"].load(self.campaign_id)
        return CampaignType.from_model(obj) if obj else None

    @strawberry.field
    async def owner(self, info: Info) -> EmployeeType | None:
        if not self.owner_employee_id:
            return None
        obj = await info.context["employee_loader"].load(self.owner_employee_id)
        return EmployeeType.from_model(obj) if obj else None


# ── Supply chain ──────────────────────────────────────────────────────────


@strawberry.type
class ReturnType:
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

    @classmethod
    def from_model(cls, obj) -> ReturnType:
        return cls(**schemas.ReturnOut.model_validate(obj).model_dump())


@strawberry.type
class ShipmentType:
    id: str
    order_id: str | None
    warehouse_id: str | None
    carrier: str | None
    tracking_number: str | None
    status: str | None
    weight_kg: float | None
    created_at: date | None
    delivered_at: date | None

    @classmethod
    def from_model(cls, obj) -> ShipmentType:
        return cls(**schemas.ShipmentOut.model_validate(obj).model_dump())

    @strawberry.field
    async def warehouse(self, info: Info) -> WarehouseType | None:
        if not self.warehouse_id:
            return None
        obj = await info.context["warehouse_loader"].load(self.warehouse_id)
        return WarehouseType.from_model(obj) if obj else None


@strawberry.type
class PurchaseOrderType:
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

    @classmethod
    def from_model(cls, obj) -> PurchaseOrderType:
        return cls(**schemas.PurchaseOrderOut.model_validate(obj).model_dump())

    @strawberry.field
    async def supplier(self, info: Info) -> SupplierType | None:
        if not self.supplier_id:
            return None
        obj = await info.context["supplier_loader"].load(self.supplier_id)
        return SupplierType.from_model(obj) if obj else None

    @strawberry.field
    async def warehouse(self, info: Info) -> WarehouseType | None:
        if not self.warehouse_id:
            return None
        obj = await info.context["warehouse_loader"].load(self.warehouse_id)
        return WarehouseType.from_model(obj) if obj else None
