"""SQLAlchemy models for the RCD Corp query API.

Hand-maintained against `foundation/data-dictionary/*.md`, not reflected —
`rcd_data`'s Postgres sink writes via plain `df.to_sql(...)` with no
declared schema (see `rcd_data/sinks/postgres_sink.py`), so there's nothing
to introspect at import time and no PK/FK constraints exist in the DB.
Every `id`/`*_id`/`*_sku` primary/foreign key column is a Python string
(`str(uuid4())`, or a SKU string for products), landing as Postgres TEXT —
hence `String` everywhere below, never `postgresql.UUID`.

`ForeignKey(...)` here is metadata-only: it documents a real logical FK
(per the data-dictionary docs) and lets SQLAlchemy build `relationship()`
joins, but it does not require — and the DB does not have — an actual FK
constraint. Only the FK columns actually used by a GraphQL nested field
(see `rcd_data/api/graphql/types.py`) get a `relationship()`; the rest stay
plain FK-documented columns to keep the mapped surface honest without
speculative joins.

`generate --sink postgres` does `DROP`+recreate every table on each run —
there are no migrations to keep in sync here, but it does mean the API can
transiently 500 if hit mid-`generate`.
"""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# Master data 

class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    email: Mapped[str | None] = mapped_column(String)
    phone: Mapped[str | None] = mapped_column(String)
    cpf_or_cnpj: Mapped[str | None] = mapped_column(String)
    segment: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String)
    state: Mapped[str | None] = mapped_column(String)
    city: Mapped[str | None] = mapped_column(String)
    signup_date: Mapped[date | None] = mapped_column(Date)
    ltv_tier: Mapped[str | None] = mapped_column(String)
    preferred_channel: Mapped[str | None] = mapped_column(String)
    loyalty_points: Mapped[int | None] = mapped_column(Integer)


class Supplier(Base):
    __tablename__ = "suppliers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String)
    rating: Mapped[float | None] = mapped_column(Float)
    lead_time_days: Mapped[int | None] = mapped_column(Integer)
    payment_terms: Mapped[str | None] = mapped_column(String)
    category: Mapped[str | None] = mapped_column(String)


class Product(Base):
    __tablename__ = "products"

    sku: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    category: Mapped[str | None] = mapped_column(String)
    subcategory: Mapped[str | None] = mapped_column(String)
    brand: Mapped[str | None] = mapped_column(String)
    cost: Mapped[float | None] = mapped_column(Float)
    price: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)
    margin: Mapped[float | None] = mapped_column(Float)
    supplier_id: Mapped[str | None] = mapped_column(ForeignKey("suppliers.id"))
    weight_kg: Mapped[float | None] = mapped_column(Float)
    launch_date: Mapped[date | None] = mapped_column(Date)
    is_active: Mapped[bool | None] = mapped_column(Boolean)


class Employee(Base):
    __tablename__ = "employees"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    email: Mapped[str | None] = mapped_column(String)
    department: Mapped[str | None] = mapped_column(String)
    role: Mapped[str | None] = mapped_column(String)
    manager_id: Mapped[str | None] = mapped_column(String)
    hire_date: Mapped[date | None] = mapped_column(Date)
    salary: Mapped[float | None] = mapped_column(Float)
    location: Mapped[str | None] = mapped_column(String)
    employment_type: Mapped[str | None] = mapped_column(String)
    level: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String)


class Store(Base):
    __tablename__ = "stores"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    region: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String)
    city: Mapped[str | None] = mapped_column(String)
    type: Mapped[str | None] = mapped_column(String)
    opening_date: Mapped[date | None] = mapped_column(Date)
    size_sqm: Mapped[int | None] = mapped_column(Integer)
    manager_employee_id: Mapped[str | None] = mapped_column(String)


class Warehouse(Base):
    __tablename__ = "warehouses"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    location: Mapped[str | None] = mapped_column(String)
    country: Mapped[str | None] = mapped_column(String)
    capacity_m3: Mapped[int | None] = mapped_column(Integer)
    manager_id: Mapped[str | None] = mapped_column(String)
    type: Mapped[str | None] = mapped_column(String)


# Sales 

class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str | None] = mapped_column(ForeignKey("customers.id"))
    store_id: Mapped[str | None] = mapped_column(ForeignKey("stores.id"))
    channel: Mapped[str | None] = mapped_column(String)
    marketplace: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String)
    subtotal: Mapped[float | None] = mapped_column(Float)
    shipping: Mapped[float | None] = mapped_column(Float)
    tax: Mapped[float | None] = mapped_column(Float)
    total: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)
    promo_code: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    date: Mapped[date | None] = mapped_column(Date)

    customer: Mapped[Customer | None] = relationship()
    store: Mapped[Store | None] = relationship()


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    product_id: Mapped[str | None] = mapped_column(ForeignKey("products.sku"))
    quantity: Mapped[int | None] = mapped_column(Integer)
    unit_price: Mapped[float | None] = mapped_column(Float)
    discount_pct: Mapped[float | None] = mapped_column(Float)
    line_total: Mapped[float | None] = mapped_column(Float)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    method: Mapped[str | None] = mapped_column(String)
    installments: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str | None] = mapped_column(String)
    gateway: Mapped[str | None] = mapped_column(String)
    processing_fee: Mapped[float | None] = mapped_column(Float)
    authorized_at: Mapped[datetime | None] = mapped_column(DateTime)
    currency: Mapped[str | None] = mapped_column(String)


# Finance

class Invoice(Base):
    __tablename__ = "invoices"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    customer_id: Mapped[str | None] = mapped_column(ForeignKey("customers.id"))
    amount: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)
    issued_at: Mapped[date | None] = mapped_column(Date)
    due_at: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str | None] = mapped_column(String)
    payment_method: Mapped[str | None] = mapped_column(String)

    customer: Mapped[Customer | None] = relationship()


# Support

class Ticket(Base):
    __tablename__ = "tickets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str | None] = mapped_column(ForeignKey("customers.id"))
    channel: Mapped[str | None] = mapped_column(String)
    category: Mapped[str | None] = mapped_column(String)
    subject: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String)
    priority: Mapped[str | None] = mapped_column(String)
    sentiment: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    resolved_at: Mapped[date | None] = mapped_column(Date)
    csat_score: Mapped[float | None] = mapped_column(Float)
    agent_id: Mapped[str | None] = mapped_column(ForeignKey("employees.id"))

    customer: Mapped[Customer | None] = relationship()
    agent: Mapped[Employee | None] = relationship()


# Marketing 


class Campaign(Base):
    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    type: Mapped[str | None] = mapped_column(String)
    channel: Mapped[str | None] = mapped_column(String)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    budget: Mapped[float | None] = mapped_column(Float)
    actual_spend: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)
    target_segment: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String)
    owner_employee_id: Mapped[str | None] = mapped_column(ForeignKey("employees.id"))


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str | None] = mapped_column(ForeignKey("customers.id"))
    source: Mapped[str | None] = mapped_column(String)
    campaign_id: Mapped[str | None] = mapped_column(ForeignKey("campaigns.id"))
    status: Mapped[str | None] = mapped_column(String)
    score: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)
    owner_employee_id: Mapped[str | None] = mapped_column(ForeignKey("employees.id"))

    customer: Mapped[Customer | None] = relationship()
    campaign: Mapped[Campaign | None] = relationship()
    owner: Mapped[Employee | None] = relationship()


# Supply chain


class Return(Base):
    __tablename__ = "returns"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    customer_id: Mapped[str | None] = mapped_column(ForeignKey("customers.id"))
    product_sku: Mapped[str | None] = mapped_column(ForeignKey("products.sku"))
    reason: Mapped[str | None] = mapped_column(String)
    quantity: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
    refund_amount: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)


class Shipment(Base):
    __tablename__ = "shipments"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str | None] = mapped_column(ForeignKey("orders.id"))
    warehouse_id: Mapped[str | None] = mapped_column(ForeignKey("warehouses.id"))
    carrier: Mapped[str | None] = mapped_column(String)
    tracking_number: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String)
    weight_kg: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[date | None] = mapped_column(Date)
    delivered_at: Mapped[date | None] = mapped_column(Date)

    warehouse: Mapped[Warehouse | None] = relationship()


class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    supplier_id: Mapped[str | None] = mapped_column(ForeignKey("suppliers.id"))
    warehouse_id: Mapped[str | None] = mapped_column(ForeignKey("warehouses.id"))
    status: Mapped[str | None] = mapped_column(String)
    total_amount: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(String)
    ordered_at: Mapped[date | None] = mapped_column(Date)
    expected_at: Mapped[date | None] = mapped_column(Date)
    received_at: Mapped[date | None] = mapped_column(Date)
    notes: Mapped[str | None] = mapped_column(Text)

    supplier: Mapped[Supplier | None] = relationship()
    warehouse: Mapped[Warehouse | None] = relationship()
