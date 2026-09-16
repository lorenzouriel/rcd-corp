"""Shared queries for customers, products, employees, stores, warehouses,
suppliers — called identically by REST and GraphQL.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Customer, Employee, Product, Store, Supplier, Warehouse
from .common import DEFAULT_LIMIT, PageResult, clamp_pagination, compute_total


def list_customers(
    session: Session,
    *,
    segment: str | None = None,
    country: str | None = None,
    ltv_tier: str | None = None,
    preferred_channel: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Customer]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Customer)
    if segment:
        stmt = stmt.where(Customer.segment == segment)
    if country:
        stmt = stmt.where(Customer.country == country)
    if ltv_tier:
        stmt = stmt.where(Customer.ltv_tier == ltv_tier)
    if preferred_channel:
        stmt = stmt.where(Customer.preferred_channel == preferred_channel)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Customer.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_customer(session: Session, customer_id: str) -> Customer | None:
    return session.get(Customer, customer_id)


def list_products(
    session: Session,
    *,
    category: str | None = None,
    subcategory: str | None = None,
    brand: str | None = None,
    is_active: bool | None = None,
    supplier_id: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Product]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Product)
    if category:
        stmt = stmt.where(Product.category == category)
    if subcategory:
        stmt = stmt.where(Product.subcategory == subcategory)
    if brand:
        stmt = stmt.where(Product.brand == brand)
    if is_active is not None:
        stmt = stmt.where(Product.is_active == is_active)
    if supplier_id:
        stmt = stmt.where(Product.supplier_id == supplier_id)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Product.sku).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_product(session: Session, sku: str) -> Product | None:
    return session.get(Product, sku)


def list_employees(
    session: Session,
    *,
    department: str | None = None,
    level: str | None = None,
    country: str | None = None,
    employment_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Employee]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Employee)
    if department:
        stmt = stmt.where(Employee.department == department)
    if level:
        stmt = stmt.where(Employee.level == level)
    if country:
        stmt = stmt.where(Employee.country == country)
    if employment_type:
        stmt = stmt.where(Employee.employment_type == employment_type)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Employee.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_employee(session: Session, employee_id: str) -> Employee | None:
    return session.get(Employee, employee_id)


def list_stores(
    session: Session,
    *,
    type: str | None = None,
    region: str | None = None,
    country: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Store]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Store)
    if type:
        stmt = stmt.where(Store.type == type)
    if region:
        stmt = stmt.where(Store.region == region)
    if country:
        stmt = stmt.where(Store.country == country)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Store.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_store(session: Session, store_id: str) -> Store | None:
    return session.get(Store, store_id)


def list_warehouses(
    session: Session,
    *,
    type: str | None = None,
    country: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Warehouse]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Warehouse)
    if type:
        stmt = stmt.where(Warehouse.type == type)
    if country:
        stmt = stmt.where(Warehouse.country == country)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Warehouse.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_warehouse(session: Session, warehouse_id: str) -> Warehouse | None:
    return session.get(Warehouse, warehouse_id)


def list_suppliers(
    session: Session,
    *,
    category: str | None = None,
    country: str | None = None,
    payment_terms: str | None = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    include_total: bool = True,
) -> PageResult[Supplier]:
    limit, offset = clamp_pagination(limit, offset)
    stmt = select(Supplier)
    if category:
        stmt = stmt.where(Supplier.category == category)
    if country:
        stmt = stmt.where(Supplier.country == country)
    if payment_terms:
        stmt = stmt.where(Supplier.payment_terms == payment_terms)

    total = compute_total(session, stmt, include_total)
    stmt = stmt.order_by(Supplier.id).limit(limit).offset(offset)
    items = session.scalars(stmt).all()
    return PageResult(items=list(items), total=total, limit=limit, offset=offset)


def get_supplier(session: Session, supplier_id: str) -> Supplier | None:
    return session.get(Supplier, supplier_id)
