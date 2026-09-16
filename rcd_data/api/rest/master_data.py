"""REST endpoints for customers, products, employees, stores, warehouses,
suppliers — thin adapters over `queries/master_data.py`.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..queries import master_data as q
from ..schemas import (
    CustomerOut,
    EmployeeOut,
    Page,
    ProductOut,
    StoreOut,
    SupplierOut,
    WarehouseOut,
)

router = APIRouter(tags=["master-data"])


@router.get("/customers", response_model=Page[CustomerOut])
def list_customers(
    segment: str | None = None,
    country: str | None = None,
    ltv_tier: str | None = None,
    preferred_channel: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[CustomerOut]:
    try:
        result = q.list_customers(
            session, segment=segment, country=country, ltv_tier=ltv_tier,
            preferred_channel=preferred_channel, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[CustomerOut](
        items=[CustomerOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/customers/{customer_id}", response_model=CustomerOut)
def get_customer(customer_id: str, session: Session = Depends(get_session)) -> CustomerOut:
    obj = q.get_customer(session, customer_id)
    if obj is None:
        raise HTTPException(404, "Customer not found")
    return CustomerOut.model_validate(obj)


@router.get("/products", response_model=Page[ProductOut])
def list_products(
    category: str | None = None,
    subcategory: str | None = None,
    brand: str | None = None,
    is_active: bool | None = None,
    supplier_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[ProductOut]:
    try:
        result = q.list_products(
            session, category=category, subcategory=subcategory, brand=brand,
            is_active=is_active, supplier_id=supplier_id, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[ProductOut](
        items=[ProductOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/products/{sku}", response_model=ProductOut)
def get_product(sku: str, session: Session = Depends(get_session)) -> ProductOut:
    obj = q.get_product(session, sku)
    if obj is None:
        raise HTTPException(404, "Product not found")
    return ProductOut.model_validate(obj)


@router.get("/employees", response_model=Page[EmployeeOut])
def list_employees(
    department: str | None = None,
    level: str | None = None,
    country: str | None = None,
    employment_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[EmployeeOut]:
    try:
        result = q.list_employees(
            session, department=department, level=level, country=country,
            employment_type=employment_type, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[EmployeeOut](
        items=[EmployeeOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/employees/{employee_id}", response_model=EmployeeOut)
def get_employee(employee_id: str, session: Session = Depends(get_session)) -> EmployeeOut:
    obj = q.get_employee(session, employee_id)
    if obj is None:
        raise HTTPException(404, "Employee not found")
    return EmployeeOut.model_validate(obj)


@router.get("/stores", response_model=Page[StoreOut])
def list_stores(
    type: str | None = None,
    region: str | None = None,
    country: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[StoreOut]:
    try:
        result = q.list_stores(
            session, type=type, region=region, country=country,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[StoreOut](
        items=[StoreOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/stores/{store_id}", response_model=StoreOut)
def get_store(store_id: str, session: Session = Depends(get_session)) -> StoreOut:
    obj = q.get_store(session, store_id)
    if obj is None:
        raise HTTPException(404, "Store not found")
    return StoreOut.model_validate(obj)


@router.get("/warehouses", response_model=Page[WarehouseOut])
def list_warehouses(
    type: str | None = None,
    country: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[WarehouseOut]:
    try:
        result = q.list_warehouses(
            session, type=type, country=country, limit=limit, offset=offset,
            include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[WarehouseOut](
        items=[WarehouseOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/warehouses/{warehouse_id}", response_model=WarehouseOut)
def get_warehouse(warehouse_id: str, session: Session = Depends(get_session)) -> WarehouseOut:
    obj = q.get_warehouse(session, warehouse_id)
    if obj is None:
        raise HTTPException(404, "Warehouse not found")
    return WarehouseOut.model_validate(obj)


@router.get("/suppliers", response_model=Page[SupplierOut])
def list_suppliers(
    category: str | None = None,
    country: str | None = None,
    payment_terms: str | None = None,
    limit: int = 50,
    offset: int = 0,
    total: bool = True,
    session: Session = Depends(get_session),
) -> Page[SupplierOut]:
    try:
        result = q.list_suppliers(
            session, category=category, country=country, payment_terms=payment_terms,
            limit=limit, offset=offset, include_total=total,
        )
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    return Page[SupplierOut](
        items=[SupplierOut.model_validate(o) for o in result.items],
        total=result.total, limit=result.limit, offset=result.offset,
    )


@router.get("/suppliers/{supplier_id}", response_model=SupplierOut)
def get_supplier(supplier_id: str, session: Session = Depends(get_session)) -> SupplierOut:
    obj = q.get_supplier(session, supplier_id)
    if obj is None:
        raise HTTPException(404, "Supplier not found")
    return SupplierOut.model_validate(obj)
