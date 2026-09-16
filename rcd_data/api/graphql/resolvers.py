"""Per-domain GraphQL Query mixins — thin adapters over the exact same
`queries/*.py` functions the REST routers call (see `queries/common.py`
for the shared pagination/filter semantics both surfaces share). Combined
into one root `Query` type in `schema.py`.
"""
from __future__ import annotations

from datetime import date, datetime

import strawberry
from graphql import GraphQLError
from strawberry.types import Info

from ..queries import finance as finance_q
from ..queries import marketing as marketing_q
from ..queries import master_data as master_q
from ..queries import sales as sales_q
from ..queries import supply_chain as supply_q
from ..queries import support as support_q
from .types import (
    CampaignType,
    CustomerType,
    EmployeeType,
    InvoiceType,
    LeadType,
    OrderItemType,
    OrderType,
    Page,
    PaymentType,
    ProductType,
    PurchaseOrderType,
    ReturnType,
    ShipmentType,
    StoreType,
    SupplierType,
    TicketType,
    WarehouseType,
)


def _raise_graphql(exc: ValueError) -> None:
    raise GraphQLError(str(exc)) from exc


@strawberry.type
class MasterDataQueries:
    @strawberry.field
    def customers(
        self, info: Info,
        segment: str | None = None, country: str | None = None,
        ltv_tier: str | None = None, preferred_channel: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[CustomerType]:
        session = info.context["session"]
        try:
            result = master_q.list_customers(
                session, segment=segment, country=country, ltv_tier=ltv_tier,
                preferred_channel=preferred_channel, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[CustomerType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def customer(self, info: Info, id: str) -> CustomerType | None:
        obj = master_q.get_customer(info.context["session"], id)
        return CustomerType.from_model(obj) if obj else None

    @strawberry.field
    def products(
        self, info: Info,
        category: str | None = None, subcategory: str | None = None,
        brand: str | None = None, is_active: bool | None = None,
        supplier_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[ProductType]:
        session = info.context["session"]
        try:
            result = master_q.list_products(
                session, category=category, subcategory=subcategory, brand=brand,
                is_active=is_active, supplier_id=supplier_id, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[ProductType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def product(self, info: Info, sku: str) -> ProductType | None:
        obj = master_q.get_product(info.context["session"], sku)
        return ProductType.from_model(obj) if obj else None

    @strawberry.field
    def employees(
        self, info: Info,
        department: str | None = None, level: str | None = None,
        country: str | None = None, employment_type: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[EmployeeType]:
        session = info.context["session"]
        try:
            result = master_q.list_employees(
                session, department=department, level=level, country=country,
                employment_type=employment_type, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[EmployeeType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def employee(self, info: Info, id: str) -> EmployeeType | None:
        obj = master_q.get_employee(info.context["session"], id)
        return EmployeeType.from_model(obj) if obj else None

    @strawberry.field
    def stores(
        self, info: Info,
        type: str | None = None, region: str | None = None, country: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[StoreType]:
        session = info.context["session"]
        try:
            result = master_q.list_stores(
                session, type=type, region=region, country=country,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[StoreType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def store(self, info: Info, id: str) -> StoreType | None:
        obj = master_q.get_store(info.context["session"], id)
        return StoreType.from_model(obj) if obj else None

    @strawberry.field
    def warehouses(
        self, info: Info,
        type: str | None = None, country: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[WarehouseType]:
        session = info.context["session"]
        try:
            result = master_q.list_warehouses(
                session, type=type, country=country, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[WarehouseType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def warehouse(self, info: Info, id: str) -> WarehouseType | None:
        obj = master_q.get_warehouse(info.context["session"], id)
        return WarehouseType.from_model(obj) if obj else None

    @strawberry.field
    def suppliers(
        self, info: Info,
        category: str | None = None, country: str | None = None,
        payment_terms: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[SupplierType]:
        session = info.context["session"]
        try:
            result = master_q.list_suppliers(
                session, category=category, country=country, payment_terms=payment_terms,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[SupplierType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def supplier(self, info: Info, id: str) -> SupplierType | None:
        obj = master_q.get_supplier(info.context["session"], id)
        return SupplierType.from_model(obj) if obj else None


@strawberry.type
class SalesQueries:
    @strawberry.field
    def orders(
        self, info: Info,
        status: str | None = None, customer_id: str | None = None,
        store_id: str | None = None, channel: str | None = None,
        created_after: datetime | None = None, created_before: datetime | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[OrderType]:
        session = info.context["session"]
        try:
            result = sales_q.list_orders(
                session, status=status, customer_id=customer_id, store_id=store_id,
                channel=channel, created_after=created_after, created_before=created_before,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[OrderType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def order(self, info: Info, id: str) -> OrderType | None:
        obj = sales_q.get_order(info.context["session"], id)
        return OrderType.from_model(obj) if obj else None

    @strawberry.field
    def order_items(
        self, info: Info,
        order_id: str | None = None, product_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[OrderItemType]:
        session = info.context["session"]
        try:
            result = sales_q.list_order_items(
                session, order_id=order_id, product_id=product_id,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[OrderItemType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def order_item(self, info: Info, id: str) -> OrderItemType | None:
        obj = sales_q.get_order_item(info.context["session"], id)
        return OrderItemType.from_model(obj) if obj else None

    @strawberry.field
    def payments(
        self, info: Info,
        order_id: str | None = None, method: str | None = None,
        status: str | None = None, gateway: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[PaymentType]:
        session = info.context["session"]
        try:
            result = sales_q.list_payments(
                session, order_id=order_id, method=method, status=status, gateway=gateway,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[PaymentType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def payment(self, info: Info, id: str) -> PaymentType | None:
        obj = sales_q.get_payment(info.context["session"], id)
        return PaymentType.from_model(obj) if obj else None


@strawberry.type
class FinanceQueries:
    @strawberry.field
    def invoices(
        self, info: Info,
        status: str | None = None, customer_id: str | None = None, order_id: str | None = None,
        issued_after: date | None = None, issued_before: date | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[InvoiceType]:
        session = info.context["session"]
        try:
            result = finance_q.list_invoices(
                session, status=status, customer_id=customer_id, order_id=order_id,
                issued_after=issued_after, issued_before=issued_before,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[InvoiceType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def invoice(self, info: Info, id: str) -> InvoiceType | None:
        obj = finance_q.get_invoice(info.context["session"], id)
        return InvoiceType.from_model(obj) if obj else None


@strawberry.type
class SupportQueries:
    @strawberry.field
    def tickets(
        self, info: Info,
        status: str | None = None, priority: str | None = None, category: str | None = None,
        customer_id: str | None = None, agent_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[TicketType]:
        session = info.context["session"]
        try:
            result = support_q.list_tickets(
                session, status=status, priority=priority, category=category,
                customer_id=customer_id, agent_id=agent_id,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[TicketType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def ticket(self, info: Info, id: str) -> TicketType | None:
        obj = support_q.get_ticket(info.context["session"], id)
        return TicketType.from_model(obj) if obj else None


@strawberry.type
class MarketingQueries:
    @strawberry.field
    def campaigns(
        self, info: Info,
        type: str | None = None, channel: str | None = None,
        status: str | None = None, target_segment: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[CampaignType]:
        session = info.context["session"]
        try:
            result = marketing_q.list_campaigns(
                session, type=type, channel=channel, status=status,
                target_segment=target_segment, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[CampaignType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def campaign(self, info: Info, id: str) -> CampaignType | None:
        obj = marketing_q.get_campaign(info.context["session"], id)
        return CampaignType.from_model(obj) if obj else None

    @strawberry.field
    def leads(
        self, info: Info,
        status: str | None = None, source: str | None = None,
        campaign_id: str | None = None, owner_employee_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[LeadType]:
        session = info.context["session"]
        try:
            result = marketing_q.list_leads(
                session, status=status, source=source, campaign_id=campaign_id,
                owner_employee_id=owner_employee_id, limit=limit, offset=offset,
                include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[LeadType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def lead(self, info: Info, id: str) -> LeadType | None:
        obj = marketing_q.get_lead(info.context["session"], id)
        return LeadType.from_model(obj) if obj else None


@strawberry.type
class SupplyChainQueries:
    # Named `returnById`/`returns` rather than `return`/Python `return_` to
    # avoid colliding with the `return` keyword as a Python method name.
    @strawberry.field
    def returns(
        self, info: Info,
        status: str | None = None, reason: str | None = None,
        customer_id: str | None = None, order_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[ReturnType]:
        session = info.context["session"]
        try:
            result = supply_q.list_returns(
                session, status=status, reason=reason, customer_id=customer_id,
                order_id=order_id, limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[ReturnType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field(name="returnById")
    def return_by_id(self, info: Info, id: str) -> ReturnType | None:
        obj = supply_q.get_return(info.context["session"], id)
        return ReturnType.from_model(obj) if obj else None

    @strawberry.field
    def shipments(
        self, info: Info,
        status: str | None = None, carrier: str | None = None,
        warehouse_id: str | None = None, order_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[ShipmentType]:
        session = info.context["session"]
        try:
            result = supply_q.list_shipments(
                session, status=status, carrier=carrier, warehouse_id=warehouse_id,
                order_id=order_id, limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[ShipmentType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def shipment(self, info: Info, id: str) -> ShipmentType | None:
        obj = supply_q.get_shipment(info.context["session"], id)
        return ShipmentType.from_model(obj) if obj else None

    @strawberry.field
    def purchase_orders(
        self, info: Info,
        status: str | None = None, supplier_id: str | None = None,
        warehouse_id: str | None = None,
        limit: int = 50, offset: int = 0, include_total: bool = True,
    ) -> Page[PurchaseOrderType]:
        session = info.context["session"]
        try:
            result = supply_q.list_purchase_orders(
                session, status=status, supplier_id=supplier_id, warehouse_id=warehouse_id,
                limit=limit, offset=offset, include_total=include_total,
            )
        except ValueError as e:
            _raise_graphql(e)
        return Page(
            items=[PurchaseOrderType.from_model(o) for o in result.items],
            total=result.total, limit=result.limit, offset=result.offset,
        )

    @strawberry.field
    def purchase_order(self, info: Info, id: str) -> PurchaseOrderType | None:
        obj = supply_q.get_purchase_order(info.context["session"], id)
        return PurchaseOrderType.from_model(obj) if obj else None
