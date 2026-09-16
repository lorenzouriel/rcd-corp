"""Per-request DataLoaders backing the GraphQL relationship fields declared
in `types.py`. Built fresh per request (see `schema.py`'s `get_context`) so
loader caches never leak across requests.

Only the FK targets actually exposed as nested fields get a loader here —
not every FK in `models.py` speculatively. If a new relationship field is
added to `types.py`, add its loader here too.
"""
from __future__ import annotations

from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session
from strawberry.dataloader import DataLoader

from ..models import Campaign, Customer, Employee, Store, Supplier, Warehouse

ModelT = TypeVar("ModelT")


def _make_loader(
    session: Session, model: type[ModelT], pk_attr: str
) -> DataLoader[str, ModelT | None]:
    pk_col = getattr(model, pk_attr)

    async def load_fn(keys: list[str]) -> list[ModelT | None]:
        rows = session.scalars(select(model).where(pk_col.in_(keys))).all()
        by_key = {getattr(row, pk_attr): row for row in rows}
        return [by_key.get(key) for key in keys]

    return DataLoader(load_fn=load_fn)


def build_dataloaders(session: Session) -> dict[str, DataLoader]:
    return {
        "customer_loader": _make_loader(session, Customer, "id"),
        "store_loader": _make_loader(session, Store, "id"),
        "employee_loader": _make_loader(session, Employee, "id"),
        "campaign_loader": _make_loader(session, Campaign, "id"),
        "warehouse_loader": _make_loader(session, Warehouse, "id"),
        "supplier_loader": _make_loader(session, Supplier, "id"),
    }
