"""Combines the per-domain Query mixins into one schema and builds the
GraphQL router. Mounted (with the API-key dependency) at `/graphql` in
`app.py`.
"""
from __future__ import annotations

import strawberry
from fastapi import Depends
from sqlalchemy.orm import Session
from strawberry.fastapi import GraphQLRouter

from ..db import get_session
from .dataloaders import build_dataloaders
from .resolvers import (
    FinanceQueries,
    MarketingQueries,
    MasterDataQueries,
    SalesQueries,
    SupplyChainQueries,
    SupportQueries,
)


@strawberry.type
class Query(
    MasterDataQueries,
    SalesQueries,
    FinanceQueries,
    SupportQueries,
    MarketingQueries,
    SupplyChainQueries,
):
    pass


schema = strawberry.Schema(query=Query)


async def get_context(session: Session = Depends(get_session)) -> dict:
    return {"session": session, **build_dataloaders(session)}


# graphql_ide defaults to GraphiQL
graphql_router = GraphQLRouter(schema, context_getter=get_context)
