"""Aggregates every domain's APIRouter onto a single router for `app.py`
to mount (with the API-key dependency applied once, at mount time).
"""
from __future__ import annotations

from fastapi import APIRouter

from . import finance, marketing, master_data, sales, supply_chain, support

router = APIRouter(prefix="/api/v1")
router.include_router(master_data.router)
router.include_router(sales.router)
router.include_router(finance.router)
router.include_router(support.router)
router.include_router(marketing.router)
router.include_router(supply_chain.router)
