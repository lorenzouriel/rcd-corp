"""Supply chain & inventory generator for RCD Corp."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid, generate_tracking_number
from ..utils.state_machines import ShipmentTracking
from ..utils.time_utils import random_date, random_datetime

RETURN_REASONS = [
    "defective", "wrong_item", "changed_mind", "damaged_in_transit",
    "not_as_described", "late_delivery", "duplicate_order",
]
RETURN_REASON_WEIGHTS = [0.25, 0.15, 0.20, 0.15, 0.10, 0.10, 0.05]

MOVEMENT_TYPES = ["inbound", "outbound", "transfer", "adjustment", "return", "damage_write_off"]
MOVEMENT_WEIGHTS = [0.35, 0.35, 0.12, 0.08, 0.07, 0.03]

PO_STATUSES = ["draft", "submitted", "approved", "shipped", "received", "cancelled"]
PO_STATUS_WEIGHTS = [0.05, 0.10, 0.15, 0.20, 0.45, 0.05]

CARRIERS = ["Correios", "DHL", "FedEx", "UPS", "Rappi", "Loggi"]


class SupplyChainGenerator(BaseGenerator):
    """Generates shipments, inventory_snapshots, purchase_orders, stock_movements, returns."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)

        shipments = self._build_shipments(cache, profile, scale)
        inventory = self._build_inventory_snapshots(cache, profile)
        purchase_orders = self._build_purchase_orders(cache, profile, scale)
        stock_movements = self._build_stock_movements(cache, profile, scale)
        returns = self._build_returns(cache, profile, scale)

        return {
            "shipments": shipments,
            "inventory_snapshots": inventory,
            "purchase_orders": purchase_orders,
            "stock_movements": stock_movements,
            "returns": returns,
        }

    def _build_shipments(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 500)
        sm = ShipmentTracking()
        order_ids = cache.sample_order_ids(n, self.rng) if len(cache.order_ids) > 0 else [new_uuid() for _ in range(n)]
        warehouse_ids = cache.sample_warehouse_ids(n, self.rng)
        statuses = [sm.run("created", self.rng) for _ in range(n)]
        created_dates = [random_date(profile.start, profile.end, self.rng) for _ in range(n)]

        delivered_ats = []
        for i, status in enumerate(statuses):
            if status == "delivered":
                d = created_dates[i] + timedelta(days=int(self.rng.integers(1, 10)))
                delivered_ats.append(d if d <= profile.end else None)
            else:
                delivered_ats.append(None)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "order_id": order_ids,
            "warehouse_id": warehouse_ids,
            "carrier": weighted_choice(CARRIERS, [0.30, 0.20, 0.15, 0.15, 0.10, 0.10], n, self.rng),
            "tracking_number": [generate_tracking_number(self.rng) for _ in range(n)],
            "status": statuses,
            "weight_kg": normal_clipped(2.0, 1.5, 0.1, 30.0, n, self.rng).round(2),
            "created_at": created_dates,
            "delivered_at": delivered_ats,
        })

    def _build_inventory_snapshots(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        from ..utils.time_utils import date_range_list
        rows = []
        skus = cache.product_skus if len(cache.product_skus) > 0 else [new_uuid()]
        warehouse_ids = cache.warehouse_ids if len(cache.warehouse_ids) > 0 else [new_uuid()]

        # Weekly snapshots to keep volume manageable
        dates = date_range_list(profile.start, profile.date_range_days)
        weekly_dates = [d for i, d in enumerate(dates) if i % 7 == 0]

        for d in weekly_dates:
            for wh_id in warehouse_ids:
                # Sample a subset of products per warehouse
                n_skus = min(len(skus), 50)
                sampled_skus = self.rng.choice(skus, size=n_skus, replace=False)
                for sku in sampled_skus:
                    on_hand = int(self.rng.integers(0, 5000))
                    reserved = int(self.rng.integers(0, min(on_hand, 200)))
                    rows.append({
                        "id": new_uuid(),
                        "product_sku": str(sku),
                        "warehouse_id": str(wh_id),
                        "date": d,
                        "quantity_on_hand": on_hand,
                        "quantity_reserved": reserved,
                        "quantity_available": on_hand - reserved,
                        "reorder_point": int(self.rng.integers(50, 500)),
                    })
        return pd.DataFrame(rows)

    def _build_purchase_orders(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(100, scale * 100)
        supplier_ids = cache.sample_supplier_ids(n, self.rng)
        warehouse_ids = cache.sample_warehouse_ids(n, self.rng)
        statuses = weighted_choice(PO_STATUSES, PO_STATUS_WEIGHTS, n, self.rng)
        ordered_dates = [random_date(profile.start, profile.end, self.rng) for _ in range(n)]

        expected_ats = [d + timedelta(days=int(self.rng.integers(3, 45))) for d in ordered_dates]
        received_ats = [
            (exp + timedelta(days=int(self.rng.integers(-2, 5)))) if st == "received" else None
            for exp, st in zip(expected_ats, statuses)
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "supplier_id": supplier_ids,
            "warehouse_id": warehouse_ids,
            "status": statuses,
            "total_amount": normal_clipped(50_000, 30_000, 1_000, 500_000, n, self.rng).round(2),
            "currency": "BRL",
            "ordered_at": ordered_dates,
            "expected_at": expected_ats,
            "received_at": received_ats,
            "notes": [None for _ in range(n)],
        })

    def _build_stock_movements(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 500)
        skus = cache.product_skus if len(cache.product_skus) > 0 else [new_uuid()]
        warehouse_ids = cache.sample_warehouse_ids(n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "product_sku": self.rng.choice(skus, size=n),
            "warehouse_id": warehouse_ids,
            "movement_type": weighted_choice(MOVEMENT_TYPES, MOVEMENT_WEIGHTS, n, self.rng),
            "quantity": self.rng.integers(1, 500, size=n),
            "unit_cost": normal_clipped(100, 80, 10, 5_000, n, self.rng).round(2),
            "reason": [f"Auto movement {i}" for i in range(n)],
            "reference_id": [new_uuid() for _ in range(n)],
            "created_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
        })

    def _build_returns(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(100, scale * 100)
        order_ids = cache.sample_order_ids(n, self.rng) if len(cache.order_ids) > 0 else [new_uuid() for _ in range(n)]
        customer_ids = cache.sample_customer_ids(n, self.rng)
        skus = cache.sample_product_skus(n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "order_id": order_ids,
            "customer_id": customer_ids,
            "product_sku": skus,
            "reason": weighted_choice(RETURN_REASONS, RETURN_REASON_WEIGHTS, n, self.rng),
            "quantity": self.rng.integers(1, 4, size=n),
            "status": weighted_choice(["requested", "approved", "received", "refunded", "rejected"], [0.1, 0.15, 0.20, 0.50, 0.05], n, self.rng),
            "created_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "refund_amount": normal_clipped(200, 150, 10, 5_000, n, self.rng).round(2),
            "currency": "BRL",
        })
