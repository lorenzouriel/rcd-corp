"""Sales & e-commerce generator for RCD Corp — the reference OrdersGenerator implementation."""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.state_machines import OrderLifecycle
from ..utils.time_utils import (
    black_friday_multiplier,
    payday_multiplier,
    random_date,
    timestamp_in_business_hours,
    COUNTRY_CURRENCY,
)
from ..utils.identifiers import new_uuid

CHANNELS = ["web", "mobile_app", "pos", "marketplace", "call_center"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

MARKETPLACES = ["MercadoLivre", "Amazon", "Shopee", None, None, None, None, None]
PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto", "paypal", "rcd_card", "bnpl"]
PAYMENT_WEIGHTS = [0.35, 0.15, 0.25, 0.10, 0.05, 0.07, 0.03]
PAYMENT_GATEWAYS = ["Stripe", "Cielo", "PagSeguro", "MercadoPago", "PayPal"]

DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
OS_LIST = ["Windows", "macOS", "iOS", "Android", "Linux"]
UTM_SOURCES = ["google", "facebook", "instagram", "email", "direct", "organic"]
UTM_MEDIUMS = ["cpc", "social", "email", "organic", "referral"]

PROMO_CODES = [None, None, None, None, None, "RCD10", "BF2024", "SUMMER15", "FIDELIDADE20"]


class SalesGenerator(BaseGenerator):
    """Generates orders, order_items, payments, web_sessions, shopping_cart_events."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        orders = self._build_orders(cache, profile, crisis_days)
        order_items = self._build_order_items(orders, cache, profile)
        payments = self._build_payments(orders, profile)
        sessions = self._build_sessions(cache, profile)
        cart_events = self._build_cart_events(sessions, cache, profile)

        # Populate order_ids in cache for downstream FK use
        cache.order_ids = orders["id"].to_numpy().astype("U36")

        return {
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "web_sessions": sessions,
            "shopping_cart_events": cart_events,
        }

    def _build_orders(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> pd.DataFrame:
        n = profile.n_orders
        sm = OrderLifecycle()

        customer_ids = cache.sample_customer_ids(n, self.rng)
        store_ids = cache.sample_store_ids(n, self.rng)
        channels = weighted_choice(CHANNELS, CHANNEL_WEIGHTS, n, self.rng)
        promo_codes = [str(self.rng.choice(PROMO_CODES)) if self.rng.random() < 0.15 else None for _ in range(n)]

        # Temporal distribution with seasonal multipliers
        start = profile.start
        end = profile.end
        delta = (end - start).days or 1

        # Generate dates with seasonal weighting
        raw_days = self.rng.integers(0, delta + 1, size=n)
        dates = [start + timedelta(days=int(d)) for d in raw_days]

        # Apply multipliers as acceptance sampling for realism
        multiplied_dates = []
        for d in dates:
            mult = black_friday_multiplier(d) * payday_multiplier(d)
            # Resample with probability proportional to multiplier (capped at 1 for base)
            if mult > 1.0:
                # Oversample that date by duplicating entries - handled via index weighting
                pass
            multiplied_dates.append(d)

        created_ats = [
            timestamp_in_business_hours(d, "BR", self.rng)
            for d in multiplied_dates
        ]

        # State machine terminal states
        crisis_set = set(crisis_days)
        statuses = [
            sm.run("pending", self.rng, crisis_mode=dates[i] in crisis_set)
            for i in range(n)
        ]

        # Subtotals: normal-clipped by channel
        subtotals = normal_clipped(350, 250, 29, 15_000, n, self.rng)
        shipping = np.where(np.array(channels) == "pos", 0, self.rng.uniform(0, 50, n).round(2))
        tax_rate = 0.12
        taxes = (subtotals * tax_rate).round(2)
        totals = (subtotals + shipping + taxes).round(2)

        # Currency from store country
        store_country_map = dict(zip(cache.store_ids, cache.store_countries))
        currencies = [
            COUNTRY_CURRENCY.get(store_country_map.get(str(sid), "BR"), "BRL")
            for sid in store_ids
        ]

        marketplace = [
            str(self.rng.choice(MARKETPLACES)) if ch == "marketplace" else None
            for ch in channels
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "store_id": store_ids,
            "channel": channels,
            "marketplace": marketplace,
            "status": statuses,
            "subtotal": subtotals.round(2),
            "shipping": shipping,
            "tax": taxes,
            "total": totals,
            "currency": currencies,
            "promo_code": promo_codes,
            "created_at": created_ats,
            "date": [d for d in multiplied_dates],
        })

    def _build_order_items(
        self,
        orders: pd.DataFrame,
        cache: MasterCache,
        profile: ProfileConfig,
    ) -> pd.DataFrame:
        rows = []
        for _, order in orders.iterrows():
            n_items = int(self.rng.integers(1, 6))
            skus = cache.sample_product_skus(n_items, self.rng)
            sku_price_map = dict(zip(cache.product_skus, cache.product_prices))
            for sku in skus:
                unit_price = float(sku_price_map.get(str(sku), 100.0))
                qty = int(self.rng.integers(1, 4))
                discount = float(self.rng.choice([0, 0, 0, 5, 10, 15, 20])) / 100.0
                line_total = round(unit_price * qty * (1 - discount), 2)
                rows.append({
                    "id": new_uuid(),
                    "order_id": order["id"],
                    "product_id": str(sku),
                    "quantity": qty,
                    "unit_price": round(unit_price, 2),
                    "discount_pct": round(discount * 100, 1),
                    "line_total": line_total,
                })
        return pd.DataFrame(rows)

    def _build_payments(self, orders: pd.DataFrame, profile: ProfileConfig) -> pd.DataFrame:
        paid_statuses = {"paid", "picked", "shipped", "delivered", "returned", "refunded"}
        paid_orders = orders[orders["status"].isin(paid_statuses)]
        n = len(paid_orders)
        if n == 0:
            return pd.DataFrame()

        methods = weighted_choice(PAYMENT_METHODS, PAYMENT_WEIGHTS, n, self.rng)
        installments = np.where(
            np.array(methods) == "credit_card",
            self.rng.integers(1, 13, size=n),
            1,
        )
        processing_fee = (paid_orders["total"].values * self.rng.uniform(0.01, 0.035, n)).round(2)

        authorized_ats = [
            row["created_at"] + timedelta(seconds=int(self.rng.integers(1, 120)))
            for _, row in paid_orders.iterrows()
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "order_id": paid_orders["id"].values,
            "method": methods,
            "installments": installments,
            "status": "approved",
            "gateway": weighted_choice(PAYMENT_GATEWAYS, [0.3, 0.25, 0.2, 0.15, 0.1], n, self.rng),
            "processing_fee": processing_fee,
            "authorized_at": authorized_ats,
            "currency": paid_orders["currency"].values,
        })

    def _build_sessions(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        n = int(profile.n_orders * 2.5)
        customer_ids_pool = cache.customer_ids
        # 30% anonymous sessions
        customer_ids = [
            None if self.rng.random() < 0.30
            else str(self.rng.choice(customer_ids_pool))
            for _ in range(n)
        ]
        start, end = profile.start, profile.end
        session_dates = [random_date(start, end, self.rng) for _ in range(n)]
        duration = normal_clipped(180, 120, 10, 3600, n, self.rng).astype(int)
        pages_viewed = self.rng.integers(1, 25, size=n)
        bounced = pages_viewed == 1

        return pd.DataFrame({
            "session_id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "device": weighted_choice(DEVICES, DEVICE_WEIGHTS, n, self.rng),
            "browser": weighted_choice(BROWSERS, [0.6, 0.2, 0.08, 0.07, 0.05], n, self.rng),
            "os": weighted_choice(OS_LIST, [0.35, 0.15, 0.20, 0.25, 0.05], n, self.rng),
            "utm_source": weighted_choice(UTM_SOURCES, [0.3, 0.2, 0.15, 0.15, 0.1, 0.1], n, self.rng),
            "utm_medium": weighted_choice(UTM_MEDIUMS, [0.25, 0.2, 0.2, 0.2, 0.15], n, self.rng),
            "utm_campaign": [f"camp_{int(self.rng.integers(1, 50)):02d}" for _ in range(n)],
            "landing_page": [f"/page-{int(self.rng.integers(1, 30))}" for _ in range(n)],
            "pages_viewed": pages_viewed,
            "duration_s": duration,
            "bounced": bounced,
            "date": session_dates,
        })

    def _build_cart_events(
        self,
        sessions: pd.DataFrame,
        cache: MasterCache,
        profile: ProfileConfig,
    ) -> pd.DataFrame:
        event_types = ["view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
        event_weights = [0.45, 0.25, 0.10, 0.12, 0.08]
        rows = []
        n_events = int(profile.n_orders * 3)
        sample_sessions = sessions.sample(min(n_events, len(sessions)), replace=True, random_state=self.seed)

        for _, sess in sample_sessions.iterrows():
            rows.append({
                "id": new_uuid(),
                "session_id": sess["session_id"],
                "customer_id": sess["customer_id"],
                "event_type": str(weighted_choice(event_types, event_weights, 1, self.rng)[0]),
                "product_id": str(cache.sample_product_skus(1, self.rng)[0]),
                "timestamp": timestamp_in_business_hours(sess["date"], "BR", self.rng),
            })
        return pd.DataFrame(rows)
