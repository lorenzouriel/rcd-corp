"""Finance & accounting generator for Aurora Corp."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import normal_clipped, weighted_choice
from ..utils.identifiers import new_uuid
from ..utils.time_utils import random_date, random_datetime, COUNTRY_CURRENCY

INVOICE_STATUSES = ["draft", "sent", "paid", "overdue", "cancelled"]
INVOICE_STATUS_WEIGHTS = [0.05, 0.10, 0.75, 0.08, 0.02]

TRANSACTION_TYPES = ["sale", "refund", "adjustment", "fee", "transfer"]
TRANSACTION_WEIGHTS = [0.70, 0.10, 0.08, 0.07, 0.05]

EXPENSE_CATEGORIES = [
    "travel", "software", "hardware", "marketing", "office_supplies",
    "consulting", "training", "meals", "utilities", "miscellaneous",
]

MERCHANT_CATEGORIES = [
    "electronics", "restaurants", "travel", "fuel", "grocery",
    "entertainment", "utilities", "healthcare", "clothing", "online_retail",
]

CARD_STATUSES = ["approved", "approved", "approved", "declined", "pending"]


class FinanceGenerator(BaseGenerator):
    """Generates invoices, transactions, expenses, budgets, aurora_card_transactions."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = profile.n_orders // 10_000

        return {
            "invoices": self._build_invoices(cache, profile, scale),
            "transactions": self._build_transactions(cache, profile, scale),
            "expenses": self._build_expenses(cache, profile, scale),
            "budgets": self._build_budgets(profile),
            "aurora_card_transactions": self._build_card_transactions(cache, profile, scale),
        }

    def _build_invoices(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 500)
        order_ids = cache.sample_order_ids(n, self.rng) if len(cache.order_ids) > 0 else [new_uuid() for _ in range(n)]
        customer_ids = cache.sample_customer_ids(n, self.rng)
        statuses = weighted_choice(INVOICE_STATUSES, INVOICE_STATUS_WEIGHTS, n, self.rng)
        issued_dates = [random_date(profile.start, profile.end, self.rng) for _ in range(n)]
        amounts = normal_clipped(800, 600, 50, 50_000, n, self.rng).round(2)
        currencies = [COUNTRY_CURRENCY.get(str(self.rng.choice(["BR", "MX", "PT", "US"])), "BRL") for _ in range(n)]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "order_id": order_ids,
            "customer_id": customer_ids,
            "amount": amounts,
            "currency": currencies,
            "issued_at": issued_dates,
            "due_at": [d + timedelta(days=30) for d in issued_dates],
            "status": statuses,
            "payment_method": weighted_choice(
                ["credit_card", "pix", "boleto", "aurora_card"], [0.4, 0.3, 0.2, 0.1], n, self.rng
            ),
        })

    def _build_transactions(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(1000, scale * 1000)
        types = weighted_choice(TRANSACTION_TYPES, TRANSACTION_WEIGHTS, n, self.rng)
        amounts = normal_clipped(500, 400, 1, 100_000, n, self.rng).round(2)
        currencies = [COUNTRY_CURRENCY.get(str(self.rng.choice(["BR", "MX", "PT", "US"])), "BRL") for _ in range(n)]
        created = [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "type": types,
            "amount": amounts,
            "currency": currencies,
            "from_account": [f"ACC-{int(self.rng.integers(1000, 9999))}" for _ in range(n)],
            "to_account": [f"ACC-{int(self.rng.integers(1000, 9999))}" for _ in range(n)],
            "created_at": created,
            "status": weighted_choice(["completed", "pending", "failed"], [0.92, 0.06, 0.02], n, self.rng),
            "reference": [f"TXN-{int(self.rng.integers(100_000, 999_999))}" for _ in range(n)],
        })

    def _build_expenses(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 200)
        employee_ids = cache.sample_employee_ids(n, self.rng)
        approver_ids = cache.sample_employee_ids(n, self.rng)
        amounts = normal_clipped(150, 200, 10, 5_000, n, self.rng).round(2)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "category": weighted_choice(EXPENSE_CATEGORIES, [0.15, 0.20, 0.10, 0.12, 0.08, 0.10, 0.08, 0.07, 0.05, 0.05], n, self.rng),
            "amount": amounts,
            "currency": "BRL",
            "department": weighted_choice(
                ["Engineering", "Marketing", "Sales", "Finance", "HR", "Other"],
                [0.2, 0.2, 0.2, 0.15, 0.1, 0.15],
                n, self.rng,
            ),
            "employee_id": employee_ids,
            "approved_by": approver_ids,
            "date": [random_date(profile.start, profile.end, self.rng) for _ in range(n)],
            "description": [f"Expense item {i}" for i in range(n)],
            "status": weighted_choice(["approved", "pending", "rejected"], [0.80, 0.15, 0.05], n, self.rng),
        })

    def _build_budgets(self, profile: ProfileConfig) -> pd.DataFrame:
        rows = []
        departments = [
            "Engineering", "Product", "Marketing", "Sales",
            "Customer Success", "Finance", "HR", "IT", "Security",
        ]
        categories = ["headcount", "software", "marketing", "capex", "opex"]
        start_year = profile.start.year
        years = list({start_year, profile.end.year})
        for dept in departments:
            for year in years:
                for quarter in range(1, 5):
                    for cat in categories:
                        planned = round(float(self.rng.uniform(50_000, 2_000_000)), 2)
                        variance = float(self.rng.normal(1.0, 0.08))
                        rows.append({
                            "id": new_uuid(),
                            "department": dept,
                            "year": year,
                            "quarter": quarter,
                            "category": cat,
                            "planned_amount": planned,
                            "actual_amount": round(planned * max(0.5, variance), 2),
                            "currency": "BRL",
                        })
        return pd.DataFrame(rows)

    def _build_card_transactions(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 500)
        customer_ids = cache.sample_customer_ids(n, self.rng)
        amounts = normal_clipped(120, 100, 5, 8_000, n, self.rng).round(2)
        statuses = weighted_choice(CARD_STATUSES, [0.7, 0.15, 0.05, 0.07, 0.03], n, self.rng)
        posted = [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "card_id": [f"CARD-{int(self.rng.integers(100_000, 999_999))}" for _ in range(n)],
            "customer_id": customer_ids,
            "merchant_category": weighted_choice(MERCHANT_CATEGORIES, [0.2] + [0.8 / 9] * 9, n, self.rng),
            "amount": amounts,
            "currency": "BRL",
            "status": statuses,
            "posted_at": posted,
        })
