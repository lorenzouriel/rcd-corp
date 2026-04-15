"""Customer support generator for Aurora Corp: tickets, messages, call center."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid
from ..utils.state_machines import TicketLifecycle
from ..utils.time_utils import random_date, random_datetime, timestamp_in_business_hours

TICKET_CATEGORIES = [
    "order_issue", "delivery_problem", "product_defect", "billing",
    "account_access", "returns_refunds", "technical_support", "general_inquiry",
]
TICKET_CATEGORY_WEIGHTS = [0.20, 0.18, 0.15, 0.12, 0.10, 0.10, 0.08, 0.07]

TICKET_CHANNELS = ["email", "chat", "phone", "social_media", "app"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

PRIORITIES = ["low", "medium", "high", "critical"]
PRIORITY_WEIGHTS = [0.30, 0.45, 0.20, 0.05]

SENTIMENTS = ["positive", "neutral", "negative"]

CALL_TYPES = ["inbound", "outbound", "callback"]
CALL_TYPE_WEIGHTS = [0.70, 0.15, 0.15]

RESOLUTIONS = ["resolved", "escalated", "no_answer", "voicemail", "transferred"]
RESOLUTION_WEIGHTS = [0.65, 0.10, 0.10, 0.08, 0.07]

SENDER_TYPES = ["customer", "agent", "bot", "system"]
SENDER_WEIGHTS = [0.50, 0.40, 0.08, 0.02]


class SupportGenerator(BaseGenerator):
    """Generates tickets, ticket_messages, call_center_calls."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)
        crisis_set = set(crisis_days)

        tickets = self._build_tickets(cache, profile, scale, crisis_set)
        messages = self._build_ticket_messages(tickets, cache, profile)
        calls = self._build_calls(cache, profile, scale, crisis_set)

        return {
            "tickets": tickets,
            "ticket_messages": messages,
            "call_center_calls": calls,
        }

    def _build_tickets(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        scale: int,
        crisis_set: set,
    ) -> pd.DataFrame:
        base_n = max(500, scale * 300)
        sm = TicketLifecycle()
        customer_ids = cache.sample_customer_ids(base_n, self.rng)
        agent_ids = cache.sample_employee_ids(base_n, self.rng)
        created_dates = [random_date(profile.start, profile.end, self.rng) for _ in range(base_n)]

        # Crisis multiplier: 5x tickets on crisis day
        extra_rows = []
        for crisis_day in crisis_set:
            n_extra = max(10, base_n // (profile.date_range_days or 1)) * 4
            for _ in range(n_extra):
                extra_rows.append(crisis_day)

        all_dates = created_dates + extra_rows
        n = len(all_dates)
        customer_ids = np.resize(customer_ids, n)
        agent_ids = np.resize(agent_ids, n)

        statuses = [
            sm.run("open", self.rng, crisis_mode=d in crisis_set)
            for d in all_dates
        ]
        priorities = weighted_choice(PRIORITIES, PRIORITY_WEIGHTS, n, self.rng)

        resolved_ats = []
        for d, st in zip(all_dates, statuses):
            if st == "closed":
                resolved_day = d + timedelta(days=int(self.rng.integers(0, 7)))
                resolved_ats.append(resolved_day if resolved_day <= profile.end else None)
            else:
                resolved_ats.append(None)

        csat_scores = [
            float(self.rng.choice([1, 2, 3, 4, 5], p=[0.05, 0.08, 0.15, 0.35, 0.37]))
            if s == "closed" else None
            for s in statuses
        ]

        sentiments = [
            "negative" if d in crisis_set else str(weighted_choice(SENTIMENTS, [0.45, 0.35, 0.20], 1, self.rng)[0])
            for d in all_dates
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "channel": weighted_choice(TICKET_CHANNELS, CHANNEL_WEIGHTS, n, self.rng),
            "category": weighted_choice(TICKET_CATEGORIES, TICKET_CATEGORY_WEIGHTS, n, self.rng),
            "subject": [f"Issue with {str(weighted_choice(TICKET_CATEGORIES, TICKET_CATEGORY_WEIGHTS, 1, self.rng)[0]).replace('_', ' ')}" for _ in range(n)],
            "status": statuses,
            "priority": priorities,
            "sentiment": sentiments,
            "created_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "resolved_at": resolved_ats,
            "csat_score": csat_scores,
            "agent_id": agent_ids,
        })

    def _build_ticket_messages(
        self, tickets: pd.DataFrame, cache: MasterCache, profile: ProfileConfig
    ) -> pd.DataFrame:
        rows = []
        for _, ticket in tickets.iterrows():
            n_msgs = int(self.rng.integers(2, 8))
            ts = ticket["created_at"]
            for msg_idx in range(n_msgs):
                ts = ts + timedelta(hours=int(self.rng.integers(0, 24)))
                sender = str(weighted_choice(SENDER_TYPES, SENDER_WEIGHTS, 1, self.rng)[0])
                rows.append({
                    "id": new_uuid(),
                    "ticket_id": ticket["id"],
                    "sender_type": sender,
                    "sender_id": ticket["customer_id"] if sender == "customer" else ticket["agent_id"],
                    "body": f"Message {msg_idx + 1} regarding ticket {ticket['category']}.",
                    "created_at": ts,
                    "is_internal_note": self.rng.random() < 0.10,
                })
        return pd.DataFrame(rows)

    def _build_calls(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        scale: int,
        crisis_set: set,
    ) -> pd.DataFrame:
        base_n = max(200, scale * 200)
        n = base_n + len(crisis_set) * max(10, base_n // (profile.date_range_days or 1)) * 2
        customer_ids = cache.sample_customer_ids(n, self.rng)
        agent_ids = cache.sample_employee_ids(n, self.rng)
        duration = normal_clipped(240, 180, 30, 2400, n, self.rng).astype(int)
        created = [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "agent_id": agent_ids,
            "duration_s": duration,
            "direction": weighted_choice(["inbound", "outbound"], [0.75, 0.25], n, self.rng),
            "call_type": weighted_choice(CALL_TYPES, CALL_TYPE_WEIGHTS, n, self.rng),
            "sentiment": weighted_choice(SENTIMENTS, [0.45, 0.35, 0.20], n, self.rng),
            "resolution": weighted_choice(RESOLUTIONS, RESOLUTION_WEIGHTS, n, self.rng),
            "created_at": created,
            "recording_url": [f"https://recordings.aurora.internal/{new_uuid()}.mp3" if self.rng.random() < 0.70 else None for _ in range(n)],
            "wait_time_s": self.rng.integers(10, 600, size=n),
        })
