"""Marketing generator for Aurora Corp: campaigns, events, email, leads, A/B tests."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice
from ..utils.identifiers import new_uuid
from ..utils.state_machines import LeadPipeline
from ..utils.time_utils import random_date, random_datetime

CAMPAIGN_TYPES = ["email", "social_paid", "search_sem", "display", "influencer", "sms", "push"]
CAMPAIGN_TYPE_WEIGHTS = [0.30, 0.25, 0.15, 0.10, 0.10, 0.05, 0.05]

CHANNELS = ["email", "instagram", "google", "facebook", "tiktok", "youtube", "linkedin"]
CHANNEL_WEIGHTS = [0.30, 0.20, 0.15, 0.15, 0.10, 0.05, 0.05]

TARGET_SEGMENTS = ["B2C", "B2B", "VIP", "all", "new_customers", "churned"]
SEGMENT_WEIGHTS = [0.35, 0.20, 0.10, 0.15, 0.12, 0.08]

EMAIL_EVENTS = ["sent", "delivered", "opened", "clicked", "bounced", "unsubscribed"]
EMAIL_WEIGHTS = [0.20, 0.50, 0.20, 0.06, 0.03, 0.01]

CAMPAIGN_EVENTS = ["impression", "click", "conversion", "video_view", "lead_form"]
CAMPAIGN_EVENT_WEIGHTS = [0.60, 0.25, 0.08, 0.05, 0.02]

LEAD_SOURCES = ["organic_search", "paid_search", "social_media", "email", "referral", "direct", "event"]
LEAD_SOURCE_WEIGHTS = [0.25, 0.20, 0.20, 0.15, 0.10, 0.05, 0.05]


class MarketingGenerator(BaseGenerator):
    """Generates campaigns, campaign_events, email_events, leads, ab_test_exposures."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)
        campaigns = self._build_campaigns(profile, scale)

        # Cache campaign IDs for downstream FK use
        cache.campaign_ids = campaigns["id"].to_numpy().astype("U36")

        return {
            "campaigns": campaigns,
            "campaign_events": self._build_campaign_events(campaigns, cache, profile, scale),
            "email_events": self._build_email_events(campaigns, cache, profile, scale),
            "leads": self._build_leads(cache, profile, scale),
            "ab_test_exposures": self._build_ab_tests(cache, profile, scale),
        }

    def _build_campaigns(self, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(20, scale * 10)
        rows = []
        for i in range(n):
            start = random_date(profile.start, profile.end - timedelta(days=14), self.rng)
            end = start + timedelta(days=int(self.rng.integers(7, 60)))
            end = min(end, profile.end)
            budget = round(float(self.rng.uniform(5_000, 500_000)), 2)
            rows.append({
                "id": new_uuid(),
                "name": f"Campaign {i + 1:03d} — {str(weighted_choice(CAMPAIGN_TYPES, CAMPAIGN_TYPE_WEIGHTS, 1, self.rng)[0]).replace('_', ' ').title()}",
                "type": str(weighted_choice(CAMPAIGN_TYPES, CAMPAIGN_TYPE_WEIGHTS, 1, self.rng)[0]),
                "channel": str(weighted_choice(CHANNELS, CHANNEL_WEIGHTS, 1, self.rng)[0]),
                "start_date": start,
                "end_date": end,
                "budget": budget,
                "actual_spend": round(budget * float(self.rng.uniform(0.5, 1.05)), 2),
                "currency": "BRL",
                "target_segment": str(weighted_choice(TARGET_SEGMENTS, SEGMENT_WEIGHTS, 1, self.rng)[0]),
                "status": "completed" if end < profile.end else "active",
                "owner_employee_id": None,
            })
        return pd.DataFrame(rows)

    def _build_campaign_events(
        self, campaigns: pd.DataFrame, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(1000, scale * 2000)
        campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else campaigns["id"].values
        customer_ids = cache.sample_customer_ids(n, self.rng)
        events = weighted_choice(CAMPAIGN_EVENTS, CAMPAIGN_EVENT_WEIGHTS, n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "campaign_id": self.rng.choice(campaign_ids, size=n),
            "customer_id": customer_ids,
            "event_type": events,
            "timestamp": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
        })

    def _build_email_events(
        self, campaigns: pd.DataFrame, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(2000, scale * 3000)
        campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else campaigns["id"].values
        customer_ids = cache.sample_customer_ids(n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "campaign_id": self.rng.choice(campaign_ids, size=n),
            "customer_id": customer_ids,
            "email": [f"user{i}@example.com" for i in range(n)],
            "event_type": weighted_choice(EMAIL_EVENTS, EMAIL_WEIGHTS, n, self.rng),
            "timestamp": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
        })

    def _build_leads(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 300)
        sm = LeadPipeline()
        customer_ids = cache.sample_customer_ids(n, self.rng)
        owner_ids = cache.sample_employee_ids(n, self.rng)
        campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else [new_uuid()] * n
        statuses = [sm.run("new", self.rng) for _ in range(n)]
        created = [random_datetime(profile.start, profile.end - timedelta(days=1), self.rng) for _ in range(n)]
        updated = [c + timedelta(hours=int(self.rng.integers(1, 720))) for c in created]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "source": weighted_choice(LEAD_SOURCES, LEAD_SOURCE_WEIGHTS, n, self.rng),
            "campaign_id": self.rng.choice(campaign_ids, size=n),
            "status": statuses,
            "score": self.rng.integers(0, 101, size=n),
            "created_at": created,
            "updated_at": updated,
            "owner_employee_id": owner_ids,
        })

    def _build_ab_tests(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(500, scale * 500)
        test_names = ["checkout_flow_v2", "homepage_hero", "email_subject_test", "pricing_page_cta", "cart_banner"]
        customer_ids = cache.sample_customer_ids(n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "test_name": self.rng.choice(test_names, size=n),
            "variant": self.rng.choice(["control", "variant_a", "variant_b"], size=n),
            "customer_id": customer_ids,
            "session_id": [new_uuid() for _ in range(n)],
            "exposed_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "converted": self.rng.random(n) < 0.12,
        })
