"""Social media generator for RCD Corp — 11 tables across 6 platforms."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid
from ..utils.time_utils import random_date, timestamp_social_peak, random_datetime

PLATFORMS = ["instagram", "tiktok", "youtube", "x", "linkedin", "facebook"]
PLATFORM_WEIGHTS = [0.30, 0.25, 0.15, 0.10, 0.10, 0.10]

POST_TYPES_BY_PLATFORM = {
    "instagram": ["image", "carousel", "reel", "story"],
    "tiktok": ["short"],
    "youtube": ["video", "short", "live"],
    "x": ["text", "image"],
    "linkedin": ["text", "image", "video"],
    "facebook": ["image", "video", "story", "text"],
}

SOCIAL_ACCOUNTS = [
    ("@rcdcorp", "rcd_corp", "brand"),
    ("@rcd_novahome", "novahome", "product"),
    ("@pulseaudio_br", "pulseaudio", "product"),
    ("@guardian_iq", "guardianiq", "product"),
    ("@rcd_online", "rcd_online", "ecommerce"),
    ("@rcd_corp_pt", "rcd_pt", "regional"),
]

SENTIMENTS = ["positive", "neutral", "negative"]
SENTIMENT_WEIGHTS_NORMAL = [0.55, 0.30, 0.15]
SENTIMENT_WEIGHTS_CRISIS = [0.20, 0.25, 0.55]

INFLUENCER_TIERS = ["nano", "micro", "mid", "macro", "mega"]
TIER_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]

REVIEW_SOURCES = ["google", "trustpilot", "reclame_aqui", "appstore", "playstore"]
REVIEW_SOURCE_WEIGHTS = [0.25, 0.20, 0.30, 0.12, 0.13]

FORUM_CATEGORIES = ["support", "tips_tricks", "product_feedback", "general", "announcements"]

DM_INTENTS = ["support", "order_inquiry", "complaint", "compliment", "product_question"]
DM_INTENT_WEIGHTS = [0.35, 0.25, 0.20, 0.10, 0.10]

TOPICS = ["product_quality", "delivery", "pricing", "promotion", "new_launch", "outage", "general"]


class SocialMediaGenerator(BaseGenerator):
    """Generates all 11 social media tables for RCD Corp."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)
        crisis_set = set(crisis_days)

        accounts = self._build_accounts()
        posts = self._build_posts(accounts, cache, profile, scale)
        metrics = self._build_metrics(posts, profile, scale)
        comments = self._build_comments(posts, cache, profile, scale, crisis_set)
        mentions = self._build_mentions(profile, scale, crisis_set)
        dms = self._build_dms(accounts, cache, profile, scale)
        partnerships = self._build_influencer_partnerships(cache, profile, scale)
        influencer_posts = self._build_influencer_posts(partnerships, profile, scale)
        forum_posts = self._build_forum_posts(cache, profile, scale)
        reviews = self._build_reviews(cache, profile, scale, crisis_set)
        ad_spend = self._build_ad_spend(cache, profile, scale)

        return {
            "social_accounts": accounts,
            "social_posts": posts,
            "social_metrics": metrics,
            "social_comments": comments,
            "social_mentions": mentions,
            "social_dms": dms,
            "influencer_partnerships": partnerships,
            "influencer_posts": influencer_posts,
            "community_forum_posts": forum_posts,
            "reviews": reviews,
            "social_ad_spend": ad_spend,
        }

    def _build_accounts(self) -> pd.DataFrame:
        rows = []
        for handle, name, acct_type in SOCIAL_ACCOUNTS:
            for platform in PLATFORMS:
                rows.append({
                    "id": new_uuid(),
                    "handle": handle,
                    "name": name,
                    "platform": platform,
                    "account_type": acct_type,
                    "follower_count": int(self.rng.integers(5_000, 2_000_000)),
                    "following_count": int(self.rng.integers(100, 2_000)),
                    "verified": self.rng.random() < 0.40,
                    "created_at": date(2010, 1, 1) + timedelta(days=int(self.rng.integers(0, 3000))),
                })
        return pd.DataFrame(rows)

    def _build_posts(
        self, accounts: pd.DataFrame, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(100, scale * 50)
        account_ids = accounts["id"].tolist()
        account_platforms = dict(zip(accounts["id"], accounts["platform"]))
        rows = []
        campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else [None]
        skus = cache.product_skus if len(cache.product_skus) > 0 else [None]
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [None]

        for _ in range(n):
            acc_id = str(self.rng.choice(account_ids))
            platform = account_platforms[acc_id]
            post_type = str(self.rng.choice(POST_TYPES_BY_PLATFORM[platform]))
            d = random_date(profile.start, profile.end, self.rng)
            posted_at = timestamp_social_peak(d, self.rng) if self.rng.random() < 0.60 else random_datetime(profile.start, profile.end, self.rng)
            n_tags = int(self.rng.integers(3, 12))
            hashtags = [f"#rcd{i}" for i in range(n_tags)]
            rows.append({
                "id": new_uuid(),
                "account_id": acc_id,
                "platform": platform,
                "post_type": post_type,
                "caption": f"Check out our latest product! #rcd #novahome #{platform}",
                "hashtags": ",".join(hashtags),
                "posted_at": posted_at,
                "campaign_id": str(self.rng.choice(campaign_ids)) if self.rng.random() < 0.60 else None,
                "product_sku": str(self.rng.choice(skus)) if self.rng.random() < 0.50 else None,
                "author_employee_id": str(self.rng.choice(employee_ids)) if len(employee_ids) > 0 else None,
                "date": d,
            })
        return pd.DataFrame(rows)

    def _build_metrics(self, posts: pd.DataFrame, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        rows = []
        for _, post in posts.iterrows():
            platform = post["platform"]
            is_video = post["post_type"] in ("reel", "short", "video", "live")
            base_reach = int(self.rng.integers(500, 500_000))
            if is_video:
                base_reach = int(base_reach * self.rng.uniform(3.0, 5.0))

            # Hourly snapshots for first 72h, then daily
            posted_at = post["posted_at"]
            if isinstance(posted_at, datetime):
                snap_ts = posted_at
            else:
                snap_ts = datetime.combine(posted_at, __import__("datetime").time(12, 0))

            for h in range(72):
                rows.append({
                    "id": new_uuid(),
                    "post_id": post["id"],
                    "snapshot_ts": snap_ts + timedelta(hours=h),
                    "impressions": int(base_reach * (1 + h / 72) * self.rng.uniform(0.9, 1.1)),
                    "reach": int(base_reach * self.rng.uniform(0.7, 1.0)),
                    "likes": int(base_reach * self.rng.uniform(0.02, 0.08)),
                    "comments": int(base_reach * self.rng.uniform(0.005, 0.02)),
                    "shares": int(base_reach * self.rng.uniform(0.002, 0.01)),
                    "saves": int(base_reach * self.rng.uniform(0.003, 0.015)),
                    "video_views": int(base_reach * self.rng.uniform(0.3, 0.8)) if is_video else 0,
                    "avg_watch_time_s": int(self.rng.uniform(10, 60)) if is_video else 0,
                    "link_clicks": int(base_reach * self.rng.uniform(0.001, 0.05)),
                    "profile_visits": int(base_reach * self.rng.uniform(0.005, 0.03)),
                })
        return pd.DataFrame(rows)

    def _build_comments(
        self, posts: pd.DataFrame, cache: MasterCache, profile: ProfileConfig, scale: int, crisis_set: set
    ) -> pd.DataFrame:
        n = max(500, scale * 500)
        post_ids = posts["id"].tolist()
        customer_ids = cache.sample_customer_ids(n, self.rng)
        post_dates = dict(zip(posts["id"], posts["date"]))
        languages = ["pt", "es", "en", "pt"]

        rows = []
        for i in range(n):
            post_id = str(self.rng.choice(post_ids))
            post_date = post_dates.get(post_id, profile.start)
            if isinstance(post_date, datetime):
                post_date = post_date.date()
            is_crisis = post_date in crisis_set
            sentiment = str(weighted_choice(
                SENTIMENTS,
                SENTIMENT_WEIGHTS_CRISIS if is_crisis else SENTIMENT_WEIGHTS_NORMAL,
                1, self.rng
            )[0])
            rows.append({
                "id": new_uuid(),
                "post_id": post_id,
                "platform": str(weighted_choice(PLATFORMS, PLATFORM_WEIGHTS, 1, self.rng)[0]),
                "customer_id": customer_ids[i],
                "parent_comment_id": None,
                "body": "Great product!" if sentiment == "positive" else ("It's ok." if sentiment == "neutral" else "Terrible experience!"),
                "sentiment": sentiment,
                "language": str(self.rng.choice(languages)),
                "posted_at": random_datetime(profile.start, profile.end, self.rng),
                "is_spam": self.rng.random() < 0.03,
                "is_moderated": self.rng.random() < 0.05,
            })
        return pd.DataFrame(rows)

    def _build_mentions(self, profile: ProfileConfig, scale: int, crisis_set: set) -> pd.DataFrame:
        n = max(200, scale * 200)
        mention_types = ["mention", "hashtag", "tag", "dm_mention"]
        rows = []
        for _ in range(n):
            d = random_date(profile.start, profile.end, self.rng)
            is_crisis = d in crisis_set
            rows.append({
                "id": new_uuid(),
                "platform": str(weighted_choice(PLATFORMS, PLATFORM_WEIGHTS, 1, self.rng)[0]),
                "mention_type": str(self.rng.choice(mention_types)),
                "source_handle": f"@user_{int(self.rng.integers(1000, 99999))}",
                "reach": int(self.rng.integers(100, 500_000)),
                "sentiment": str(weighted_choice(
                    SENTIMENTS,
                    SENTIMENT_WEIGHTS_CRISIS if is_crisis else SENTIMENT_WEIGHTS_NORMAL,
                    1, self.rng
                )[0]),
                "url": f"https://platform.example.com/post/{int(self.rng.integers(100_000, 999_999))}",
                "body": "Loving the new NovaHome Hub X2!",
                "detected_at": random_datetime(profile.start, profile.end, self.rng),
                "topic": str(self.rng.choice(TOPICS)),
            })
        return pd.DataFrame(rows)

    def _build_dms(
        self, accounts: pd.DataFrame, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(200, scale * 150)
        account_ids = accounts["id"].tolist()
        customer_ids = cache.sample_customer_ids(n, self.rng)
        intents = weighted_choice(DM_INTENTS, DM_INTENT_WEIGHTS, n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "platform": weighted_choice(PLATFORMS, PLATFORM_WEIGHTS, n, self.rng),
            "account_id": self.rng.choice(account_ids, size=n),
            "customer_id": customer_ids,
            "direction": weighted_choice(["inbound", "outbound"], [0.70, 0.30], n, self.rng),
            "intent": intents,
            "body": ["Hello, I need help with my order." for _ in range(n)],
            "created_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "converted_to_ticket_id": [None for _ in range(n)],
        })

    def _build_influencer_partnerships(
        self, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(20, scale * 10)
        rows = []
        for i in range(n):
            tier = str(weighted_choice(INFLUENCER_TIERS, TIER_WEIGHTS, 1, self.rng)[0])
            contract_multiplier = {"nano": 1_000, "micro": 5_000, "mid": 20_000, "macro": 80_000, "mega": 300_000}
            contract_value = round(float(self.rng.uniform(
                contract_multiplier[tier] * 0.5,
                contract_multiplier[tier] * 2.0
            )), 2)
            latest_start = max(profile.start, profile.end - timedelta(days=30))
            start = random_date(profile.start, latest_start, self.rng)
            end = start + timedelta(days=int(self.rng.integers(14, 90)))
            campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else [new_uuid()]
            rows.append({
                "id": new_uuid(),
                "handle": f"@influencer_{i:03d}",
                "platform": str(weighted_choice(PLATFORMS, PLATFORM_WEIGHTS, 1, self.rng)[0]),
                "tier": tier,
                "follower_count": int(self.rng.integers(5_000, 10_000_000)),
                "contract_value": contract_value,
                "currency": "BRL",
                "campaign_id": str(self.rng.choice(campaign_ids)),
                "start_date": start,
                "end_date": end,
                "status": "completed" if end < profile.end else "active",
            })
        return pd.DataFrame(rows)

    def _build_influencer_posts(
        self, partnerships: pd.DataFrame, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        rows = []
        for _, p in partnerships.iterrows():
            n_posts = int(self.rng.integers(1, 6))
            for _ in range(n_posts):
                impressions = int(self.rng.integers(10_000, 5_000_000))
                engagement_rate = float(self.rng.uniform(0.01, 0.08))
                rows.append({
                    "id": new_uuid(),
                    "influencer_id": p["id"],
                    "post_url": f"https://platform.example.com/post/{int(self.rng.integers(1_000_000, 9_999_999))}",
                    "platform": p["platform"],
                    "posted_at": random_datetime(profile.start, profile.end, self.rng),
                    "impressions": impressions,
                    "engagement": round(impressions * engagement_rate),
                    "clicks": int(impressions * self.rng.uniform(0.005, 0.03)),
                    "conversions": int(impressions * self.rng.uniform(0.0005, 0.005)),
                    "attributed_revenue": round(float(self.rng.uniform(500, 50_000)), 2),
                    "currency": "BRL",
                })
        return pd.DataFrame(rows)

    def _build_forum_posts(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(200, scale * 100)
        customer_ids = cache.sample_customer_ids(n, self.rng)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "customer_id": customer_ids,
            "category": weighted_choice(FORUM_CATEGORIES, [0.30, 0.25, 0.20, 0.15, 0.10], n, self.rng),
            "title": [f"Forum post {i + 1}" for i in range(n)],
            "body": ["This is a forum post body." for _ in range(n)],
            "created_at": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "upvotes": self.rng.integers(0, 500, size=n),
            "reply_count": self.rng.integers(0, 50, size=n),
            "status": weighted_choice(["open", "answered", "closed"], [0.40, 0.40, 0.20], n, self.rng),
        })

    def _build_reviews(
        self, cache: MasterCache, profile: ProfileConfig, scale: int, crisis_set: set
    ) -> pd.DataFrame:
        n = max(200, scale * 200)
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [None]
        rows = []
        for _ in range(n):
            d = random_date(profile.start, profile.end, self.rng)
            is_crisis = d in crisis_set
            rating = int(self.rng.choice(
                [1, 2, 3, 4, 5],
                p=[0.30, 0.25, 0.15, 0.15, 0.15] if is_crisis else [0.05, 0.08, 0.12, 0.30, 0.45]
            ))
            rows.append({
                "id": new_uuid(),
                "source": str(weighted_choice(REVIEW_SOURCES, REVIEW_SOURCE_WEIGHTS, 1, self.rng)[0]),
                "rating": rating,
                "title": f"{'Great' if rating >= 4 else 'Poor'} product experience",
                "body": "Loved it!" if rating >= 4 else "Not what I expected.",
                "posted_at": random_datetime(profile.start, profile.end, self.rng),
                "response_body": "Thank you for your feedback!" if self.rng.random() < 0.40 else None,
                "response_employee_id": str(self.rng.choice(employee_ids)) if self.rng.random() < 0.40 else None,
                "product_sku": str(cache.sample_product_skus(1, self.rng)[0]) if len(cache.product_skus) > 0 else None,
            })
        return pd.DataFrame(rows)

    def _build_ad_spend(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        rows = []
        from ..utils.time_utils import date_range_list
        dates = date_range_list(profile.start, profile.date_range_days)
        campaign_ids = cache.campaign_ids if len(cache.campaign_ids) > 0 else [new_uuid()]
        for d in dates:
            for platform in PLATFORMS:
                spend = round(float(self.rng.uniform(100, 10_000)), 2)
                impressions = int(spend * self.rng.uniform(200, 1000))
                ctr = float(self.rng.uniform(0.01, 0.05))
                clicks = int(impressions * ctr)
                cvr = float(self.rng.uniform(0.005, 0.03))
                rows.append({
                    "date": d,
                    "platform": platform,
                    "campaign_id": str(self.rng.choice(campaign_ids)),
                    "ad_set_id": f"ADSET-{int(self.rng.integers(1000, 9999))}",
                    "spend": spend,
                    "impressions": impressions,
                    "clicks": clicks,
                    "conversions": int(clicks * cvr),
                    "currency": "BRL",
                })
        return pd.DataFrame(rows)
