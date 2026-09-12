# RCD Corp — Marketing & Brand Guidelines

In-universe policy spanning the `marketing` and `social_media` domains
(`rcd_data/generators/marketing.py`, `social_media.py`). Schema:
[[data-dictionary/marketing]], [[data-dictionary/social-media]]. Privacy
treatment: [[ropa]] §3–§4.

## Brand accounts

6 handles across 6 platforms each (Instagram, TikTok, YouTube, X,
LinkedIn, Facebook) — see [[company-overview]] for the handle list.
Platform-appropriate formats only: Reels/Stories on Instagram, Shorts on
TikTok, Video/Shorts/Live on YouTube, and so on
(`social_media.py::POST_TYPES_BY_PLATFORM`) — never post a format a
platform doesn't support.

## Campaign types

email (30%), social_paid (25%), search_sem (15%), display (10%),
influencer (10%), sms (5%), push (5%), run across 7 target-segment options
(B2C, B2B, VIP, all, new_customers, churned). Actual spend typically lands
50–105% of planned budget — a campaign materially over 105% of budget is
worth a variance flag.

## Lead scoring & funnel

Leads flow `new → contacted → qualified → proposal → won/lost`, with a
`nurturing` loop for not-yet-ready prospects
(`utils/state_machines.py::LeadPipeline`). Lead sources: organic_search
(25%), paid_search (20%), social_media (20%), email (15%), referral (10%),
direct (5%), event (5%). Score is 0–100.

## A/B testing

Standing experiments: `checkout_flow_v2`, `homepage_hero`,
`email_subject_test`, `pricing_page_cta`, `cart_banner`, each with
`control`/`variant_a`/`variant_b` arms. Baseline conversion is ~12%
regardless of arm in the synthetic data — don't expect a real lift signal
between variants here; that's a generator simplification, not a finding.

## Social moderation

Comments are auto-flagged `is_spam` (~3%) and `is_moderated` (~5%) —
moderation queue volume should be sized against the moderated rate, not
total comment volume. Sentiment runs positive/neutral/negative at a 55/30/15
baseline split; **on a crisis day it inverts to 20/25/55**, applied
consistently across `social_comments`, `social_mentions`, and `reviews` —
see [[incident-log/README]] for how a real incident would move these three
signals together.

## Influencer program

5 tiers by follower count — nano, micro, mid, macro, mega — with contract
value scaling roughly 5x per tier step (nano ~R$1-2k, mega ~R$150-600k per
partnership). Engagement rate on sponsored posts runs 1–8% of impressions.
