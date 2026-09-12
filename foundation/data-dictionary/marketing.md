# Data Dictionary — Marketing

Source: `rcd_data/generators/marketing.py` (`MarketingGenerator`). Lead
status comes from `utils/state_machines.py::LeadPipeline`. PII/retention
detail: [[ropa]] §3, [[data-classification-retention]].

## `campaigns`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key — cached into `MasterCache.campaign_ids` for downstream FK use |
| `name` | string | `"Campaign {seq} — {Type}"` |
| `type` | enum | email, social_paid, search_sem, display, influencer, sms, push |
| `channel` | enum | email, instagram, google, facebook, tiktok, youtube, linkedin |
| `start_date`, `end_date` | date | 7–60 day span, capped at profile end |
| `budget`, `actual_spend` | decimal | `actual_spend` = 50–105% of `budget` |
| `currency` | string | Always `BRL` |
| `target_segment` | enum | B2C, B2B, VIP, all, new_customers, churned |
| `status` | string | `completed` if `end_date < profile.end`, else `active` |
| `owner_employee_id` | uuid FK → `employees.id`, nullable | Always `None` today — no owner-assignment logic exists yet |

## `campaign_events`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `campaign_id` | uuid FK → `campaigns.id` | |
| `customer_id` | uuid FK → `customers.id` | |
| `event_type` | enum | impression, click, conversion, video_view, lead_form |
| `timestamp` | timestamp | |

## `email_events`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `campaign_id` | uuid FK → `campaigns.id` | |
| `customer_id` | uuid FK → `customers.id` | |
| `email` | string | **Placeholder, not real** — always `user{i}@example.com`, does not match `customers.email` |
| `event_type` | enum | sent, delivered, opened, clicked, bounced, unsubscribed |
| `timestamp` | timestamp | |

## `leads`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id` | A lead is modeled as an existing customer row, not a separate prospect entity |
| `source` | enum | organic_search, paid_search, social_media, email, referral, direct, event |
| `campaign_id` | uuid FK → `campaigns.id` | |
| `status` | string | Terminal state from `LeadPipeline` |
| `score` | integer | 0–100 |
| `created_at`, `updated_at` | timestamp | `updated_at` = `created_at` + 1–720h |
| `owner_employee_id` | uuid FK → `employees.id` | Sales owner |

## `ab_test_exposures`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `test_name` | enum | checkout_flow_v2, homepage_hero, email_subject_test, pricing_page_cta, cart_banner |
| `variant` | enum | control \| variant_a \| variant_b |
| `customer_id` | uuid FK → `customers.id` | |
| `session_id` | uuid | Independently generated — **not** an FK to `web_sessions.session_id` |
| `exposed_at` | timestamp | |
| `converted` | boolean | ~12% true, independent of `variant` |
