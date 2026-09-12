# Data Dictionary — Social Media

Source: `rcd_data/generators/social_media.py` (`SocialMediaGenerator`) — 11
tables across 6 platforms (`instagram`, `tiktok`, `youtube`, `x`, `linkedin`,
`facebook`) and 6 brand accounts (`@rcdcorp`, `@rcd_novahome`,
`@pulseaudio_br`, `@guardian_iq`, `@rcd_online`, `@rcd_corp_pt`). Sentiment
distribution shifts on crisis days (55% negative vs. 15% baseline) across
`social_comments`, `social_mentions`, and `reviews`. PII/retention detail:
[[ropa]] §4, [[data-classification-retention]].

## `social_accounts`

One row per (brand account × platform) — 36 rows total. `id`, `handle`,
`name`, `platform`, `account_type` (`brand`/`product`/`ecommerce`/`regional`),
`follower_count`, `following_count`, `verified`, `created_at`. No personal data.

## `social_posts`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `account_id` | uuid FK → `social_accounts.id` | |
| `platform` | string | Copied from the account |
| `post_type` | string | Platform-appropriate (e.g. `reel`/`story` only on Instagram) |
| `caption`, `hashtags` | string | Placeholder text |
| `posted_at` | timestamp | 60% weighted to platform peak hours (`timestamp_social_peak`) |
| `campaign_id` | uuid FK → `campaigns.id`, nullable | ~60% of posts |
| `product_sku` | string FK → `products.sku`, nullable | ~50% of posts |
| `author_employee_id` | uuid FK → `employees.id` | |
| `date` | date | |

## `social_metrics`

Hourly snapshots for the **first 72 hours** after each post — 72 rows per
post. `id`, `post_id` (FK → `social_posts.id`), `snapshot_ts`, `impressions`,
`reach`, `likes`, `comments`, `shares`, `saves`, `video_views`,
`avg_watch_time_s`, `link_clicks`, `profile_visits`. Video-type posts get a
3–5x reach multiplier. No personal data.

## `social_comments`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `post_id` | uuid FK → `social_posts.id` | |
| `platform` | string | Independently re-sampled — **not guaranteed to match** `social_posts.platform` for the same `post_id` |
| `customer_id` | uuid FK → `customers.id` | |
| `parent_comment_id` | uuid, nullable | Always `None` — no threaded-reply structure is modeled |
| `body` | string | One of 3 fixed placeholder strings keyed to `sentiment` |
| `sentiment` | enum | `positive` \| `neutral` \| `negative` — crisis-aware |
| `language`, `posted_at` | string, timestamp | |
| `is_spam`, `is_moderated` | boolean | ~3% / ~5% |

## `social_mentions`

Public brand mentions not tied to a specific RCD post: `id`, `platform`,
`mention_type` (mention/hashtag/tag/dm_mention), `source_handle` (synthetic
`@user_#####`, not a real account), `reach`, `sentiment` (crisis-aware),
`url`, `body`, `detected_at`, `topic`.

## `social_dms`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `platform` | string | |
| `account_id` | uuid FK → `social_accounts.id` | |
| `customer_id` | uuid FK → `customers.id` | |
| `direction` | enum | `inbound` (70%) \| `outbound` (30%) |
| `intent` | enum | support, order_inquiry, complaint, compliment, product_question |
| `body` | string | Fixed placeholder text, same for every row |
| `created_at` | timestamp | |
| `converted_to_ticket_id` | uuid, nullable | Always `None` — no DM→ticket linkage logic exists yet |

**Restricted** — private correspondence, treat like `ticket_messages`, not
like public post content.

## `influencer_partnerships`

`id`, `handle` (synthetic `@influencer_###`), `platform`, `tier`
(nano/micro/mid/macro/mega), `follower_count`, `contract_value`, `currency`
(always BRL), `campaign_id` (FK → `campaigns.id`), `start_date`, `end_date`,
`status`. Commercial/contractual data, not personal data (influencers are
identified by handle only, not a real name).

## `influencer_posts`

1–5 rows per partnership: `id`, `influencer_id` (FK →
`influencer_partnerships.id`), `post_url`, `platform`, `posted_at`,
`impressions`, `engagement`, `clicks`, `conversions`, `attributed_revenue`,
`currency`.

## `community_forum_posts`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id` | |
| `category` | enum | support, tips_tricks, product_feedback, general, announcements |
| `title`, `body` | string | Placeholder text |
| `created_at` | timestamp | |
| `upvotes`, `reply_count` | integer | |
| `status` | enum | `open` \| `answered` \| `closed` |

## `reviews`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `source` | enum | google, trustpilot, reclame_aqui, appstore, playstore |
| `rating` | integer | 1–5, crisis-aware distribution (skews low on crisis days) |
| `title`, `body` | string | Placeholder text keyed to rating |
| `posted_at` | timestamp | |
| `response_body`, `response_employee_id` | string / uuid FK → `employees.id`, both nullable | ~40% of reviews get a response |
| `product_sku` | string FK → `products.sku`, nullable | |

**No `customer_id` field** — reviews are not linked back to the customer who
wrote them.

## `social_ad_spend`

One row per (date × platform) — daily grain, no PK column of its own besides
the implicit `(date, platform)` pair. `date`, `platform`, `campaign_id` (FK →
`campaigns.id`), `ad_set_id` (synthetic `ADSET-####`), `spend`,
`impressions`, `clicks`, `conversions`, `currency` (always BRL). No personal
data.
