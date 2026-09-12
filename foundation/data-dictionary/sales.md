# Data Dictionary — Sales

Source: `rcd_data/generators/sales.py` (`SalesGenerator`). Order lifecycle
logic lives in `utils/state_machines.py::OrderLifecycle` (has a
`_crisis_overrides()` hook — cancellation rates spike on crisis days).
PII/retention detail: [[ropa]] §1, [[data-classification-retention]].

## `orders`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id` | |
| `store_id` | uuid FK → `stores.id` | |
| `channel` | enum | `web` \| `mobile_app` \| `pos` \| `marketplace` \| `call_center` |
| `marketplace` | string, nullable | Only set when `channel = "marketplace"`: `MercadoLivre` \| `Amazon` \| `Shopee` |
| `status` | string | Terminal state from `OrderLifecycle`, crisis-aware |
| `subtotal`, `shipping`, `tax`, `total` | decimal | `tax` = 12% flat on subtotal; `shipping` = 0 for `pos` |
| `currency` | string | Derived from `store_id`'s country via `COUNTRY_CURRENCY` |
| `promo_code` | string, nullable | ~15% of orders |
| `created_at` | timestamp | Business-hours-weighted (`timestamp_in_business_hours`, BR calendar) |
| `date` | date | Calendar date of `created_at` |

## `order_items`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `order_id` | uuid FK → `orders.id` | |
| `product_id` | string FK → `products.sku` | Despite the name, holds a SKU, not a UUID |
| `quantity` | integer | 1–3 |
| `unit_price` | decimal | Snapshot of `products.price` at order time |
| `discount_pct` | decimal | 0, 5, 10, 15, or 20 |
| `line_total` | decimal | `unit_price * quantity * (1 - discount)` |

## `payments`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `order_id` | uuid FK → `orders.id` | Only orders in a paid-or-later status get a payment row |
| `method` | enum | `credit_card` \| `debit_card` \| `pix` \| `boleto` \| `paypal` \| `rcd_card` \| `bnpl` |
| `installments` | integer | 1–12 for `credit_card`, else 1 |
| `status` | string | Always `"approved"` today — no declined/pending payment rows are modeled |
| `gateway` | enum | `Stripe` \| `Cielo` \| `PagSeguro` \| `MercadoPago` \| `PayPal` |
| `processing_fee` | decimal | 1–3.5% of order total |
| `authorized_at` | timestamp | `orders.created_at` + 1–120s |
| `currency` | string | Copied from `orders.currency` |

## `web_sessions`

| Column | Type | Description |
|---|---|---|
| `session_id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id`, nullable | ~30% anonymous |
| `device` | enum | `desktop` \| `mobile` \| `tablet` |
| `browser`, `os` | string | |
| `utm_source`, `utm_medium`, `utm_campaign` | string | Synthetic attribution tags, not linked to real `campaigns` rows |
| `landing_page` | string | `/page-{1..29}` placeholder |
| `pages_viewed` | integer | 1–24 |
| `duration_s` | integer | |
| `bounced` | boolean | `pages_viewed == 1` |
| `date` | date | |

## `shopping_cart_events`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `session_id` | uuid FK → `web_sessions.session_id` | |
| `customer_id` | uuid FK → `customers.id`, nullable | Copied from the parent session |
| `event_type` | enum | `view` \| `add_to_cart` \| `remove_from_cart` \| `checkout_start` \| `purchase` |
| `product_id` | string FK → `products.sku` | |
| `timestamp` | timestamp | Business-hours-weighted |

Row count ≈ `n_orders * 3`; not deduplicated per session — a session can
appear many times.
