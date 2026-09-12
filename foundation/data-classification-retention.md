# RCD Corp — Data Classification & Retention Policy

Resolves the "TBD" retention column left open in [[ropa]] and the open item
flagged in [[data-privacy-lgpd]] §8. Assigns a sensitivity classification and
a retention period to every one of the 55 tables `rcd_data` produces (see
`foundation/lakehouse-architecture.md` for the full table-to-sink map).

Design-only / narrative document. **No retention job exists in this
codebase** — `rcd_data generate` replaces, `rcd_data stream` only appends;
nothing here is enforced automatically. Periods below are RCD Corp's stated
*policy* (a reasonable, industry-typical starting point), not yet backed by
tooling — flag to Legal/Finance before treating any period as final.

## Classification levels

| Level | Meaning | Handling |
|---|---|---|
| **Public** | Already publicly visible (published social content, public reviews) | No special handling; still subject to platform ToS |
| **Internal** | Business-operational, low individual-privacy impact | Default access: employees on a need-to-know basis |
| **Confidential** | Identifies a specific customer/employee or reveals commercially sensitive terms | Access-controlled; logged access recommended |
| **Restricted** | High-impact personal or financial data (payment, salary, recordings, private messages) | Encrypted at rest, access-controlled + audited, shortest defensible retention |

---

## Master data

| Table | Classification | Retention | Note |
|---|---|---|---|
| `customers` | Confidential | Active relationship + 5 years after last order/account closure | Tied to fiscal retention on `invoices`; anonymize after, don't hard-delete if fiscal linkage still required |
| `employees` | Restricted | Employment + 5 years post-termination | Contains `salary`; CLT labor-claim limitation period is 2 years post-termination / 5 years while employed — 5-year post-termination is the conservative superset; confirm with Legal per country (`employees.country`) |
| `products` | Internal | Indefinite (reference data) | No personal data |
| `suppliers` | Internal | Contract duration + 5 years | Legal-entity data, not personal data under LGPD |
| `stores`, `warehouses` | Internal | Indefinite (reference data) | No personal data |
| `fx_rates` | Internal | Indefinite (reference data) | No personal data |

## Order & payment processing (see [[ropa]] §1)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `orders`, `order_items` | Confidential | 5 years | Aligned to Brazilian fiscal document retention (tax authorities may audit up to 5 years, CTN Art. 173/174) |
| `payments`, `invoices`, `transactions` | Restricted | 5 years | Fiscal/financial record-keeping obligation (LGPD Art. 7 II legal-obligation basis) |
| `rcd_card_transactions` | Restricted | 5 years, tokenized `card_id` only | No raw PAN ever stored — see [[data-privacy-lgpd]] §5 |
| `returns` | Confidential | 5 years | Tied to the originating order's fiscal retention |

## Customer support (see [[ropa]] §2)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `tickets`, `ticket_messages` | Confidential | 3 years | Covers most consumer-protection complaint windows |
| `call_center_calls` metadata | Confidential | 3 years | `recording_url` governed separately below |
| `call_center_calls.recording_url` (audio) | Restricted | 180 days | Voice recordings carry disproportionate storage/privacy cost relative to value beyond near-term QA review; delete audio file at 180 days even if the row (sans URL) is kept for the 3-year metadata window |

## Marketing & communications (see [[ropa]] §3)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `campaigns`, `campaign_events` | Internal | 2 years | Campaign performance reporting window |
| `email_events`, `leads`, `ab_test_exposures` | Confidential | 2 years, or immediately on consent withdrawal | `email_events.event_type = "unsubscribed"` should trigger suppression — not currently enforced by any pipeline in this repo |

## Social media engagement (see [[ropa]] §4)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `social_posts`, `social_metrics`, `reviews`, `community_forum_posts` | Public | As long as published; internal metrics copy 2 years | Public-facing content is platform-controlled, not RCD Corp-controlled |
| `social_comments`, `social_mentions` | Internal | 1 year | Sentiment/trend analysis window |
| `social_dms` | Restricted | 1 year | Private 1:1 correspondence — treat like support tickets, not public content |
| `influencer_partnerships`, `influencer_posts`, `social_ad_spend` | Internal | Contract duration + 5 years | Commercial/contractual record |

## Web & app behavioral tracking (see [[ropa]] §5)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `web_sessions`, `shopping_cart_events` | Internal | 1 year | Anonymous-session rows (`customer_id IS NULL`) pose lower risk but keep the same window for simplicity |

## Employment & HR (see [[ropa]] §6)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `attendance` | Confidential | Employment + 5 years | Same basis as `employees` |
| `performance_reviews`, `training_records`, `engagement_surveys` | Confidential | Employment + 5 years | `engagement_surveys` should be reported in aggregate wherever possible to reduce individual-level exposure |
| `recruitment_pipeline` | Confidential | 1 year from decision for non-hires; converts to `employees` retention on hire | Common practice: retain rejected-candidate data briefly for anti-discrimination defense, then anonymize `candidate_name`/`candidate_email` |

## Manufacturing & operations (see [[ropa]] §7)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `production_runs`, `quality_checks` | Internal | 5 years | Product-liability/quality traceability window |
| `maintenance_events` | Internal | 5 years | Same basis |
| `machine_telemetry` (loadtest profile only) | Internal | 90 days raw, then aggregate/discard | High-volume IoT; no personal data |

## Security & observability (see [[ropa]] §8)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `security_events` | Restricted | 1 year minimum | Needed for incident investigation; do not shorten below what [[data-privacy-lgpd]] §10 breach-notification analysis might require after the fact |
| `errors`, `deployments` | Internal | 1 year | Standard engineering-ops window |
| `app_logs`, `api_requests` (loadtest profile only) | Internal | 30–90 days raw | High-volume; `user_id`/`ip_address` present — shorten if storage cost pushes back, don't lengthen without a reason tied to §8 |

## Supply chain (see [[ropa]] §9)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `purchase_orders`, `shipments`, `stock_movements`, `inventory_snapshots` | Internal | 5 years | Aligned to fiscal retention for consistency with `orders`/`invoices` |

## Finance (non-customer)

| Table | Classification | Retention | Note |
|---|---|---|---|
| `expenses` | Confidential | 5 years | Contains `employee_id`/`approved_by`; fiscal record-keeping basis |
| `budgets` | Internal | Indefinite (reference/planning data) | No personal data |

---

## Maintenance

Update this file in lockstep with [[ropa]]: a new processing activity there
needs a classification + retention row here before it's considered fully
registered. Any period marked with a "confirm with Legal" note should be
resolved before this policy is relied on for a real deployment.
