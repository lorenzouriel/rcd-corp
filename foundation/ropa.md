# RCD Corp — Record of Processing Activities (ROPA)

Registro de Operações de Tratamento de Dados Pessoais, required under LGPD
Art. 37. Companion to [[data-privacy-lgpd]] — that document sets the rules,
this one is the inventory those rules apply to. Registered by *processing
activity* (as LGPD requires), not by raw table, since several tables usually
serve one activity.

Design-only / narrative document, grounded in the tables `rcd_data` actually
generates (see `foundation/lakehouse-architecture.md` for the full 55-table
map). Update this file whenever a generator adds, removes, or repurposes a
personal-data field.

Legend — **Sensitivity**: `Std` (ordinary personal data) · `Fin` (financial/
payment) · `None` (no personal data, included for FK completeness only).
No activity below touches LGPD Art. 5 II special-category data — see
[[data-privacy-lgpd]] §5.

---

## 1. Order & payment processing

| | |
|---|---|
| **Tables** | `orders`, `order_items`, `payments`, `invoices`, `transactions`, `rcd_card_transactions`, `returns` |
| **Purpose** | Fulfill purchase contracts, process payment, issue tax invoices, handle returns |
| **Data subjects** | Customers |
| **Personal data** | `customer_id` → resolves to name/email/phone/CPF-CNPJ in `customers`; card token (`card_id`, not a real PAN) and transaction amounts in `rcd_card_transactions` |
| **Legal basis** | Contract execution (Art. 7 V); legal obligation for `invoices`/`transactions` (tax record-keeping, Art. 7 II) |
| **Recipients** | Payment processor (operator) for `rcd_card` method; no other third-party sharing |
| **Retention** | TBD — see [[data-privacy-lgpd]] §8 (open item) |
| **Cross-border transfer** | Yes — consolidated in BR regardless of customer's `country` |
| **Sensitivity** | Fin |

## 2. Customer support

| | |
|---|---|
| **Tables** | `tickets`, `ticket_messages`, `call_center_calls` |
| **Purpose** | Resolve customer issues, quality/QA review of agent interactions |
| **Data subjects** | Customers, support agents (`employees` via `agent_id`) |
| **Personal data** | `customer_id`, free-text `body` in `ticket_messages`, `recording_url` (voice audio) in `call_center_calls` |
| **Legal basis** | Contract execution (Art. 7 V); legitimate interest for QA (Art. 7 IX) |
| **Recipients** | None external; recordings hosted at `recordings.rcd.internal` |
| **Retention** | TBD |
| **Cross-border transfer** | Yes |
| **Sensitivity** | Std (voice recordings — treat with extra access controls even though not biometric-classified; see [[data-privacy-lgpd]] §5) |

## 3. Marketing & communications

| | |
|---|---|
| **Tables** | `campaigns`, `campaign_events`, `email_events`, `leads`, `ab_test_exposures` |
| **Purpose** | Run campaigns, measure engagement, lead scoring, product experimentation |
| **Data subjects** | Customers, prospects (leads not yet converted to customers) |
| **Personal data** | `customer_id`, contact `email` in `email_events`, lead `score` |
| **Legal basis** | Consent (Art. 7 I) for marketing sends; legitimate interest (Art. 7 IX) for on-site A/B testing |
| **Recipients** | Email/SMS delivery vendor (operator) |
| **Retention** | TBD; unsubscribes (`email_events.event_type = "unsubscribed"`) should suppress future sends — not currently enforced anywhere in the generator or a real send pipeline |
| **Cross-border transfer** | Yes |
| **Sensitivity** | Std |

## 4. Social media engagement

| | |
|---|---|
| **Tables** | `social_accounts`, `social_posts`, `social_metrics`, `social_comments`, `social_mentions`, `social_dms`, `reviews`, `influencer_partnerships`, `influencer_posts`, `community_forum_posts`, `social_ad_spend` |
| **Purpose** | Brand engagement, DM-based support/sales, sentiment monitoring, influencer program management |
| **Data subjects** | Customers/public individuals interacting with RCD Corp's social accounts (identified only by `handle`/`account_id`, not resolved to `customers` records) |
| **Personal data** | Social handles, DM `content`/`intent`, comment text |
| **Legal basis** | Consent — platform ToS + user-initiated contact (Art. 7 I); legitimate interest for public sentiment monitoring (Art. 7 IX) |
| **Recipients** | Social platform providers (joint/independent controllers per their own terms, outside RCD Corp's control) |
| **Retention** | TBD |
| **Cross-border transfer** | Yes — platform data is inherently global |
| **Sensitivity** | Std |

## 5. Web & app behavioral tracking

| | |
|---|---|
| **Tables** | `web_sessions`, `shopping_cart_events` |
| **Purpose** | Site analytics, cart-abandonment recovery, conversion measurement |
| **Data subjects** | Customers and anonymous visitors |
| **Personal data** | `customer_id` (nullable — anonymous sessions exist), `device` type |
| **Legal basis** | Legitimate interest (Art. 7 IX); consent where cookies are used for marketing attribution |
| **Recipients** | None external today (no analytics vendor modeled) |
| **Retention** | TBD |
| **Cross-border transfer** | Yes |
| **Sensitivity** | Std |

## 6. Employment & HR

| | |
|---|---|
| **Tables** | `employees`, `attendance`, `performance_reviews`, `training_records`, `recruitment_pipeline`, `engagement_surveys` |
| **Purpose** | Payroll and employment administration, performance management, hiring, workforce engagement |
| **Data subjects** | Employees, job candidates (`candidate_email` in `recruitment_pipeline`) |
| **Personal data** | Name, email, `salary`, `department`, `manager_id`, review scores, candidate contact info |
| **Legal basis** | Contract execution — employment contract (Art. 7 V); legal obligation for payroll/`salary` records (Art. 7 II) |
| **Recipients** | None external; internally restricted to HR + reporting manager |
| **Retention** | TBD — labor-law record retention typically exceeds standard commercial retention; flag explicitly when [[data-privacy-lgpd]] §8 is resolved |
| **Cross-border transfer** | Yes — employees exist across BR/MX/PT/US but HR systems consolidate in BR |
| **Sensitivity** | Std (no health/union/political data modeled — see [[data-privacy-lgpd]] §5) |

## 7. Manufacturing & operations attribution

| | |
|---|---|
| **Tables** | `production_runs`, `quality_checks`, `maintenance_events` (via `operator_id`/`technician_id`) |
| **Purpose** | Attribute plant-floor actions to the responsible employee for quality/safety accountability |
| **Data subjects** | Employees (machine operators, maintenance technicians) |
| **Personal data** | `employee_id` reference only — no other personal fields |
| **Legal basis** | Legitimate interest — safety/quality accountability (Art. 7 IX) |
| **Recipients** | None external |
| **Retention** | TBD |
| **Cross-border transfer** | No — plant-local |
| **Sensitivity** | Std |

## 8. Security & observability

| | |
|---|---|
| **Tables** | `security_events`, `deployments` (via `deployed_by`), `api_requests` (loadtest profile only, carries `ip_address`) |
| **Purpose** | Intrusion detection, change-management audit trail, infrastructure monitoring |
| **Data subjects** | Employees (`deployed_by`), and any individual whose request hits the platform (`ip_address`) |
| **Personal data** | `employee_id`, IP address (LGPD treats IP as personal data when linkable to an individual) |
| **Legal basis** | Legitimate interest — security monitoring (Art. 7 IX); legal obligation where incident logs are needed for breach response (Art. 7 II) |
| **Recipients** | None external |
| **Retention** | TBD — security logs often need a *longer* minimum retention than commercial data for incident investigation; resolve alongside [[data-privacy-lgpd]] §8 |
| **Cross-border transfer** | No — infrastructure-local |
| **Sensitivity** | Std |

## 9. Supplier relationship management

| | |
|---|---|
| **Tables** | `suppliers`, `purchase_orders`, `shipments`, `stock_movements` |
| **Purpose** | Procurement and logistics |
| **Data subjects** | None directly — `suppliers.name`/CNPJ are legal-entity data, not personal data, under LGPD (personal data is defined re: identified/identifiable *natural* persons) |
| **Personal data** | None modeled (no named supplier contact person exists in the schema today) |
| **Legal basis** | N/A |
| **Recipients** | N/A |
| **Retention** | N/A |
| **Cross-border transfer** | N/A |
| **Sensitivity** | None |

---

## Maintenance

When a new generator or table is added: identify which activity above it
extends (or whether it's a new activity), add/update the row, and re-check
[[data-privacy-lgpd]] §4 (legal bases) and §5 (special-category statement)
for consistency.
