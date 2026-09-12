# Data Dictionary — Finance

Source: `rcd_data/generators/finance.py` (`FinanceGenerator`). Row counts
scale off `profile.n_orders // 10_000` ("scale"). PII/retention detail:
[[ropa]] §1, [[data-classification-retention]].

## `invoices`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `order_id` | uuid FK → `orders.id` | Sampled from `MasterCache.order_ids`; not guaranteed 1:1 with a real order once volumes diverge |
| `customer_id` | uuid FK → `customers.id` | |
| `amount` | decimal | |
| `currency` | string | Randomly one of BRL/MXN/EUR/USD per row — **not** derived from `customer_id`'s actual country |
| `issued_at` | date | |
| `due_at` | date | `issued_at + 30 days` |
| `status` | enum | `draft` \| `sent` \| `paid` \| `overdue` \| `cancelled` |
| `payment_method` | enum | `credit_card` \| `pix` \| `boleto` \| `rcd_card` |

## `transactions`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `type` | enum | `sale` \| `refund` \| `adjustment` \| `fee` \| `transfer` |
| `amount` | decimal | |
| `currency` | string | Random per row, same caveat as `invoices.currency` |
| `from_account`, `to_account` | string | Synthetic `ACC-####` ledger account codes — not linked to any other table |
| `created_at` | timestamp | |
| `status` | enum | `completed` \| `pending` \| `failed` |
| `reference` | string | Synthetic `TXN-######` |

## `expenses`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `category` | enum | travel, software, hardware, marketing, office_supplies, consulting, training, meals, utilities, miscellaneous |
| `amount` | decimal | |
| `currency` | string | Always `BRL` |
| `department` | enum | Engineering, Marketing, Sales, Finance, HR, Other — **note:** this is a narrower ad-hoc list, not `config.yaml`'s 13-department list |
| `employee_id` | uuid FK → `employees.id` | Submitter |
| `approved_by` | uuid FK → `employees.id` | Approver — sampled independently, not validated against an approval hierarchy |
| `date` | date | |
| `description` | string | Placeholder text (`"Expense item {i}"`) |
| `status` | enum | `approved` \| `pending` \| `rejected` |

## `budgets`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `department` | enum | 9 departments (subset of `config.yaml`'s list — excludes Supply Chain, Manufacturing, Legal) |
| `year`, `quarter` | integer | Covers the profile's start and end years |
| `category` | enum | `headcount` \| `software` \| `marketing` \| `capex` \| `opex` |
| `planned_amount`, `actual_amount` | decimal | `actual` = `planned * variance` (normal around 1.0, floored at 0.5) |
| `currency` | string | Always `BRL` |

No personal data.

## `rcd_card_transactions`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `card_id` | string | Synthetic `CARD-######` token — **not a real PAN**, no card-number field exists anywhere in this schema |
| `customer_id` | uuid FK → `customers.id` | |
| `merchant_category` | enum | electronics, restaurants, travel, fuel, grocery, entertainment, utilities, healthcare, clothing, online_retail |
| `amount` | decimal | |
| `currency` | string | Always `BRL` |
| `status` | enum | `approved` \| `declined` \| `pending` (weighted ~70/15/rest split across a few statuses) |
| `posted_at` | timestamp | |
