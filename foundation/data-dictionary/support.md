# Data Dictionary — Customer Support

Source: `rcd_data/generators/support.py` (`SupportGenerator`). Ticket status
comes from `utils/state_machines.py::TicketLifecycle` — crisis days apply a
5x ticket-volume multiplier (`crisis.ticket_multiplier` in `config.yaml`)
and force negative sentiment. PII/retention detail: [[ropa]] §2;
[[data-classification-retention]].

## `tickets`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id` | |
| `channel` | enum | email, chat, phone, social_media, app |
| `category` | enum | order_issue, delivery_problem, product_defect, billing, account_access, returns_refunds, technical_support, general_inquiry |
| `subject` | string | `"Issue with {category}"` |
| `status` | string | Terminal state from `TicketLifecycle`, crisis-aware |
| `priority` | enum | low, medium, high, critical |
| `sentiment` | enum | Forced `negative` on crisis days, else weighted positive/neutral/negative |
| `created_at` | timestamp | |
| `resolved_at` | timestamp, nullable | Only when `status = "closed"` |
| `csat_score` | integer 1–5, nullable | Only when closed |
| `agent_id` | uuid FK → `employees.id` | |

## `ticket_messages`

2–7 messages per ticket, timestamps walking forward from `tickets.created_at`.

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `ticket_id` | uuid FK → `tickets.id` | |
| `sender_type` | enum | customer, agent, bot, system |
| `sender_id` | uuid | `tickets.customer_id` if sender is customer, else `tickets.agent_id` — **not populated** for `bot`/`system` senders (holds the agent ID by default in that branch) |
| `body` | string | Placeholder text (`"Message {n} regarding ticket {category}."`) |
| `created_at` | timestamp | |
| `is_internal_note` | boolean | ~10% |

## `call_center_calls`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `customer_id` | uuid FK → `customers.id` | |
| `agent_id` | uuid FK → `employees.id` | |
| `duration_s` | integer | Normal(240, 180), clipped [30, 2400] |
| `direction` | enum | `inbound` (75%) \| `outbound` (25%) |
| `call_type` | enum | inbound, outbound, callback |
| `sentiment` | enum | positive, neutral, negative |
| `resolution` | enum | resolved, escalated, no_answer, voicemail, transferred |
| `created_at` | timestamp | |
| `recording_url` | string, nullable | ~70% of calls; points at `recordings.rcd.internal` — **Restricted**, see [[data-classification-retention]] for the shorter audio-specific retention window |
| `wait_time_s` | integer | 10–600 |
