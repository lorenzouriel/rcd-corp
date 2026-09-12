# RCD Corp — Customer Support SLA & Escalation

In-universe policy for the `support` domain
(`rcd_data/generators/support.py`). Schema: [[data-dictionary/support]].
Privacy treatment: [[ropa]] §2.

## Channels & categories

Channels: email (35%), chat (30%), phone (20%), social_media (10%), app
(5%). Categories, by volume: order_issue (20%), delivery_problem (18%),
product_defect (15%), billing (12%), account_access (10%),
returns_refunds (10%), technical_support (8%), general_inquiry (7%).

## Priority & target handling

| Priority | Share | Suggested first-response target |
|---|---|---|
| critical | 5% | 1 hour |
| high | 20% | 4 hours |
| medium | 45% | 1 business day |
| low | 30% | 2 business days |

(Targets are policy, not enforced anywhere in the generator — there's no
SLA-breach field in the `tickets` schema.)

## Ticket lifecycle & escalation

`open → in_progress → waiting_customer → resolved → closed`, with
`escalated` as a side-branch back into `in_progress`
(`utils/state_machines.py::TicketLifecycle`). Baseline escalation rate out
of `open` is 15%; **on a crisis day it triples to 45%**, and the
`in_progress → escalated` rate goes from 10% to 50%. Crisis-day tickets are
also forced to `negative` sentiment regardless of the normal
positive/neutral/negative mix. See [[incident-log/README]] for how to write
up a specific crisis day.

## CSAT

Collected only on `closed` tickets, 1–5 scale, skewed positive in the
baseline data (37% give a 5, 5% give a 1) — this baseline is **not**
crisis-adjusted in the generator today, so a crisis day's CSAT distribution
looks the same as a normal day's even though sentiment and escalation rate
are both worse. Worth knowing if you build a CSAT-vs-crisis correlation
demo off this dataset — the correlation isn't actually encoded on the CSAT
field itself.

## Call center

Resolution codes: resolved (65%), escalated (10%), no_answer (10%),
voicemail (8%), transferred (7%). ~70% of calls have a recording
(`call_center_calls.recording_url`) — see [[data-classification-retention]]
for the 180-day audio retention window, shorter than the 3-year window for
the call metadata itself.
