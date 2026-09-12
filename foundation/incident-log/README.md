# RCD Corp — Crisis Day Incident Log

Explains the `crisis_days` mechanism in `rcd_data` and how to write a
postmortem-style narrative for one, so the structured anomaly a crisis day
produces has a matching unstructured "why" — useful for demos that combine
structured and unstructured data (RAG over incident write-ups correlated
against the metrics that moved).

## How crisis days are chosen

`utils/time_utils.py::generate_crisis_days(start, end, freq_per_month, rng)`
picks `crisis_freq_per_month` random calendar days per month across the
profile's date range (`config.yaml` → `profiles.<name>.crisis_freq_per_month`).
**Crisis days are generic — the generator does not encode a cause.** A
given run's crisis days are just dates; whether that's a payment outage, a
product recall, a data breach, or bad PR is undetermined by the data itself.
That's what a postmortem in this folder supplies: a plausible cause,
written after the fact, for one specific crisis day.

Note this doc is written against the mechanism's current behavior, not a
specific run's actual dates — `generate_crisis_days` is seed-dependent, so
plug in the real date(s) from `rcd-data generate`'s log output
(`log.info("crisis_days", days=[...])`) when you adapt a template below.

## What actually moves on a crisis day

Cross-referenced against the generator source, not the (unused — see below)
`config.yaml` crisis parameters:

| Signal | Baseline | Crisis day |
|---|---|---|
| Order cancellation at `pending` (`OrderLifecycle`) | 18% | 50% |
| Shipment outcome once `shipped` | 88% delivered / 9% returned / 3% lost | 60% delivered / 30% returned / 10% lost |
| Ticket escalation at `open` (`TicketLifecycle`) | 15% | 45% |
| Ticket escalation at `in_progress` | 10% | 50% |
| Ticket sentiment | weighted positive/neutral/negative | forced `negative` |
| Ticket volume | baseline rate | + ~4x extra tickets that day (`support.py::_build_tickets`) |
| Call volume | baseline rate | + ~2x extra calls that day |
| Social sentiment (`social_comments`, `social_mentions`, `reviews`) | 55/30/15 positive/neutral/negative | 20/25/55 |
| Review star rating | skewed toward 4–5 | skewed toward 1–2 |

**Known gap**: `config.yaml`'s `crisis:` block (`sentiment_shift`,
`ticket_multiplier: 5`, `social_negative_multiplier: 3`, `duration_days: 3`)
is declared but **never read by any generator** — each generator hardcodes
its own crisis-day constants instead (as tabulated above), and no crisis
lasts more than a single calendar day (`duration_days` is unused). Don't
tune the config block expecting it to change generator behavior until
that's wired up.

## Writing a postmortem

1. Pick (or generate) a crisis day.
2. Pick a plausible cause — see the two templates in this folder for a
   payment-processing outage and a product-quality recall.
3. Write it in standard postmortem shape: impact window, what customers
   experienced, what support/social saw, root cause, resolution, follow-ups.
4. Keep the narrative consistent with the table above — e.g. a product
   recall should show up in `reviews`/`social_comments` sentiment and
   `returns` reason codes more than in payment-specific fields, whereas a
   payment outage should show up in `orders` cancellation and `tickets`
   category `billing`/`order_issue`.

## Templates in this folder

- [payment-outage-template.md](payment-outage-template.md)
- [product-recall-template.md](product-recall-template.md)
