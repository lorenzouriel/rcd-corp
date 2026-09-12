# Incident Template — Product Quality Issue

Illustrative postmortem for a crisis day whose cause is a product-quality
problem surfacing publicly. Replace `{DATE}` and the product with the
actual crisis day and a real SKU from `products` (see [[README]]).

---

**Incident**: NovaHome Hub X2 overheating reports — {DATE}
**Duration**: 3 days (matches `crisis.duration_days` intent — see the
[[README]] note that this isn't actually enforced by the generator)
**Severity**: High
**Status**: Under review

## Summary

A cluster of customers reported their NovaHome Hub X2 running unusually
hot, first surfacing on social media (`social_comments`,
`topic = product_quality`) before reaching support tickets in volume.

## Impact

- `reviews` for the affected SKU skewed sharply toward 1–2 stars for the
  duration of the crisis window (see [[README]]'s crisis-mode rating
  distribution).
- `tickets` volume spiked in the `product_defect` category; ticket
  sentiment forced negative and escalation rate roughly tripled.
- `returns` volume for the SKU rose, reason weighted toward `defective`.
- Order cancellations rose for pending orders containing the SKU — cautious
  customers backing out before the return-safety of delivery.
- No abnormal `payments`/`invoices` impact — this is a product-quality
  crisis, not a financial-systems one; use that absence as the
  discriminator from the payment-outage template.

## Root cause

Batch of units from a production run at the Manaus factory
(`production_runs.factory_location = "Manaus"`) shipped despite a
below-threshold `quality_checks.pass_rate` on final QA (fictional — tie to
an actual `production_run_id`/`quality_checks` row if you're annotating a
specific generated dataset).

## Resolution

Affected batch identified via `production_runs` → `quality_checks` lookup;
proactive outreach to customers who received units from that batch
(joinable via `order_items.product_id` + order date range).

## Follow-ups

- Tighten the final-QA pass-rate threshold for thermal-sensitive SKUs (see
  [[manufacturing-safety-qa]]).
- Add a batch/lot-number field to `production_runs` so affected-unit
  lookups don't require a date-range approximation.
