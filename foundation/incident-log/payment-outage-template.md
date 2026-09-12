# Incident Template — Payment Gateway Outage

Illustrative postmortem for a crisis day whose cause is a payment-processing
failure. Replace `{DATE}` with the actual crisis day you're annotating (see
[[README]]) and adjust names/times to match.

---

**Incident**: Elevated checkout failures — {DATE}
**Duration**: ~6 hours (10:00–16:00 BRT)
**Severity**: High
**Status**: Resolved

## Summary

One of RCD Corp's payment gateways (Cielo or PagSeguro, per
`generators/finance.py::PAYMENT_GATEWAYS`) returned elevated authorization
timeouts starting mid-morning. Customers attempting checkout on `web` and
`mobile_app` saw failed or stuck payment confirmations; `pos` (in-store)
was unaffected since it doesn't route through the same gateway pool.

## Impact

- Orders stuck at `pending` had roughly 3x the normal cancellation rate as
  customers gave up and abandoned checkout (see [[README]] for the
  `OrderLifecycle` crisis-mode numbers).
- Support saw a spike in `billing` and `order_issue` category tickets,
  escalating faster than usual as agents couldn't confirm payment status
  with the gateway.
- Social mentions/comments turned negative, mostly `topic = pricing` or
  `general` rather than product-quality complaints — a useful discriminator
  from a product-recall-caused crisis day.

## Root cause

Gateway-side capacity issue during a routine failover test that ran longer
than planned (fictional — adapt to whatever's plausible for your scenario).

## Resolution

Gateway provider rolled back the failover change; RCD Corp's checkout
service retried queued authorizations once the gateway recovered.

## Follow-ups

- Add automatic gateway failover to a secondary provider (MercadoPago) for
  future capacity events.
- Add customer-facing checkout-status messaging so failed payments don't
  read as a silent hang.
