# RCD Corp — Sales Playbook

In-universe SOP for the `sales` domain (`rcd_data/generators/sales.py`).
Schema: [[data-dictionary/sales]]. Privacy treatment: [[ropa]] §1.

## Channels

`web` (35%), `mobile_app` (30%), `pos` (20%), `marketplace` (10%),
`call_center` (5%). Marketplace orders additionally route through
MercadoLivre, Amazon, or Shopee.

## Order lifecycle

`pending → paid → picked → shipped → delivered`, with `cancelled`,
`returned`, `refunded`, and `lost` as alternate terminal states
(`utils/state_machines.py::OrderLifecycle`). Baseline cancellation rate at
`pending` is 18%; on a crisis day (see [[incident-log/README]]) it jumps to
50%, and in-transit orders are far more likely to arrive `returned` or
`lost` instead of `delivered`.

## Pricing & promotions

Tax is a flat 12% on subtotal. Shipping is waived for in-store (`pos`)
purchases. ~15% of orders carry a promo code: `RCD10`, `BF2024`,
`SUMMER15`, or `FIDELIDADE20`. Line-item discounts are one of 0/5/10/15/20%,
applied per order line, independent of any order-level promo code.

## Seasonal demand (design intent vs. current behavior)

The generator computes a demand multiplier from two effects meant to model
real seasonal patterns:

- **Black Friday / Cyber Monday**: 10x on the days themselves, 3x during
  BF week, 2x through the rest of December (`black_friday_multiplier`)
- **Brazilian payday effect**: 2x order volume on the 5th and 20th of each
  month (`payday_multiplier`)

**Known gap**: `_build_orders` computes this multiplier but never applies
it to actually skew order volume or resample dates — the multiplier is
computed and discarded (see the `pass` in that loop). If you're relying on
this dataset to demonstrate a Black-Friday or payday spike, it isn't there
today; that's a generator fix, not a documentation issue.

## Returns

Return reasons, weighted: defective (25%), changed_mind (20%), wrong_item
(15%), damaged_in_transit (15%), not_as_described (10%), late_delivery
(10%), duplicate_order (5%). Return status: requested → approved → received
→ refunded (or rejected, 5% of the time). Returns are tracked in the
`supply_chain` domain (`returns` table), not `sales` — see
[[data-dictionary/supply-chain]].

## Payment acceptance

See [[finance-policies]] for the full payment-method and gateway list.
Every payment in the synthetic data is recorded as `approved` — no
declined/failed customer payment attempts are modeled, so don't expect a
payment-failure-rate metric to be meaningful against this dataset as-is.
