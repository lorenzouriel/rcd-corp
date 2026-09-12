# RCD Corp — Finance Policies

In-universe policy grounding the `finance` domain
(`rcd_data/generators/finance.py`). See [[data-dictionary/finance]] for
schema and [[ropa]] §1 for the privacy treatment of anything customer-linked.

## Invoicing

Invoices carry a 30-day payment term (`due_at = issued_at + 30 days`,
`generators/finance.py::_build_invoices`). Accepted invoice payment methods:
`credit_card`, `pix`, `boleto`, `rcd_card`. Status lifecycle is not a
formal state machine in the generator — `draft → sent → paid → overdue →
cancelled` are sampled independently per invoice, weighted 5/10/75/8/2%.

## Expense policy

Reimbursable categories: travel, software, hardware, marketing, office
supplies, consulting, training, meals, utilities, miscellaneous. Every
expense requires an `approved_by` employee distinct from the submitter in
the org chart — **note**: the generator samples `approved_by` independently
of `employees.manager_id`, so in the synthetic data an expense's approver is
not guaranteed to be the submitter's actual manager; a real approval
workflow would enforce that. ~80% of submitted expenses are approved, 15%
pending, 5% rejected.

## Budget process

Budgets are planned annually and tracked quarterly across 9 departments
(Engineering, Product, Marketing, Sales, Customer Success, Finance, HR, IT,
Security) in 5 categories: headcount, software, marketing, capex, opex.
Actuals are expected to land within roughly ±20% of plan; anything outside
that band in a real reporting cycle should trigger a variance review with
the department owner.

## Payment infrastructure

Customer-facing payment methods span `credit_card`, `debit_card`, `pix`,
`boleto`, `paypal`, `rcd_card` (RCD Corp's co-branded card), and `bnpl`
(buy-now-pay-later), routed through five gateways: Stripe, Cielo,
PagSeguro, MercadoPago, PayPal. Installments (1–12x) are only offered on
`credit_card`.

**`rcd_card` transactions never store a full card number.** `card_id`
(format `CARD-######`) is an internal token; treat any future integration
that would introduce a real PAN field as a PCI-DSS scope change requiring
Security + Legal sign-off before shipping — see [[data-privacy-lgpd]] §5.

## Currency handling — known gap

`invoices.currency` and `transactions.currency` are assigned randomly per
row in the generator, independent of the customer's actual `country`/
`products.currency` (which is always BRL). Don't build a real
currency-consistency report against this synthetic data without accounting
for that — it's a generator simplification, not a real multi-currency
reconciliation signal.
