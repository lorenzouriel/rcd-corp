# Data Dictionary — Supply Chain

Source: `rcd_data/generators/supply_chain.py` (`SupplyChainGenerator`).
Shipment status comes from `utils/state_machines.py::ShipmentTracking`. No
personal data except `returns.customer_id`. PII/retention detail: [[ropa]]
§9 (procurement) and §1 (returns, tied to order processing);
[[data-classification-retention]].

## `shipments`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `order_id` | uuid FK → `orders.id` | |
| `warehouse_id` | uuid FK → `warehouses.id` | |
| `carrier` | enum | Correios, DHL, FedEx, UPS, Rappi, Loggi |
| `tracking_number` | string | Synthetic, format per `utils/identifiers.py::generate_tracking_number` |
| `status` | string | Terminal state from `ShipmentTracking` |
| `weight_kg` | decimal | |
| `created_at` | date | |
| `delivered_at` | date, nullable | Set only when `status = "delivered"` and within profile range |

## `inventory_snapshots`

Weekly grain (every 7th calendar day) × up to 50 sampled SKUs × every
warehouse. `id`, `product_sku` (FK → `products.sku`), `warehouse_id` (FK →
`warehouses.id`), `date`, `quantity_on_hand`, `quantity_reserved`,
`quantity_available` (`on_hand - reserved`), `reorder_point`. No personal data.

## `purchase_orders`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `supplier_id` | uuid FK → `suppliers.id` | |
| `warehouse_id` | uuid FK → `warehouses.id` | |
| `status` | enum | draft, submitted, approved, shipped, received, cancelled |
| `total_amount` | decimal | |
| `currency` | string | Always `BRL` |
| `ordered_at` | date | |
| `expected_at` | date | `ordered_at` + 3–45 days |
| `received_at` | date, nullable | Set only when `status = "received"` |
| `notes` | string, nullable | Always `None` — no free-text notes are ever generated |

## `stock_movements`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `product_sku` | string FK → `products.sku` | |
| `warehouse_id` | uuid FK → `warehouses.id` | |
| `movement_type` | enum | inbound, outbound, transfer, adjustment, return, damage_write_off |
| `quantity` | integer | |
| `unit_cost` | decimal | |
| `reason` | string | Placeholder text (`"Auto movement {i}"`) |
| `reference_id` | uuid | Independently generated — **not** an FK to any other table's PK |
| `created_at` | timestamp | |

## `returns`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `order_id` | uuid FK → `orders.id` | |
| `customer_id` | uuid FK → `customers.id` | |
| `product_sku` | string FK → `products.sku` | |
| `reason` | enum | defective, wrong_item, changed_mind, damaged_in_transit, not_as_described, late_delivery, duplicate_order |
| `quantity` | integer | 1–3 |
| `status` | enum | requested, approved, received, refunded, rejected |
| `created_at` | timestamp | |
| `refund_amount` | decimal | |
| `currency` | string | Always `BRL` |
