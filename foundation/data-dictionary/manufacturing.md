# Data Dictionary — Manufacturing

Source: `rcd_data/generators/manufacturing.py` (`ManufacturingGenerator`).
Three factories: São Paulo (45%), Manaus (35%), Monterrey (20%), 20 machines
each (`MCH-{factory3}-{001..020}`). Production status comes from
`utils/state_machines.py::ProductionRunStatus`. `machine_telemetry` is
generated via `generate_chunked()` and **only exists under the `loadtest`
profile** — `demo`/`standard` never produce it. PII/retention detail:
[[ropa]] §7; [[data-classification-retention]].

## `production_runs`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `product_sku` | string FK → `products.sku` | |
| `factory_location` | enum | São Paulo \| Manaus \| Monterrey |
| `planned_qty`, `actual_qty` | integer | `actual = planned * efficiency` (efficiency 0.75–1.05) |
| `status` | string | Terminal state from `ProductionRunStatus` |
| `started_at` | timestamp | |
| `completed_at` | timestamp, nullable | Only when `status = "completed"` |
| `operator_id` | uuid FK → `employees.id` | |
| `shift` | enum | morning \| afternoon \| night |

## `machine_telemetry` (loadtest profile only, chunked/partitioned)

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `machine_id` | string | `MCH-{factory3}-{seq}` — not an FK to another table (machines aren't a master-data entity) |
| `factory` | string | |
| `timestamp` | timestamp | 5-minute cadence, 96 readings/machine/day (one 8h shift) |
| `date` | date | Parquet partition column |
| `temperature_c` | decimal | Normal(75, 5), clipped [20, 120] |
| `vibration_hz` | decimal | Normal(50, 3) |
| `power_kw` | decimal | Uniform(10, 150) |
| `production_rate` | decimal | Normal(100, 10) |
| `status` | enum | `normal` \| `alert` |
| `alert_triggered` | boolean | `temperature_c > 100` or 2% random |

## `quality_checks`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `production_run_id` | uuid FK → `production_runs.id` | 1–3 checks per run |
| `inspector_id` | uuid FK → `employees.id` | |
| `check_type` | enum | visual_inspection, functional_test, dimensional_check, stress_test, final_qa |
| `pass_rate` | decimal | Beta(9,1) — skewed high |
| `defects_found` | integer | `(1 - pass_rate) * actual_qty` of the parent run |
| `sample_size` | integer | |
| `result` | enum | `pass` (pass_rate ≥ 0.95) \| `fail` |
| `checked_at` | timestamp | Copied from the parent run's `started_at` |

## `maintenance_events`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `machine_id` | string | Same synthetic ID space as `machine_telemetry.machine_id` |
| `factory` | string | |
| `type` | enum | preventive, corrective, emergency, calibration |
| `description` | string | Placeholder text |
| `technician_id` | uuid FK → `employees.id` | |
| `started_at`, `completed_at` | timestamp | `completed_at = started_at + downtime_h` |
| `downtime_h` | decimal | |
| `cost` | decimal | |
| `currency` | string | Always `BRL` |
