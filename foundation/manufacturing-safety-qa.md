# RCD Corp — Manufacturing Safety & Quality Policy

In-universe policy for the `manufacturing` domain
(`rcd_data/generators/manufacturing.py`). Schema:
[[data-dictionary/manufacturing]]. Privacy treatment: [[ropa]] §7.

## Facilities

Three factories, each with 20 machines (`MCH-{factory3}-{001..020}`):

| Factory | Share of production volume |
|---|---|
| São Paulo | 45% |
| Manaus | 35% |
| Monterrey | 20% |

Shifts: morning (40%), afternoon (35%), night (25%).

## Production run lifecycle

`scheduled → in_progress → quality_check → completed`, with `halted`,
`rework`, `cancelled`, and `scrapped` as failure/recovery branches
(`utils/state_machines.py::ProductionRunStatus`). Actual output runs
75–105% of planned quantity depending on line efficiency that shift.

## Quality control

1–3 inspections per run across 5 check types: visual_inspection,
functional_test, dimensional_check, stress_test, final_qa. **Pass
threshold: 95% pass rate.** Defect count is derived as
`(1 - pass_rate) × actual_qty` for the run — i.e. QA defect counts scale
with how much was actually produced, not a fixed sample.

## Maintenance

Types: preventive (40%), corrective (35%), emergency (15%), calibration
(10%). Downtime is right-skewed (mean ~4h, up to 72h for the worst
emergency events) — a technician-level maintenance cost/downtime report
should expect long-tail outliers, not a normal distribution.

## Machine telemetry & alerting (loadtest profile only)

Every machine reports every 5 minutes across an 8-hour shift:
`temperature_c` (baseline 75°C ± 5), `vibration_hz` (baseline 50Hz ± 3),
`power_kw`, `production_rate`. **Alert threshold: `temperature_c > 100°C`**,
or a 2% random spurious-alert rate layered on top (sensor noise). This
table only exists when generating with `--profile loadtest` — `demo` and
`standard` never populate it; see the note in
[[data-dictionary/manufacturing]].
