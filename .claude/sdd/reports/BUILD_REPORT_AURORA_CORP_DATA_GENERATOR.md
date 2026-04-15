# BUILD REPORT: Aurora Corp Synthetic Data Generator

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AURORA_CORP_DATA_GENERATOR |
| **Build Date** | 2026-04-14 |
| **Author** | build-agent |
| **DESIGN** | [DESIGN_AURORA_CORP_DATA_GENERATOR.md](../features/DESIGN_AURORA_CORP_DATA_GENERATOR.md) |
| **Status** | Complete |

---

## Files Created

| # | File | Status | Notes |
|---|------|--------|-------|
| 1 | `aurora_data/config.yaml` | Created | 3 profiles (demo/standard/loadtest), company config, crisis config |
| 2 | `aurora_data/utils/distributions.py` | Created | `pareto_ltv`, `weighted_choice`, `normal_clipped`, `ltv_tier`, `seasonal_multipliers` |
| 3 | `aurora_data/utils/state_machines.py` | Created | 6 lifecycles: Order, Ticket, Lead, Shipment, Recruitment, ProductionRun |
| 4 | `aurora_data/utils/time_utils.py` | Created | `black_friday_multiplier`, `payday_multiplier`, `generate_crisis_days`, `timestamp_*` |
| 5 | `aurora_data/utils/identifiers.py` | Created | `generate_cpf`, `generate_cnpj` (valid check digits), `generate_sku`, `generate_tracking_number` |
| 6 | `aurora_data/utils/fx.py` | Created | Daily FX rates via log-normal random walk, all 4 currency pairs |
| 7 | `aurora_data/sinks/csv_sink.py` | Created | `CSVSink.write()` with structlog |
| 8 | `aurora_data/sinks/parquet_sink.py` | Created | `ParquetSink.write()` with date-partitioning via PyArrow |
| 9 | `aurora_data/sinks/postgres_sink.py` | Created | `PostgresSink.write()` via SQLAlchemy; reads `AURORA_POSTGRES_URL` env var |
| 10 | `aurora_data/generators/__init__.py` | Created | Exports all 10 generator classes |
| 11 | `aurora_data/generators/base.py` | Created | `BaseGenerator`, `MasterCache`, `ProfileConfig`, `SinkDispatcher`, `load_profile` |
| 12 | `aurora_data/generators/master_data.py` | Created | 7 tables; named Aurora stores/warehouses; Faker multi-locale |
| 13 | `aurora_data/generators/sales.py` | Created | 5 tables; `OrdersGenerator` reference pattern; seasonal multipliers; crisis injection |
| 14 | `aurora_data/generators/finance.py` | Created | 5 tables; budgets by dept/quarter/year |
| 15 | `aurora_data/generators/marketing.py` | Created | 5 tables; `LeadPipeline` state machine |
| 16 | `aurora_data/generators/social_media.py` | Created | 11 tables; hourly metrics 72h; crisis sentiment shift; influencer tiers |
| 17 | `aurora_data/generators/supply_chain.py` | Created | 5 tables; `ShipmentTracking` state machine; weekly inventory snapshots |
| 18 | `aurora_data/generators/manufacturing.py` | Created | 4 tables; `generate_chunked()` override for `machine_telemetry` (5-min IoT readings) |
| 19 | `aurora_data/generators/hr.py` | Created | 5 tables; weekday-only attendance; `RecruitmentFunnel` state machine |
| 20 | `aurora_data/generators/support.py` | Created | 3 tables; crisis 5x ticket spike; `TicketLifecycle` state machine |
| 21 | `aurora_data/generators/observability.py` | Created | 5 tables; `generate_chunked()` for `app_logs` + `api_requests` |
| 22 | `aurora_data/main.py` | Created | `typer` CLI; `generate`, `validate`, `info` commands; tqdm progress; structlog |
| 23 | `aurora_data/tests/test_referential_integrity.py` | Created | 30+ tests across 6 categories; statistical sanity checks; CPF/CNPJ format |
| 24 | `aurora_data/__init__.py` | Created | Package init |
| 25 | `aurora_data/utils/__init__.py` | Created | Package init |
| 26 | `aurora_data/sinks/__init__.py` | Created | Package init |
| 27 | `aurora_data/tests/__init__.py` | Created | Package init |
| 28 | `requirements.txt` | Created | All pinned dependencies |
| 29 | `pyproject.toml` | Created | `aurora-data` CLI entry point; `setuptools` build |
| 30 | `docker-compose.yml` | Created | Postgres service + generator service (profile=run) |
| 31 | `Dockerfile` | Created | Python 3.11-slim; installs deps; sets entry point |
| 32 | `README.md` | Created | Setup, CLI reference, schema map, dashboard→datasets, tuning guide |

**Total files: 32** (27 specified + 5 supporting: Dockerfile, package inits, BUILD_REPORT)

---

## Design Decisions Implemented

| Decision | Implementation |
|----------|---------------|
| `BaseGenerator` ABC with `generate() → dict[str, DataFrame]` | `generators/base.py:BaseGenerator` |
| `generate_chunked()` iterator for high-volume tables | `generators/manufacturing.py`, `generators/observability.py` |
| `MasterCache` with NumPy arrays for FK sampling | `generators/base.py:MasterCache` with `sample_*()` methods |
| `SinkDispatcher` decouples generators from output format | `generators/base.py:SinkDispatcher.from_flag()` |
| `ProfileConfig` Pydantic model validates config at startup | `generators/base.py:ProfileConfig` |
| High-volume tables (`machine_telemetry`, `api_requests`, `app_logs`) → Parquet-only on loadtest | `generators/base.py:SinkDispatcher.write_all()` |
| Crisis days injection via `generate_crisis_days()` | `utils/time_utils.py:generate_crisis_days()` |

---

## Acceptance Tests Coverage

| AT-ID | Scenario | Covered By |
|-------|----------|-----------|
| AT-001 | Demo profile completes, FK-valid | `test_referential_integrity.py::TestMasterDataExists` |
| AT-002 | `validate` passes, 0 orphan FKs | `test_referential_integrity.py::_assert_no_orphans()` — 15+ FK checks |
| AT-003 | Deterministic output with `--seed 42` | Seeding in `main.py:_seed_all()` |
| AT-004 | Loadtest Parquet, no OOM | `generate_chunked()` in manufacturing + observability |
| AT-005 | Black Friday visible in orders | `time_utils.py:black_friday_multiplier()` |
| AT-006 | Crisis simulation fires | `support.py:_build_tickets()` (5x), `social_media.py:_build_comments()` (sentiment shift) |
| AT-007 | Multi-currency present | `TestStatisticalSanity::test_orders_multi_currency`, `test_fx_rates_all_currencies_present` |
| AT-008 | Postgres sink functional | `postgres_sink.py` + docker-compose |
| AT-009 | Selective domain generation | `main.py:_resolve_domains()` + `--only` flag |
| AT-010 | All order statuses present | `TestStatisticalSanity::test_orders_status_distribution_has_all_terminals` |

---

## Known Limitations / Follow-ups

| Item | Notes |
|------|-------|
| `machine_telemetry` on demo profile | Still generates in-memory (not chunked) — only chunked on loadtest profile. For demo this is fine (small volume). |
| `web_sessions.date` column | Added to support parquet partitioning; not in original PROMPT schema — harmless addition. |
| `orders.date` column | Added as a derived column from `created_at` for Parquet partitioning. |
| Store manager FK | `stores.manager_employee_id` set to `None` at generation time — employees are generated after stores. Could be backfilled in a post-processing step. |
| `social_metrics` volume | Hourly snapshots × posts can be large on standard/loadtest. Consider adjusting `n_posts` in config if needed. |

---

## Next Step

**Ready for:** `/ship .claude/sdd/features/DEFINE_AURORA_CORP_DATA_GENERATOR.md`
