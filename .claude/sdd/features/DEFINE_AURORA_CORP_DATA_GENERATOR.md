# DEFINE: Aurora Corp Synthetic Data Generator

> A modular, reproducible Python CLI that generates realistic, interconnected synthetic operational data for a fictional enterprise across 10 business domains, writing to CSV, Parquet, and Postgres sinks.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AURORA_CORP_DATA_GENERATOR |
| **Date** | 2026-04-14 |
| **Author** | define-agent |
| **Status** | Ready for Design |
| **Clarity Score** | 15/15 |
| **Source** | BRAINSTORM_AURORA_CORP_DATA_GENERATOR.md + PROMPT.md |

---

## Problem Statement

Data engineers, platform engineers, and analytics engineers building demo pipelines, load-testing infrastructure, or validating dashboard queries lack realistic multi-domain synthetic data that is referentially consistent, statistically plausible, and reproducible. Existing tools generate isolated tables with no cross-domain FK relationships, no temporal behavioral rules, and no configurable volume profiles — making them unsuitable for production-grade demos or load tests.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Data Engineer / Architect | Designs and demos data pipelines | Cannot demonstrate pipelines to stakeholders without realistic, interconnected data that looks credible |
| Platform Engineer | Validates ingestion infrastructure | Cannot load-test at 50M+ row scale without a generator that handles memory constraints and produces partitioned output |
| Analytics Engineer | Builds and validates dashboards | Cannot trust dashboard queries without FK-valid data across all 10 domains and correct temporal/seasonal distributions |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Generate all 10 data domains (Master Data, Sales, Finance, Marketing, Social Media, Supply Chain, Manufacturing, HR, Support, Observability) as complete, runnable generators — no skeletons |
| **MUST** | Write output to all three sinks: CSV, Parquet (date-partitioned for high-volume tables), and Postgres via SQLAlchemy |
| **MUST** | Support three CLI profiles: `demo` (~10k rows/fact table), `standard` (~1M orders / 90 days), `loadtest` (~50M rows / 2 years) — all must work correctly |
| **MUST** | Maintain referential integrity: master data generated first and cached; fact generators sample FK values from cache; zero orphan FKs |
| **MUST** | Reproducibility: `--seed` flag (default 42) seeds Faker, `random`, and NumPy so identical seed → identical output |
| **MUST** | `BaseGenerator` abstract class enforces uniform interface: FK sampling, sink dispatch, seasonal throttling, crisis mode |
| **MUST** | `OrdersGenerator` as fully-worked reference implementation demonstrating all patterns |
| **SHOULD** | State machines implemented as Python classes with transition probabilities for all 6 lifecycles (Order, Ticket, Lead, Shipment, Recruitment, ProductionRun) |
| **SHOULD** | Temporal realism: business hours per timezone, Black Friday 10x, Brazilian payday 2x, social engagement peaks 19h–22h BRT |
| **SHOULD** | Statistical realism: Pareto LTV, normal-clipped distributions for salaries/order values, weighted categorical choices |
| **SHOULD** | Multi-currency: BRL/MXN/EUR/USD with daily FX rates via random walk |
| **SHOULD** | Crisis simulation: one day/month triggers 5x tickets, 3x negative social comments, CSAT drop, sentiment shift |
| **COULD** | `docker-compose.yml` spinning up Postgres and running generator end-to-end |
| **COULD** | CPF/CNPJ generation with valid check digits |

---

## Success Criteria

- [ ] `python main.py generate --profile demo --seed 42` completes in under 60 seconds on a standard laptop and produces FK-valid data across all 10 domains
- [ ] `python main.py generate --profile standard` produces ≥1M order rows across 90 days with correct seasonal distribution (Black Friday visible in daily totals)
- [ ] `python main.py generate --profile loadtest --sink parquet` completes without OOM on a 16GB RAM machine and produces ≥50M total rows
- [ ] `python main.py validate` passes all referential integrity checks with zero orphan foreign keys across all generated tables
- [ ] `python main.py generate --sink all` successfully writes to CSV files, Parquet files, and a running Postgres instance simultaneously
- [ ] Running with `--seed 42` twice produces byte-identical output on the same profile
- [ ] Crisis simulation produces a measurable sentiment shift (positive % drops from ~55% to ~20%) and ticket volume spike (≥5x baseline) on the designated crisis day
- [ ] Generated data supports all 12 target dashboards: Sales KPI, Funnel, Cohort Retention, Marketing Attribution, Social Media Performance, Brand Health, Inventory Turnover, OEE, Support CSAT/SLA, HR Headcount, Financial P&L, Executive 360

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Demo profile completes cleanly | Fresh environment, `--seed 42`, `--profile demo` | `python main.py generate` | Exits 0; all expected CSV files present; no missing FK references |
| AT-002 | Referential integrity validation passes | Standard profile data generated | `python main.py validate` | Output: "All checks passed. 0 orphan FKs." |
| AT-003 | Deterministic output | Standard profile run twice with `--seed 42` | Compare outputs | Byte-identical CSV/Parquet files |
| AT-004 | Loadtest Parquet output | Loadtest profile, `--sink parquet` | Generator runs to completion | No OOM; `machine_telemetry` and `api_requests` are date-partitioned; total rows ≥50M |
| AT-005 | Black Friday multiplier visible | Standard profile, any seed | Inspect `orders` table | Order count on Black Friday date is ≥8x the weekly average |
| AT-006 | Crisis simulation fires | Any profile with crisis enabled | Inspect `tickets` and `social_comments` on crisis day | Ticket volume ≥5x baseline; negative sentiment % ≥50% on crisis day |
| AT-007 | Multi-currency present | Any profile | Inspect `orders`, `payments`, `invoices` | Rows with BRL, MXN, EUR, and USD all present; `fx_rates` table covers full date range |
| AT-008 | Postgres sink functional | Postgres running via docker-compose | `python main.py generate --profile demo --sink postgres` | All tables created and populated; row counts match expected profile volumes |
| AT-009 | Selective domain generation | Any profile | `python main.py generate --only social_media,support --sink parquet` | Only social media and support tables generated; master data cache loaded as dependency |
| AT-010 | State machine completeness | Standard profile | Inspect `orders.status` distribution | All statuses present: pending, paid, picked, shipped, delivered, cancelled, returned, refunded |

---

## Out of Scope

- Real external API calls — all data is generated locally, no outbound HTTP
- Streaming output (Kafka, Kinesis, Pub/Sub) — batch generation only
- Cloud storage upload (S3, GCS, ADLS) — local filesystem output only
- Authentication or multi-user access to generated data
- Real production Postgres — docker-compose for local development use only
- Real CPF/CNPJ lookup or government validation — check-digit algorithm only
- UI or web interface — CLI only
- Incremental/delta generation (append to existing data) — full regeneration per run
- Schema migration tooling — tables created fresh each run

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Python 3.11+ only | All syntax and library features must be 3.11-compatible |
| Technical | Stack locked: Faker, Mimesis, NumPy, Pandas, PyArrow, SQLAlchemy, psycopg2-binary, tqdm, Pydantic | No alternative libraries for core generation or output |
| Technical | High-volume tables (`machine_telemetry`, `api_requests`) must use date-partitioned Parquet — no Postgres write on loadtest | Prevents OOM; design must route these tables to Parquet sink regardless of `--sink` flag on loadtest profile |
| Technical | All monetary values must carry a `currency` column (BRL/MXN/EUR/USD) | Schema design must include currency on every financial table |
| Technical | Reproducibility: `--seed 42` must produce identical output across runs | All RNG sources (Faker, `random`, NumPy) must be seeded before generation starts |
| Technical | Referential integrity by construction: FK values must be sampled from in-memory master data cache | Design must enforce generation order: master data → fact tables |
| Runtime | loadtest profile must complete without OOM on 16GB RAM | Generators must use chunked/streaming writes, not accumulate full datasets in memory |
| Quality | CPF/CNPJ fields must pass valid check-digit validation | `identifiers.py` must implement the BR government check-digit algorithm |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `aurora_data/` at project root | Matches PROMPT spec; self-contained Python package |
| **Entry Point** | `aurora_data/main.py` | `argparse` or `typer` CLI; `__main__` entry point |
| **KB Domains** | Python data engineering, synthetic data patterns | Faker/Mimesis idioms, Pandas chunked I/O, PyArrow partitioned write |
| **IaC Impact** | `docker-compose.yml` for local Postgres only | No cloud infrastructure; Postgres is a local development dependency |
| **Logging** | `structlog` for structured JSON logs | All generators emit structured logs with domain, table, row_count, elapsed_s |

### Project Structure (from PROMPT spec)

```
aurora_data/
├── config.yaml              # volumes, date ranges, locales, output paths
├── main.py                  # orchestrator CLI
├── generators/
│   ├── __init__.py
│   ├── base.py              # BaseGenerator abstract class
│   ├── master_data.py       # customers, products, employees, suppliers, stores, warehouses, fx_rates
│   ├── sales.py             # orders, order_items, payments, cart_events, sessions
│   ├── finance.py           # invoices, transactions, expenses, budgets, aurora_card_transactions
│   ├── marketing.py         # campaigns, campaign_events, email_events, leads, ab_test_exposures
│   ├── social_media.py      # social_accounts, social_posts, social_metrics, social_comments,
│   │                        # social_mentions, social_dms, influencer_partnerships, influencer_posts,
│   │                        # community_forum_posts, reviews, social_ad_spend
│   ├── supply_chain.py      # shipments, inventory_snapshots, purchase_orders, stock_movements, returns
│   ├── manufacturing.py     # production_runs, machine_telemetry, quality_checks, maintenance_events
│   ├── hr.py                # attendance, performance_reviews, training_records, recruitment_pipeline, engagement_surveys
│   ├── support.py           # tickets, ticket_messages, call_center_calls
│   └── observability.py     # app_logs, api_requests, errors, deployments, security_events
├── utils/
│   ├── distributions.py     # pareto_ltv, weighted_choice, normal_clipped, seasonal_curve
│   ├── state_machines.py    # OrderLifecycle, TicketLifecycle, LeadPipeline, ShipmentTracking, RecruitmentFunnel, ProductionRunStatus
│   ├── time_utils.py        # business_hours, black_friday, payday_multiplier, crisis_windows
│   ├── identifiers.py       # cpf/cnpj validators, SKU generator, tracking numbers
│   └── fx.py                # daily FX rates with random walk
├── sinks/
│   ├── csv_sink.py
│   ├── parquet_sink.py
│   └── postgres_sink.py
└── tests/
    └── test_referential_integrity.py
```

---

## Data Contract

### Generated Output Tables by Domain

| Domain | Tables | Volume (standard) | Sink |
|--------|--------|------------------|------|
| Master Data | customers, products, employees, suppliers, stores, warehouses, fx_rates | ~50k rows total | CSV + Parquet + Postgres |
| Sales | orders, order_items, payments, shopping_cart_events, web_sessions | ~1M orders + items | CSV + Parquet + Postgres |
| Finance | invoices, transactions, expenses, budgets, aurora_card_transactions | ~500k rows | CSV + Parquet + Postgres |
| Marketing | campaigns, campaign_events, email_events, leads, ab_test_exposures | ~300k rows | CSV + Parquet + Postgres |
| Social Media | social_accounts, social_posts, social_metrics, social_comments, social_mentions, social_dms, influencer_partnerships, influencer_posts, community_forum_posts, reviews, social_ad_spend | ~500k rows | CSV + Parquet + Postgres |
| Supply Chain | shipments, inventory_snapshots, purchase_orders, stock_movements, returns | ~300k rows | CSV + Parquet + Postgres |
| Manufacturing | production_runs, quality_checks, maintenance_events | ~100k rows | CSV + Parquet + Postgres |
| Manufacturing IoT | machine_telemetry | High volume | **Partitioned Parquet only** |
| HR | attendance, performance_reviews, training_records, recruitment_pipeline, engagement_surveys | ~100k rows | CSV + Parquet + Postgres |
| Support | tickets, ticket_messages, call_center_calls | ~200k rows | CSV + Parquet + Postgres |
| Observability | errors, deployments, security_events | ~100k rows | CSV + Parquet + Postgres |
| Observability (high vol) | app_logs, api_requests | High volume | **Partitioned Parquet only** |

### Key Schema Constraints

| Column Pattern | Constraint |
|----------------|-----------|
| All `*_id` FK columns | Must reference a valid PK in master data cache — enforced at generation time |
| `currency` | One of: BRL, MXN, EUR, USD — present on all monetary tables |
| `cpf` / `cnpj` | Must pass BR check-digit validation |
| `created_at` / `*_at` timestamps | Must fall within profile date range; business hours per country timezone |
| `status` columns | Must be a valid terminal or intermediate state per the relevant state machine |

### Freshness SLAs

| Layer | Target | Notes |
|-------|--------|-------|
| Generated output | Immediately available post-run | No streaming; all data written synchronously per run |
| Postgres tables | Populated within single run | Tables dropped and recreated on each run (no incremental) |

### Completeness Metrics

- 100% of FK values in fact tables must exist in master data cache (enforced structurally)
- Zero null values on NOT NULL columns (enforced via Pydantic schemas)
- All status distributions must sum to 100% across state machine terminal states

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | 16GB RAM is sufficient for loadtest profile with chunked writes | Would require streaming generator architecture or reduced loadtest target | [x] Addressed by constraint: chunked I/O required |
| A-002 | Single-node local execution — no distributed compute needed | Would require Spark/Dask instead of Pandas | [x] Confirmed: local Python only |
| A-003 | Postgres sink uses truncate-and-reload (not upsert) per run | Incremental append would require change tracking | [x] Out of scope confirmed |
| A-004 | `--only` flag for selective domain generation still requires master data cache | If domains could be generated independently, orchestration would be simpler | [x] Master data always generated as dependency |
| A-005 | All three profiles use the same code paths with different volume knobs in `config.yaml` | If loadtest requires fundamentally different algorithms, `BaseGenerator` would need two modes | [ ] Validate during Design phase |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Specific pain point with named user groups and concrete impact |
| Users | 3 | Three personas with distinct roles and measurable pain points |
| Goals | 3 | MoSCoW-prioritized, full scope confirmed, no ambiguity |
| Success | 3 | Eight criteria, all measurable with specific numbers |
| Scope | 3 | Six explicit out-of-scope items confirmed during brainstorm |
| **Total** | **15/15** | Ready for Design |

---

## Open Questions

None — ready for Design.

All ambiguities resolved during brainstorm session:
- Full scope (all 10 domains) confirmed
- All three sinks (CSV + Parquet + Postgres) confirmed as first-class
- All three profiles (demo / standard / loadtest) confirmed
- Approach A (full project in one pass) confirmed
- No platform-specific assumptions

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-04-14 | define-agent | Initial version from BRAINSTORM_AURORA_CORP_DATA_GENERATOR.md |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_AURORA_CORP_DATA_GENERATOR.md`
