# BRAINSTORM: Aurora Corp Synthetic Data Generator

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AURORA_CORP_DATA_GENERATOR |
| **Date** | 2026-04-14 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Build a complete, runnable Python project that generates realistic, interconnected operational data for a fictional mid-to-large enterprise called "Aurora Corp", writing output to CSV, Parquet, and Postgres.

**Context Gathered:**
- Project directory is empty — clean slate, no existing code or patterns to inherit
- No existing sample data or reference dashboards available
- PROMPT.md provides an unusually complete specification: company profile, product catalog, full schema definitions for 10 data domains, state machine specs, behavioral rules, and CLI interface

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `aurora_data/` at project root | Matches PROMPT spec exactly |
| Relevant Patterns | Faker + Mimesis + NumPy + Pandas | Standard synthetic data stack |
| IaC / Infra | `docker-compose.yml` for Postgres | Required — Postgres sink depends on it |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | What is the primary goal for this project? | Testing/demo data platform — feed a real data platform with rich synthetic data for demos or load testing | Output quality and realism are non-negotiable; data must look convincing to stakeholders |
| 2 | Which data platform(s) will consume this data? | Multiple / not decided yet — sink-agnostic design needed | All three sinks (CSV, Parquet, Postgres) must work reliably; no platform-specific assumptions |
| 3 | Which domains are highest priority for v1? | Full scope — all 10 domains from day one | No phased approach; all generators must be complete and runnable, not skeletons |
| 4 | Which CLI profile is used most day-to-day? | All three (demo / standard / loadtest) equally | Performance is a real constraint; loadtest profile must handle 50M+ rows without OOM |
| 5 | Any existing samples or reference assets? | None available — build from spec | The PROMPT.md serves as the complete source of truth; no grounding data needed |
| 6 | How important is the Postgres sink vs CSV/Parquet? | All three equally — CSV, Parquet, and Postgres all required | `docker-compose.yml` and `postgres_sink.py` are first-class deliverables |

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | N/A | 0 | No existing data |
| Output examples | N/A | 0 | No reference outputs |
| Ground truth | N/A | 0 | No verified data |
| Related code | PROMPT.md | 1 | Full specification with schema definitions |

**How spec will be used:**
- Product catalog names/prices used verbatim (NovaHome, PulseAudio, GuardianIQ SKUs)
- Schema definitions in PROMPT used as Pydantic model blueprints
- Behavioral rules (payday multipliers, Black Friday, sentiment drift) implemented directly from spec

---

## Approaches Explored

### Approach A: Full project in one consistent pass ⭐ Recommended

**Description:** Generate the entire project structure in a single pass — all 10 generator modules, all 3 sinks, utilities, state machines, config, tests, and documentation — using `OrdersGenerator` as the fully-worked reference pattern that all other generators follow consistently.

**Pros:**
- Single consistent pass = uniform patterns, naming, and style across all 10 domains
- No integration debt — `main.py` orchestrator works end-to-end immediately
- Matches the PROMPT's `OUTPUT FORMAT` section exactly (project tree → config → utils → generators → sinks → main → tests → README)
- All three sinks wired from the start; no retrofitting required
- `BaseGenerator` abstract class enforces consistent interface before any concrete generator is written

**Cons:**
- Large output — many files to review at once
- If a design decision in `BaseGenerator` needs changing, it ripples across all generators

**Why Recommended:** The PROMPT spec is detailed enough that all patterns are fully defined upfront. There is no ambiguity that would benefit from an iterative approach. The user confirmed full scope from day one with all three sinks required — a phased approach would only add coordination overhead.

---

### Approach B: Core-first, domains-second

**Description:** Build Master Data + Sales fully first, validate patterns, then layer in remaining 8 domains in a second pass.

**Pros:**
- Faster to a first working run
- Easier to catch `BaseGenerator` design issues before they propagate

**Cons:**
- Two-pass build adds coordination overhead
- Given full-scope requirement, this just defers work without reducing it
- Spec is clear enough that pattern validation isn't needed before committing to full scope

---

### Approach C: Skeleton-first, fill in later

**Description:** Generate all files as minimal runnable skeletons with TODOs, intended to be filled in incrementally.

**Pros:**
- Fastest initial output

**Cons:**
- Does not meet the use case — demo/loadtest data requires complete generators that produce realistic data
- Skeletons produce empty or trivially fake data, unusable for dashboard demos or load tests

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-04-14 |
| **Reasoning** | Full scope required from day one, all three sinks needed, spec is detailed enough to build from directly without phasing |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | All 10 domains fully implemented (not skeletons) | Demo/loadtest use case requires real data | Skeleton-first approach |
| 2 | `OrdersGenerator` as the canonical reference pattern | All other generators must follow the same FK sampling, state machine, seasonal, and multi-currency pattern | Each generator designed independently |
| 3 | `BaseGenerator` abstract class enforces interface before any concrete generator | Consistency across 10 domains; FK sampling + sink dispatch must be uniform | Ad-hoc generator functions |
| 4 | All three sinks (CSV, Parquet, Postgres) treated as first-class | User confirmed all three are equally important | Postgres as optional/deferred |
| 5 | `docker-compose.yml` required deliverable | Postgres sink requires it to be usable out of the box | Manual Postgres setup docs |
| 6 | Three profiles (demo/standard/loadtest) must all work | User confirmed all three used equally | Single configurable profile |
| 7 | Reproducibility via `--seed` on every generator | Demo/loadtest use case requires deterministic data for regression testing | Random seed per run |
| 8 | State machines as Python classes with transition probabilities | 6 lifecycles specified (Order, Ticket, Lead, Shipment, Recruitment, ProductionRun) | Hardcoded status distributions |

---

## Features Removed (YAGNI)

No features were removed. The PROMPT spec is already well-scoped for the demo/loadtest use case. Every domain and feature maps to at least one of the 12 target dashboards listed in the spec.

---

## Data Engineering Context

### Source Systems (Generated)

| Domain | Type | Volume (standard profile) | Volume (loadtest) |
|--------|------|--------------------------|-------------------|
| Master Data | Generated once, cached | ~50k rows total | ~500k rows total |
| Orders + Items | Fact table | ~1M orders / 90 days | ~50M rows / 2 years |
| Machine Telemetry | High-volume IoT | Partitioned Parquet | Partitioned Parquet |
| API Requests / App Logs | High-volume observability | Partitioned Parquet | Partitioned Parquet |
| Social Metrics | Time-series snapshots | Hourly 72h + daily | Hourly 72h + daily |

### Data Flow

```text
config.yaml (profiles)
    → main.py orchestrator
    → master_data generators (customers, products, employees, suppliers, stores, warehouses, fx_rates)
    → [cache master data in memory / parquet]
    → fact generators (sales, finance, marketing, social_media, supply_chain, manufacturing, hr, support, observability)
        [FK sampling from master cache]
        [state machine transitions]
        [seasonal + behavioral rules]
        [multi-currency via fx_rates]
        [crisis simulation if enabled]
    → sinks (csv_sink / parquet_sink / postgres_sink)
    → tests/test_referential_integrity.py (validation)
```

### Key Data Engineering Decisions

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | How is referential integrity maintained? | Master data generated first, cached in memory; fact generators sample FK values from cache | No orphan FKs possible by construction |
| 2 | How are high-volume tables handled? | `machine_telemetry` and `api_requests` written as date-partitioned Parquet only | Avoids OOM on loadtest profile |
| 3 | How is multi-currency handled? | `fx_rates` generator produces daily rates with random walk; all monetary fields carry `currency` column | Supports BRL/MXN/EUR/USD natively |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Primary use case (Q1) | ✅ | Testing/demo data platform | No adjustment needed |
| Platform target (Q2) | ✅ | Sink-agnostic, multiple platforms | Confirmed all three sinks equally important |
| Domain scope (Q3) | ✅ | Full scope, all 10 domains | No phasing |
| Technical decisions summary | ✅ | All correct | No adjustments |

---

## Suggested Requirements for /define

### Problem Statement (Draft)

Build a modular, reproducible Python 3.11+ project that generates realistic, interconnected synthetic operational data for a fictional enterprise (Aurora Corp), across 10 business domains, written to CSV, Parquet, and Postgres sinks, supporting demo, standard, and loadtest volume profiles.

### Target Users

| User | Pain Point |
|------|------------|
| Data engineer / architect | Needs realistic multi-domain data to demo pipelines and dashboards to stakeholders |
| Platform engineer | Needs high-volume data (50M+ rows) to load-test ingestion infrastructure |
| Analytics engineer | Needs consistent, FK-valid data to build and validate dashboard queries |

### Success Criteria (Draft)

- [ ] `python main.py generate --profile demo` completes in under 60 seconds and produces valid FK-joined data
- [ ] `python main.py generate --profile standard` produces ~1M orders across 90 days with correct seasonal patterns
- [ ] `python main.py generate --profile loadtest` produces 50M+ rows without OOM on a 16GB laptop
- [ ] `python main.py validate` passes all referential integrity checks with zero orphan FKs
- [ ] All three sinks (CSV, Parquet, Postgres) work with `--sink all`
- [ ] `--seed 42` produces identical output across runs
- [ ] Crisis simulation produces measurable sentiment shift and ticket spike
- [ ] All 10 domains produce data that supports their target dashboards

### Constraints Identified

- Python 3.11+ only
- Stack locked: Faker, Mimesis, NumPy, Pandas, PyArrow, SQLAlchemy, psycopg2-binary, tqdm, Pydantic
- Reproducibility: every run with same seed must produce identical output
- High-volume tables (`machine_telemetry`, `api_requests`) must use partitioned Parquet — no Postgres write for loadtest
- CPF/CNPJ must pass valid check-digit validation
- All monetary values must carry a `currency` column

### Out of Scope (Confirmed)

- Real external API calls (all data is generated locally)
- Streaming output (Kafka, Kinesis) — batch generation only
- Cloud storage upload (S3, GCS, ADLS) — local file output only
- Authentication or multi-user access to generated data
- Real Postgres in production — docker-compose for local use only

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 6 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 0 |
| Validations Completed | 4 |
| Selected Approach | A — Full project in one consistent pass |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_AURORA_CORP_DATA_GENERATOR.md`
