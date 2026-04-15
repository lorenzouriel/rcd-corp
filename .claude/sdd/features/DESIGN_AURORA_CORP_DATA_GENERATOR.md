# DESIGN: Aurora Corp Synthetic Data Generator

> Technical design for implementing a modular, reproducible Python CLI that generates realistic synthetic operational data for Aurora Corp across 10 business domains.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | AURORA_CORP_DATA_GENERATOR |
| **Date** | 2026-04-14 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_AURORA_CORP_DATA_GENERATOR.md](./DEFINE_AURORA_CORP_DATA_GENERATOR.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌──────────────────────────────────────────────────────────────────────┐
│                       CLI ENTRY POINT                                │
│               python main.py generate --profile demo                 │
│                  --seed 42 --sink all --only sales,hr                │
└──────────────────────┬───────────────────────────────────────────────┘
                       │ loads
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        config.yaml                                   │
│          profiles: demo / standard / loadtest                        │
│          date_range, volumes, crisis_freq, output_paths              │
└──────────────────────┬───────────────────────────────────────────────┘
                       │ configures
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATOR  (main.py)                          │
│  1. Seed all RNG (Faker / random / NumPy)                            │
│  2. Build SinkDispatcher from --sink flag                            │
│  3. Generate MASTER DATA → populate MasterCache                      │
│  4. Generate FACT DOMAINS (parallel-safe, sequential by default)     │
│  5. run validate  →  test_referential_integrity.py                   │
└───┬───────────────────────────────┬────────────────────────┬─────────┘
    │                               │                        │
    ▼                               ▼                        ▼
┌──────────────┐         ┌─────────────────────┐    ┌────────────────┐
│ MasterCache  │         │  GENERATORS (×10)   │    │ SinkDispatcher │
│              │◄────────│  BaseGenerator ABC  │───►│                │
│ customers[]  │  sample │  .generate(cache,   │    │ csv_sink       │
│ products[]   │  FK     │    profile, crisis) │    │ parquet_sink   │
│ employees[]  │  values │                     │    │ postgres_sink  │
│ suppliers[]  │         │  master_data.py      │    └────────────────┘
│ stores[]     │         │  sales.py            │
│ warehouses[] │         │  finance.py          │
│ fx_rates[]   │         │  marketing.py        │
└──────────────┘         │  social_media.py     │
                         │  supply_chain.py     │
                         │  manufacturing.py    │    ┌────────────────┐
                         │  hr.py               │    │  UTILITIES     │
                         │  support.py          │◄───│                │
                         │  observability.py    │    │ distributions  │
                         └─────────────────────┘    │ state_machines │
                                                     │ time_utils     │
                                                     │ identifiers    │
                                                     │ fx             │
                                                     └────────────────┘
```

### Generation Order (enforced by orchestrator)

```text
Phase 1 — Master Data (must complete before any fact generator):
  fx_rates → suppliers → stores → warehouses → employees → products → customers

Phase 2 — Fact Domains (can run in any order; all depend on Phase 1 cache):
  sales → finance → marketing → social_media → supply_chain
  → manufacturing → hr → support → observability

Phase 3 — Validation:
  python main.py validate  →  test_referential_integrity.py
```

---

## Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| `main.py` | CLI orchestrator; seeds RNG; runs phases in order | typer, structlog |
| `config.yaml` | Profile definitions; all tunable knobs | PyYAML |
| `generators/base.py` | `BaseGenerator` ABC enforcing uniform interface | Python ABC, Pydantic |
| `generators/master_data.py` | Generates 7 master tables; populates `MasterCache` | Faker (pt_BR/es_MX/pt_PT/en_US), Mimesis |
| `generators/sales.py` | Orders, order_items, payments, sessions, cart_events | Faker, state_machines, time_utils |
| `generators/finance.py` | Invoices, transactions, expenses, budgets, aurora_card | Faker, distributions |
| `generators/marketing.py` | Campaigns, events, email, leads, A/B tests | Faker, state_machines |
| `generators/social_media.py` | 11 social tables; engagement peaks; influencers | Faker, time_utils, distributions |
| `generators/supply_chain.py` | Shipments, inventory, POs, stock movements, returns | state_machines |
| `generators/manufacturing.py` | Production runs, telemetry (high-vol), QC, maintenance | state_machines, distributions |
| `generators/hr.py` | Attendance, reviews, training, recruitment, surveys | state_machines, distributions |
| `generators/support.py` | Tickets, messages, call center | state_machines, time_utils |
| `generators/observability.py` | App logs, API requests (high-vol), errors, deployments | distributions, time_utils |
| `utils/distributions.py` | Statistical distributions: Pareto LTV, normal-clipped, weighted_choice | NumPy |
| `utils/state_machines.py` | 6 lifecycle state machines with transition probabilities | Python dataclasses |
| `utils/time_utils.py` | Business hours, Black Friday, payday, crisis windows | datetime, pytz |
| `utils/identifiers.py` | CPF/CNPJ with valid check digits, SKU, tracking numbers | Pure Python |
| `utils/fx.py` | Daily FX rates: BRL/MXN/EUR/USD via random walk | NumPy |
| `sinks/csv_sink.py` | Write DataFrames to CSV files | Pandas |
| `sinks/parquet_sink.py` | Write DataFrames to date-partitioned Parquet | PyArrow |
| `sinks/postgres_sink.py` | Write DataFrames to Postgres via SQLAlchemy | SQLAlchemy, psycopg2 |
| `tests/test_referential_integrity.py` | Validate all FK joins; assert zero orphan rows | pytest, Pandas |
| `requirements.txt` + `pyproject.toml` | Dependency management | pip / build |
| `docker-compose.yml` | Local Postgres for sink testing | Docker |
| `README.md` | Setup, CLI examples, schema map, dashboard→datasets | Markdown |

---

## Key Decisions

### Decision 1: BaseGenerator as Abstract Base Class with `generate()` → `dict[str, pd.DataFrame]`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** Ten generator modules must follow identical patterns (FK sampling, sink dispatch, seasonal throttling, crisis injection) so the orchestrator can treat them uniformly.

**Choice:** `BaseGenerator` is a Python ABC with a single abstract method `generate(cache, profile, crisis_day) -> dict[str, pd.DataFrame]`. Each concrete generator returns a mapping of `table_name → DataFrame`. The orchestrator passes the return value to `SinkDispatcher.write_all()`.

**Rationale:** Returning a `dict[str, DataFrame]` keeps generators pure (no side effects), makes unit testing trivial (mock the cache, assert the dict), and lets the dispatcher handle sink routing without generators knowing which sinks are active.

**Alternatives Rejected:**
1. Generators write directly to sinks — rejected because generators become untestable in isolation and coupling prevents swapping sinks
2. Generator yields rows one at a time — rejected because Pandas vectorized ops are faster and DataFrames fit in memory for demo/standard profiles; chunked write is handled at the sink level

**Consequences:**
- For loadtest profile, high-volume generators (`manufacturing.py`, `observability.py`) must generate in chunks and call the sink per-chunk rather than returning a single giant DataFrame — handled via `generate_chunked()` override

---

### Decision 2: In-memory `MasterCache` as a typed dataclass of Pandas Series/arrays

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** All fact generators need to sample FK values (customer_id, product_id, store_id, etc.) without reading from disk, since referential integrity must be guaranteed by construction.

**Choice:** After Phase 1 completes, a `MasterCache` dataclass holds NumPy arrays of PK values for each master entity. Fact generators call `cache.sample_customer_ids(n)`, `cache.sample_product_skus(n)`, etc. — these are vectorized `np.random.choice` calls.

**Rationale:** NumPy arrays are memory-efficient (~8 bytes/int64 vs pandas Series overhead), and `np.random.choice` is the fastest way to sample with or without replacement. The typed dataclass makes IDE autocomplete work and catches missing FK sources at design time.

**Alternatives Rejected:**
1. Read master data from Parquet files per fact generator — rejected because disk I/O per generator would be slow and adds I/O ordering complexity
2. Use a dict of lists — rejected because `np.random.choice` on a list requires conversion anyway; NumPy arrays are the canonical form

**Consequences:**
- For loadtest profile with ~500k master rows, cache uses ~100MB RAM — acceptable given 16GB constraint

---

### Decision 3: State machines as dataclasses with `transition(state, rng) -> str`

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** Six lifecycles (Order, Ticket, Lead, Shipment, Recruitment, ProductionRun) need to produce realistic status distributions and sequences, not just random picks from a status list.

**Choice:** Each state machine is a dataclass holding a `transitions: dict[str, dict[str, float]]` matrix. A `transition(current_state, rng)` method picks the next state via `rng.choice` weighted by the probability row. Generators call `sm.run(start_state, max_steps, rng)` to produce a terminal state and optionally a full event log.

**Rationale:** Explicit probability matrices are readable, testable, and tunable via `config.yaml`. They produce realistic distributions (e.g., 70% delivered, 15% cancelled, 8% returned) without hardcoded logic.

**Alternatives Rejected:**
1. Enum-based state machine with if/elif chains — rejected because untestable and hard to tune
2. External library (transitions, pytransitions) — rejected because adds a dependency for a pattern simple enough to implement in 50 lines

**Consequences:**
- Crisis mode can inject modified transition matrices (higher cancellation, lower delivery) without changing generator code

---

### Decision 4: Chunked write for high-volume tables via `generate_chunked()` override

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** `machine_telemetry` and `api_requests` can reach 10M+ rows on loadtest profile. Accumulating these in a single DataFrame would OOM a 16GB machine.

**Choice:** `BaseGenerator` exposes a `generate_chunked(cache, profile, crisis_day, chunk_size) -> Iterator[dict[str, pd.DataFrame]]` method. High-volume generators (`manufacturing.py`, `observability.py`) override this instead of `generate()`. The orchestrator detects which method to call. All chunks are written to date-partitioned Parquet only (not CSV or Postgres) on loadtest profile.

**Rationale:** Iterator pattern keeps memory footprint bounded at `chunk_size` rows regardless of total volume. Date partitioning enables downstream consumers to query by date without reading the entire dataset.

**Alternatives Rejected:**
1. Write all rows to a temp file then process — rejected because doubles disk I/O
2. Dask DataFrames — rejected because adds a heavy dependency for a pattern solvable with a plain iterator

**Consequences:**
- Loadtest profile for high-volume tables always routes to Parquet regardless of `--sink` flag — documented in README

---

### Decision 5: `config.yaml` as single source of truth for all volume knobs

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** Three profiles (demo/standard/loadtest) control row counts, date ranges, crisis frequency, and chunk sizes across 10 domains. These must be tunable without code changes.

**Choice:** `config.yaml` has a top-level `profiles` key with three entries. Each profile entry contains `n_customers`, `n_orders`, `date_range_days`, `crisis_freq_per_month`, `chunk_size`, and per-domain volume multipliers. Generators receive a `ProfileConfig` Pydantic model, not raw YAML.

**Rationale:** Config-driven design enables the "Tuning Guide" section of README and lets users create custom profiles without touching Python. Pydantic validation catches typos/type errors at startup before any generation runs.

**Alternatives Rejected:**
1. CLI flags for each volume knob — rejected because 30+ flags make the CLI unusable
2. Python constants in each generator — rejected because tuning requires code changes and is error-prone

**Consequences:**
- Adding a new profile requires only a YAML edit, not code changes

---

### Decision 6: `SinkDispatcher` decouples generators from output format

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-04-14 |

**Context:** `--sink csv|parquet|postgres|all` must route the same DataFrame to one or more sinks without generators knowing which sinks are active.

**Choice:** `SinkDispatcher` is initialized with the active sink list and exposes `write(table_name, df, partition_col=None)`. It fans out to all active sinks. Generators never import sink modules.

**Rationale:** Single responsibility — generators produce data, dispatcher handles routing. Adding a new sink (e.g., Delta Lake) requires only a new sink class and a dispatcher registration, no generator changes.

**Alternatives Rejected:**
1. Generator calls each sink directly — rejected because generators become coupled to output format
2. Pass sink instances to generators via dependency injection — rejected because overkill; dispatcher is simpler and sufficient

**Consequences:**
- High-volume tables can override routing in the dispatcher (always Parquet on loadtest) without touching generators

---

## File Manifest

| # | File | Action | Purpose | Dependencies |
|---|------|--------|---------|--------------|
| 1 | `aurora_data/config.yaml` | Create | Profile definitions; all volume/date/crisis knobs | None |
| 2 | `aurora_data/utils/distributions.py` | Create | `pareto_ltv`, `weighted_choice`, `normal_clipped`, `seasonal_curve` | None |
| 3 | `aurora_data/utils/state_machines.py` | Create | 6 lifecycle state machines with transition probability matrices | None |
| 4 | `aurora_data/utils/time_utils.py` | Create | `business_hours`, `black_friday_dates`, `payday_multiplier`, `crisis_windows` | None |
| 5 | `aurora_data/utils/identifiers.py` | Create | CPF/CNPJ check-digit generation, SKU generator, tracking numbers | None |
| 6 | `aurora_data/utils/fx.py` | Create | Daily FX rate generator with random walk (BRL/MXN/EUR/USD) | 2 |
| 7 | `aurora_data/sinks/csv_sink.py` | Create | Write DataFrame to CSV; create output dir if needed | None |
| 8 | `aurora_data/sinks/parquet_sink.py` | Create | Write DataFrame to date-partitioned Parquet via PyArrow | None |
| 9 | `aurora_data/sinks/postgres_sink.py` | Create | Write DataFrame to Postgres via SQLAlchemy; create tables from schema | None |
| 10 | `aurora_data/generators/__init__.py` | Create | Package init; exports all generator classes | None |
| 11 | `aurora_data/generators/base.py` | Create | `BaseGenerator` ABC; `MasterCache` dataclass; `ProfileConfig` Pydantic model; `SinkDispatcher` | 2, 3, 4, 7, 8, 9 |
| 12 | `aurora_data/generators/master_data.py` | Create | Customers, products, employees, suppliers, stores, warehouses, fx_rates | 5, 6, 11 |
| 13 | `aurora_data/generators/sales.py` | Create | Orders, order_items, payments, shopping_cart_events, web_sessions | 3, 4, 11 |
| 14 | `aurora_data/generators/finance.py` | Create | Invoices, transactions, expenses, budgets, aurora_card_transactions | 2, 11 |
| 15 | `aurora_data/generators/marketing.py` | Create | Campaigns, campaign_events, email_events, leads, ab_test_exposures | 3, 4, 11 |
| 16 | `aurora_data/generators/social_media.py` | Create | 11 social tables: posts, metrics, comments, mentions, DMs, influencers, reviews, ad_spend | 2, 4, 11 |
| 17 | `aurora_data/generators/supply_chain.py` | Create | Shipments, inventory_snapshots, purchase_orders, stock_movements, returns | 3, 11 |
| 18 | `aurora_data/generators/manufacturing.py` | Create | Production_runs, machine_telemetry (chunked), quality_checks, maintenance_events | 3, 4, 11 |
| 19 | `aurora_data/generators/hr.py` | Create | Attendance, performance_reviews, training_records, recruitment_pipeline, engagement_surveys | 2, 3, 11 |
| 20 | `aurora_data/generators/support.py` | Create | Tickets, ticket_messages, call_center_calls | 3, 4, 11 |
| 21 | `aurora_data/generators/observability.py` | Create | App_logs (chunked), api_requests (chunked), errors, deployments, security_events | 4, 11 |
| 22 | `aurora_data/main.py` | Create | CLI orchestrator via typer; seeds RNG; phases 1→2→3; `generate` and `validate` commands | 1, 10–21 |
| 23 | `aurora_data/tests/test_referential_integrity.py` | Create | pytest; loads Parquet/CSV output; asserts zero orphan FKs across all domains | 11 |
| 24 | `requirements.txt` | Create | Pinned dependencies | None |
| 25 | `pyproject.toml` | Create | Package metadata; build config; entry point | None |
| 26 | `docker-compose.yml` | Create | Postgres service for local sink testing | None |
| 27 | `README.md` | Create | Setup, CLI reference, schema map, dashboard→datasets table, tuning guide | None |

**Total Files:** 27

---

## Code Patterns

### Pattern 1: BaseGenerator abstract class

```python
# aurora_data/generators/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterator
import numpy as np
import pandas as pd
from pydantic import BaseModel

class ProfileConfig(BaseModel):
    name: str
    n_customers: int
    n_products: int
    n_employees: int
    n_orders: int
    date_range_days: int
    crisis_freq_per_month: int
    chunk_size: int
    start_date: str  # ISO format "YYYY-MM-DD"

@dataclass
class MasterCache:
    customer_ids: np.ndarray = field(default_factory=lambda: np.array([], dtype="U36"))
    product_skus: np.ndarray = field(default_factory=lambda: np.array([], dtype="U20"))
    employee_ids: np.ndarray = field(default_factory=lambda: np.array([], dtype="U36"))
    supplier_ids: np.ndarray = field(default_factory=lambda: np.array([], dtype="U36"))
    store_ids: np.ndarray = field(default_factory=lambda: np.array([], dtype="U36"))
    warehouse_ids: np.ndarray = field(default_factory=lambda: np.array([], dtype="U36"))
    fx_rates: pd.DataFrame = field(default_factory=pd.DataFrame)

    def sample_customer_ids(self, n: int, rng: np.random.Generator) -> np.ndarray:
        return rng.choice(self.customer_ids, size=n, replace=True)

    def sample_product_skus(self, n: int, rng: np.random.Generator) -> np.ndarray:
        return rng.choice(self.product_skus, size=n, replace=True)

    # ... similar helpers for other FK domains

class BaseGenerator(ABC):
    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self.rng = np.random.default_rng(seed)

    @abstractmethod
    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_day: date | None,
    ) -> dict[str, pd.DataFrame]:
        """Return {table_name: DataFrame}. Override for standard-volume tables."""
        ...

    def generate_chunked(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_day: date | None,
        chunk_size: int,
    ) -> Iterator[dict[str, pd.DataFrame]]:
        """Override for high-volume tables. Default: yields generate() result as single chunk."""
        yield self.generate(cache, profile, crisis_day)
```

---

### Pattern 2: OrdersGenerator — fully-worked reference implementation

```python
# aurora_data/generators/sales.py  (abbreviated key patterns)
import uuid
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.state_machines import OrderLifecycle
from ..utils.time_utils import timestamp_in_window, payday_multiplier, black_friday_multiplier
from ..utils.distributions import weighted_choice

CHANNELS = ["web", "mobile_app", "pos", "marketplace", "call_center"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]
CURRENCIES_BY_COUNTRY = {"BR": "BRL", "MX": "MXN", "PT": "EUR", "US": "USD"}

class SalesGenerator(BaseGenerator):
    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_day: date | None,
    ) -> dict[str, pd.DataFrame]:
        orders = self._build_orders(cache, profile, crisis_day)
        order_items = self._build_order_items(orders, cache, profile)
        payments = self._build_payments(orders, profile)
        sessions = self._build_sessions(cache, profile)
        cart_events = self._build_cart_events(sessions, cache, profile)
        return {
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "web_sessions": sessions,
            "shopping_cart_events": cart_events,
        }

    def _build_orders(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_day: date | None,
    ) -> pd.DataFrame:
        n = profile.n_orders
        sm = OrderLifecycle()

        # FK sampling — guaranteed referential integrity
        customer_ids = cache.sample_customer_ids(n, self.rng)
        store_ids = cache.sample_store_ids(n, self.rng)

        # Temporal distribution with seasonal multipliers
        start = date.fromisoformat(profile.start_date)
        created_at = [
            timestamp_in_window(
                base_date=start + timedelta(days=int(self.rng.integers(0, profile.date_range_days))),
                multiplier=payday_multiplier(d) * black_friday_multiplier(d),
                rng=self.rng,
            )
            for d in [start + timedelta(days=int(i)) for i in self.rng.integers(0, profile.date_range_days, n)]
        ]

        # State machine terminal states
        statuses = [sm.run(start_state="pending", rng=self.rng) for _ in range(n)]

        # Multi-currency via store country
        currencies = [CURRENCIES_BY_COUNTRY.get("BR", "BRL")] * n  # simplified; real impl uses store lookup

        # Crisis injection: inflate cancellation rate on crisis day
        if crisis_day:
            crisis_mask = np.array([ts.date() == crisis_day for ts in created_at])
            for i in np.where(crisis_mask)[0]:
                statuses[i] = sm.run("pending", rng=self.rng, crisis_mode=True)

        return pd.DataFrame({
            "id": [str(uuid.uuid4()) for _ in range(n)],
            "customer_id": customer_ids,
            "store_id": store_ids,
            "channel": weighted_choice(CHANNELS, CHANNEL_WEIGHTS, n, self.rng),
            "status": statuses,
            "currency": currencies,
            "created_at": created_at,
        })
```

---

### Pattern 3: State machine with crisis mode override

```python
# aurora_data/utils/state_machines.py
from dataclasses import dataclass, field
import numpy as np

@dataclass
class StateMachine:
    transitions: dict[str, dict[str, float]]
    terminal_states: set[str]

    def run(
        self,
        start_state: str,
        rng: np.random.Generator,
        max_steps: int = 20,
        crisis_mode: bool = False,
    ) -> str:
        state = start_state
        overrides = self._crisis_overrides() if crisis_mode else {}
        for _ in range(max_steps):
            if state in self.terminal_states:
                return state
            row = overrides.get(state, self.transitions[state])
            states = list(row.keys())
            probs = list(row.values())
            state = rng.choice(states, p=probs)
        return state

    def _crisis_overrides(self) -> dict[str, dict[str, float]]:
        return {}  # override in subclasses

class OrderLifecycle(StateMachine):
    def __init__(self) -> None:
        super().__init__(
            transitions={
                "pending":   {"paid": 0.82, "cancelled": 0.18},
                "paid":      {"picked": 0.93, "cancelled": 0.07},
                "picked":    {"shipped": 0.97, "cancelled": 0.03},
                "shipped":   {"delivered": 0.88, "returned": 0.09, "lost": 0.03},
                "delivered": {"returned": 0.05, "delivered": 0.95},
                "returned":  {"refunded": 0.90, "returned": 0.10},
            },
            terminal_states={"cancelled", "delivered", "refunded", "lost"},
        )

    def _crisis_overrides(self) -> dict[str, dict[str, float]]:
        return {
            "pending": {"paid": 0.50, "cancelled": 0.50},
            "shipped": {"delivered": 0.60, "returned": 0.30, "lost": 0.10},
        }
```

---

### Pattern 4: `config.yaml` structure (three profiles)

```yaml
# aurora_data/config.yaml
output:
  base_path: "./output"
  csv_path: "./output/csv"
  parquet_path: "./output/parquet"
  partition_col: "date"

profiles:
  demo:
    n_customers: 1_000
    n_products: 250
    n_employees: 200
    n_orders: 10_000
    date_range_days: 30
    crisis_freq_per_month: 1
    chunk_size: 5_000
    start_date: "2024-01-01"

  standard:
    n_customers: 50_000
    n_products: 250
    n_employees: 4_200
    n_orders: 1_000_000
    date_range_days: 90
    crisis_freq_per_month: 1
    chunk_size: 50_000
    start_date: "2024-01-01"

  loadtest:
    n_customers: 500_000
    n_products: 250
    n_employees: 4_200
    n_orders: 20_000_000
    date_range_days: 730
    crisis_freq_per_month: 2
    chunk_size: 200_000
    start_date: "2022-01-01"

company:
  name: "Aurora Corp"
  ticker: "AURC"
  founded: 2008
  hq: "São Paulo"
  locales: ["pt_BR", "es_MX", "pt_PT", "en_US"]
  countries: ["BR", "MX", "PT", "US"]
  currencies: ["BRL", "MXN", "EUR", "USD"]

crisis:
  sentiment_shift:
    positive_baseline: 0.55
    positive_crisis: 0.20
    negative_baseline: 0.15
    negative_crisis: 0.55
  ticket_multiplier: 5
  social_negative_multiplier: 3
  duration_days: 3
```

---

### Pattern 5: Temporal realism utilities

```python
# aurora_data/utils/time_utils.py
from datetime import date, datetime, time, timedelta
import numpy as np

BUSINESS_HOURS = {
    "BR": (9, 18),   # BRT UTC-3
    "MX": (9, 18),   # CST UTC-6
    "PT": (9, 18),   # WET UTC+0
    "US": (9, 18),   # EST UTC-5
}

SOCIAL_PEAK_HOURS = (19, 22)  # BRT

def black_friday_multiplier(d: date) -> float:
    """Return 10.0 on Black Friday/Cyber Monday, 3.0 in Black Friday week, 2.0 in December, else 1.0."""
    if d.month == 11 and d.weekday() == 4:  # last Friday of November
        if d == _last_friday_of_november(d.year):
            return 10.0
    if d.month == 11 and d.weekday() == 0 and d.isocalendar().week == _last_friday_of_november(d.year).isocalendar().week + 1:
        return 10.0  # Cyber Monday
    if d.month == 11 and _last_friday_of_november(d.year) - timedelta(days=7) <= d <= _last_friday_of_november(d.year):
        return 3.0
    if d.month == 12:
        return 2.0
    return 1.0

def payday_multiplier(d: date) -> float:
    """Brazilian payday effect: 5th and 20th of month → 2x orders."""
    if d.day in (5, 20):
        return 2.0
    return 1.0

def timestamp_in_window(base_date: date, multiplier: float, rng: np.random.Generator, country: str = "BR") -> datetime:
    """Generate a realistic timestamp within business hours for the given date."""
    h_start, h_end = BUSINESS_HOURS[country]
    hour = int(rng.integers(h_start, h_end))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime.combine(base_date, time(hour, minute, second))
```

---

### Pattern 6: Parquet sink with date partitioning

```python
# aurora_data/sinks/parquet_sink.py
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class ParquetSink:
    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)

    def write(self, table_name: str, df: pd.DataFrame, partition_col: str | None = None) -> None:
        out_dir = self.base_path / table_name
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df)
        if partition_col and partition_col in df.columns:
            pq.write_to_dataset(
                table,
                root_path=str(out_dir),
                partition_cols=[partition_col],
                existing_data_behavior="overwrite_or_ignore",
            )
        else:
            pq.write_table(table, str(out_dir / "data.parquet"))
```

---

### Pattern 7: CLI orchestrator structure

```python
# aurora_data/main.py  (abbreviated)
import random
from datetime import date
from typing import Annotated
import numpy as np
import typer
import structlog
from faker import Faker

from .generators import (
    MasterDataGenerator, SalesGenerator, FinanceGenerator,
    MarketingGenerator, SocialMediaGenerator, SupplyChainGenerator,
    ManufacturingGenerator, HRGenerator, SupportGenerator, ObservabilityGenerator,
)
from .generators.base import MasterCache, SinkDispatcher, load_profile

app = typer.Typer()
log = structlog.get_logger()

DOMAIN_MAP = {
    "master_data": MasterDataGenerator,
    "sales": SalesGenerator,
    "finance": FinanceGenerator,
    "marketing": MarketingGenerator,
    "social_media": SocialMediaGenerator,
    "supply_chain": SupplyChainGenerator,
    "manufacturing": ManufacturingGenerator,
    "hr": HRGenerator,
    "support": SupportGenerator,
    "observability": ObservabilityGenerator,
}

@app.command()
def generate(
    profile: str = "demo",
    seed: int = 42,
    sink: str = "parquet",
    only: Annotated[str | None, typer.Option()] = None,
    config: str = "aurora_data/config.yaml",
) -> None:
    # 1. Seed all RNG sources
    random.seed(seed)
    np.random.seed(seed)
    faker = Faker(["pt_BR", "es_MX", "pt_PT", "en_US"])
    Faker.seed(seed)

    # 2. Load profile config
    profile_cfg = load_profile(config, profile)
    dispatcher = SinkDispatcher.from_flag(sink, profile_cfg)

    # 3. Phase 1: master data
    cache = MasterCache()
    master_gen = MasterDataGenerator(seed=seed)
    master_tables = master_gen.generate(cache, profile_cfg, crisis_day=None)
    cache.populate(master_tables)
    dispatcher.write_all(master_tables)

    # 4. Pick crisis day
    crisis_day = _pick_crisis_day(profile_cfg, seed)

    # 5. Phase 2: fact domains
    domains = _resolve_domains(only)
    for domain_name in domains:
        gen = DOMAIN_MAP[domain_name](seed=seed)
        log.info("generating", domain=domain_name, profile=profile)
        if hasattr(gen, "generate_chunked"):
            for chunk in gen.generate_chunked(cache, profile_cfg, crisis_day, profile_cfg.chunk_size):
                dispatcher.write_all(chunk, force_parquet_for_high_vol=True)
        else:
            tables = gen.generate(cache, profile_cfg, crisis_day)
            dispatcher.write_all(tables)

    log.info("generation_complete", profile=profile, crisis_day=str(crisis_day))

@app.command()
def validate(output: str = "./output") -> None:
    """Run referential integrity checks on generated data."""
    import subprocess, sys
    result = subprocess.run(["pytest", "aurora_data/tests/test_referential_integrity.py", "-v"], check=False)
    sys.exit(result.returncode)

if __name__ == "__main__":
    app()
```

---

## Data Flow

```text
1. CLI parses args → loads ProfileConfig from config.yaml
   │
   ▼
2. RNG seeded: random.seed(N), np.random.seed(N), Faker.seed(N)
   │
   ▼
3. Phase 1 — MasterDataGenerator.generate() → dict[str, DataFrame]
   │          fx_rates → suppliers → stores → warehouses → employees → products → customers
   │          cache.populate(master_tables)  ←── FK arrays built in memory
   │          SinkDispatcher.write_all(master_tables)
   │
   ▼
4. crisis_day = pick random day(s) from date range per crisis_freq_per_month
   │
   ▼
5. Phase 2 — for each fact domain:
   │    gen.generate(cache, profile, crisis_day) → dict[str, DataFrame]
   │    or gen.generate_chunked(...) → Iterator[dict[str, DataFrame]]
   │         │
   │         ├── FK values: cache.sample_*_ids(n, rng)  →  referential integrity guaranteed
   │         ├── Temporal: timestamp_in_window() × seasonal multipliers
   │         ├── Statistical: pareto_ltv(), normal_clipped(), weighted_choice()
   │         ├── State machines: OrderLifecycle.run() per row
   │         └── Crisis: modified transition probabilities on crisis_day
   │
   ▼
6. SinkDispatcher.write_all(tables)
   ├── csv_sink.write(table_name, df)           → ./output/csv/{table_name}.csv
   ├── parquet_sink.write(table_name, df, "date") → ./output/parquet/{table_name}/date=YYYY-MM-DD/
   └── postgres_sink.write(table_name, df)      → postgres://localhost/aurora_corp
   │
   ▼
7. python main.py validate
   └── pytest test_referential_integrity.py
       └── Load Parquet/CSV → pd.merge() on FKs → assert len(orphans) == 0
```

---

## Integration Points

| External System | Integration Type | Notes |
|-----------------|-----------------|-------|
| Postgres (local) | SQLAlchemy + psycopg2 | `docker-compose.yml` provides service; connection string from config or env var `AURORA_POSTGRES_URL` |
| Filesystem (CSV) | Pandas `to_csv()` | Creates `./output/csv/` tree |
| Filesystem (Parquet) | PyArrow `write_to_dataset()` | Creates `./output/parquet/{table}/date=YYYY-MM-DD/` tree |
| Docker | docker-compose.yml | Postgres only; no other services |

---

## Pipeline Architecture

### Generation DAG

```text
[config.yaml]
     │
     ▼
[main.py orchestrator]
     │
     ├─► [fx_rates] ─────────────────────────────────────┐
     ├─► [suppliers] ────────────────────────────────────┐│
     ├─► [stores] ───────────────────────────────────────││
     ├─► [warehouses] ───────────────────► MasterCache ──┤│
     ├─► [employees] ────────────────────────────────────││
     ├─► [products] ─────────────────────────────────────││
     └─► [customers] ────────────────────────────────────┘│
                                                          │
     ┌────────────────────────────────────────────────────┘
     │ all fact generators sample from MasterCache
     ├─► [sales]          → orders, order_items, payments, sessions, cart_events
     ├─► [finance]        → invoices, transactions, expenses, budgets, aurora_card
     ├─► [marketing]      → campaigns, events, email, leads, ab_tests
     ├─► [social_media]   → posts, metrics, comments, mentions, DMs, influencers, reviews
     ├─► [supply_chain]   → shipments, inventory, POs, movements, returns
     ├─► [manufacturing]  → production_runs, machine_telemetry*, quality_checks
     ├─► [hr]             → attendance, reviews, training, recruitment, surveys
     ├─► [support]        → tickets, messages, calls
     └─► [observability]  → app_logs*, api_requests*, errors, deployments
                                  * chunked Parquet only on loadtest
```

### Partition Strategy

| Table | Partition Column | Granularity | Rationale |
|-------|-----------------|-------------|-----------|
| `machine_telemetry` | `date` | daily | High volume IoT; queries are time-windowed |
| `api_requests` | `date` | daily | High volume; log analysis is time-windowed |
| `app_logs` | `date` | daily | High volume; debugging queries filter by date |
| `orders` | `date` | daily | Query patterns: daily revenue aggregations |
| `social_metrics` | `date` | daily | Time-series snapshots queried by date range |
| All others | None | Single file | Standard volume; no partition benefit |

### Data Quality Gates

| Gate | Tool | Threshold | Action on Failure |
|------|------|-----------|-------------------|
| Zero orphan FKs | pytest + Pandas merge | 0 orphan rows | Exit non-zero; block downstream |
| No null PKs | pytest assert | 0 nulls on `id` columns | Exit non-zero |
| Status distribution completeness | pytest | All terminal states present | Warn only |
| CPF/CNPJ check digit | identifiers.py at generation time | 100% valid | Raise ValueError at generation |
| Currency enum | Pydantic model | One of BRL/MXN/EUR/USD | Raise ValidationError at generation |

---

## Testing Strategy

| Test Type | Scope | Files | Tools | Coverage Goal |
|-----------|-------|-------|-------|---------------|
| Unit — distributions | `pareto_ltv`, `weighted_choice`, `normal_clipped` | `tests/test_distributions.py` | pytest | 100% function coverage |
| Unit — state machines | All 6 lifecycles; terminal state reachability; crisis override | `tests/test_state_machines.py` | pytest | All states reachable |
| Unit — time utils | Black Friday dates, payday days, business hour constraints | `tests/test_time_utils.py` | pytest | 2023–2025 date range |
| Unit — identifiers | CPF/CNPJ check digit correctness | `tests/test_identifiers.py` | pytest | 1000 generated values |
| Integration — generators | MasterDataGenerator + SalesGenerator with mock cache | `tests/test_generators.py` | pytest | Demo profile end-to-end |
| Integration — sinks | CSV/Parquet/Postgres round-trip | `tests/test_sinks.py` | pytest + tmp_path | Write + read back |
| Acceptance — FK integrity | Zero orphan FKs across all tables (standard profile) | `tests/test_referential_integrity.py` | pytest + Pandas | AT-002 |
| Acceptance — determinism | Byte-identical output with same seed | `tests/test_determinism.py` | pytest | AT-003 |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| Invalid `--profile` value | Pydantic `ValidationError` at config load; print valid profiles and exit 1 | No |
| Postgres connection failure | SQLAlchemy `OperationalError`; log connection string (masked); exit 1 with message to check docker-compose | No |
| Output directory not writable | `PermissionError` caught in sink; log path; exit 1 | No |
| FK array empty (master data not generated) | `MasterCache` raises `RuntimeError("cache not populated: run master_data first")`; exit 1 | No |
| OOM during loadtest | Not caught — chunked write is the prevention; README documents chunk_size tuning knob | N/A |
| Invalid CPF/CNPJ generated | `ValueError` in `identifiers.py`; indicates algorithm bug — fail fast | No |

---

## Configuration Reference

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `profiles.demo.n_orders` | int | 10,000 | Order rows for demo profile |
| `profiles.standard.n_orders` | int | 1,000,000 | Order rows for standard profile |
| `profiles.loadtest.n_orders` | int | 20,000,000 | Order rows for loadtest profile |
| `profiles.*.chunk_size` | int | varies | Rows per chunk for high-volume generators |
| `profiles.*.date_range_days` | int | varies | Date range for temporal distribution |
| `profiles.*.crisis_freq_per_month` | int | 1 | Crisis events per month |
| `output.parquet_path` | str | `./output/parquet` | Root for Parquet output |
| `output.csv_path` | str | `./output/csv` | Root for CSV output |
| `crisis.ticket_multiplier` | float | 5.0 | Ticket volume multiplier on crisis day |
| `crisis.social_negative_multiplier` | float | 3.0 | Negative comment multiplier on crisis day |

---

## Security Considerations

- No real PII generated — all names, emails, CPFs are synthetic and seeded; no real individuals
- Postgres credentials: read from `AURORA_POSTGRES_URL` env var; never hardcoded in config.yaml or source
- `docker-compose.yml` Postgres uses a local-only binding (`127.0.0.1:5432`); not exposed to network
- No outbound HTTP calls at runtime — fully offline operation

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Logging | `structlog` structured JSON logs; every generator emits `{domain, table, n_rows, elapsed_s}` |
| Progress | `tqdm` progress bar per domain on stdout; suppressed with `--quiet` flag |
| Timing | `time.perf_counter()` per phase; summary table printed at end of run |
| Validation output | pytest `-v` output; exit code propagated to shell for CI integration |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-04-14 | design-agent | Initial version from DEFINE_AURORA_CORP_DATA_GENERATOR.md |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_AURORA_CORP_DATA_GENERATOR.md`
