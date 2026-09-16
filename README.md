<div align="center">
  <img src="docs/logos/logo-nobg.png" alt="logo" width="160">
</div>

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Python CLI that generates realistic, interconnected operational data for **RCD (Real Company Data) Corp**, a fictional mid-to-large enterprise, across **10 business domains** and **50+ tables**, writing to **CSV**, **Parquet**, **JSONL**, **XLSX**, **Postgres**, **SQL Server**, **MongoDB**, and **Redis** — including live streaming into every one of them. A **REST + GraphQL query API** (`rcd-data serve`) sits on top of the Postgres sink for querying the generated data directly.
```
RCD Corp — founded 2008, HQ São Paulo (BR), offices in Mexico City, Lisbon, Miami
~4,200 employees · ~$1.2B annual revenue · Ticker: RCDC
```

## Quick Start
```bash
# Install
pip install -e .

# Generate demo data (~10k rows/fact table) → Parquet
rcd-data generate --profile demo --seed 42 --sink parquet

# Generate standard data (~1M orders) → all sinks
rcd-data generate --profile standard --sink all

# Generate only social media and support domains
rcd-data generate --profile demo --only social_media,support --sink csv

# Stream live batches — appends 25 rows/domain every 5 min to every active sink
# (files AND databases, if configured — see the stream options table below)
rcd-data stream --profile demo --seed 42 --rows-per-tick 25 --interval 300

# Validate referential integrity
RCD_OUTPUT_DIR=./output rcd-data validate

# Show profile sizes
rcd-data info
```

## Setup
### Local (Python 3.11+)
```bash
git clone https://github.com/lorenzouriel/rcd-corp
cd rcd-corp
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

### With Postgres (docker-compose)
```bash
docker compose up -d postgres
export RCD_POSTGRES_URL="postgresql+psycopg2://rcd:rcd@localhost:5432/rcd_corp"
$env:RCD_POSTGRES_URL="postgresql+psycopg2://rcd:rcd@localhost:5432/rcd_corp"
rcd-data generate --profile demo --sink all
```

### With SQL Server (docker-compose)
Requires the [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server) installed locally (already baked into the Docker image).
```bash
docker compose up -d sqlserver sqlserver-init
export RCD_SQLSERVER_URL="mssql+pyodbc://sa:Rcd!Passw0rd@localhost:14330/rcd_corp?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
$env:RCD_SQLSERVER_URL="mssql+pyodbc://sa:Rcd!Passw0rd@localhost:14330/rcd_corp?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
rcd-data generate --profile demo --sink sqlserver
```

### With MongoDB (docker-compose)
```bash
docker compose up -d mongodb
export RCD_MONGODB_URL="mongodb://rcd:rcd@localhost:27017/rcd_corp?authSource=admin"
$env:RCD_MONGODB_URL="mongodb://rcd:rcd@localhost:27017/rcd_corp?authSource=admin"
rcd-data generate --profile demo --sink mongodb
```
One collection per table; `generate` drops and refills each collection, `stream` inserts.

### With Redis (docker-compose)
```bash
docker compose up -d redis
export RCD_REDIS_URL="redis://:rcd@localhost:6379/0"
$env:RCD_REDIS_URL="redis://:rcd@localhost:6379/0"
rcd-data generate --profile demo --sink redis
```
Each table becomes a Redis Stream (`rcd:{table}`), one JSON-encoded entry per row, capped at `RCD_REDIS_STREAM_MAXLEN` (default 100,000, approximate trim) — a bounded recent-window sink, not full historical storage like the other sinks.

### Docker (full end-to-end)
```bash
docker compose --profile run up --build
```

### Running as a standing service (home lab / Tailscale)
Turns this into a standing service: generate once into every sink, then keep
`rcd-data stream` appending to all of them (files + all four databases)
continuously. Database access is over [Tailscale](https://tailscale.com)
rather than a public port — no router/firewall exposure at all. See
[DEPLOY.md](docs/DEPLOY.md) for the full runbook. Built on the `streamer` service
in `docker-compose.yml` and `.env.example`.

## CLI Reference
```
rcd-data generate [OPTIONS]
rcd-data stream   [OPTIONS]
rcd-data validate [OPTIONS]
rcd-data info     [OPTIONS]
rcd-data serve    [OPTIONS]
```

### `generate` options
| Option | Default | Description |
|--------|---------|-------------|
| `--profile` | `demo` | Volume profile: `demo` · `standard` · `loadtest` |
| `--seed` | `42` | Random seed — same seed = identical output |
| `--sink` | `parquet` | Output sink: `csv` · `parquet` · `jsonl` · `xlsx` · `postgres` · `sqlserver` · `mongodb` · `redis` · `all` |
| `--only` | (all) | Comma-separated domain list to generate selectively |
| `--config` | built-in | Path to a custom `config.yaml` |

### `stream` options

Requires a prior `generate` run — reads the existing output to rebuild dimension FK tables, then continuously appends small batches to the same directory.

| Option | Default | Description |
|--------|---------|-------------|
| `--profile` | `demo` | Profile to use for FK pool sizing |
| `--seed` | `42` | Base seed; tick N uses `seed + N` to avoid duplicate PKs |
| `--sink` | `parquet` | Output sink: `csv` · `parquet` · `jsonl` · `xlsx` · `postgres` · `sqlserver` · `mongodb` · `redis` · `all` |
| `--rows-per-tick` | `25` | Approximate rows per domain per tick |
| `--interval` | `300` | Seconds between ticks; `0` = fire once and exit |
| `--config` | built-in | Path to a custom `config.yaml` |

```bash
# One-time setup
rcd-data generate --profile demo --seed 42 --sink parquet

# Live streaming (runs until Ctrl+C)
rcd-data stream --profile demo --seed 42 --rows-per-tick 25 --interval 300

# Single tick — useful for testing
rcd-data stream --profile demo --seed 42 --interval 0

# Stream straight into Postgres/SQL Server too (append-only, no dedup)
rcd-data generate --profile demo --seed 42 --sink all
rcd-data stream --profile demo --seed 42 --sink all --interval 300
```

### `validate` options
| Option | Default | Description |
|--------|---------|-------------|
| `--output` | `./output` | Path to generated output root |
| `--format` | `parquet` | Format to validate: `parquet` · `csv` |

### `serve` options
| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `127.0.0.1` | Bind host (also `RCD_API_HOST`) |
| `--port` / `-p` | `8000` | Bind port (also `RCD_API_PORT`) |
| `--reload` | off | Auto-reload on code changes (dev only) |

Reads whatever `rcd-data generate --sink postgres` last wrote via `RCD_POSTGRES_URL`. See [Query API](#query-api) below.

### Domain names for `--only`
```
master_data  sales  finance  marketing  social_media
supply_chain  manufacturing  hr  support  observability
```

## Query API

REST + GraphQL over the `postgres` sink (16 core-business tables — customers, products, employees, stores, warehouses, suppliers, orders, order_items, payments, invoices, tickets, campaigns, leads, returns, shipments, purchase_orders). Requires a prior `rcd-data generate --sink postgres` and both APIs share one filter/pagination implementation (`rcd_data/api/queries/`), so REST and GraphQL can't drift in behavior.

```bash
rcd-data generate --profile demo --seed 42 --sink postgres
rcd-data serve --host 127.0.0.1 --port 8000   # add --reload for dev
```

Every request needs an `X-API-Key` header. Defaults to `rcd-dev-key` locally (`RCD_API_KEY` env var) — override it for anything beyond localhost, same posture as `POSTGRES_PASSWORD=rcd`.

```bash
# REST
curl -H "X-API-Key: rcd-dev-key" \
  "http://127.0.0.1:8000/api/v1/orders?status=delivered&limit=5"

# GraphQL (interactive GraphiQL IDE also at this URL in a browser)
curl -X POST http://127.0.0.1:8000/graphql \
  -H "X-API-Key: rcd-dev-key" -H "Content-Type: application/json" \
  -d '{"query": "{ orders(status: \"delivered\", limit: 5) { total items { id status customer { name } } } }"}'
```

GraphQL exposes FK relationships as nested fields (`order { customer { ... } }`, `ticket { agent { ... } }`, `lead { campaign { ... } owner { ... } }`, `shipment { warehouse { ... } }`, `purchase_order { supplier { ... } warehouse { ... } }`, `invoice { customer { ... } }`), backed by per-request DataLoaders so nesting doesn't N+1. REST stays flat (FK ids only) — no `?expand=`.

Every list endpoint takes `limit` (default 50, max 200 — over that is a `422`, never silently clamped), `offset`, and `total`/`includeTotal` (default on; skip it at `loadtest`-profile row counts where a `COUNT(*)` gets expensive). Filters:

| Table | Filters |
|---|---|
| `customers` | `segment`, `country`, `ltv_tier`, `preferred_channel` |
| `products` | `category`, `subcategory`, `brand`, `is_active`, `supplier_id` |
| `employees` | `department`, `level`, `country`, `employment_type` |
| `stores` | `type`, `region`, `country` |
| `warehouses` | `type`, `country` |
| `suppliers` | `category`, `country`, `payment_terms` |
| `orders` | `status`, `customer_id`, `store_id`, `channel`, `created_after`/`created_before` |
| `order_items` | `order_id`, `product_id` |
| `payments` | `order_id`, `method`, `status`, `gateway` |
| `invoices` | `status`, `customer_id`, `order_id`, `issued_after`/`issued_before` |
| `tickets` | `status`, `priority`, `category`, `customer_id`, `agent_id` |
| `campaigns` | `type`, `channel`, `status`, `target_segment` |
| `leads` | `status`, `source`, `campaign_id`, `owner_employee_id` |
| `returns` | `status`, `reason`, `customer_id`, `order_id` |
| `shipments` | `status`, `carrier`, `warehouse_id`, `order_id` |
| `purchase_orders` | `status`, `supplier_id`, `warehouse_id` |

`*_after` is inclusive (`>=`), `*_before` is exclusive (`<`).

### With the Query API (docker-compose)
```bash
docker compose up -d postgres
export RCD_POSTGRES_URL="postgresql+psycopg2://rcd:rcd@localhost:5432/rcd_corp"
rcd-data generate --profile demo --sink postgres
docker compose --profile run up -d api   # or just `rcd-data serve` locally
```

## Volume Profiles
| Profile | Customers | Orders | Employees | Date Range | Approx Total Rows |
|---------|-----------|--------|-----------|------------|-------------------|
| `demo` | 1,000 | 10,000 | 200 | 360 days | ~200k |
| `standard` | 50,000 | 1,000,000 | 4,200 | 360 days | ~15M |
| `loadtest` | 500,000 | 20,000,000 | 4,200 | 1,461 days | ~200M+ |

> **Note:** `machine_telemetry` and `api_requests` / `app_logs` are written as date-partitioned Parquet only on the `loadtest` profile regardless of `--sink`, to prevent OOM.

## Output Structure
```
output/
├── csv/
│   ├── customers.csv
│   ├── orders.csv          ← stream appends rows, header preserved
│   └── ...
├── jsonl/
│   └── orders.jsonl        ← stream appends lines
├── xlsx/
│   └── orders.xlsx         ← stream rewrites the whole file each tick (see caveat below)
└── parquet/
    ├── customers/
    │   └── data.parquet
    ├── orders/
    │   ├── data.parquet           ← original from generate
    │   └── stream_<ts>.parquet    ← appended by stream (one file per tick)
    ├── machine_telemetry/
    │   └── date=2024-01-01/       ← date-partitioned; stream adds new partitions
    │       └── ...
    └── ...
```

Parquet consumers (`pd.read_parquet(dir)`, DuckDB, Spark) automatically read all files in a table directory, so historical and streamed rows are always combined.

If `--sink` includes `postgres`/`sqlserver`, `stream` appends directly into those tables too (plain `INSERT`, no dedup/upsert) — same growth model as the files, just rows instead of new files. There's no retention/pruning built in for any sink yet, so long-running `stream` deployments grow disk (and DB size) without bound; see [DEPLOY.md](docs/DEPLOY.md)'s ongoing-operations section.

**MongoDB:** one collection per table. `generate` drops and refills each collection (`if_exists="replace"` equivalent); `stream` inserts new documents. No dedup/upsert, same as the relational DB sinks.

**Redis:** each table is a Redis Stream (`rcd:{table}`, `XADD`), one JSON-encoded entry per row in a single `data` field — this is the one sink that is *not* full historical storage: streams are capped at `RCD_REDIS_STREAM_MAXLEN` (default 100,000, approximate trim), so `generate`'s replace semantics are delete-then-refill, and long-running `stream` deployments self-trim instead of growing unbounded like the other sinks.

**XLSX caveat:** Excel caps a sheet at 1,048,576 rows — tables that exceed it get silently truncated with a warning, so XLSX is realistically only usable at the `demo` profile. Under `stream`, the XLSX sink also does a full read-modify-rewrite of the file on every tick, which gets slower as the file grows — avoid it for long-running streams at anything beyond `demo` scale.

## Schema Reference
### Master Data
| Table | Key Columns | Description |
|-------|-------------|-------------|
| `customers` | `id`, `segment`, `country`, `ltv_tier`, `cpf_or_cnpj` | 75% B2C, 20% B2B, 5% VIP; Pareto LTV |
| `products` | `sku`, `brand`, `category`, `price`, `currency` | 9 RCD branded + up to 200 third-party SKUs |
| `employees` | `id`, `department`, `level`, `salary`, `manager_id` | 7-level IC hierarchy + 5-level management |
| `suppliers` | `id`, `country`, `rating`, `lead_time_days` | |
| `stores` | `id`, `type`, `country`, `city` | flagship/standard/outlet/pop_up/online/warehouse |
| `warehouses` | `id`, `type`, `location`, `capacity_m3` | central/regional/dark_store |
| `fx_rates` | `date`, `from_currency`, `to_currency`, `rate` | BRL/MXN/EUR/USD — daily random walk |

### Sales & E-commerce
| Table | Key Columns | Description |
|-------|-------------|-------------|
| `orders` | `id`, `customer_id`, `store_id`, `status`, `total`, `currency` | State machine: pending→paid→shipped→delivered |
| `order_items` | `order_id`, `product_id`, `quantity`, `line_total` | FK: orders + products |
| `payments` | `order_id`, `method`, `gateway`, `processing_fee` | pix/credit_card/boleto/rcd_card/bnpl |
| `web_sessions` | `session_id`, `customer_id`, `device`, `utm_source` | 30% anonymous sessions |
| `shopping_cart_events` | `session_id`, `product_id`, `event_type` | view/add/remove/checkout/purchase |

### Finance
| Table | Key Columns |
|-------|-------------|
| `invoices` | `order_id`, `customer_id`, `amount`, `status` |
| `transactions` | `type`, `amount`, `currency`, `status` |
| `expenses` | `employee_id`, `category`, `amount`, `department` |
| `budgets` | `department`, `year`, `quarter`, `planned_amount`, `actual_amount` |
| `rcd_card_transactions` | `card_id`, `customer_id`, `merchant_category`, `amount` |

### Marketing
| Table | Key Columns |
|-------|-------------|
| `campaigns` | `id`, `type`, `channel`, `budget`, `status` |
| `campaign_events` | `campaign_id`, `customer_id`, `event_type` |
| `email_events` | `campaign_id`, `customer_id`, `event_type` (sent/opened/clicked/bounced) |
| `leads` | `customer_id`, `status` (state machine: new→won/lost), `score` |
| `ab_test_exposures` | `test_name`, `variant`, `customer_id`, `converted` |

### Social Media
| Table | Key Columns |
|-------|-------------|
| `social_accounts` | `handle`, `platform`, `follower_count` |
| `social_posts` | `account_id`, `platform`, `post_type`, `campaign_id` |
| `social_metrics` | `post_id`, `snapshot_ts`, `impressions`, `reach`, `likes` — hourly 72h then daily |
| `social_comments` | `post_id`, `customer_id`, `sentiment`, `language` |
| `social_mentions` | `platform`, `sentiment`, `topic` |
| `social_dms` | `account_id`, `customer_id`, `intent` |
| `influencer_partnerships` | `handle`, `tier`, `contract_value`, `campaign_id` |
| `influencer_posts` | `influencer_id`, `impressions`, `conversions`, `attributed_revenue` |
| `community_forum_posts` | `customer_id`, `category`, `upvotes`, `reply_count` |
| `reviews` | `source`, `rating`, `response_body`, `response_employee_id` |
| `social_ad_spend` | `date`, `platform`, `campaign_id`, `spend`, `conversions` |

### Supply Chain
| Table | Key Columns |
|-------|-------------|
| `shipments` | `order_id`, `warehouse_id`, `carrier`, `tracking_number`, `status` |
| `inventory_snapshots` | `product_sku`, `warehouse_id`, `date`, `quantity_on_hand` |
| `purchase_orders` | `supplier_id`, `warehouse_id`, `status`, `total_amount` |
| `stock_movements` | `product_sku`, `warehouse_id`, `movement_type`, `quantity` |
| `returns` | `order_id`, `customer_id`, `product_sku`, `reason`, `refund_amount` |

### Manufacturing
| Table | Key Columns |
|-------|-------------|
| `production_runs` | `product_sku`, `factory_location`, `status`, `planned_qty`, `actual_qty` |
| `machine_telemetry` | `machine_id`, `factory`, `timestamp`, `temperature_c`, `alert_triggered` — **partitioned Parquet** |
| `quality_checks` | `production_run_id`, `pass_rate`, `defects_found`, `result` |
| `maintenance_events` | `machine_id`, `factory`, `type`, `downtime_h`, `cost` |

### HR
| Table | Key Columns |
|-------|-------------|
| `attendance` | `employee_id`, `date`, `status`, `hours_worked`, `overtime_h` |
| `performance_reviews` | `employee_id`, `reviewer_id`, `period`, `score`, `rating` |
| `training_records` | `employee_id`, `course_name`, `provider`, `score`, `passed` |
| `recruitment_pipeline` | `position`, `department`, `status` (state machine: applied→hired/rejected) |
| `engagement_surveys` | `employee_id`, `period`, `engagement_score`, `nps` |

### Support
| Table | Key Columns |
|-------|-------------|
| `tickets` | `customer_id`, `category`, `status`, `priority`, `sentiment`, `csat_score` |
| `ticket_messages` | `ticket_id`, `sender_type`, `body` |
| `call_center_calls` | `customer_id`, `agent_id`, `duration_s`, `sentiment`, `resolution` |

### Observability
| Table | Key Columns |
|-------|-------------|
| `app_logs` | `service`, `level`, `message`, `trace_id` — **partitioned Parquet** |
| `api_requests` | `method`, `path`, `status_code`, `duration_ms` — **partitioned Parquet** |
| `errors` | `service`, `error_type`, `severity`, `resolved` |
| `deployments` | `service`, `version`, `environment`, `status` |
| `security_events` | `event_type`, `severity`, `source_ip`, `resolved` |

## Dashboard → Dataset Mapping
| Dashboard | Primary Tables |
|-----------|---------------|
| Sales / Revenue KPI | `orders`, `order_items`, `payments`, `fx_rates` |
| Funnel | `web_sessions`, `shopping_cart_events`, `orders` |
| Cohort Retention | `customers`, `orders` (signup_date + order dates) |
| Marketing Attribution & CAC/LTV | `campaigns`, `campaign_events`, `leads`, `ab_test_exposures`, `customers` |
| Social Media Performance | `social_posts`, `social_metrics`, `social_ad_spend`, `influencer_posts` |
| Brand Health & Sentiment | `social_comments`, `social_mentions`, `reviews`, `tickets` |
| Inventory Turnover | `inventory_snapshots`, `stock_movements`, `purchase_orders` |
| OEE (Overall Equipment Effectiveness) | `production_runs`, `machine_telemetry`, `maintenance_events`, `quality_checks` |
| Support CSAT & SLA | `tickets`, `ticket_messages`, `call_center_calls` |
| HR Headcount / Attrition | `employees`, `attendance`, `performance_reviews`, `recruitment_pipeline` |
| Financial P&L | `invoices`, `transactions`, `expenses`, `budgets`, `rcd_card_transactions` |
| Executive 360 | All of the above |

## Behavioral Rules Implemented
| Rule | Implementation |
|------|---------------|
| Black Friday 10x | `utils/time_utils.py:black_friday_multiplier()` |
| Brazilian payday 2x (5th/20th) | `utils/time_utils.py:payday_multiplier()` |
| December holiday surge 2x | Built into seasonal curve |
| Social engagement peaks 19–22h BRT | `utils/time_utils.py:timestamp_social_peak()` |
| Reels/Shorts 3–5x reach | `generators/social_media.py:_build_metrics()` |
| Crisis sentiment shift (55%→20% positive) | `generators/social_media.py:_build_comments()` using `SENTIMENT_WEIGHTS_CRISIS` |
| Crisis ticket spike (5x) | `generators/support.py:_build_tickets()` |
| Customer LTV Pareto (top 20% = 80%) | `utils/distributions.py:pareto_ltv()` |

## Reproducibility
Same seed → identical output:
```bash
rcd-data generate --profile demo --seed 42 --sink csv
rcd-data generate --profile demo --seed 42 --sink csv  # byte-identical
```

All RNG sources are seeded at startup: `random.seed(N)`, `np.random.seed(N)`, `Faker.seed(N)`.

## Tuning Guide
Edit `rcd_data/config.yaml` to tune output volumes without code changes.
| Knob | Where | Effect |
|------|-------|--------|
| `profiles.*.n_orders` | config.yaml | Controls orders + all downstream fact table row counts |
| `profiles.*.n_customers` | config.yaml | Master data size; affects FK cardinality |
| `profiles.*.date_range_days` | config.yaml | Temporal spread; affects seasonal patterns |
| `profiles.*.crisis_freq_per_month` | config.yaml | Number of crisis events per month |
| `profiles.*.chunk_size` | config.yaml | Rows per chunk for high-volume generators (tune for RAM) |
| `profiles.*.start_date` | config.yaml | Shifts entire date range |
| `crisis.ticket_multiplier` | config.yaml | Ticket volume on crisis days |
| `crisis.social_negative_multiplier` | config.yaml | Negative comment spike on crisis days |
| `crisis.sentiment_shift.*` | config.yaml | Positive/negative % baseline and crisis values |

### Custom profile example
```yaml
profiles:
  my_profile:
    n_customers: 10000
    n_products: 250
    n_employees: 500
    n_orders: 200000
    n_stores: 50
    n_warehouses: 5
    n_suppliers: 30
    date_range_days: 60
    crisis_freq_per_month: 1
    chunk_size: 20000
    start_date: "2024-06-01"
```

```bash
rcd-data generate --profile my_profile --sink parquet
```

## Project Structure
```
rcd_data/
├── config.yaml              # Volume profiles and company config
├── main.py                  # CLI entry point (typer) — generate, stream, validate, info
├── generators/
│   ├── base.py              # BaseGenerator (+ generate_batch), MasterCache, ProfileConfig, SinkDispatcher (+ append_all)
│   ├── master_data.py       # customers, products, employees, suppliers, stores, warehouses, fx_rates
│   ├── sales.py             # orders, order_items, payments, web_sessions, cart_events
│   ├── finance.py           # invoices, transactions, expenses, budgets, rcd_card
│   ├── marketing.py         # campaigns, events, email, leads, ab_tests
│   ├── social_media.py      # 11 social tables
│   ├── supply_chain.py      # shipments, inventory, POs, movements, returns
│   ├── manufacturing.py     # production_runs, machine_telemetry (chunked), quality, maintenance
│   ├── hr.py                # attendance, reviews, training, recruitment, surveys
│   ├── support.py           # tickets, messages, calls
│   └── observability.py     # logs (chunked), requests (chunked), errors, deployments, security
├── utils/
│   ├── distributions.py     # pareto_ltv, weighted_choice, normal_clipped
│   ├── state_machines.py    # 6 lifecycle state machines
│   ├── time_utils.py        # seasonal multipliers, crisis days, business hours
│   ├── identifiers.py       # CPF/CNPJ check digits, SKU, tracking numbers
│   └── fx.py                # daily FX rate random walk
├── sinks/
│   ├── csv_sink.py          # supports append mode (stream command)
│   ├── parquet_sink.py      # date-partitioned via PyArrow; stream_{ts}.parquet for append
│   ├── jsonl_sink.py        # supports append mode (stream command)
│   ├── xlsx_sink.py         # capped at Excel's 1,048,576 rows/sheet; demo profile only
│   ├── postgres_sink.py     # SQLAlchemy + psycopg2; append=True → INSERT, else replace
│   ├── sqlserver_sink.py    # SQLAlchemy + pyodbc (ODBC Driver 18); append=True → INSERT, else replace
│   ├── mongodb_sink.py      # pymongo; one collection per table; append=True → insert, else drop+insert
│   └── redis_sink.py        # redis-py; one capped Stream (XADD) per table; append=True → add, else delete+refill
├── api/                     # Query API — REST + GraphQL over the postgres sink
│   ├── db.py                # engine/session factory (RCD_POSTGRES_URL, same convention as postgres_sink.py)
│   ├── auth.py               # X-API-Key dependency shared by both REST and GraphQL
│   ├── models.py              # SQLAlchemy models, hand-maintained for the 16 curated tables
│   ├── schemas.py              # Pydantic response schemas (REST only) + generic Page[T]
│   ├── queries/                 # list_/get_ per table — the ONE place filter/pagination logic lives
│   ├── rest/                     # thin FastAPI routers calling queries/*.py
│   ├── graphql/                   # hand-written Strawberry types/resolvers calling the same queries/*.py
│   └── app.py                      # FastAPI app: mounts REST + GraphQL behind auth, plus /health
└── tests/
    ├── test_referential_integrity.py
    ├── test_output_completeness.py   # fails (not skips) on a missing/empty expected table
    ├── test_api.py                    # REST/GraphQL parity, auth, pagination, N+1 guard — real Postgres
    └── _sink_helpers.py               # shared DB/Mongo/Redis client helpers for the test modules above
```

Deployment-related files live outside `rcd_data/`: [DEPLOY.md](docs/DEPLOY.md) (home-lab/Tailscale runbook), `docker-compose.yml` (`generator` + `streamer` + `api` services, gated behind `--profile run`), `.env.example`, and `.github/workflows/validate-datasets.yml` (CI: generates + validates against every sink, including real Postgres/SQL Server/MongoDB/Redis service containers).
