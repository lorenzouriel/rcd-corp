## PROMPT

You are a senior data engineer specializing in synthetic data generation with **Python 3.11+** using **Faker**, **Mimesis**, **NumPy**, and **Pandas**. Your task is to build a complete, runnable Python project that generates realistic, interconnected operational data for a fictional mid-to-large enterprise called **"Aurora Corp"**, and writes it to **CSV, Parquet, and optionally Postgres**.

### COMPANY PROFILE (use these concrete details everywhere)

**Aurora Corp** — founded 2008, headquartered in São Paulo (BR), with regional offices in Mexico City, Lisbon, and Miami. ~4,200 employees, ~$1.2B annual revenue, publicly traded (ticker: `AURC`).

**Business lines:**
- **Aurora Retail** — 180 physical stores across LATAM + Iberia (consumer electronics, appliances, smart home).
- **Aurora Online** — e-commerce in 6 countries + marketplaces (MercadoLivre, Amazon, Shopee).
- **Aurora Labs** — 3 factories (São Paulo, Manaus, Monterrey) manufacturing IoT devices under sub-brands **NovaHome**, **PulseAudio**, **GuardianIQ**.
- **Aurora Financial Services** — Aurora Card (credit), BNPL, store financing.
- **Aurora Cloud** — B2B inventory APIs.

**Sample product catalog (use these exact names):**
- NovaHome SmartPlug Mini (BRL 89), NovaHome Thermostat Pro (BRL 799), NovaHome Hub X2 (BRL 1,299)
- PulseAudio Earbuds Lite (BRL 249), PulseAudio Soundbar 5.1 (BRL 2,499), PulseAudio Studio Headphones (BRL 1,899)
- GuardianIQ Cam 360 (BRL 599), GuardianIQ Doorbell Pro (BRL 899), GuardianIQ Alarm Kit (BRL 1,499)
- ~200 third-party SKUs (Samsung, LG, Sony, Philips, Electrolux, Dell).

**Departments:** Engineering, Product, Data & Analytics, Marketing, Sales, Customer Success, Supply Chain, Manufacturing, Finance, HR, Legal, IT, Security.

**Sample stores:** Aurora Paulista (SP flagship), Aurora Ipanema (RJ), Aurora Reforma (CDMX), Aurora Chiado (Lisbon), Aurora Brickell (Miami).

### TECHNICAL REQUIREMENTS

1. **Stack:** Python 3.11+, `faker` (with `pt_BR`, `es_MX`, `pt_PT`, `en_US` locales), `mimesis`, `numpy`, `pandas`, `pyarrow`, `sqlalchemy`, `psycopg2-binary`, `tqdm`, `pydantic` for schemas.
2. **Reproducibility:** every script accepts `--seed` (default 42) and seeds Faker, random, and NumPy.
3. **Modular project structure:**
   ```
   aurora_data/
   ├── config.yaml              # volumes, date ranges, locales, output paths
   ├── main.py                  # orchestrator CLI (argparse/typer)
   ├── generators/
   │   ├── __init__.py
   │   ├── base.py              # BaseGenerator abstract class
   │   ├── master_data.py       # customers, products, employees, suppliers, stores, warehouses
   │   ├── sales.py             # orders, order_items, payments, cart_events, sessions
   │   ├── finance.py
   │   ├── marketing.py
   │   ├── social_media.py
   │   ├── supply_chain.py
   │   ├── manufacturing.py
   │   ├── hr.py
   │   ├── support.py
   │   └── observability.py
   ├── utils/
   │   ├── distributions.py     # pareto_ltv, weighted_choice, normal_clipped, seasonal_curve
   │   ├── state_machines.py    # OrderLifecycle, TicketLifecycle, LeadPipeline, etc.
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
4. **Referential integrity:** master data is generated first and cached in memory (or on disk as parquet); fact generators sample FK values from those caches. A final `tests/` step validates no orphan FKs.
5. **Temporal realism:**
   - Business hours per country/timezone.
   - Weekend dips for B2B, weekend peaks for retail.
   - Black Friday/Cyber Monday 10x multiplier, December holiday surge.
   - Brazilian payday (5th/20th) 2x multiplier.
   - Social engagement peaks 19h–22h BRT.
6. **Statistical realism:**
   - Pareto distribution for customer LTV (top 20% = 80% revenue).
   - Normal-clipped distributions for order values, salaries, session durations.
   - Weighted categorical choices for segments, channels, statuses.
   - Sentiment drift during crisis windows (baseline 55/30/15 pos/neu/neg → 20/25/55).
7. **State machines** implemented as Python classes with transition probabilities:
   - `OrderLifecycle`: pending→paid→picked→shipped→delivered | cancelled | returned | refunded
   - `TicketLifecycle`: open→in_progress→waiting_customer→resolved→closed | escalated
   - `LeadPipeline`: new→contacted→qualified→proposal→won | lost | nurturing
   - `ShipmentTracking`, `RecruitmentFunnel`, `ProductionRunStatus`
8. **Multi-currency:** BR→BRL, MX→MXN, PT→EUR, US→USD; include `fx_rates` generator (random walk from a base rate).
9. **Crisis simulation mode:** one day/month, a product defect triggers 5x tickets, 3x negative social comments, CSAT drop, response campaign spawned.
10. **Output sinks** selectable via CLI: `--sink csv|parquet|postgres|all`. Parquet preferred for large tables (partitioned by date where appropriate).
11. **CLI examples:**
    ```bash
    python main.py generate --profile demo        # ~10k rows per fact table
    python main.py generate --profile standard    # ~1M orders, 90 days
    python main.py generate --profile loadtest    # ~50M rows, 2 years
    python main.py generate --only social_media,support --sink parquet
    python main.py validate                       # FK integrity checks
    ```

### CATEGORIES AND DATASETS

**1. Core Master Data**
- `customers` — id (uuid4), name, email, phone, cpf_or_cnpj (valid check digits), segment (B2C 75%/B2B 20%/VIP 5%), country, state, city, signup_date, ltv_tier (Pareto), preferred_channel, loyalty_points
- `products` — sku, name, category, subcategory, brand, cost, price, currency, margin, supplier_id, weight_kg, launch_date, is_active
- `employees` — id, name, email, department, role, manager_id (tree structure, ~7 levels), hire_date, salary (normal by level), location, employment_type, level (IC1–IC7, M1–M5)
- `suppliers` — id, name, country, rating (1–5), lead_time_days, payment_terms, category
- `stores` — id, name, region, country, city, type (flagship/standard/outlet/pop_up/online/warehouse), opening_date, size_sqm, manager_employee_id
- `warehouses` — id, name, location, capacity_m3, manager_id, type (central/regional/dark_store)
- `fx_rates` — date, from_currency, to_currency, rate

**2. Sales & E-commerce**
- `orders` — id, customer_id, store_id, channel (web/mobile_app/pos/marketplace/call_center), marketplace, status (state machine), subtotal, shipping, tax, total, currency, promo_code, created_at
- `order_items` — order_id, product_id, quantity, unit_price, discount_pct, line_total
- `shopping_cart_events` — session_id, customer_id, event_type, product_id, timestamp
- `web_sessions` — session_id, customer_id (nullable), device, browser, os, utm_source, utm_medium, utm_campaign, landing_page, pages_viewed, duration_s, bounced
- `payments` — order_id, method (credit_card/debit_card/pix/boleto/paypal/aurora_card/bnpl), installments, status, gateway, processing_fee, authorized_at

**3. Finance & Accounting**
- `invoices`, `transactions`, `expenses`, `budgets`, `aurora_card_transactions` (card_id, customer_id, merchant_category, amount, status, posted_at)

**4. Marketing**
- `campaigns`, `campaign_events`, `email_events`, `leads` (state machine), `ab_test_exposures`

**5. Social Media & Community** *(Instagram, TikTok, YouTube, X, LinkedIn, Facebook + branded forum)*
- `social_accounts` — handles like `@auroracorp`, `@aurora_novahome`, `@pulseaudio_br`, `@guardian_iq`
- `social_posts` — id, account_id, platform, post_type (image/carousel/reel/short/video/story/live/text), caption, hashtags (list), posted_at, campaign_id, product_sku, author_employee_id
- `social_metrics` — post_id, snapshot_ts, impressions, reach, likes, comments, shares, saves, video_views, avg_watch_time_s, link_clicks, profile_visits *(hourly for 72h, then daily)*
- `social_comments` — id, post_id, platform, customer_id, parent_comment_id, body, sentiment, language, posted_at, is_spam, is_moderated
- `social_mentions` — id, platform, mention_type, source_handle, reach, sentiment, url, body, detected_at, topic
- `social_dms` — id, platform, account_id, customer_id, direction, intent, body, created_at, converted_to_ticket_id
- `influencer_partnerships` — id, handle, platform, tier (nano/micro/mid/macro/mega), contract_value, currency, campaign_id, start_date, end_date, status
- `influencer_posts` — id, influencer_id, post_url, platform, posted_at, impressions, engagement, clicks, conversions, attributed_revenue
- `community_forum_posts` — id, customer_id, category, title, body, created_at, upvotes, reply_count, status
- `reviews` — id, source (google/trustpilot/reclame_aqui/appstore/playstore), rating (1–5), title, body, posted_at, response_body, response_employee_id
- `social_ad_spend` — date, platform, campaign_id, ad_set_id, spend, impressions, clicks, conversions, currency

*Behavioral rules to implement in code: engagement peaks 19h–22h BRT; Reels/Shorts get 3–5x reach of static; negative sentiment spikes 24–48h after launches and outages; influencer posts cause 6h website traffic surge.*

**6. Supply Chain & Inventory**
- `shipments` (state machine), `inventory_snapshots`, `purchase_orders`, `stock_movements`, `returns`

**7. Manufacturing / IoT**
- `production_runs` (state machine), `machine_telemetry` *(high volume — write as partitioned parquet)*, `quality_checks`, `maintenance_events`

**8. HR & People Analytics**
- `attendance`, `performance_reviews`, `training_records`, `recruitment_pipeline` (state machine), `engagement_surveys`

**9. Customer Support**
- `tickets` (state machine), `ticket_messages`, `call_center_calls`

**10. IT / Application Observability**
- `app_logs`, `api_requests` *(high volume)*, `errors`, `deployments`, `security_events`

### DESIRED DASHBOARDS (data must support these)
Sales/Revenue KPI, Funnel, Cohort Retention, Marketing Attribution & CAC/LTV, **Social Media Performance**, **Brand Health & Sentiment**, Inventory Turnover, OEE, Support CSAT & SLA, HR Headcount/Attrition, Financial P&L, Executive 360.

### DELIVERABLES

1. **Full Python project code** — every file listed in the structure above, production-quality (type hints, docstrings, logging via `structlog`, `__main__` entry points).
2. **`config.yaml`** with three profiles (`demo`, `standard`, `loadtest`) controlling row counts, date ranges, and crisis frequency.
3. **`requirements.txt`** and **`pyproject.toml`**.
4. **`README.md`** with setup, CLI examples, output schema, and a dashboard → datasets mapping table.
5. **`docker-compose.yml`** that spins up Postgres and runs the generator end-to-end.
6. **One representative `BaseGenerator` implementation** and **one fully-worked example** (`OrdersGenerator`) showing: FK sampling, state machine usage, seasonal throttling, crisis mode, multi-currency, and parquet output — so the pattern is crystal clear and the rest can be filled in consistently.
7. **`tests/test_referential_integrity.py`** validating FK joins across the generated tables.
8. **Tuning guide** at the end of the README explaining which knobs change volume, date range, crisis frequency, and sentiment baseline.

### OUTPUT FORMAT

Start with the project tree, then `config.yaml`, then `requirements.txt`, then utils modules, then `BaseGenerator`, then the fully-worked `OrdersGenerator`, then skeletons for the remaining generators (complete enough to run), then sinks, then `main.py`, then tests, then `README.md`. Use clear section headers for each file.
