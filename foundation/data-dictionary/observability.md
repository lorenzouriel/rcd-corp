# Data Dictionary — IT & Application Observability

Source: `rcd_data/generators/observability.py` (`ObservabilityGenerator`).
10 synthetic services (`api-gateway`, `order-service`, `payment-service`,
`product-service`, `customer-service`, `inventory-service`,
`notification-service`, `auth-service`, `search-service`,
`analytics-service`). `app_logs` and `api_requests` are generated via
`generate_chunked()` and **only exist under the `loadtest` profile**.
PII/retention detail: [[ropa]] §8; [[data-classification-retention]].

## `errors`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `timestamp` | timestamp | |
| `service` | enum | One of the 10 services |
| `error_type` | enum | NullPointerException, TimeoutError, DatabaseConnectionError, ValidationError, AuthenticationError, PaymentGatewayError, InventoryError, RateLimitExceeded |
| `message`, `stack_trace` | string | Placeholder text |
| `trace_id` | uuid | |
| `severity` | enum | low, medium, high, critical |
| `resolved` | boolean | ~85% |
| `environment` | enum | production (70%), staging (20%), development (10%) |

## `deployments`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `service` | enum | One of the 10 services |
| `version` | string | `v{1-4}.{0-19}.{0-99}` |
| `environment` | enum | production, staging, development |
| `status` | enum | success (88%), failed (8%), rolled_back (4%) |
| `deployed_by` | uuid FK → `employees.id` | |
| `started_at`, `completed_at` | timestamp | `completed_at = started_at + 2–30 min` |
| `commit_sha` | string | 7-char hex-like slice of a UUID |
| `pr_number` | integer | 1000–9999, not linked to a real PR |

## `security_events`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `timestamp` | timestamp | |
| `event_type` | enum | failed_login, suspicious_ip, rate_limit_exceeded, privilege_escalation_attempt, data_export_anomaly, api_key_rotation |
| `severity` | enum | low, medium, high, critical |
| `source_ip` | string | Synthetic IPv4 — **personal data** under LGPD when linkable to an individual |
| `user_id` | uuid FK → `employees.id`, nullable | ~60% populated |
| `description` | string | Placeholder text |
| `resolved` | boolean | ~90% |
| `service` | enum | One of the 10 services |

**Restricted** — see [[data-classification-retention]].

## `app_logs` (loadtest profile only, chunked/partitioned)

500 log lines per service per day. `id`, `timestamp`, `date` (partition
column), `service`, `level` (DEBUG/INFO/WARNING/ERROR/CRITICAL), `message`
(placeholder), `trace_id`, `span_id`, `user_id` (uuid, ~70% populated, not
an FK to any table), `duration_ms`.

## `api_requests` (loadtest profile only, chunked/partitioned)

1,000 requests per service per day, limited to the top 5 services. `id`,
`timestamp`, `date` (partition column), `method`, `path`, `status_code`,
`duration_ms`, `user_id` (uuid, ~60% populated, not an FK), `ip_address`
(synthetic IPv4 — **personal data**), `service`, `request_size_bytes`,
`response_size_bytes`.
