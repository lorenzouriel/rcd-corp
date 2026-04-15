"""IT & Application Observability generator for Aurora Corp — includes chunked high-volume tables."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterator

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid
from ..utils.time_utils import random_date, random_datetime

SERVICES = [
    "api-gateway", "order-service", "payment-service", "product-service",
    "customer-service", "inventory-service", "notification-service",
    "auth-service", "search-service", "analytics-service",
]

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOG_LEVEL_WEIGHTS = [0.10, 0.60, 0.18, 0.10, 0.02]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
METHOD_WEIGHTS = [0.60, 0.25, 0.08, 0.04, 0.03]

STATUS_CODES = [200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 422, 500, 502, 503]
STATUS_WEIGHTS = [0.40, 0.15, 0.10, 0.05, 0.10, 0.05, 0.04, 0.02, 0.01, 0.03, 0.02, 0.015, 0.005, 0.005]

ERROR_TYPES = [
    "NullPointerException", "TimeoutError", "DatabaseConnectionError",
    "ValidationError", "AuthenticationError", "PaymentGatewayError",
    "InventoryError", "RateLimitExceeded",
]

ENVIRONMENTS = ["production", "staging", "development"]
ENV_WEIGHTS = [0.70, 0.20, 0.10]

SECURITY_EVENT_TYPES = [
    "failed_login", "suspicious_ip", "rate_limit_exceeded",
    "privilege_escalation_attempt", "data_export_anomaly", "api_key_rotation",
]

API_PATHS = [
    "/api/v1/orders", "/api/v1/products", "/api/v1/customers",
    "/api/v1/payments", "/api/v1/inventory", "/api/v1/auth/token",
    "/api/v2/orders", "/api/v2/search", "/health", "/metrics",
]


class ObservabilityGenerator(BaseGenerator):
    """Generates errors, deployments, security_events, and chunked app_logs + api_requests."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)
        return {
            "errors": self._build_errors(profile, scale),
            "deployments": self._build_deployments(cache, profile, scale),
            "security_events": self._build_security_events(cache, profile, scale),
        }

    def generate_chunked(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
        chunk_size: int,
    ) -> Iterator[dict[str, pd.DataFrame]]:
        """Yield standard tables first, then stream app_logs and api_requests in chunks."""
        tables = self.generate(cache, profile, crisis_days)
        yield tables

        yield from self._stream_app_logs(profile, chunk_size)
        yield from self._stream_api_requests(profile, chunk_size)

    def _stream_app_logs(
        self, profile: ProfileConfig, chunk_size: int
    ) -> Iterator[dict[str, pd.DataFrame]]:
        from ..utils.time_utils import date_range_list
        dates = date_range_list(profile.start, profile.date_range_days)
        records = []
        logs_per_service_per_day = 500

        for d in dates:
            for service in SERVICES:
                for _ in range(logs_per_service_per_day):
                    level = str(weighted_choice(LOG_LEVELS, LOG_LEVEL_WEIGHTS, 1, self.rng)[0])
                    ts = datetime.combine(d, __import__("datetime").time(
                        int(self.rng.integers(0, 24)),
                        int(self.rng.integers(0, 60)),
                        int(self.rng.integers(0, 60)),
                    ))
                    records.append({
                        "id": new_uuid(),
                        "timestamp": ts,
                        "date": d,
                        "service": service,
                        "level": level,
                        "message": f"{level}: request processed by {service}",
                        "trace_id": new_uuid(),
                        "span_id": new_uuid()[:8],
                        "user_id": new_uuid() if self.rng.random() < 0.70 else None,
                        "duration_ms": int(self.rng.exponential(50)),
                    })
                    if len(records) >= chunk_size:
                        yield {"app_logs": pd.DataFrame(records)}
                        records = []

        if records:
            yield {"app_logs": pd.DataFrame(records)}

    def _stream_api_requests(
        self, profile: ProfileConfig, chunk_size: int
    ) -> Iterator[dict[str, pd.DataFrame]]:
        from ..utils.time_utils import date_range_list
        dates = date_range_list(profile.start, profile.date_range_days)
        records = []
        requests_per_service_per_day = 1000

        for d in dates:
            for service in SERVICES[:5]:  # Limit to top-5 services for volume control
                for _ in range(requests_per_service_per_day):
                    status_code = int(self.rng.choice(STATUS_CODES, p=[w / sum(STATUS_WEIGHTS) for w in STATUS_WEIGHTS]))
                    ts = datetime.combine(d, __import__("datetime").time(
                        int(self.rng.integers(0, 24)),
                        int(self.rng.integers(0, 60)),
                        int(self.rng.integers(0, 60)),
                    ))
                    records.append({
                        "id": new_uuid(),
                        "timestamp": ts,
                        "date": d,
                        "method": str(weighted_choice(HTTP_METHODS, METHOD_WEIGHTS, 1, self.rng)[0]),
                        "path": str(self.rng.choice(API_PATHS)),
                        "status_code": status_code,
                        "duration_ms": int(self.rng.exponential(80)),
                        "user_id": new_uuid() if self.rng.random() < 0.60 else None,
                        "ip_address": f"{int(self.rng.integers(1, 255))}.{int(self.rng.integers(0, 255))}.{int(self.rng.integers(0, 255))}.{int(self.rng.integers(1, 255))}",
                        "service": service,
                        "request_size_bytes": int(self.rng.integers(64, 65536)),
                        "response_size_bytes": int(self.rng.integers(128, 1048576)),
                    })
                    if len(records) >= chunk_size:
                        yield {"api_requests": pd.DataFrame(records)}
                        records = []

        if records:
            yield {"api_requests": pd.DataFrame(records)}

    def _build_errors(self, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(100, scale * 100)
        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "timestamp": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "service": self.rng.choice(SERVICES, size=n),
            "error_type": self.rng.choice(ERROR_TYPES, size=n),
            "message": [f"Error in service: {str(self.rng.choice(ERROR_TYPES))}" for _ in range(n)],
            "stack_trace": [f"at {str(self.rng.choice(SERVICES))}.method(file.py:42)" for _ in range(n)],
            "trace_id": [new_uuid() for _ in range(n)],
            "severity": weighted_choice(["low", "medium", "high", "critical"], [0.30, 0.40, 0.20, 0.10], n, self.rng),
            "resolved": self.rng.random(n) < 0.85,
            "environment": weighted_choice(ENVIRONMENTS, ENV_WEIGHTS, n, self.rng),
        })

    def _build_deployments(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(50, scale * 20)
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        started = [random_datetime(profile.start, profile.end - timedelta(hours=1), self.rng) for _ in range(n)]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "service": self.rng.choice(SERVICES, size=n),
            "version": [f"v{int(self.rng.integers(1, 5))}.{int(self.rng.integers(0, 20))}.{int(self.rng.integers(0, 100))}" for _ in range(n)],
            "environment": weighted_choice(ENVIRONMENTS, ENV_WEIGHTS, n, self.rng),
            "status": weighted_choice(["success", "failed", "rolled_back"], [0.88, 0.08, 0.04], n, self.rng),
            "deployed_by": self.rng.choice(employee_ids, size=n),
            "started_at": started,
            "completed_at": [s + timedelta(minutes=int(self.rng.integers(2, 30))) for s in started],
            "commit_sha": [new_uuid()[:7] for _ in range(n)],
            "pr_number": self.rng.integers(1000, 9999, size=n),
        })

    def _build_security_events(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(50, scale * 50)
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [None]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "timestamp": [random_datetime(profile.start, profile.end, self.rng) for _ in range(n)],
            "event_type": self.rng.choice(SECURITY_EVENT_TYPES, size=n),
            "severity": weighted_choice(["low", "medium", "high", "critical"], [0.40, 0.35, 0.20, 0.05], n, self.rng),
            "source_ip": [f"{int(self.rng.integers(1, 255))}.{int(self.rng.integers(0, 255))}.{int(self.rng.integers(0, 255))}.{int(self.rng.integers(1, 255))}" for _ in range(n)],
            "user_id": [str(self.rng.choice(employee_ids)) if self.rng.random() < 0.60 else None for _ in range(n)],
            "description": [f"Security event: {str(self.rng.choice(SECURITY_EVENT_TYPES))}" for _ in range(n)],
            "resolved": self.rng.random(n) < 0.90,
            "service": self.rng.choice(SERVICES, size=n),
        })
