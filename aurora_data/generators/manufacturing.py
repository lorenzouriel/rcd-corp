"""Manufacturing & IoT generator for Aurora Corp — includes chunked machine_telemetry."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterator

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid
from ..utils.state_machines import ProductionRunStatus
from ..utils.time_utils import random_date, random_datetime

FACTORIES = ["São Paulo", "Manaus", "Monterrey"]
FACTORY_WEIGHTS = [0.45, 0.35, 0.20]

CHECK_TYPES = ["visual_inspection", "functional_test", "dimensional_check", "stress_test", "final_qa"]
MAINTENANCE_TYPES = ["preventive", "corrective", "emergency", "calibration"]
MACHINE_IDS = [f"MCH-{factory[:3].upper()}-{i:03d}" for factory in FACTORIES for i in range(1, 21)]


class ManufacturingGenerator(BaseGenerator):
    """Generates production_runs, machine_telemetry (chunked), quality_checks, maintenance_events."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        scale = max(1, profile.n_orders // 10_000)
        production_runs = self._build_production_runs(cache, profile, scale)
        quality_checks = self._build_quality_checks(production_runs, cache, profile)
        maintenance = self._build_maintenance_events(cache, profile, scale)

        return {
            "production_runs": production_runs,
            "quality_checks": quality_checks,
            "maintenance_events": maintenance,
        }

    def generate_chunked(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
        chunk_size: int,
    ) -> Iterator[dict[str, pd.DataFrame]]:
        """Yield standard tables once, then stream machine_telemetry in chunks."""
        tables = self.generate(cache, profile, crisis_days)
        yield tables

        yield from self._stream_telemetry(profile, chunk_size)

    def _build_production_runs(self, cache: MasterCache, profile: ProfileConfig, scale: int) -> pd.DataFrame:
        n = max(100, scale * 100)
        sm = ProductionRunStatus()
        skus = cache.product_skus if len(cache.product_skus) > 0 else [new_uuid()]
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        factories = weighted_choice(FACTORIES, FACTORY_WEIGHTS, n, self.rng)
        statuses = [sm.run("scheduled", self.rng) for _ in range(n)]
        started = [random_datetime(profile.start, profile.end - timedelta(days=1), self.rng) for _ in range(n)]
        planned_qty = self.rng.integers(500, 10_000, size=n)
        efficiency = self.rng.uniform(0.75, 1.05, size=n)
        actual_qty = (planned_qty * efficiency).astype(int)

        completed_ats = [
            s + timedelta(hours=int(self.rng.uniform(8, 72)))
            if st == "completed" else None
            for s, st in zip(started, statuses)
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "product_sku": self.rng.choice(skus, size=n),
            "factory_location": factories,
            "planned_qty": planned_qty,
            "actual_qty": actual_qty,
            "status": statuses,
            "started_at": started,
            "completed_at": completed_ats,
            "operator_id": self.rng.choice(employee_ids, size=n),
            "shift": weighted_choice(["morning", "afternoon", "night"], [0.40, 0.35, 0.25], n, self.rng),
        })

    def _stream_telemetry(
        self, profile: ProfileConfig, chunk_size: int
    ) -> Iterator[dict[str, pd.DataFrame]]:
        """Stream machine telemetry row-by-row in chunks to avoid OOM."""
        from ..utils.time_utils import date_range_list
        dates = date_range_list(profile.start, profile.date_range_days)
        records = []
        machines_per_factory = 20
        interval_minutes = 5

        for d in dates:
            for factory in FACTORIES:
                for machine_num in range(1, machines_per_factory + 1):
                    machine_id = f"MCH-{factory[:3].upper()}-{machine_num:03d}"
                    # Generate readings every 5 minutes for 8h shift = 96 readings
                    n_readings = 96
                    for minute_offset in range(0, n_readings * interval_minutes, interval_minutes):
                        ts = datetime.combine(d, __import__("datetime").time(8, 0)) + timedelta(minutes=minute_offset)
                        temp = float(self.rng.normal(75, 5))
                        temp = max(20, min(120, temp))
                        alert = temp > 100 or float(self.rng.random()) < 0.02
                        records.append({
                            "id": new_uuid(),
                            "machine_id": machine_id,
                            "factory": factory,
                            "timestamp": ts,
                            "date": d,
                            "temperature_c": round(temp, 2),
                            "vibration_hz": round(float(self.rng.normal(50, 3)), 2),
                            "power_kw": round(float(self.rng.uniform(10, 150)), 2),
                            "production_rate": round(float(self.rng.normal(100, 10)), 1),
                            "status": "alert" if alert else "normal",
                            "alert_triggered": alert,
                        })
                        if len(records) >= chunk_size:
                            yield {"machine_telemetry": pd.DataFrame(records)}
                            records = []

        if records:
            yield {"machine_telemetry": pd.DataFrame(records)}

    def _build_quality_checks(
        self, production_runs: pd.DataFrame, cache: MasterCache, profile: ProfileConfig
    ) -> pd.DataFrame:
        rows = []
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        for _, run in production_runs.iterrows():
            n_checks = int(self.rng.integers(1, 4))
            for _ in range(n_checks):
                pass_rate = float(self.rng.beta(9, 1))
                rows.append({
                    "id": new_uuid(),
                    "production_run_id": run["id"],
                    "inspector_id": str(self.rng.choice(employee_ids)),
                    "check_type": str(self.rng.choice(CHECK_TYPES)),
                    "pass_rate": round(pass_rate, 4),
                    "defects_found": int((1 - pass_rate) * int(run["actual_qty"])),
                    "sample_size": int(self.rng.integers(10, 100)),
                    "result": "pass" if pass_rate >= 0.95 else "fail",
                    "checked_at": run["started_at"],
                })
        return pd.DataFrame(rows)

    def _build_maintenance_events(
        self, cache: MasterCache, profile: ProfileConfig, scale: int
    ) -> pd.DataFrame:
        n = max(50, scale * 50)
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        machine_ids = MACHINE_IDS
        factories = weighted_choice(FACTORIES, FACTORY_WEIGHTS, n, self.rng)
        started = [random_datetime(profile.start, profile.end - timedelta(hours=8), self.rng) for _ in range(n)]
        downtime_h = normal_clipped(4, 6, 0.5, 72, n, self.rng).round(1)

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "machine_id": self.rng.choice(machine_ids, size=n),
            "factory": factories,
            "type": weighted_choice(MAINTENANCE_TYPES, [0.40, 0.35, 0.15, 0.10], n, self.rng),
            "description": [f"Maintenance event {i}" for i in range(n)],
            "technician_id": self.rng.choice(employee_ids, size=n),
            "started_at": started,
            "completed_at": [s + timedelta(hours=float(h)) for s, h in zip(started, downtime_h)],
            "downtime_h": downtime_h,
            "cost": normal_clipped(500, 400, 50, 20_000, n, self.rng).round(2),
            "currency": "BRL",
        })
