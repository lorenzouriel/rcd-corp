"""HR & People Analytics generator for RCD Corp."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import weighted_choice, normal_clipped
from ..utils.identifiers import new_uuid
from ..utils.state_machines import RecruitmentFunnel
from ..utils.time_utils import random_date, random_datetime

ATTENDANCE_STATUSES = ["present", "absent", "late", "remote", "holiday", "sick_leave"]
ATTENDANCE_WEIGHTS = [0.70, 0.03, 0.05, 0.15, 0.04, 0.03]

REVIEW_RATINGS = ["exceeds_expectations", "meets_expectations", "below_expectations", "needs_improvement"]
REVIEW_RATING_WEIGHTS = [0.20, 0.60, 0.15, 0.05]

TRAINING_PROVIDERS = ["Coursera", "Udemy", "LinkedIn Learning", "Internal", "AWS Training", "Google Cloud", "Databricks Academy"]

DEPARTMENTS = [
    "Engineering", "Product", "Data & Analytics", "Marketing", "Sales",
    "Customer Success", "Supply Chain", "Manufacturing", "Finance", "HR", "IT",
]

POSITIONS = [
    "Software Engineer", "Data Engineer", "Product Manager", "Marketing Analyst",
    "Sales Representative", "Customer Success Manager", "Supply Chain Analyst",
    "HR Business Partner", "Financial Analyst", "DevOps Engineer", "Security Analyst",
]


class HRGenerator(BaseGenerator):
    """Generates attendance, performance_reviews, training_records, recruitment_pipeline, engagement_surveys."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        return {
            "attendance": self._build_attendance(cache, profile),
            "performance_reviews": self._build_performance_reviews(cache, profile),
            "training_records": self._build_training_records(cache, profile),
            "recruitment_pipeline": self._build_recruitment(cache, profile),
            "engagement_surveys": self._build_engagement_surveys(cache, profile),
        }

    def _build_attendance(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        from ..utils.time_utils import date_range_list
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        # Sample at most 500 employees for attendance to keep volume manageable
        n_emp = min(len(employee_ids), 500)
        sampled_employees = self.rng.choice(employee_ids, size=n_emp, replace=False)
        dates = date_range_list(profile.start, profile.date_range_days)
        weekdays = [d for d in dates if d.weekday() < 5]  # Mon–Fri only
        rows = []
        for emp_id in sampled_employees:
            for d in weekdays:
                status = str(weighted_choice(ATTENDANCE_STATUSES, ATTENDANCE_WEIGHTS, 1, self.rng)[0])
                hours = 0.0
                if status in ("present", "remote"):
                    hours = float(normal_clipped(8.0, 1.0, 4.0, 12.0, 1, self.rng)[0])
                elif status == "late":
                    hours = float(normal_clipped(6.0, 1.0, 3.0, 8.0, 1, self.rng)[0])
                overtime = max(0.0, hours - 8.0)
                rows.append({
                    "id": new_uuid(),
                    "employee_id": str(emp_id),
                    "date": d,
                    "check_in": f"0{int(8 + self.rng.integers(-1, 2))}:00:00" if status in ("present", "remote") else None,
                    "check_out": f"{int(17 + self.rng.integers(-1, 3))}:00:00" if status in ("present", "remote") else None,
                    "status": status,
                    "hours_worked": round(hours, 2),
                    "overtime_h": round(overtime, 2),
                })
        return pd.DataFrame(rows)

    def _build_performance_reviews(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        n = min(len(employee_ids), 1000)
        reviewee_ids = self.rng.choice(employee_ids, size=n, replace=False)
        reviewer_ids = self.rng.choice(employee_ids, size=n, replace=True)
        start_year = profile.start.year

        rows = []
        for i in range(n):
            period = f"{start_year}-H{int(self.rng.integers(1, 3))}"
            score = float(normal_clipped(3.5, 0.8, 1.0, 5.0, 1, self.rng)[0])
            rows.append({
                "id": new_uuid(),
                "employee_id": str(reviewee_ids[i]),
                "reviewer_id": str(reviewer_ids[i]),
                "period": period,
                "score": round(score, 2),
                "rating": str(weighted_choice(REVIEW_RATINGS, REVIEW_RATING_WEIGHTS, 1, self.rng)[0]),
                "comments": f"Performance review for period {period}.",
                "reviewed_at": random_date(profile.start, profile.end, self.rng),
            })
        return pd.DataFrame(rows)

    def _build_training_records(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        n = min(len(employee_ids), 1000)
        sampled = self.rng.choice(employee_ids, size=n, replace=True)
        started = [random_date(profile.start, profile.end - timedelta(days=14), self.rng) for _ in range(n)]
        completed = [
            s + timedelta(days=int(self.rng.integers(1, 30)))
            if self.rng.random() < 0.85 else None
            for s in started
        ]
        scores = [
            float(self.rng.uniform(60, 100)) if c is not None else None
            for c in completed
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "employee_id": sampled,
            "course_name": [f"Course {int(self.rng.integers(1, 50)):02d}: {str(self.rng.choice(['Python', 'SQL', 'Cloud', 'Data Engineering', 'Leadership', 'Security']))}" for _ in range(n)],
            "provider": weighted_choice(TRAINING_PROVIDERS, [0.20, 0.20, 0.15, 0.25, 0.08, 0.07, 0.05], n, self.rng),
            "started_at": started,
            "completed_at": completed,
            "score": [round(s, 1) if s is not None else None for s in scores],
            "passed": [s is not None and s >= 70 for s in scores],
            "credits": self.rng.integers(1, 10, size=n),
        })

    def _build_recruitment(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        scale = max(1, len(cache.employee_ids) // 1000)
        n = max(50, scale * 50)
        sm = RecruitmentFunnel()
        recruiter_ids = cache.sample_employee_ids(n, self.rng)
        statuses = [sm.run("applied", self.rng) for _ in range(n)]
        recruitment_end = max(profile.start, profile.end - timedelta(days=30))
        applied = [random_datetime(profile.start, recruitment_end, self.rng) for _ in range(n)]
        hired_ats = [
            app + timedelta(days=int(self.rng.integers(14, 90)))
            if st == "hired" else None
            for app, st in zip(applied, statuses)
        ]

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "position": self.rng.choice(POSITIONS, size=n),
            "department": weighted_choice(DEPARTMENTS, [1 / len(DEPARTMENTS)] * len(DEPARTMENTS), n, self.rng),
            "candidate_name": [f"Candidate {i + 1:04d}" for i in range(n)],
            "candidate_email": [f"candidate{i}@email.com" for i in range(n)],
            "status": statuses,
            "applied_at": applied,
            "hired_at": hired_ats,
            "recruiter_id": recruiter_ids,
            "source": weighted_choice(
                ["linkedin", "referral", "job_board", "direct", "agency"],
                [0.35, 0.25, 0.20, 0.10, 0.10], n, self.rng
            ),
        })

    def _build_engagement_surveys(self, cache: MasterCache, profile: ProfileConfig) -> pd.DataFrame:
        employee_ids = cache.employee_ids if len(cache.employee_ids) > 0 else [new_uuid()]
        n = min(len(employee_ids), 2000)
        sampled = self.rng.choice(employee_ids, size=n, replace=True)
        start_year = profile.start.year

        return pd.DataFrame({
            "id": [new_uuid() for _ in range(n)],
            "employee_id": sampled,
            "period": [f"{start_year}-Q{int(self.rng.integers(1, 5))}" for _ in range(n)],
            "engagement_score": normal_clipped(3.8, 0.8, 1.0, 5.0, n, self.rng).round(2),
            "satisfaction_score": normal_clipped(3.6, 0.9, 1.0, 5.0, n, self.rng).round(2),
            "nps": self.rng.integers(-100, 101, size=n),
            "would_recommend": self.rng.random(n) < 0.72,
            "submitted_at": [random_date(profile.start, profile.end, self.rng) for _ in range(n)],
        })
