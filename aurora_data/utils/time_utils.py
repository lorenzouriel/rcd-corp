"""Temporal realism utilities for Aurora Corp data generation."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

import numpy as np

BUSINESS_HOURS: dict[str, tuple[int, int]] = {
    "BR": (9, 18),
    "MX": (9, 18),
    "PT": (9, 18),
    "US": (9, 18),
}

SOCIAL_PEAK_START = 19
SOCIAL_PEAK_END = 22

COUNTRY_CURRENCY = {
    "BR": "BRL",
    "MX": "MXN",
    "PT": "EUR",
    "US": "USD",
}


def _last_friday_of_november(year: int) -> date:
    d = date(year, 11, 30)
    while d.weekday() != 4:
        d -= timedelta(days=1)
    return d


def black_friday_multiplier(d: date) -> float:
    """Return order volume multiplier: 10x on BF/CM, 3x in BF week, 2x in December."""
    bf = _last_friday_of_november(d.year)
    cm = bf + timedelta(days=3)
    if d == bf or d == cm:
        return 10.0
    week_start = bf - timedelta(days=7)
    if week_start <= d <= bf:
        return 3.0
    if d.month == 12:
        return 2.0
    return 1.0


def payday_multiplier(d: date) -> float:
    """Brazilian payday effect: 5th and 20th of month → 2x orders."""
    if d.day in (5, 20):
        return 2.0
    return 1.0


def generate_crisis_days(
    start: date,
    end: date,
    freq_per_month: int,
    rng: np.random.Generator,
) -> list[date]:
    """Generate crisis days spread across the date range at freq_per_month rate."""
    crisis_days: list[date] = []
    current = start
    while current <= end:
        if current.month == 12:
            month_end = date(current.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(current.year, current.month + 1, 1) - timedelta(days=1)
        month_end = min(month_end, end)
        days_in_month = (month_end - current).days + 1
        for _ in range(min(freq_per_month, days_in_month)):
            offset = int(rng.integers(0, days_in_month))
            candidate = current + timedelta(days=offset)
            if candidate not in crisis_days:
                crisis_days.append(candidate)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return crisis_days


def timestamp_in_business_hours(
    d: date,
    country: str,
    rng: np.random.Generator,
) -> datetime:
    """Return a datetime within business hours for the given country."""
    h_start, h_end = BUSINESS_HOURS.get(country, (9, 18))
    hour = int(rng.integers(h_start, h_end))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime.combine(d, time(hour, minute, second))


def timestamp_social_peak(d: date, rng: np.random.Generator) -> datetime:
    """Return a datetime during social media peak hours (19–22 BRT)."""
    hour = int(rng.integers(SOCIAL_PEAK_START, SOCIAL_PEAK_END))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime.combine(d, time(hour, minute, second))


def random_datetime(start: date, end: date, rng: np.random.Generator) -> datetime:
    """Return a random datetime uniformly distributed in [start, end]."""
    delta_days = (end - start).days
    d = start + timedelta(days=int(rng.integers(0, delta_days + 1)))
    hour = int(rng.integers(0, 24))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime.combine(d, time(hour, minute, second))


def random_date(start: date, end: date, rng: np.random.Generator) -> date:
    delta = (end - start).days
    return start + timedelta(days=int(rng.integers(0, delta + 1)))


def date_range_list(start: date, n_days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(n_days)]
