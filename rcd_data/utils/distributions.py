"""Statistical distribution utilities for RCD Corp data generation."""
from __future__ import annotations

import numpy as np


def pareto_ltv(n: int, rng: np.random.Generator, scale: float = 1.0) -> np.ndarray:
    """Pareto LTV distribution — top 20% of customers generate ~80% of revenue.

    Uses Pareto shape alpha ≈ 1.16 which approximates the 80/20 rule.
    Returns values in [0, scale * large_number]; caller should clip/normalize.
    """
    alpha = 1.16
    return rng.pareto(alpha, size=n) * scale + scale


def weighted_choice(
    choices: list,
    weights: list[float],
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Sample n items from choices with given probability weights."""
    w = np.array(weights, dtype=float)
    w /= w.sum()
    return rng.choice(choices, size=n, p=w)


def normal_clipped(
    mean: float,
    std: float,
    low: float,
    high: float,
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Normal distribution clipped to [low, high] — oversample then filter."""
    result = np.empty(0)
    while len(result) < n:
        batch = rng.normal(mean, std, size=max(n * 2, 1000))
        result = np.concatenate([result, batch[(batch >= low) & (batch <= high)]])
    return result[:n]


def ltv_tier(ltv_values: np.ndarray) -> np.ndarray:
    """Classify LTV values into tiers: bronze / silver / gold / platinum."""
    p20 = np.percentile(ltv_values, 20)
    p50 = np.percentile(ltv_values, 50)
    p80 = np.percentile(ltv_values, 80)
    tiers = np.where(
        ltv_values >= p80, "platinum",
        np.where(ltv_values >= p50, "gold",
        np.where(ltv_values >= p20, "silver", "bronze")),
    )
    return tiers


def seasonal_multipliers(dates: list, base: float = 1.0) -> np.ndarray:
    """Return a multiplier array aligned to the given dates list."""
    from datetime import date as date_type
    result = []
    for d in dates:
        m = d.month if isinstance(d, date_type) else d.month
        if m == 12:
            result.append(base * 2.0)
        elif m == 11:
            result.append(base * 1.5)
        elif m in (1, 2):
            result.append(base * 0.7)
        else:
            result.append(base)
    return np.array(result)
