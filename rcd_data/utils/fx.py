"""Foreign exchange rate generator with random walk for RCD Corp."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

CURRENCIES = ["BRL", "MXN", "EUR", "USD"]

BASE_RATES_TO_USD: dict[str, float] = {
    "BRL": 0.20,
    "MXN": 0.058,
    "EUR": 1.08,
    "USD": 1.00,
}


def _rate(from_ccy: str, to_ccy: str) -> float:
    from_usd = BASE_RATES_TO_USD[from_ccy]
    to_usd = BASE_RATES_TO_USD[to_ccy]
    return from_usd / to_usd


def generate_fx_rates(
    start: date,
    end: date,
    rng: np.random.Generator,
    daily_volatility: float = 0.005,
) -> pd.DataFrame:
    """Generate daily FX rates for all CURRENCIES pairs via log-normal random walk."""
    n_days = (end - start).days + 1
    dates = [start + timedelta(days=i) for i in range(n_days)]
    records = []

    for from_ccy in CURRENCIES:
        for to_ccy in CURRENCIES:
            if from_ccy == to_ccy:
                for d in dates:
                    records.append(
                        {"date": d, "from_currency": from_ccy, "to_currency": to_ccy, "rate": 1.0}
                    )
                continue

            base = _rate(from_ccy, to_ccy)
            shocks = rng.normal(0.0, daily_volatility, size=n_days)
            log_rates = np.log(base) + np.cumsum(shocks)
            rates = np.exp(log_rates)

            for d, r in zip(dates, rates):
                records.append(
                    {
                        "date": d,
                        "from_currency": from_ccy,
                        "to_currency": to_ccy,
                        "rate": round(float(r), 6),
                    }
                )

    return pd.DataFrame(records)
