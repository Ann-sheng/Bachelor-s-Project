# ================================================================
# FILE    : src/helpers.py
# PURPOSE : Utility functions shared between both generator scripts
# ================================================================

import numpy as np
import pandas as pd
from datetime import date, timedelta


# ── Date utilities ────────────────────────────────────────────────

def date_range_days(start: str, end: str) -> int:
    """Number of calendar days between two YYYY-MM-DD strings."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return (e - s).days


def generate_dates(n: int, start: str, end: str, rng: np.random.Generator) -> pd.Series:
    """
    Generate n random dates between start and end.
    Applies a mild weight so later dates appear slightly more often
    (simulating growing brand popularity).
    """
    delta = date_range_days(start, end)
    # Linear upward weight: later dates are ~2x more likely than earliest
    weights = np.linspace(1.0, 2.0, delta)
    weights /= weights.sum()

    day_offsets = rng.choice(delta, size=n, p=weights)
    base = pd.Timestamp(start)
    return base + pd.to_timedelta(day_offsets, unit="D")


def add_business_days(dates: pd.Series, min_days: int, max_days: int,
                      rng: np.random.Generator) -> pd.Series:
    """Add random business days to a date Series (for shipped/delivery dates)."""
    offsets = rng.integers(min_days, max_days + 1, size=len(dates))
    return dates + pd.to_timedelta(offsets, unit="D")


# ── ID generators ─────────────────────────────────────────────────

def make_transaction_ids(n: int, prefix: str, start: int = 1) -> list:
    """e.g. prefix='OFF', start=1 → ['OFF0000001', 'OFF0000002', ...]"""
    return [f"{prefix}{str(i).zfill(7)}" for i in range(start, start + n)]


def make_entity_ids(n: int, prefix: str, start: int = 1) -> list:
    """e.g. prefix='CUST', start=1 → ['CUST000001', ...]"""
    return [f"{prefix}{str(i).zfill(6)}" for i in range(start, start + n)]


def make_short_ids(n: int, prefix: str, start: int = 1) -> list:
    """e.g. prefix='PROD', start=1 → ['PROD001', ...]"""
    return [f"{prefix}{str(i).zfill(3)}" for i in range(start, start + n)]


# ── Personal data generators ──────────────────────────────────────

def make_emails(firstnames, lastnames, domains, rng: np.random.Generator) -> list:
    """Deterministic-ish email from name + random suffix."""
    n = len(firstnames)
    seps      = rng.choice(["", ".", "_"], size=n)
    suffixes  = rng.integers(1, 999, size=n)
    chosen_domains = rng.choice(domains, size=n)
    return [
        f"{fn.lower()}{sep}{ln.lower()}{sfx}@{dom}"
        for fn, ln, sep, sfx, dom
        in zip(firstnames, lastnames, seps, suffixes, chosen_domains)
    ]


def make_phones(n: int, rng: np.random.Generator) -> list:
    """Generate US-style phone numbers."""
    areas   = rng.integers(200, 999, size=n)
    prefix  = rng.integers(200, 999, size=n)
    lines   = rng.integers(1000, 9999, size=n)
    return [f"+1-{a}-{p}-{l}" for a, p, l in zip(areas, prefix, lines)]


def make_salaries(titles: list, salary_ranges: dict, rng: np.random.Generator) -> list:
    """Map title → random salary within defined range."""
    salaries = []
    for title in titles:
        lo, hi = salary_ranges.get(title, (30_000, 50_000))
        # Round to nearest 500
        val = int(rng.integers(lo, hi) / 500) * 500
        salaries.append(val)
    return salaries


# ── Sale amount calculation ───────────────────────────────────────

def calc_sale_amounts(quantities: np.ndarray,
                      unit_prices: np.ndarray,
                      discounts: np.ndarray) -> np.ndarray:
    """
    sale_amount = quantity * unit_price * (1 - discount / 100)
    Rounded to 2 decimal places.
    """
    raw = quantities * unit_prices * (1 - discounts / 100)
    return np.round(raw, 2)


# ── Weighted random sampling wrapper ──────────────────────────────

def weighted_choice(choices: list, weights: list, n: int,
                    rng: np.random.Generator) -> np.ndarray:
    """Sample n items from choices with given weights."""
    idx = rng.choice(len(choices), size=n, p=np.array(weights) / np.sum(weights))
    return np.array(choices)[idx]