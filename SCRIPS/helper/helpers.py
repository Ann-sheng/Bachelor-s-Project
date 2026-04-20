# Utility functions shared between both generator scripts

from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import date, datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def date_range_days(start: str, end: str) -> int:
    """Number of calendar days between two YYYY-MM-DD strings."""
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def generate_dates(
    n: int,
    start: str,
    end: str,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Generate *n* random dates between *start* and *end*.
    Applies a mild weight so later dates appear slightly more often
    (simulating growing brand popularity).
    """
    delta = date_range_days(start, end)
    if delta <= 0:
        raise ValueError(f"end date '{end}' must be after start date '{start}'")
    weights = np.linspace(1.0, 2.0, delta)
    weights /= weights.sum()
    day_offsets = rng.choice(delta, size=n, p=weights)
    base = pd.Timestamp(start)
    return base + pd.to_timedelta(day_offsets, unit="D")


def add_calendar_days(
    dates: pd.Series,
    min_days: int,
    max_days: int,
    rng: np.random.Generator,
) -> pd.Series:
    """
    Add a random number of calendar days (between min_days and max_days inclusive)
    to each date in *dates*. Note: this adds plain calendar days — weekends are
    not skipped. Rename from add_business_days to clarify actual behaviour.
    """
    offsets = rng.integers(min_days, max_days + 1, size=len(dates))
    return dates + pd.to_timedelta(offsets, unit="D")


# ---------------------------------------------------------------------------
# ID generators
# ---------------------------------------------------------------------------

def make_transaction_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    """e.g. prefix='OFF', start=1  →  ['OFF0000001', 'OFF0000002', ...]"""
    return [f"{prefix}{i:07d}" for i in range(start, start + n)]


def make_entity_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    """e.g. prefix='CUST', start=1  →  ['CUST000001', ...]"""
    return [f"{prefix}{i:06d}" for i in range(start, start + n)]


# ---------------------------------------------------------------------------
# Personal data generators
# ---------------------------------------------------------------------------

def make_emails(
    firstnames,
    lastnames,
    domains: list[str],
    rng: np.random.Generator,
) -> list[str]:
    """Deterministic-ish email from name + random suffix."""
    n              = len(firstnames)
    seps           = rng.choice(["", ".", "_"], size=n)
    suffixes       = rng.integers(1, 999, size=n)
    chosen_domains = rng.choice(domains, size=n)
    return [
        f"{fn.lower()}{sep}{ln.lower()}{sfx}@{dom}"
        for fn, ln, sep, sfx, dom
        in zip(firstnames, lastnames, seps, suffixes, chosen_domains)
    ]


def make_phones(n: int, rng: np.random.Generator) -> list[str]:
    """Generate US-style phone numbers."""
    areas  = rng.integers(200, 999, size=n)
    prefix = rng.integers(200, 999, size=n)
    lines  = rng.integers(1000, 9999, size=n)
    return [f"+1-{a}-{p}-{l}" for a, p, l in zip(areas, prefix, lines)]


def make_salaries(
    titles: list[str],
    salary_ranges: dict,
    rng: np.random.Generator,
) -> list[int]:
    """Map title → random salary within its defined range, rounded to $500."""
    return [
        int(rng.integers(*salary_ranges.get(t, (30_000, 50_000))) / 500) * 500
        for t in titles
    ]


# ---------------------------------------------------------------------------
# Sale amount calculation
# ---------------------------------------------------------------------------

def calc_sale_amounts(
    quantities: np.ndarray,
    unit_prices: np.ndarray,
    discounts: np.ndarray,
) -> np.ndarray:
    """
    sale_amount = quantity × unit_price × (1 − discount / 100)
    Rounded to 2 decimal places.
    """
    return np.round(quantities * unit_prices * (1 - discounts / 100), 2)


# ---------------------------------------------------------------------------
# Weighted random sampling
# ---------------------------------------------------------------------------

def weighted_choice(
    choices: list,
    weights: list,
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Sample *n* items from *choices* with given *weights*."""
    w   = np.asarray(weights, dtype=float)
    idx = rng.choice(len(choices), size=n, p=w / w.sum())
    return np.array(choices)[idx]


# ---------------------------------------------------------------------------
# Shared Parquet / batch helpers  (used by both generator scripts)
# ---------------------------------------------------------------------------

def attach_entity_cols(
    df: pd.DataFrame,
    source_df: pd.DataFrame,
    cols: list[str],
    idx: np.ndarray,
) -> None:
    """In-place attach of *cols* from *source_df* rows selected by *idx*."""
    for col in cols:
        df[col] = source_df[col].values[idx]


def add_batch_metadata(df: pd.DataFrame, load_type: str, batch_id: str) -> pd.DataFrame:
    """
    Prepend audit columns so every row carries its lineage.
    batch_dt is derived from batch_id (format: YYYYMMDDTHHMMSSz) so it
    stays consistent with the filename timestamp — no second datetime.now() call.
    """
    df = df.copy()
    try:
        batch_dt = datetime.strptime(batch_id, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        batch_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df.insert(0, "batch_id",  batch_id)
    df.insert(1, "load_type", load_type)
    df.insert(2, "batch_dt",  batch_dt)
    return df


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame as snappy-compressed Parquet."""
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    mb = path.stat().st_size / 1e6
    print(f"  Saved {path.name}  ({len(df):,} rows, {mb:.1f} MB)")