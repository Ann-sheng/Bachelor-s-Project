# Utility functions shared between both generator scripts

from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import date, datetime, timezone
from pathlib import Path


# Date utilities

def date_range_days(start: str, end: str) -> int:
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def generate_dates(
    n: int,
    start: str,
    end: str,
    rng: np.random.Generator,
) -> pd.Series:
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
    offsets = rng.integers(min_days, max_days + 1, size=len(dates))
    return dates + pd.to_timedelta(offsets, unit="D")



# ID generators

def make_transaction_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    return [f"{prefix}{i:07d}" for i in range(start, start + n)]


def make_entity_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    return [f"{prefix}{i:06d}" for i in range(start, start + n)]



# Personal data generators

def make_emails(
    firstnames,
    lastnames,
    domains: list[str],
    rng: np.random.Generator,
) -> list[str]:
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
    areas  = rng.integers(100, 999, size=n)
    prefix = rng.integers(100, 999, size=n)
    lines  = rng.integers(100, 999, size=n)
    return [f"+995-{a}-{p}-{l}" for a, p, l in zip(areas, prefix, lines)]


def make_salaries(
    titles: list[str],
    salary_ranges: dict,
    rng: np.random.Generator,
) -> list[int]:
    return [
        int(rng.integers(*salary_ranges.get(t, (30_000, 50_000))) / 500) * 500
        for t in titles
    ]



# Sale amount calculatio

def calc_sale_amounts(
    quantities: np.ndarray,
    unit_prices: np.ndarray,
    discounts: np.ndarray,
) -> np.ndarray:
    return np.round(quantities * unit_prices * (1 - discounts / 100), 2)



# Weighted random sampling
def weighted_choice(
    choices: list,
    weights: list,
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    w   = np.asarray(weights, dtype=float)
    idx = rng.choice(len(choices), size=n, p=w / w.sum())
    return np.array(choices)[idx]



# Shared Parquet / batch helpers 

def attach_entity_cols(
    df: pd.DataFrame,
    source_df: pd.DataFrame,
    cols: list[str],
    idx: np.ndarray,
) -> None:
    for col in cols:
        df[col] = source_df[col].values[idx]


def add_batch_metadata(df: pd.DataFrame, load_type: str, batch_id: str) -> pd.DataFrame:
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
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    mb = path.stat().st_size / 1e6
    print(f"  Saved {path.name}  ({len(df):,} rows, {mb:.1f} MB)")