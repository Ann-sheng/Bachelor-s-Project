# Utility functions shared between both generator scripts

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd


# ── Date utilities ─────────────────────────────────────────────────────────────

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


# ── ID generators ──────────────────────────────────────────────────────────────

def make_transaction_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    return [f"{prefix}{i:07d}" for i in range(start, start + n)]


def make_entity_ids(n: int, prefix: str, start: int = 1) -> list[str]:
    return [f"{prefix}{i:06d}" for i in range(start, start + n)]


# ── Personal data generators ───────────────────────────────────────────────────

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


# ── Sale amount calculation ────────────────────────────────────────────────────

def calc_sale_amounts(
    quantities: np.ndarray,
    unit_prices: np.ndarray,
    discounts: np.ndarray,
) -> np.ndarray:
    return np.round(quantities * unit_prices * (1 - discounts / 100), 2)


# ── Weighted random sampling ───────────────────────────────────────────────────

def weighted_choice(
    choices: list,
    weights: list,
    n: int,
    rng: np.random.Generator,
) -> np.ndarray:
    w   = np.asarray(weights, dtype=float)
    idx = rng.choice(len(choices), size=n, p=w / w.sum())
    return np.array(choices)[idx]


# ── DataFrame helpers ──────────────────────────────────────────────────────────

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


# ── Cloud upload primitives ────────────────────────────────────────────────────

def upload_df(cloud, df: pd.DataFrame, key: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        return cloud.upload(tmp_path, key)
    finally:
        os.unlink(tmp_path)


def upload_json(cloud, data: dict, key: str) -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        json.dump(data, tmp, indent=2)
        tmp_path = tmp.name
    try:
        return cloud.upload(tmp_path, key)
    finally:
        os.unlink(tmp_path)


# ── Cloud reference state ──────────────────────────────────────────────────────


_REF_FRAMES: dict[str, str] = {
    "customers_df":      "reference/customers.parquet",
    "products_df":       "reference/products.parquet",
    "store_branches_df": "reference/store_branches.parquet",
    "employees_off_df":  "reference/employees_off.parquet",
    "employees_onl_df":  "reference/employees_onl.parquet",
    "shipping_df":       "reference/shipping.parquet",
}
_REF_STATE_KEY = "reference/state.json"


def upload_reference_state(
    cloud,
    counters: dict,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    store_branches_df: pd.DataFrame,
    employees_off_df: pd.DataFrame,
    employees_onl_df: pd.DataFrame,
    shipping_df: pd.DataFrame,
) -> None:
    frames = {
        "customers_df":      customers_df,
        "products_df":       products_df,
        "store_branches_df": store_branches_df,
        "employees_off_df":  employees_off_df,
        "employees_onl_df":  employees_onl_df,
        "shipping_df":       shipping_df,
    }
    for df_name, df in frames.items():
        print(f"  Uploaded → {upload_df(cloud, df, _REF_FRAMES[df_name])}")
    print(f"  Uploaded → {upload_json(cloud, counters, _REF_STATE_KEY)}")
    print("  Reference state uploaded to cloud")


def download_reference_state(cloud) -> tuple[dict, dict]:
    dataframes: dict[str, pd.DataFrame] = {}

    for df_name, key in _REF_FRAMES.items():
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            cloud.download(key, tmp_path)
            dataframes[df_name] = pd.read_parquet(tmp_path)
        finally:
            os.unlink(tmp_path)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        cloud.download(_REF_STATE_KEY, tmp_path)
        with open(tmp_path) as fh:
            counters = json.load(fh)
    finally:
        os.unlink(tmp_path)

    print(" Reference state downloaded from cloud")
    return dataframes, counters