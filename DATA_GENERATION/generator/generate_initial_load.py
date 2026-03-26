# Generate 500 000-row initial load as Parquet files, upload to cloud storage.

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dotenv import load_dotenv
load_dotenv()

from vintage_data import (
    PRODUCT_CATALOG, SUPPLIERS, STORE_BRANCHES,
    OFFLINE_EMPLOYEE_TITLES, ONLINE_EMPLOYEE_TITLES,
    SHIPPING_OPTIONS,
    OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS,
    ONLINE_PAYMENT_METHODS,  ONLINE_PAYMENT_WEIGHTS,
    OFFLINE_CURRENCIES,      OFFLINE_CURR_WEIGHTS,
    ONLINE_CURRENCIES,       ONLINE_CURR_WEIGHTS,
    OFFLINE_CHANNELS,
    ONLINE_CHANNELS,         ONLINE_CHANNEL_WEIGHTS,
    OFFLINE_SHIPMENT_STATUSES,
    ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS,
    DISCOUNT_VALUES,          DISCOUNT_WEIGHTS,
    CUSTOMER_COUNTRIES,       US_CITIES, INTL_CITIES,
    FIRST_NAMES, LAST_NAMES,  EMAIL_DOMAINS,
    OFFLINE_SALARY_RANGES,    ONLINE_SALARY_RANGES,
)
from helpers import (
    generate_dates, add_calendar_days,
    make_transaction_ids, make_entity_ids,
    make_emails, make_phones, make_salaries,
    calc_sale_amounts, weighted_choice,
    attach_entity_cols, add_batch_metadata, save_parquet,
)
from cloud_storage import get_cloud_client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEED        = int(os.environ.get("SEED_INITIAL", 42))
N_OFFLINE   = 500_000
N_ONLINE    = 500_000
N_CUSTOMERS = 14_000
N_EMP_OFF   = 150
N_EMP_ONL   = 80

START_DATE  = "2020-01-01"
END_DATE    = "2023-12-31"

OUTPUT_DIR  = Path(os.environ.get("DATA_INITIAL_DIR",  "data/initial"))
REF_DIR     = Path(os.environ.get("DATA_REF_DIR",      "data/reference"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

LOAD_TYPE = "INITIAL"

# Normalize discount weights once at module level to avoid rng.choice errors
# if DISCOUNT_WEIGHTS has floating-point imprecision.
_DISC_W = np.array(DISCOUNT_WEIGHTS, dtype=float)
_DISC_W /= _DISC_W.sum()


# ---------------------------------------------------------------------------
# Entity builders
# ---------------------------------------------------------------------------

def build_suppliers() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "supplier_id": f"SUPP{i:02d}", "supplier_name": name,
            "supplier_email": email, "supplier_number": phone,
            "supplier_primary_contact": contact, "supplier_location": location,
        }
        for i, (name, email, phone, contact, location) in enumerate(SUPPLIERS, 1)
    ])


def build_products(suppliers_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    rows, prod_num = [], 1
    sup_ids = suppliers_df["supplier_id"].tolist()
    for category, items in PRODUCT_CATALOG.items():
        primary_sup_idx = prod_num % len(sup_ids)
        for name, cost, price, warranty in items:
            sup_idx = primary_sup_idx if rng.random() > 0.2 else int(rng.integers(len(sup_ids)))
            rows.append({
                "product_id": f"PROD{prod_num:03d}", "product_category": category,
                "product_name": name, "product_unit_cost": cost,
                "product_unit_price": price, "product_warranty_period": warranty,
                "supplier_id": sup_ids[sup_idx],
            })
            prod_num += 1
    return pd.DataFrame(rows).merge(suppliers_df, on="supplier_id", how="left")


def build_store_branches() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "store_branch_id": f"BRANCH{i:02d}",
            "store_branch_city": city, "store_branch_state": state,
            "store_branch_phone_number": phone,
            "store_branch_operating_days": days,
            "store_branch_operating_hours": hours,
        }
        for i, (city, state, phone, days, hours) in enumerate(STORE_BRANCHES, 1)
    ])


def build_offline_employees(
    store_branches_df: pd.DataFrame,
    n: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    branch_ids  = store_branches_df["store_branch_id"].tolist()
    branch_idx  = rng.integers(0, len(branch_ids), size=n)
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    titles      = rng.choice(OFFLINE_EMPLOYEE_TITLES, size=n)
    df = pd.DataFrame({
        "employee_id":        make_entity_ids(n, "EMPO"),
        "employee_firstname": first_names,
        "employee_lastname":  last_names,
        "employee_title":     titles,
        "employee_salary":    make_salaries(titles.tolist(), OFFLINE_SALARY_RANGES, rng),
        "store_branch_id":    [branch_ids[i] for i in branch_idx],
    })
    df["employee_email"]        = make_emails(df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng)
    df["employee_phone_number"] = make_phones(n, rng)
    return df.merge(store_branches_df, on="store_branch_id", how="left")


def build_online_employees(n: int, rng: np.random.Generator) -> pd.DataFrame:
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    titles      = rng.choice(ONLINE_EMPLOYEE_TITLES, size=n)
    df = pd.DataFrame({
        "employee_id":        make_entity_ids(n, "EMPW"),
        "employee_firstname": first_names,
        "employee_lastname":  last_names,
        "employee_title":     titles,
        "employee_salary":    make_salaries(titles.tolist(), ONLINE_SALARY_RANGES, rng),
    })
    df["employee_email"]        = make_emails(df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng)
    df["employee_phone_number"] = make_phones(n, rng)
    return df


def build_customers(n: int, rng: np.random.Generator, start_id: int = 1) -> pd.DataFrame:
    first_names     = rng.choice(FIRST_NAMES, size=n)
    last_names      = rng.choice(LAST_NAMES,  size=n)
    countries_list  = list(CUSTOMER_COUNTRIES.keys())
    country_weights = np.array(list(CUSTOMER_COUNTRIES.values()), dtype=float)
    countries = rng.choice(countries_list, size=n, p=country_weights / country_weights.sum())
    cities = [
        rng.choice(US_CITIES if c == "United States" else INTL_CITIES.get(c, ["Unknown"]))
        for c in countries
    ]
    df = pd.DataFrame({
        "customer_id":        make_entity_ids(n, "CUST", start=start_id),
        "customer_firstname": first_names,
        "customer_lastname":  last_names,
        "customer_country":   countries,
        "customer_city":      cities,
    })
    df["customer_email"]        = make_emails(df["customer_firstname"], df["customer_lastname"], EMAIL_DOMAINS, rng)
    df["customer_phone_number"] = make_phones(n, rng)
    return df


def build_shipping_options() -> pd.DataFrame:
    return pd.DataFrame([
        {"shipping_id": f"SHIP{i:02d}", "shipping_method": m, "shipping_carrier": c}
        for i, (m, c) in enumerate(SHIPPING_OPTIONS, 1)
    ])


# ---------------------------------------------------------------------------
# Transaction builders
# ---------------------------------------------------------------------------

def build_offline_transactions(
    n: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    employees_df: pd.DataFrame,
    rng: np.random.Generator,
    tx_id_start: int = 1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} offline transactions...")
    cust_idx   = rng.choice(len(customers_df), size=n)
    prod_probs = np.array([1 / (i + 1) ** 0.5 for i in range(len(products_df))])
    prod_idx   = rng.choice(len(products_df), size=n, p=prod_probs / prod_probs.sum())
    emp_idx    = rng.choice(len(employees_df), size=n)

    quantities   = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts    = rng.choice(DISCOUNT_VALUES, size=n, p=_DISC_W)  # FIX: normalized weights
    sale_amounts = calc_sale_amounts(quantities, products_df["product_unit_price"].values[prod_idx], discounts)
    tx_dates     = generate_dates(n, START_DATE, END_DATE, rng)

    df = pd.DataFrame({
        "transaction_id":              make_transaction_ids(n, "OFF", start=tx_id_start),
        "transaction_dt":              pd.to_datetime(tx_dates).strftime("%Y-%m-%d"),
        "transaction_shipped_dt":      None,   # offline: collected in-store
        "transaction_delivery_dt":     None,
        "transaction_payment_method":  weighted_choice(OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS, n, rng),
        "transaction_currency_paid":   weighted_choice(OFFLINE_CURRENCIES,      OFFLINE_CURR_WEIGHTS,    n, rng),
        "transaction_sales_channel":   "In-Store",
        "transaction_shipment_status": "Collected In-Store",
        "transaction_quantity_sold":   quantities,
        "transaction_discount_pct":    discounts,
        "transaction_sale_amount":     sale_amounts,
    })

    attach_entity_cols(df, customers_df, [
        "customer_id", "customer_firstname", "customer_lastname", "customer_email",
        "customer_phone_number", "customer_country", "customer_city",
    ], cust_idx)
    attach_entity_cols(df, products_df, [
        "product_id", "product_category", "product_name", "product_unit_cost",
        "product_unit_price", "product_warranty_period", "supplier_id",
        "supplier_name", "supplier_email", "supplier_number",
        "supplier_primary_contact", "supplier_location",
    ], prod_idx)
    attach_entity_cols(df, employees_df, [
        "employee_id", "employee_firstname", "employee_lastname", "employee_title",
        "employee_email", "employee_phone_number", "employee_salary",
        "store_branch_id", "store_branch_city", "store_branch_state",
        "store_branch_phone_number", "store_branch_operating_days",
        "store_branch_operating_hours",
    ], emp_idx)

    df["shipping_id"] = df["shipping_method"] = df["shipping_carrier"] = None

    assert df["transaction_id"].is_unique, "Duplicate offline transaction IDs!"
    return df


def build_online_transactions(
    n: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    employees_df: pd.DataFrame,
    shipping_df: pd.DataFrame,
    rng: np.random.Generator,
    tx_id_start: int = 1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} online transactions...")
    cust_idx   = rng.choice(len(customers_df), size=n)
    prod_probs = np.array([1 / (i + 1) ** 0.45 for i in range(len(products_df))])
    prod_idx   = rng.choice(len(products_df), size=n, p=prod_probs / prod_probs.sum())
    emp_idx    = rng.choice(len(employees_df), size=n)
    ship_idx   = rng.choice(len(shipping_df),  size=n)

    quantities   = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts    = rng.choice(DISCOUNT_VALUES, size=n, p=_DISC_W)  # FIX: normalized weights
    sale_amounts = calc_sale_amounts(quantities, products_df["product_unit_price"].values[prod_idx], discounts)
    ship_status  = weighted_choice(ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS, n, rng)
    tx_dates     = generate_dates(n, START_DATE, END_DATE, rng)

    tx_ts          = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates  = add_calendar_days(tx_ts, 1, 3, rng)
    delivery_dates = add_calendar_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")

    # FIX: Pending/Processing/Cancelled orders have no shipped or delivery date.
    # Previously only delivery_dt was nulled; shipped_dt was always populated.
    mask_no_ship   = pd.Series(ship_status).isin(["Pending", "Processing", "Cancelled"])
    shipped_arr    = shipped_dates.dt.strftime("%Y-%m-%d").copy()
    shipped_arr[mask_no_ship.values]  = None
    delivery_arr   = delivery_dates.copy()
    delivery_arr[mask_no_ship.values] = None

    df = pd.DataFrame({
        "transaction_id":              make_transaction_ids(n, "ONL", start=tx_id_start),
        "transaction_dt":              tx_ts.dt.strftime("%Y-%m-%d"),
        "transaction_shipped_dt":      shipped_arr.values,
        "transaction_delivery_dt":     delivery_arr.values,
        "transaction_payment_method":  weighted_choice(ONLINE_PAYMENT_METHODS, ONLINE_PAYMENT_WEIGHTS, n, rng),
        "transaction_currency_paid":   weighted_choice(ONLINE_CURRENCIES,      ONLINE_CURR_WEIGHTS,    n, rng),
        "transaction_sales_channel":   weighted_choice(ONLINE_CHANNELS,        ONLINE_CHANNEL_WEIGHTS, n, rng),
        "transaction_shipment_status": ship_status,
        "transaction_quantity_sold":   quantities,
        "transaction_discount_pct":    discounts,
        "transaction_sale_amount":     sale_amounts,
    })

    attach_entity_cols(df, customers_df, [
        "customer_id", "customer_firstname", "customer_lastname", "customer_email",
        "customer_phone_number", "customer_country", "customer_city",
    ], cust_idx)
    attach_entity_cols(df, products_df, [
        "product_id", "product_category", "product_name", "product_unit_cost",
        "product_unit_price", "product_warranty_period", "supplier_id",
        "supplier_name", "supplier_email", "supplier_number",
        "supplier_primary_contact", "supplier_location",
    ], prod_idx)
    attach_entity_cols(df, employees_df, [
        "employee_id", "employee_firstname", "employee_lastname", "employee_title",
        "employee_email", "employee_phone_number", "employee_salary",
    ], emp_idx)
    attach_entity_cols(df, shipping_df, [
        "shipping_id", "shipping_method", "shipping_carrier",
    ], ship_idx)

    df["store_branch_id"] = df["store_branch_city"] = df["store_branch_state"] = None
    df["store_branch_phone_number"] = df["store_branch_operating_days"] = None
    df["store_branch_operating_hours"] = None

    assert df["transaction_id"].is_unique, "Duplicate online transaction IDs!"
    return df


# ---------------------------------------------------------------------------
# Reference state  (replaces pickle)
# ---------------------------------------------------------------------------

def save_reference_state(
    ref_dir: Path,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    store_branches_df: pd.DataFrame,
    employees_off_df: pd.DataFrame,
    employees_onl_df: pd.DataFrame,
    shipping_df: pd.DataFrame,
    counters: dict,
) -> None:
    """
    Persist master entity DataFrames as Parquet files and ID counters as JSON.
    This replaces the original pickle approach which was Python-version-sensitive,
    binary-opaque, and a potential security risk (pickle can execute arbitrary code
    on load).
    """
    customers_df.to_parquet(     ref_dir / "customers.parquet",      index=False)
    products_df.to_parquet(      ref_dir / "products.parquet",        index=False)
    store_branches_df.to_parquet(ref_dir / "store_branches.parquet",  index=False)
    employees_off_df.to_parquet( ref_dir / "employees_off.parquet",   index=False)
    employees_onl_df.to_parquet( ref_dir / "employees_onl.parquet",   index=False)
    shipping_df.to_parquet(      ref_dir / "shipping.parquet",        index=False)
    with open(ref_dir / "state.json", "w") as fh:
        json.dump(counters, fh, indent=2)
    print(f"  ✓ Reference state saved to {ref_dir}/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("\n╔═════════════════════╗")
    print("║   Initial Load      ║")
    print("╚═════════════════════╝\n")

    rng      = np.random.default_rng(SEED)
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cloud    = get_cloud_client()

    print("▶ Building master entities...")
    suppliers_df      = build_suppliers()
    products_df       = build_products(suppliers_df, rng)
    store_branches_df = build_store_branches()
    employees_off_df  = build_offline_employees(store_branches_df, N_EMP_OFF, rng)
    employees_onl_df  = build_online_employees(N_EMP_ONL, rng)
    customers_df      = build_customers(N_CUSTOMERS, rng)
    shipping_df       = build_shipping_options()

    print(
        f"  ✓ {len(suppliers_df)} suppliers | {len(products_df)} products | "
        f"{len(store_branches_df)} branches | {len(employees_off_df)} offline emps | "
        f"{len(employees_onl_df)} online emps | {len(customers_df):,} customers | "
        f"{len(shipping_df)} shipping options"
    )

    print("\n▶ Generating transactions...")
    offline_df = build_offline_transactions(N_OFFLINE, customers_df, products_df, employees_off_df, rng)
    online_df  = build_online_transactions( N_ONLINE,  customers_df, products_df, employees_onl_df, shipping_df, rng)

    offline_df = add_batch_metadata(offline_df, LOAD_TYPE, batch_ts)
    online_df  = add_batch_metadata(online_df,  LOAD_TYPE, batch_ts)

    print("\n▶ Saving Parquet files locally...")
    off_file = OUTPUT_DIR / f"offline_initial_{batch_ts}.parquet"
    onl_file = OUTPUT_DIR / f"online_initial_{batch_ts}.parquet"
    save_parquet(offline_df, off_file)
    save_parquet(online_df,  onl_file)

    print("\n▶ Uploading to cloud storage...")
    off_key = f"offline/initial/offline_initial_{batch_ts}.parquet"
    onl_key = f"online/initial/online_initial_{batch_ts}.parquet"
    print(f"  Uploaded → {cloud.upload(str(off_file), off_key)}")
    print(f"  Uploaded → {cloud.upload(str(onl_file), onl_key)}")

    print("\n▶ Saving reference state...")
    save_reference_state(
        ref_dir=REF_DIR,
        customers_df=customers_df,
        products_df=products_df,
        store_branches_df=store_branches_df,
        employees_off_df=employees_off_df,
        employees_onl_df=employees_onl_df,
        shipping_df=shipping_df,
        counters={
            "last_customer_id": N_CUSTOMERS,
            "last_product_id":  len(products_df),
            "last_off_tx_id":   N_OFFLINE,
            "last_onl_tx_id":   N_ONLINE,
            "initial_off_key":  off_key,
            "initial_onl_key":  onl_key,
        },
    )

    print("\n── Summary ──────────────────────────────────────")
    print(f"  Batch ID    : {batch_ts}")
    print(f"  Offline rows: {len(offline_df):,}  |  Revenue: ${offline_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Online rows : {len(online_df):,}   |  Revenue: ${online_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Date range  : {START_DATE} → {END_DATE}")
    print("\nInitial load complete.\n")


if __name__ == "__main__":
    main()