# Generate 250 000-row incremental Parquet files with deliberate SCD2 change
# scenarios, upload to cloud.

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
    PRODUCT_CATALOG, FIRST_NAMES, LAST_NAMES, EMAIL_DOMAINS,
    US_CITIES, INTL_CITIES, CUSTOMER_COUNTRIES,
    OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS,
    ONLINE_PAYMENT_METHODS,  ONLINE_PAYMENT_WEIGHTS,
    OFFLINE_CURRENCIES,      OFFLINE_CURR_WEIGHTS,
    ONLINE_CURRENCIES,       ONLINE_CURR_WEIGHTS,
    ONLINE_CHANNELS,         ONLINE_CHANNEL_WEIGHTS,
    ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS,
    DISCOUNT_VALUES,          DISCOUNT_WEIGHTS,
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


# Configuration

SEED_INCR  = int(os.environ.get("SEED_INCREMENTAL", 99))
N_OFFLINE  = 250_000
N_ONLINE   = 250_000
START_DATE = "2024-01-01"
END_DATE   = "2024-06-30"

# SCD2 change volumes
SCD2_CUSTOMER_CITY_CHANGES    = 400
SCD2_CUSTOMER_CONTACT_CHANGES = 200
SCD2_EMPLOYEE_PROMOTIONS      = 80
SCD2_EMPLOYEE_RAISES          = 40
NEW_CUSTOMERS                 = 1_500
NEW_PRODUCTS                  = 15

REF_DIR    = Path(os.environ.get("DATA_REF_DIR",         "data/reference"))
OUTPUT_DIR = Path(os.environ.get("DATA_INCREMENTAL_DIR", "data/incremental"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOAD_TYPE = "INCREMENTAL"

# Normalize discount weights once at module level 
_DISC_W = np.array(DISCOUNT_WEIGHTS, dtype=float)
_DISC_W /= _DISC_W.sum()



# Reference state helpers  

def load_reference_state(ref_dir: Path) -> tuple[dict, dict]:
    state_path = ref_dir / "state.json"
    if not state_path.exists():
        raise FileNotFoundError(
            f"{state_path} not found. Run generate_initial_load.py first."
        )
    with open(state_path) as fh:
        counters = json.load(fh)

    dataframes = {
        "customers_df":      pd.read_parquet(ref_dir / "customers.parquet"),
        "products_df":       pd.read_parquet(ref_dir / "products.parquet"),
        "store_branches_df": pd.read_parquet(ref_dir / "store_branches.parquet"),
        "employees_off_df":  pd.read_parquet(ref_dir / "employees_off.parquet"),
        "employees_onl_df":  pd.read_parquet(ref_dir / "employees_onl.parquet"),
        "shipping_df":       pd.read_parquet(ref_dir / "shipping.parquet"),
    }
    return dataframes, counters


def save_reference_state(ref_dir: Path, dataframes: dict, counters: dict) -> None:
    mapping = {
        "customers_df":      "customers.parquet",
        "products_df":       "products.parquet",
        "store_branches_df": "store_branches.parquet",
        "employees_off_df":  "employees_off.parquet",
        "employees_onl_df":  "employees_onl.parquet",
        "shipping_df":       "shipping.parquet",
    }
    for key, filename in mapping.items():
        dataframes[key].to_parquet(ref_dir / filename, index=False)
    with open(ref_dir / "state.json", "w") as fh:
        json.dump(counters, fh, indent=2)
    print(f"  ✓ Reference state updated in {ref_dir}/")



# SCD2 change scenarios

def apply_customer_scd2_changes(
    df: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict]:
    df      = df.copy().reset_index(drop=True)  
    n       = len(df)
    summary = {}


    city_col    = df.columns.get_loc("customer_city")
    country_col = df.columns.get_loc("customer_country")
    id_col      = df.columns.get_loc("customer_id")

    city_idx = rng.choice(n, size=SCD2_CUSTOMER_CITY_CHANGES, replace=False)
    changed_cities = []
    for row_pos in city_idx.tolist():
        country  = df.iloc[row_pos, country_col]
        pool     = US_CITIES if country == "United States" else INTL_CITIES.get(country, US_CITIES)
        choices  = [c for c in pool if c != df.iloc[row_pos, city_col]] or pool
        old_city = df.iloc[row_pos, city_col]
        new_city = rng.choice(choices)
        df.iloc[row_pos, city_col] = new_city
        changed_cities.append((df.iloc[row_pos, id_col], old_city, new_city))
    summary["city_changes"] = changed_cities[:5]

    used_idx    = set(city_idx.tolist())
    avail_idx   = np.array([i for i in range(n) if i not in used_idx])
    contact_idx = rng.choice(avail_idx, size=SCD2_CUSTOMER_CONTACT_CHANGES, replace=False)

    fn       = df["customer_firstname"].values[contact_idx]
    ln       = df["customer_lastname"].values[contact_idx]
    seps     = rng.choice(["", ".", "_"], size=len(contact_idx))
    domains  = rng.choice(EMAIL_DOMAINS,  size=len(contact_idx))
    suffixes = rng.integers(1, 999,       size=len(contact_idx))
    new_emails = [
        f"{f.lower()}{s}{l.lower()}{sfx}@{d}"
        for f, l, s, sfx, d in zip(fn, ln, seps, suffixes, domains)
    ]
    new_phones = make_phones(len(contact_idx), rng)

    email_col = df.columns.get_loc("customer_email")
    phone_col = df.columns.get_loc("customer_phone_number")
    df.iloc[contact_idx, email_col] = new_emails
    df.iloc[contact_idx, phone_col] = new_phones

    summary["total_changed"] = SCD2_CUSTOMER_CITY_CHANGES + SCD2_CUSTOMER_CONTACT_CHANGES
    return df, summary


def apply_employee_offline_scd2(
    df: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, list]:
    """Simulate promotions for offline employees."""
    df = df.copy().reset_index(drop=True) 
    promo_map = {
        "Stock Associate":        "Sales Associate",
        "Cashier":                "Sales Associate",
        "Sales Associate":        "Senior Sales Associate",
        "Senior Sales Associate": "Brand Stylist",
        "Brand Stylist":          "Shift Supervisor",
        "Shift Supervisor":       "Floor Manager",
        "Floor Manager":          "Assistant Manager",
        "Assistant Manager":      "Store Manager",
    }

    promo_idx  = rng.choice(len(df), size=SCD2_EMPLOYEE_PROMOTIONS, replace=False)
    old_titles = df["employee_title"].values[promo_idx]
    new_titles = np.array([promo_map.get(t, t) for t in old_titles])
    changed    = new_titles != old_titles

    new_salaries = np.array([
        int(rng.integers(*OFFLINE_SALARY_RANGES.get(nt, (30_000, 50_000))) / 500) * 500
        if ch else df["employee_salary"].values[idx]
        for idx, nt, ch in zip(promo_idx, new_titles, changed)
    ])

    title_col  = df.columns.get_loc("employee_title")
    salary_col = df.columns.get_loc("employee_salary")
    changed_idx = promo_idx[changed]
    df.iloc[changed_idx, title_col]  = new_titles[changed]
    df.iloc[changed_idx, salary_col] = new_salaries[changed]

    id_col = df.columns.get_loc("employee_id")
    changes = [
        (df.iloc[idx, id_col], old, new)
        for idx, old, new, ch in zip(promo_idx, old_titles, new_titles, changed)
        if ch
    ]
    return df, changes


def apply_employee_online_scd2(
    df: pd.DataFrame,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, list]:
    """Salary raises for online employees — fully vectorized."""
    df = df.copy().reset_index(drop=True)  

    raise_idx    = rng.choice(len(df), size=SCD2_EMPLOYEE_RAISES, replace=False)
    old_salaries = df["employee_salary"].values[raise_idx].astype(float)
    raise_pcts   = rng.choice([3, 5, 7, 10], size=SCD2_EMPLOYEE_RAISES).astype(float)
    new_salaries = (old_salaries * (1 + raise_pcts / 100) / 500).astype(int) * 500

    salary_col = df.columns.get_loc("employee_salary")
    df.iloc[raise_idx, salary_col] = new_salaries

    id_col = df.columns.get_loc("employee_id")
    changes = [
        (df.iloc[idx, id_col], int(old), int(new))
        for idx, old, new in zip(raise_idx, old_salaries, new_salaries)
    ]
    return df, changes



# New entity builders

def add_new_customers(
    df: pd.DataFrame,
    n: int,
    last_id: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    fn            = rng.choice(FIRST_NAMES, size=n)
    ln            = rng.choice(LAST_NAMES,  size=n)
    cw            = np.array(list(CUSTOMER_COUNTRIES.values()), dtype=float)
    new_countries = rng.choice(list(CUSTOMER_COUNTRIES.keys()), size=n, p=cw / cw.sum())
    cities        = [
        rng.choice(US_CITIES if c == "United States" else INTL_CITIES.get(c, ["Unknown"]))
        for c in new_countries
    ]
    df_new = pd.DataFrame({
        "customer_id":        make_entity_ids(n, "CUST", start=last_id + 1),
        "customer_firstname": fn,
        "customer_lastname":  ln,
        "customer_country":   new_countries,
        "customer_city":      cities,
    })
    df_new["customer_email"]        = make_emails(df_new["customer_firstname"], df_new["customer_lastname"], EMAIL_DOMAINS, rng)
    df_new["customer_phone_number"] = make_phones(n, rng)
    return pd.concat([df, df_new], ignore_index=True)


def add_new_products(
    df: pd.DataFrame,
    last_id: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    new_items = [
        ("Vintage Corduroy Blazer",      38, 108, 24, "Jackets & Outerwear"),
        ("90s Neon Ski Pants",           25,  75,  6, "Activewear"),
        ("Patchwork Denim Jacket",       32,  92, 18, "Jackets & Outerwear"),
        ("Vintage Lace Cami Top",        12,  36,  0, "T-Shirts & Tops"),
        ("Retro Terrycloth Polo",        14,  44,  6, "T-Shirts & Tops"),
        ("80s Satin Bomber Jacket",      42, 118, 24, "Jackets & Outerwear"),
        ("Wide Brim Felt Hat",           11,  34,  6, "Accessories"),
        ("Vintage Platform Boots",       48, 142, 24, "Footwear"),
        ("70s Patchwork Maxi Skirt",     22,  66,  6, "Dresses & Skirts"),
        ("Retro Velour Tracksuit Top",   19,  58, 12, "Activewear"),
        ("Vintage Silk Pyjama Shirt",    20,  60,  6, "Shirts & Blouses"),
        ("Barrel Leg Denim Jeans",       24,  72, 12, "Denim"),
        ("Chunky Soled Loafers",         36, 108, 24, "Footwear"),
        ("Retro Crochet Bucket Bag",     18,  54, 12, "Accessories"),
        ("Vintage Metallic Mini Skirt",  20,  60,  6, "Dresses & Skirts"),
    ]
    existing_sup_ids = df["supplier_id"].unique().tolist()
    new_rows = []
    for i, (name, cost, price, warranty, category) in enumerate(new_items[:NEW_PRODUCTS], 1):
        sup_id  = rng.choice(existing_sup_ids)
        sup_row = df[df["supplier_id"] == sup_id].iloc[0]
        new_rows.append({
            "product_id": f"PROD{last_id + i:03d}", "product_category": category,
            "product_name": name, "product_unit_cost": cost,
            "product_unit_price": price, "product_warranty_period": warranty,
            "supplier_id": sup_id,
            "supplier_name": sup_row["supplier_name"],
            "supplier_email": sup_row["supplier_email"],
            "supplier_number": sup_row["supplier_number"],
            "supplier_primary_contact": sup_row["supplier_primary_contact"],
            "supplier_location": sup_row["supplier_location"],
        })
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)



# Transaction builders  (incremental)

def build_offline_incr(
    n: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    employees_df: pd.DataFrame,
    rng: np.random.Generator,
    tx_id_start: int = 1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} incremental offline transactions...")
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
        "transaction_shipped_dt":      None,
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


def build_online_incr(
    n: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    employees_df: pd.DataFrame,
    shipping_df: pd.DataFrame,
    rng: np.random.Generator,
    tx_id_start: int = 1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} incremental online transactions...")
    cust_idx   = rng.choice(len(customers_df), size=n)
    prod_probs = np.array([1 / (i + 1) ** 0.45 for i in range(len(products_df))])
    prod_idx   = rng.choice(len(products_df), size=n, p=prod_probs / prod_probs.sum())
    emp_idx    = rng.choice(len(employees_df), size=n)
    ship_idx   = rng.choice(len(shipping_df),  size=n)

    quantities   = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts    = rng.choice(DISCOUNT_VALUES, size=n, p=_DISC_W)  
    sale_amounts = calc_sale_amounts(quantities, products_df["product_unit_price"].values[prod_idx], discounts)
    ship_status  = weighted_choice(ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS, n, rng)
    tx_dates     = generate_dates(n, START_DATE, END_DATE, rng)

    tx_ts          = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates  = add_calendar_days(tx_ts, 1, 3, rng)
    delivery_dates = add_calendar_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")

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



# Main
def main() -> None:
    print(" Incremental Load ")

    rng      = np.random.default_rng(SEED_INCR)
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cloud    = get_cloud_client()

    print("Loading reference state...")
    dataframes, counters  = load_reference_state(REF_DIR)
    customers_df          = dataframes["customers_df"]
    products_df           = dataframes["products_df"]
    store_branches_df     = dataframes["store_branches_df"]
    employees_off_df      = dataframes["employees_off_df"]
    employees_onl_df      = dataframes["employees_onl_df"]
    shipping_df           = dataframes["shipping_df"]
    last_cust_id          = counters["last_customer_id"]
    last_prod_id          = counters["last_product_id"]
    last_off_tx_id        = counters["last_off_tx_id"]
    last_onl_tx_id        = counters["last_onl_tx_id"]

    print(" Applying SCD2 change scenarios...")
    customers_df,     cust_summary = apply_customer_scd2_changes(customers_df, rng)
    employees_off_df, off_promos   = apply_employee_offline_scd2(employees_off_df, rng)
    employees_onl_df, onl_raises   = apply_employee_online_scd2(employees_onl_df, rng)

    print("\n Adding new entities...")
    customers_df = add_new_customers(customers_df, NEW_CUSTOMERS, last_cust_id, rng)
    products_df  = add_new_products(products_df, last_prod_id, rng)
    print(f" DONE +{NEW_CUSTOMERS} new customers | +{NEW_PRODUCTS} new products")

    print(f"\n Generating transactions (date range: {START_DATE} → {END_DATE})...")
    offline_df = build_offline_incr(
        N_OFFLINE, customers_df, products_df, employees_off_df, rng,
        tx_id_start=last_off_tx_id + 1,
    )
    online_df = build_online_incr(
        N_ONLINE, customers_df, products_df, employees_onl_df, shipping_df, rng,
        tx_id_start=last_onl_tx_id + 1,
    )

    offline_df = add_batch_metadata(offline_df, LOAD_TYPE, batch_ts)
    online_df  = add_batch_metadata(online_df,  LOAD_TYPE, batch_ts)

    print("\n Saving Parquet files...")
    off_file = OUTPUT_DIR / f"offline_incremental_{batch_ts}.parquet"
    onl_file = OUTPUT_DIR / f"online_incremental_{batch_ts}.parquet"
    save_parquet(offline_df, off_file)
    save_parquet(online_df,  onl_file)

    print("\n Uploading to cloud storage...")
    off_key = f"offline/incremental/offline_incremental_{batch_ts}.parquet"
    onl_key = f"online/incremental/online_incremental_{batch_ts}.parquet"
    print(f"  Uploaded → {cloud.upload(str(off_file), off_key)}")
    print(f"  Uploaded → {cloud.upload(str(onl_file), onl_key)}")

    print("\n Updating reference state...")
    save_reference_state(
        ref_dir=REF_DIR,
        dataframes={
            "customers_df":      customers_df,
            "products_df":       products_df,
            "store_branches_df": store_branches_df,
            "employees_off_df":  employees_off_df,
            "employees_onl_df":  employees_onl_df,
            "shipping_df":       shipping_df,
        },
        counters={
            **counters,
            "last_customer_id": last_cust_id  + NEW_CUSTOMERS,
            "last_product_id":  last_prod_id  + NEW_PRODUCTS,
            "last_off_tx_id":   last_off_tx_id + N_OFFLINE,
            "last_onl_tx_id":   last_onl_tx_id + N_ONLINE,
        },
    )

    print("\n── SCD2 Changes Summary ─────────────────────────────────")
    print(f"  Customers city changed    : {SCD2_CUSTOMER_CITY_CHANGES}")
    print(f"  Customers contact changed : {SCD2_CUSTOMER_CONTACT_CHANGES}")
    print(f"  Offline employees promoted: {SCD2_EMPLOYEE_PROMOTIONS}")
    print(f"  Online employees raised   : {SCD2_EMPLOYEE_RAISES}")
    print(f"  New customers             : {NEW_CUSTOMERS}")
    print(f"  New products              : {NEW_PRODUCTS}")
    print(f"\n  Sample city changes       : {cust_summary['city_changes'][:3]}")
    print(f"  Sample promotions         : {off_promos[:3]}")
    print(f"\n  Offline TX IDs: OFF{last_off_tx_id+1:07d} → OFF{last_off_tx_id+N_OFFLINE:07d}")
    print(f"  Online  TX IDs: ONL{last_onl_tx_id+1:07d} → ONL{last_onl_tx_id+N_ONLINE:07d}")
    print(f"  Batch ID      : {batch_ts}")
    print("\nIncremental load complete.\n")


if __name__ == "__main__":
    main()