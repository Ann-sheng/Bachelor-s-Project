
# Generate 500 000-row initial load as Parquet files, upload to cloud storage.

import os
import sys
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(_file_), "..", "src"))
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
    generate_dates, add_business_days,
    make_transaction_ids, make_entity_ids,
    make_emails, make_phones, make_salaries,
    calc_sale_amounts, weighted_choice,
)
from cloud_storage import get_cloud_client

# CONFIGURATION

SEED         = int(os.environ.get("SEED_INITIAL", 42))
N_OFFLINE    = 500_000
N_ONLINE     = 500_000
N_CUSTOMERS  = 14_000
N_EMP_OFF    = 150
N_EMP_ONL    = 80

START_DATE   = "2020-01-01"
END_DATE     = "2023-12-31"

OUTPUT_DIR   = Path(os.environ.get("DATA_INITIAL_DIR", "data/initial"))
REF_DIR      = Path(os.environ.get("DATA_REF_DIR",     "data/reference"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(SEED)

BATCH_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
LOAD_TYPE = "INITIAL"


# ENTITY BUILDERS  

def build_suppliers() -> pd.DataFrame:
    rows = []
    for i, (name, email, phone, contact, location) in enumerate(SUPPLIERS, 1):
        rows.append({
            "supplier_id": f"SUPP{i:02d}", "supplier_name": name,
            "supplier_email": email, "supplier_number": phone,
            "supplier_primary_contact": contact, "supplier_location": location,
        })
    return pd.DataFrame(rows)


def build_products(suppliers_df: pd.DataFrame) -> pd.DataFrame:
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
    rows = []
    for i, (city, state, phone, days, hours) in enumerate(STORE_BRANCHES, 1):
        rows.append({
            "store_branch_id": f"BRANCH{i:02d}",
            "store_branch_city": city, "store_branch_state": state,
            "store_branch_phone_number": phone,
            "store_branch_operating_days": days,
            "store_branch_operating_hours": hours,
        })
    return pd.DataFrame(rows)


def build_offline_employees(store_branches_df: pd.DataFrame, n: int) -> pd.DataFrame:
    branch_ids  = store_branches_df["store_branch_id"].tolist()
    branch_idx  = rng.integers(0, len(branch_ids), size=n)
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    titles      = rng.choice(OFFLINE_EMPLOYEE_TITLES, size=n)
    salaries    = make_salaries(titles.tolist(), OFFLINE_SALARY_RANGES, rng)
    emp_ids     = make_entity_ids(n, "EMPO")
    df = pd.DataFrame({
        "employee_id": emp_ids, "employee_firstname": first_names,
        "employee_lastname": last_names, "employee_title": titles,
        "employee_salary": salaries,
        "store_branch_id": [branch_ids[i] for i in branch_idx],
    })
    df["employee_email"]        = make_emails(df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng)
    df["employee_phone_number"] = make_phones(n, rng)
    return df.merge(store_branches_df, on="store_branch_id", how="left")


def build_online_employees(n: int) -> pd.DataFrame:
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    titles      = rng.choice(ONLINE_EMPLOYEE_TITLES, size=n)
    salaries    = make_salaries(titles.tolist(), ONLINE_SALARY_RANGES, rng)
    emp_ids     = make_entity_ids(n, "EMPW")
    df = pd.DataFrame({
        "employee_id": emp_ids, "employee_firstname": first_names,
        "employee_lastname": last_names, "employee_title": titles,
        "employee_salary": salaries,
    })
    df["employee_email"]        = make_emails(df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng)
    df["employee_phone_number"] = make_phones(n, rng)
    return df


def build_customers(n: int, start_id: int = 1) -> pd.DataFrame:
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    cust_ids    = make_entity_ids(n, "CUST", start=start_id)
    countries_list  = list(CUSTOMER_COUNTRIES.keys())
    country_weights = list(CUSTOMER_COUNTRIES.values())
    countries = rng.choice(countries_list, size=n,
                           p=np.array(country_weights) / sum(country_weights))
    cities = [
        rng.choice(US_CITIES if c == "United States" else INTL_CITIES.get(c, ["Unknown"]))
        for c in countries
    ]
    df = pd.DataFrame({
        "customer_id": cust_ids, "customer_firstname": first_names,
        "customer_lastname": last_names, "customer_country": countries,
        "customer_city": cities,
    })
    df["customer_email"]        = make_emails(df["customer_firstname"], df["customer_lastname"], EMAIL_DOMAINS, rng)
    df["customer_phone_number"] = make_phones(n, rng)
    return df


def build_shipping_options() -> pd.DataFrame:
    return pd.DataFrame([
        {"shipping_id": f"SHIP{i:02d}", "shipping_method": m, "shipping_carrier": c}
        for i, (m, c) in enumerate(SHIPPING_OPTIONS, 1)
    ])



# TRANSACTION BUILDERS

def build_offline_transactions(
    n, customers_df, products_df, employees_df, tx_id_start=1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} offline transactions...")
    cust_idx = rng.choice(len(customers_df), size=n)
    prod_probs = np.array([1/(i+1)**0.5 for i in range(len(products_df))])
    prod_idx = rng.choice(len(products_df), size=n, p=prod_probs / prod_probs.sum())
    emp_idx  = rng.choice(len(employees_df), size=n)

    tx_ids      = make_transaction_ids(n, "OFF", start=tx_id_start)
    tx_dates    = generate_dates(n, START_DATE, END_DATE, rng)
    quantities  = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts   = rng.choice(DISCOUNT_VALUES, size=n, p=np.array(DISCOUNT_WEIGHTS))
    sale_amounts = calc_sale_amounts(quantities, products_df["product_unit_price"].values[prod_idx], discounts)

    df = pd.DataFrame({
        "transaction_id":              tx_ids,
        "transaction_dt":              pd.to_datetime(tx_dates).strftime("%Y-%m-%d"),
        "transaction_shipped_dt":      None,         # offline: no shipping
        "transaction_delivery_dt":     None,         # offline: no delivery
        "transaction_payment_method":  weighted_choice(OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS, n, rng),
        "transaction_currency_paid":   weighted_choice(OFFLINE_CURRENCIES,      OFFLINE_CURR_WEIGHTS,    n, rng),
        "transaction_sales_channel":   "In-Store",
        "transaction_shipment_status": "Collected In-Store",
        "transaction_quantity_sold":   quantities,
        "transaction_discount_pct":    discounts,
        "transaction_sale_amount":     sale_amounts,
    })

    for col in ["customer_id","customer_firstname","customer_lastname","customer_email",
                "customer_phone_number","customer_country","customer_city"]:
        df[col] = customers_df[col].values[cust_idx]

    for col in ["product_id","product_category","product_name","product_unit_cost",
                "product_unit_price","product_warranty_period","supplier_id",
                "supplier_name","supplier_email","supplier_number",
                "supplier_primary_contact","supplier_location"]:
        df[col] = products_df[col].values[prod_idx]

    for col in ["employee_id","employee_firstname","employee_lastname","employee_title",
                "employee_email","employee_phone_number","employee_salary",
                "store_branch_id","store_branch_city","store_branch_state",
                "store_branch_phone_number","store_branch_operating_days",
                "store_branch_operating_hours"]:
        df[col] = employees_df[col].values[emp_idx]

    df["shipping_id"]      = None
    df["shipping_method"]  = None
    df["shipping_carrier"] = None

    return df


def build_online_transactions(
    n, customers_df, products_df, employees_df, shipping_df, tx_id_start=1,
) -> pd.DataFrame:
    print(f"  Generating {n:,} online transactions...")
    cust_idx  = rng.choice(len(customers_df), size=n)
    prod_probs = np.array([1/(i+1)**0.45 for i in range(len(products_df))])
    prod_idx  = rng.choice(len(products_df), size=n, p=prod_probs / prod_probs.sum())
    emp_idx   = rng.choice(len(employees_df), size=n)
    ship_idx  = rng.choice(len(shipping_df),  size=n)

    tx_ids      = make_transaction_ids(n, "ONL", start=tx_id_start)
    tx_dates    = generate_dates(n, START_DATE, END_DATE, rng)
    quantities  = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts   = rng.choice(DISCOUNT_VALUES, size=n, p=np.array(DISCOUNT_WEIGHTS))
    sale_amounts = calc_sale_amounts(quantities, products_df["product_unit_price"].values[prod_idx], discounts)
    ship_status  = weighted_choice(ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS, n, rng)

    tx_ts           = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates   = add_business_days(tx_ts, 1, 3, rng)
    delivery_dates  = add_business_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")
    mask_no_deliv   = pd.Series(ship_status).isin(["Pending", "Processing", "Cancelled"])
    delivery_arr    = delivery_dates.copy()
    delivery_arr[mask_no_deliv.values] = None

    df = pd.DataFrame({
        "transaction_id":              tx_ids,
        "transaction_dt":              tx_ts.dt.strftime("%Y-%m-%d"),
        "transaction_shipped_dt":      shipped_dates.dt.strftime("%Y-%m-%d"),
        "transaction_delivery_dt":     delivery_arr.values,
        "transaction_payment_method":  weighted_choice(ONLINE_PAYMENT_METHODS, ONLINE_PAYMENT_WEIGHTS, n, rng),
        "transaction_currency_paid":   weighted_choice(ONLINE_CURRENCIES,      ONLINE_CURR_WEIGHTS,    n, rng),
        "transaction_sales_channel":   weighted_choice(ONLINE_CHANNELS,        ONLINE_CHANNEL_WEIGHTS, n, rng),
        "transaction_shipment_status": ship_status,
        "transaction_quantity_sold":   quantities,
        "transaction_discount_pct":    discounts,
        "transaction_sale_amount":     sale_amounts,
    })

    for col in ["customer_id","customer_firstname","customer_lastname","customer_email",
                "customer_phone_number","customer_country","customer_city"]:
        df[col] = customers_df[col].values[cust_idx]

    for col in ["product_id","product_category","product_name","product_unit_cost",
                "product_unit_price","product_warranty_period","supplier_id",
                "supplier_name","supplier_email","supplier_number",
                "supplier_primary_contact","supplier_location"]:
        df[col] = products_df[col].values[prod_idx]

    for col in ["employee_id","employee_firstname","employee_lastname","employee_title",
                "employee_email","employee_phone_number","employee_salary"]:
        df[col] = employees_df[col].values[emp_idx]

    for col in ["shipping_id","shipping_method","shipping_carrier"]:
        df[col] = shipping_df[col].values[ship_idx]

    for col in ["store_branch_id","store_branch_city","store_branch_state",
                "store_branch_phone_number","store_branch_operating_days",
                "store_branch_operating_hours"]:
        df[col] = None

    return df

# PARQUET HELPERS


def add_batch_metadata(df: pd.DataFrame, load_type: str, batch_id: str) -> pd.DataFrame:
    """
    Add audit columns used by the staging table.
    These are stored IN the Parquet file so every row carries its lineage.
    """
    df = df.copy()
    df.insert(0, "batch_id",   batch_id)
    df.insert(1, "load_type",  load_type) 
    df.insert(2, "batch_dt",   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    return df


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame as snappy-compressed Parquet."""
    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )
    mb = path.stat().st_size / 1e6
    print(f"  Saved {path.name}  ({len(df):,} rows, {mb:.1f} MB)")


def upload_and_report(cloud, local_path: Path, remote_key: str) -> None:
    uri = cloud.upload(str(local_path), remote_key)
    print(f"  Uploaded → {uri}")



# MAIN

def main():
    print("\n╔═══════════════════╗")
    print("║  Initial Load       ║")
    print("╚═════════════════════╝\n")

    cloud = get_cloud_client()

    #  Build master entities
    print("▶ Building master entities...")
    suppliers_df      = build_suppliers()
    products_df       = build_products(suppliers_df)
    store_branches_df = build_store_branches()
    employees_off_df  = build_offline_employees(store_branches_df, N_EMP_OFF)
    employees_onl_df  = build_online_employees(N_EMP_ONL)
    customers_df      = build_customers(N_CUSTOMERS)
    shipping_df       = build_shipping_options()

    print(f"  ✓ {len(suppliers_df)} suppliers | {len(products_df)} products | "
          f"{len(store_branches_df)} branches | {len(employees_off_df)} offline emps | "
          f"{len(employees_onl_df)} online emps | {len(customers_df):,} customers | "
          f"{len(shipping_df)} shipping options")

    # Build transactions
    print("\n Generating transactions...")
    offline_df = build_offline_transactions(
        N_OFFLINE, customers_df, products_df, employees_off_df, tx_id_start=1
    )
    online_df = build_online_transactions(
        N_ONLINE, customers_df, products_df, employees_onl_df, shipping_df, tx_id_start=1
    )

    # Add batch metadata 
    offline_df = add_batch_metadata(offline_df, LOAD_TYPE, BATCH_TS)
    online_df  = add_batch_metadata(online_df,  LOAD_TYPE, BATCH_TS)

    # Save Parquet locally
    print("\n Saving Parquet files locally...")
    off_file   = OUTPUT_DIR / f"offline_initial_{BATCH_TS}.parquet"
    onl_file   = OUTPUT_DIR / f"online_initial_{BATCH_TS}.parquet"
    save_parquet(offline_df, off_file)
    save_parquet(online_df,  onl_file)

    #  Upload to cloud 
    print("\n Uploading to cloud storage...")
    off_key = f"offline/initial/offline_initial_{BATCH_TS}.parquet"
    onl_key = f"online/initial/online_initial_{BATCH_TS}.parquet"
    upload_and_report(cloud, off_file, off_key)
    upload_and_report(cloud, onl_file, onl_key)

    #  Save reference data for incremental generator 
    print("\n Saving reference data (master_data.pkl)...")
    master = {
        "suppliers_df":      suppliers_df,
        "products_df":       products_df,
        "store_branches_df": store_branches_df,
        "employees_off_df":  employees_off_df,
        "employees_onl_df":  employees_onl_df,
        "customers_df":      customers_df,
        "shipping_df":       shipping_df,
        "last_customer_id":  N_CUSTOMERS,
        "last_product_id":   len(products_df),
        "last_off_tx_id":    N_OFFLINE,
        "last_onl_tx_id":    N_ONLINE,
        "initial_off_key":   off_key,
        "initial_onl_key":   onl_key,
    }
    ref_path = REF_DIR / "master_data.pkl"
    import pickle
    with open(ref_path, "wb") as f:
        pickle.dump(master, f)
    print(f"  ✓ Saved {ref_path}")

    # Quick stats 
    print("\n── Summary ──────────────────────────────────────")
    print(f"  Batch ID:      {BATCH_TS}")
    print(f"  Offline rows:  {len(offline_df):,}  |  Revenue: ${offline_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Online rows:   {len(online_df):,}   |  Revenue: ${online_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Date range:    {START_DATE} → {END_DATE}")
    print(f"  Cloud prefix:  offline/initial/  &  online/initial/")
    print("\nInitial load complete.\n")


if _name_ == "_main_":
    main()
