

# Generate 250 000-row incremental Parquet files with deliberate SCD2 change scenarios, upload to cloud.


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
    PRODUCT_CATALOG, FIRST_NAMES, LAST_NAMES, EMAIL_DOMAINS,
    US_CITIES, INTL_CITIES, CUSTOMER_COUNTRIES,
    OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS,
    ONLINE_PAYMENT_METHODS,  ONLINE_PAYMENT_WEIGHTS,
    OFFLINE_CURRENCIES,      OFFLINE_CURR_WEIGHTS,
    ONLINE_CURRENCIES,       ONLINE_CURR_WEIGHTS,
    ONLINE_CHANNELS,         ONLINE_CHANNEL_WEIGHTS,
    ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS,
    DISCOUNT_VALUES,          DISCOUNT_WEIGHTS,
    OFFLINE_SALARY_RANGES,   ONLINE_SALARY_RANGES,
)
from helpers import (
    generate_dates, add_business_days,
    make_transaction_ids, make_entity_ids,
    make_emails, make_phones, make_salaries,
    calc_sale_amounts, weighted_choice,
)
from cloud_storage import get_cloud_client

# Configuration
SEED_INCR   = int(os.environ.get("SEED_INCREMENTAL", 99))
N_OFFLINE   = 250_000
N_ONLINE    = 250_000
START_DATE  = "2024-01-01"
END_DATE    = "2024-06-30"

# SCD2 volumes 
SCD2_CUSTOMER_CITY_CHANGES    = 400
SCD2_CUSTOMER_CONTACT_CHANGES = 200
SCD2_EMPLOYEE_PROMOTIONS      = 80
SCD2_EMPLOYEE_RAISES          = 40
NEW_CUSTOMERS                 = 1_500
NEW_PRODUCTS                  = 15

REF_DIR    = Path(os.environ.get("DATA_REF_DIR",         "data/reference"))
OUTPUT_DIR = Path(os.environ.get("DATA_INCREMENTAL_DIR", "data/incremental"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(SEED_INCR)

BATCH_TS  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
LOAD_TYPE = "INCREMENTAL"


# SCD2 CHANGE SCENARIOS

def apply_customer_scd2_changes(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:

    df    = df.copy()
    n     = len(df)
    summary = {}

    #  City change (customer moved)
    city_idx = rng.choice(n, size=SCD2_CUSTOMER_CITY_CHANGES, replace=False)
    changed_cities = []
    for idx in city_idx.tolist():
        country = df.at[idx, "customer_country"]
        if country == "United States":
            choices = [c for c in US_CITIES if c != df.at[idx, "customer_city"]]
            new_city = rng.choice(choices) if choices else rng.choice(US_CITIES)
        else:
            options  = INTL_CITIES.get(country, US_CITIES)
            new_city = rng.choice(options)
        old_city = df.at[idx, "customer_city"]
        df.at[idx, "customer_city"] = new_city
        changed_cities.append((df.at[idx, "customer_id"], old_city, new_city))
    summary["city_changes"] = changed_cities[:5]  

    # Email + phone change
    used_idx  = set(city_idx.tolist())
    avail_idx = [i for i in range(n) if i not in used_idx]
    contact_idx = rng.choice(avail_idx, size=SCD2_CUSTOMER_CONTACT_CHANGES, replace=False)
    for idx in contact_idx.tolist():
        fn, ln = df.at[idx, "customer_firstname"], df.at[idx, "customer_lastname"]
        sep    = rng.choice(["", ".", "_"])
        domain = rng.choice(EMAIL_DOMAINS)
        suffix = int(rng.integers(1, 999))
        df.at[idx, "customer_email"]        = f"{fn.lower()}{sep}{ln.lower()}{suffix}@{domain}"
        df.at[idx, "customer_phone_number"] = make_phones(1, rng)[0]

    summary["total_changed"] = SCD2_CUSTOMER_CITY_CHANGES + SCD2_CUSTOMER_CONTACT_CHANGES
    return df, summary


def apply_employee_offline_scd2(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:

    # Simulate promotions for offline employees.
    df      = df.copy()
    n       = len(df)
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
    promo_idx = rng.choice(n, size=SCD2_EMPLOYEE_PROMOTIONS, replace=False)
    changes   = []
    for idx in promo_idx.tolist():
        old_title = df.at[idx, "employee_title"]
        new_title = promo_map.get(old_title, old_title)
        if new_title != old_title:
            lo, hi = OFFLINE_SALARY_RANGES.get(new_title, (30_000, 50_000))
            new_salary = int(rng.integers(lo, hi) / 500) * 500
            df.at[idx, "employee_title"]  = new_title
            df.at[idx, "employee_salary"] = new_salary
            changes.append((df.at[idx, "employee_id"], old_title, new_title))
    return df, changes


def apply_employee_online_scd2(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    #Salary raises for online employees.
    df      = df.copy()
    n       = len(df)
    raise_idx = rng.choice(n, size=SCD2_EMPLOYEE_RAISES, replace=False)
    changes   = []
    for idx in raise_idx.tolist():
        old_salary = df.at[idx, "employee_salary"]
        raise_pct  = int(rng.choice([3, 5, 7, 10]))
        new_salary = int(old_salary * (1 + raise_pct / 100) / 500) * 500
        df.at[idx, "employee_salary"] = new_salary
        changes.append((df.at[idx, "employee_id"], old_salary, new_salary))
    return df, changes


def add_new_customers(df: pd.DataFrame, n: int, last_id: int) -> pd.DataFrame:
    fn          = rng.choice(FIRST_NAMES, size=n)
    ln          = rng.choice(LAST_NAMES,  size=n)
    cust_ids    = make_entity_ids(n, "CUST", start=last_id + 1)
    countries   = list(CUSTOMER_COUNTRIES.keys())
    cw          = np.array(list(CUSTOMER_COUNTRIES.values()))
    new_countries = rng.choice(countries, size=n, p=cw / cw.sum())
    cities = [
        rng.choice(US_CITIES if c == "United States" else INTL_CITIES.get(c, ["Unknown"]))
        for c in new_countries
    ]
    df_new = pd.DataFrame({
        "customer_id": cust_ids, "customer_firstname": fn,
        "customer_lastname": ln, "customer_country": new_countries,
        "customer_city": cities,
    })
    df_new["customer_email"]        = make_emails(df_new["customer_firstname"], df_new["customer_lastname"], EMAIL_DOMAINS, rng)
    df_new["customer_phone_number"] = make_phones(n, rng)
    return pd.concat([df, df_new], ignore_index=True)


def add_new_products(df: pd.DataFrame, last_id: int) -> pd.DataFrame:
    new_items = [
        ("Vintage Corduroy Blazer",         38, 108, 24, "Jackets & Outerwear"),
        ("90s Neon Ski Pants",              25,  75,  6, "Activewear"),
        ("Patchwork Denim Jacket",          32,  92, 18, "Jackets & Outerwear"),
        ("Vintage Lace Cami Top",           12,  36,  0, "T-Shirts & Tops"),
        ("Retro Terrycloth Polo",           14,  44,  6, "T-Shirts & Tops"),
        ("80s Satin Bomber Jacket",         42, 118, 24, "Jackets & Outerwear"),
        ("Wide Brim Felt Hat",              11,  34,  6, "Accessories"),
        ("Vintage Platform Boots",          48, 142, 24, "Footwear"),
        ("70s Patchwork Maxi Skirt",        22,  66,  6, "Dresses & Skirts"),
        ("Retro Velour Tracksuit Top",      19,  58, 12, "Activewear"),
        ("Vintage Silk Pyjama Shirt",       20,  60,  6, "Shirts & Blouses"),
        ("Barrel Leg Denim Jeans",          24,  72, 12, "Denim"),
        ("Chunky Soled Loafers",            36, 108, 24, "Footwear"),
        ("Retro Crochet Bucket Bag",        18,  54, 12, "Accessories"),
        ("Vintage Metallic Mini Skirt",     20,  60,  6, "Dresses & Skirts"),
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
            "supplier_id": sup_id, "supplier_name": sup_row["supplier_name"],
            "supplier_email": sup_row["supplier_email"], "supplier_number": sup_row["supplier_number"],
            "supplier_primary_contact": sup_row["supplier_primary_contact"],
            "supplier_location": sup_row["supplier_location"],
        })
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)



# TRANSACTION BUILDERS

def build_offline_incr(n, customers_df, products_df, employees_df, tx_id_start) -> pd.DataFrame:
    print(f"  Generating {n:,} incremental offline transactions...")
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
    for col in ["shipping_id","shipping_method","shipping_carrier"]:
        df[col] = None
    return df


def build_online_incr(n, customers_df, products_df, employees_df, shipping_df, tx_id_start) -> pd.DataFrame:
    print(f"  Generating {n:,} incremental online transactions...")
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

    tx_ts          = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates  = add_business_days(tx_ts, 1, 3, rng)
    delivery_dates = add_business_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")
    mask_no_deliv  = pd.Series(ship_status).isin(["Pending", "Processing", "Cancelled"])
    delivery_arr   = delivery_dates.copy()
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
    df = df.copy()
    df.insert(0, "batch_id",  batch_id)
    df.insert(1, "load_type", load_type)
    df.insert(2, "batch_dt",  datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    return df


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    mb = path.stat().st_size / 1e6
    print(f"  Saved {path.name}  ({len(df):,} rows, {mb:.1f} MB)")



# MAIN

def main():
    print("\n╔════════════════════════════╗")
    print("║   Incremental Load           ║")
    print("╚══════════════════════════════╝\n")

    cloud = get_cloud_client()

    #  Load reference from initial run 
    ref_path = REF_DIR / "master_data.pkl"
    if not ref_path.exists():
        raise FileNotFoundError(
            f"{ref_path} not found. Run generate_initial_load.py first."
        )
    with open(ref_path, "rb") as f:
        import pickle
        ref = pickle.load(f)

    suppliers_df      = ref["suppliers_df"]
    products_df       = ref["products_df"]
    store_branches_df = ref["store_branches_df"]
    employees_off_df  = ref["employees_off_df"]
    employees_onl_df  = ref["employees_onl_df"]
    customers_df      = ref["customers_df"]
    shipping_df       = ref["shipping_df"]
    last_cust_id      = ref["last_customer_id"]
    last_prod_id      = ref["last_product_id"]
    last_off_tx_id    = ref["last_off_tx_id"]
    last_onl_tx_id    = ref["last_onl_tx_id"]

    # Apply SCD2 changes
    print("▶ Applying SCD2 change scenarios...")
    customers_df, cust_summary  = apply_customer_scd2_changes(customers_df)
    employees_off_df, off_promos = apply_employee_offline_scd2(employees_off_df)
    employees_onl_df, onl_raises = apply_employee_online_scd2(employees_onl_df)

    # Add new entities
    print("\n▶ Adding new entities...")
    customers_df = add_new_customers(customers_df, NEW_CUSTOMERS, last_cust_id)
    products_df  = add_new_products(products_df, last_prod_id)
    print(f"  ✓ +{NEW_CUSTOMERS} new customers | +{NEW_PRODUCTS} new products")

    # Build transactions
    print("\n▶ Generating transactions (date range: {START_DATE} → {END_DATE})...")
    offline_df = build_offline_incr(
        N_OFFLINE, customers_df, products_df, employees_off_df,
        tx_id_start=last_off_tx_id + 1,
    )
    online_df = build_online_incr(
        N_ONLINE, customers_df, products_df, employees_onl_df, shipping_df,
        tx_id_start=last_onl_tx_id + 1,
    )

    # Add batch metadata
    offline_df = add_batch_metadata(offline_df, LOAD_TYPE, BATCH_TS)
    online_df  = add_batch_metadata(online_df,  LOAD_TYPE, BATCH_TS)

    # Save Parquet 
    print("\n Saving Parquet files...")
    off_file = OUTPUT_DIR / f"offline_incremental_{BATCH_TS}.parquet"
    onl_file = OUTPUT_DIR / f"online_incremental_{BATCH_TS}.parquet"
    save_parquet(offline_df, off_file)
    save_parquet(online_df,  onl_file)

    #  Upload to cloud 
    print("\n Uploading to cloud storage...")
    off_key = f"offline/incremental/offline_incremental_{BATCH_TS}.parquet"
    onl_key = f"online/incremental/online_incremental_{BATCH_TS}.parquet"
    uri_off = cloud.upload(str(off_file), off_key)
    uri_onl = cloud.upload(str(onl_file), onl_key)
    print(f"  Uploaded → {uri_off}")
    print(f"  Uploaded → {uri_onl}")

    # SCD2 change summary 
    print("\n── SCD2 Changes Summary ────────────────────────────────")
    print(f"  Customers city changed:     {SCD2_CUSTOMER_CITY_CHANGES}")
    print(f"  Customers contact changed:  {SCD2_CUSTOMER_CONTACT_CHANGES}")
    print(f"  Offline employees promoted: {SCD2_EMPLOYEE_PROMOTIONS}")
    print(f"  Online employees raised:    {SCD2_EMPLOYEE_RAISES}")
    print(f"  New customers (new biz key):{NEW_CUSTOMERS}")
    print(f"  New products:               {NEW_PRODUCTS}")
    print(f"\n  Sample city changes:        {cust_summary['city_changes'][:3]}")
    print(f"  Sample promotions:          {off_promos[:3]}")
    print(f"\n  These changed rows carry new attribute values but same")
    print(f"  business keys. BL_3NF LOAD_CE_CUSTOMERS_SCD will detect")
    print(f"  the diff against the active SCD2 row and create new versions.")

    print(f"\n  Offline TX IDs: OFF{last_off_tx_id+1:07d} → OFF{last_off_tx_id+N_OFFLINE:07d}")
    print(f"  Online  TX IDs: ONL{last_onl_tx_id+1:07d} → ONL{last_onl_tx_id+N_ONLINE:07d}")
    print(f"  Batch ID: {BATCH_TS}")
    print("\n Incremental load complete.\n")


if _name_ == "_main_":
    main()