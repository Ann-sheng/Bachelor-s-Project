#!/usr/bin/env python3
# ================================================================
# FILE    : generate_incremental_load.py
# PURPOSE : Generate 250 000-row incremental CSVs for testing
#           the incremental loading pipeline including SCD2 changes.
#
# SCD2 TEST SCENARIOS INCLUDED:
#   ① ~400 customers changed city (moved)         → SCD2 new version
#   ② ~200 customers changed email + phone         → SCD2 new version
#   ③ ~80 offline employees got promoted           → SCD2 new version
#   ④ ~40 online employees received salary raise   → SCD2 new version
#   ⑤ 15 new products added to catalog             → new rows in CE_PRODUCTS
#   ⑥ 1 500 brand-new customers added             → new rows in CE_CUSTOMERS_SCD
#
# OUTPUTS:
#   data/incremental/offline_sales_incremental.csv
#   data/incremental/online_sales_incremental.csv
#
# USAGE:
#   python generate_incremental_load.py
#   (run AFTER generate_initial_load.py)
# ================================================================

import os
import sys
import pickle
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
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
    OFFLINE_EMPLOYEE_TITLES,
)
from helpers import (
    generate_dates, add_business_days,
    make_transaction_ids, make_entity_ids, make_short_ids,
    make_emails, make_phones, make_salaries,
    calc_sale_amounts, weighted_choice,
)

# ── Configuration ─────────────────────────────────────────────────
SEED_INCR           = 99          # different seed from initial
N_OFFLINE           = 250_000
N_ONLINE            = 250_000

# Incremental date range: continues after initial load
START_DATE          = "2024-01-01"
END_DATE            = "2024-06-30"

# SCD2 change volumes
N_CUSTOMERS_CHANGED_CITY    = 400
N_CUSTOMERS_CHANGED_CONTACT = 200
N_EMPLOYEES_PROMOTED        = 80
N_EMPLOYEES_RAISE           = 40
N_NEW_CUSTOMERS             = 1_500
N_NEW_PRODUCTS              = 15

OUTPUT_INCR = "data/incremental"
REF_PATH    = "data/reference/master_data.pkl"
os.makedirs(OUTPUT_INCR, exist_ok=True)

rng = np.random.default_rng(SEED_INCR)


# ════════════════════════════════════════════════════════════════
# 1. LOAD REFERENCE DATA
# ════════════════════════════════════════════════════════════════

def load_reference() -> dict:
    if not os.path.exists(REF_PATH):
        raise FileNotFoundError(
            f"Reference data not found at '{REF_PATH}'.\n"
            "Run generate_initial_load.py first."
        )
    with open(REF_PATH, "rb") as f:
        return pickle.load(f)


# ════════════════════════════════════════════════════════════════
# 2. APPLY SCD2 CHANGES
# ════════════════════════════════════════════════════════════════

def apply_customer_changes(customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create changed-customer rows.
    These will TRIGGER SCD2 new versions when loaded into BL_3NF.

    Returns the full customers_df with some rows having updated values.
    The changed rows keep the SAME customer_id (business key),
    so BL_3NF detects the change and creates a new SCD2 version.
    """
    df = customers_df.copy()
    n  = len(df)

    # ① City change: customer moved
    city_change_idx = rng.choice(n, size=N_CUSTOMERS_CHANGED_CITY, replace=False)
    for idx in city_change_idx:
        country = df.at[idx, "customer_country"]
        if country == "United States":
            new_city = rng.choice([c for c in US_CITIES
                                   if c != df.at[idx, "customer_city"]])
        else:
            options = INTL_CITIES.get(country, US_CITIES)
            new_city = rng.choice(options)
        df.at[idx, "customer_city"] = new_city

    # ② Email + phone change: customer updated contact info
    contact_change_idx = rng.choice(
        [i for i in range(n) if i not in city_change_idx],  # don't double-change
        size=N_CUSTOMERS_CHANGED_CONTACT,
        replace=False
    )
    for idx in contact_change_idx:
        fn = df.at[idx, "customer_firstname"]
        ln = df.at[idx, "customer_lastname"]
        sep = rng.choice(["", ".", "_"])
        domain = rng.choice(EMAIL_DOMAINS)
        suffix = rng.integers(1, 999)
        df.at[idx, "customer_email"]        = f"{fn.lower()}{sep}{ln.lower()}{suffix}@{domain}"
        df.at[idx, "customer_phone_number"] = make_phones(1, rng)[0]

    changed_mask = pd.Series(False, index=df.index)
    changed_mask.iloc[city_change_idx]    = True
    changed_mask.iloc[contact_change_idx] = True

    print(f"  ✓ {N_CUSTOMERS_CHANGED_CITY} customers: city changed (moved)")
    print(f"  ✓ {N_CUSTOMERS_CHANGED_CONTACT} customers: email + phone changed")

    return df, changed_mask


def apply_employee_changes_offline(employees_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate promotions and salary raises for offline employees.
    Changed rows keep same employee_id → BL_3NF creates SCD2 version.
    """
    df = pd.DataFrame(employees_df)
    n  = len(df)

    # ③ Promotions: title + salary increase
    promo_idx = rng.choice(n, size=N_EMPLOYEES_PROMOTED, replace=False)
    for idx in promo_idx:
        current_title = df.at[idx, "employee_title"]
        # Move up one level in title hierarchy
        promo_map = {
            "Stock Associate":          "Sales Associate",
            "Cashier":                  "Sales Associate",
            "Sales Associate":          "Senior Sales Associate",
            "Senior Sales Associate":   "Brand Stylist",
            "Brand Stylist":            "Shift Supervisor",
            "Shift Supervisor":         "Floor Manager",
            "Floor Manager":            "Assistant Manager",
            "Assistant Manager":        "Store Manager",
        }
        new_title = promo_map.get(current_title, current_title)
        if new_title != current_title:
            df.at[idx, "employee_title"] = new_title
            lo, hi = OFFLINE_SALARY_RANGES.get(new_title, (30_000, 50_000))
            df.at[idx, "employee_salary"] = int(rng.integers(lo, hi) / 500) * 500

    print(f"  ✓ {N_EMPLOYEES_PROMOTED} offline employees promoted (title + salary SCD2 change)")
    return df


def apply_employee_changes_online(employees_df: pd.DataFrame) -> pd.DataFrame:
    """Salary raise for online employees (no title change)."""
    df = pd.DataFrame(employees_df)
    n  = len(df)

    raise_idx = rng.choice(n, size=N_EMPLOYEES_RAISE, replace=False)
    for idx in raise_idx:
        current_salary = df.at[idx, "employee_salary"]
        raise_pct = rng.choice([3, 5, 7, 10])
        new_salary = int(current_salary * (1 + raise_pct / 100) / 500) * 500
        df.at[idx, "employee_salary"] = new_salary

    print(f"  ✓ {N_EMPLOYEES_RAISE} online employees received salary raise (SCD2 change)")
    return df


def add_new_customers(existing_df: pd.DataFrame, n: int, last_id: int) -> pd.DataFrame:
    """Create brand-new customers not seen in initial load."""

    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    cust_ids    = make_entity_ids(n, "CUST", start=last_id + 1)

    countries_list  = list(CUSTOMER_COUNTRIES.keys())
    country_weights = list(CUSTOMER_COUNTRIES.values())
    countries = rng.choice(countries_list, size=n,
                           p=np.array(country_weights) / sum(country_weights))

    cities = []
    for c in countries:
        if c == "United States":
            cities.append(rng.choice(US_CITIES))
        else:
            cities.append(rng.choice(INTL_CITIES.get(c, ["Unknown"])))

    df_new = pd.DataFrame({
        "customer_id":           cust_ids,
        "customer_firstname":    first_names,
        "customer_lastname":     last_names,
        "customer_country":      countries,
        "customer_city":         cities,
    })
    df_new["customer_email"]        = make_emails(
        df_new["customer_firstname"], df_new["customer_lastname"], EMAIL_DOMAINS, rng
    )
    df_new["customer_phone_number"] = make_phones(n, rng)

    print(f"  ✓ {n} brand-new customers added (IDs CUST{last_id+1:06d}–CUST{last_id+n:06d})")
    return pd.concat([existing_df, df_new], ignore_index=True)


def add_new_products(products_df: pd.DataFrame, last_id: int, n: int = 15) -> pd.DataFrame:
    """
    Add a small batch of new products to the catalog.
    These are new vintage pieces Retrograde Collective just sourced.
    """
    new_items = [
        ("Vintage Corduroy Blazer",             38,  108, 24, "Jackets & Outerwear"),
        ("90s Neon Ski Pants",                  25,   75,  6, "Activewear"),
        ("Patchwork Denim Jacket",              32,   92, 18, "Jackets & Outerwear"),
        ("Vintage Lace Cami Top",               12,   36,  0, "T-Shirts & Tops"),
        ("Retro Terrycloth Polo",               14,   44,  6, "T-Shirts & Tops"),
        ("80s Satin Bomber Jacket",             42,  118, 24, "Jackets & Outerwear"),
        ("Wide Brim Felt Hat",                  11,   34,  6, "Accessories"),
        ("Vintage Platform Boots",              48,  142, 24, "Footwear"),
        ("70s Patchwork Maxi Skirt",            22,   66,  6, "Dresses & Skirts"),
        ("Retro Velour Tracksuit Top",          19,   58, 12, "Activewear"),
        ("Vintage Silk Pyjama Shirt",           20,   60,  6, "Shirts & Blouses"),
        ("Barrel Leg Denim Jeans",              24,   72, 12, "Denim"),
        ("Chunky Soled Loafers",                36,  108, 24, "Footwear"),
        ("Retro Crochet Bucket Bag",            18,   54, 12, "Accessories"),
        ("Vintage Metallic Mini Skirt",         20,   60,  6, "Dresses & Skirts"),
    ]

    existing_sup_ids = products_df["supplier_id"].unique().tolist()
    new_rows = []
    for i, (name, cost, price, warranty, category) in enumerate(new_items[:n], start=1):
        sup_id = rng.choice(existing_sup_ids)
        sup_row = products_df[products_df["supplier_id"] == sup_id].iloc[0]
        new_rows.append({
            "product_id":               f"PROD{str(last_id + i).zfill(3)}",
            "product_category":         category,
            "product_name":             name,
            "product_unit_cost":        cost,
            "product_unit_price":       price,
            "product_warranty_period":  warranty,
            "supplier_id":              sup_id,
            "supplier_name":            sup_row["supplier_name"],
            "supplier_email":           sup_row["supplier_email"],
            "supplier_number":          sup_row["supplier_number"],
            "supplier_primary_contact": sup_row["supplier_primary_contact"],
            "supplier_location":        sup_row["supplier_location"],
        })

    df_new = pd.DataFrame(new_rows)
    combined = pd.concat([products_df, df_new], ignore_index=True)
    print(f"  ✓ {n} new products added (IDs PROD{last_id+1:03d}–PROD{last_id+n:03d})")
    return combined


# ════════════════════════════════════════════════════════════════
# 3. TRANSACTION BUILDERS  (same signature as initial, reused)
# ════════════════════════════════════════════════════════════════

def build_offline_transactions_incr(
    n:              int,
    customers_df:   pd.DataFrame,
    products_df:    pd.DataFrame,
    employees_df:   pd.DataFrame,
    tx_id_start:    int,
) -> pd.DataFrame:

    print(f"  Sampling {n:,} offline transactions (incremental)...")

    cust_idx = rng.choice(len(customers_df), size=n)
    prod_idx = rng.choice(
        len(products_df), size=n,
        p=np.array([1/(i+1)**0.5 for i in range(len(products_df))])
         / sum(1/(i+1)**0.5 for i in range(len(products_df)))
    )
    emp_idx  = rng.choice(len(employees_df), size=n)

    tx_ids      = make_transaction_ids(n, "OFF", start=tx_id_start)
    tx_dates    = generate_dates(n, START_DATE, END_DATE, rng)
    quantities  = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts   = rng.choice(DISCOUNT_VALUES, size=n, p=np.array(DISCOUNT_WEIGHTS))

    unit_prices  = products_df["product_unit_price"].values[prod_idx]
    sale_amounts = calc_sale_amounts(quantities, unit_prices, discounts)

    pay_methods  = weighted_choice(OFFLINE_PAYMENT_METHODS, OFFLINE_PAYMENT_WEIGHTS, n, rng)
    currencies   = weighted_choice(OFFLINE_CURRENCIES, OFFLINE_CURR_WEIGHTS, n, rng)

    cust_cols = ["customer_id","customer_firstname","customer_lastname",
                 "customer_email","customer_phone_number",
                 "customer_country","customer_city"]
    prod_cols = ["product_id","product_category","product_name",
                 "product_unit_cost","product_unit_price","product_warranty_period",
                 "supplier_id","supplier_name","supplier_email",
                 "supplier_number","supplier_primary_contact","supplier_location"]
    emp_cols  = ["employee_id","employee_firstname","employee_lastname",
                 "employee_title","employee_email","employee_phone_number",
                 "employee_salary",
                 "store_branch_id","store_branch_city","store_branch_state",
                 "store_branch_phone_number",
                 "store_branch_operating_days","store_branch_operating_hours"]

    df = pd.DataFrame({
        "transaction_id":              tx_ids,
        "transaction_dt":              pd.to_datetime(tx_dates).strftime("%Y-%m-%d"),
        "transaction_payment_method":  pay_methods,
        "transaction_currency_paid":   currencies,
        "transaction_sales_channel":   "In-Store",
        "transaction_shipment_status": "Collected In-Store",
        "transaction_quantity_sold":   quantities,
        "transaction_discount_pct":    discounts,
        "transaction_sale_amount":     sale_amounts,
    })
    for col in cust_cols: df[col] = customers_df[col].values[cust_idx]
    for col in prod_cols: df[col] = products_df[col].values[prod_idx]
    for col in emp_cols:  df[col] = employees_df[col].values[emp_idx]
    return df


def build_online_transactions_incr(
    n:              int,
    customers_df:   pd.DataFrame,
    products_df:    pd.DataFrame,
    employees_df:   pd.DataFrame,
    shipping_df:    pd.DataFrame,
    tx_id_start:    int,
) -> pd.DataFrame:

    print(f"  Sampling {n:,} online transactions (incremental)...")

    cust_idx  = rng.choice(len(customers_df), size=n)
    prod_idx  = rng.choice(
        len(products_df), size=n,
        p=np.array([1/(i+1)**0.45 for i in range(len(products_df))])
         / sum(1/(i+1)**0.45 for i in range(len(products_df)))
    )
    emp_idx   = rng.choice(len(employees_df), size=n)
    ship_idx  = rng.choice(len(shipping_df),  size=n)

    tx_ids      = make_transaction_ids(n, "ONL", start=tx_id_start)
    tx_dates    = generate_dates(n, START_DATE, END_DATE, rng)
    quantities  = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts   = rng.choice(DISCOUNT_VALUES, size=n, p=np.array(DISCOUNT_WEIGHTS))

    unit_prices  = products_df["product_unit_price"].values[prod_idx]
    sale_amounts = calc_sale_amounts(quantities, unit_prices, discounts)

    pay_methods  = weighted_choice(ONLINE_PAYMENT_METHODS,  ONLINE_PAYMENT_WEIGHTS,  n, rng)
    currencies   = weighted_choice(ONLINE_CURRENCIES,       ONLINE_CURR_WEIGHTS,     n, rng)
    channels     = weighted_choice(ONLINE_CHANNELS,         ONLINE_CHANNEL_WEIGHTS,  n, rng)
    ship_status  = weighted_choice(ONLINE_SHIPMENT_STATUSES,ONLINE_SHIPMENT_WEIGHTS, n, rng)

    tx_ts         = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates = add_business_days(tx_ts, 1, 3, rng)
    delivery_dates= add_business_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")
    shipped_str   = shipped_dates.dt.strftime("%Y-%m-%d")

    mask_no_deliv = pd.Series(ship_status).isin(["Pending", "Processing", "Cancelled"])
    delivery_arr  = pd.array(delivery_dates, dtype=object)
    delivery_arr[mask_no_deliv.values] = None

    cust_cols = ["customer_id","customer_firstname","customer_lastname",
                 "customer_email","customer_phone_number",
                 "customer_country","customer_city"]
    prod_cols = ["product_id","product_category","product_name",
                 "product_unit_cost","product_unit_price","product_warranty_period",
                 "supplier_id","supplier_name","supplier_email",
                 "supplier_number","supplier_primary_contact","supplier_location"]
    emp_cols  = ["employee_id","employee_firstname","employee_lastname",
                 "employee_title","employee_email","employee_phone_number",
                 "employee_salary"]
    ship_cols = ["shipping_id","shipping_method","shipping_carrier"]

    df = pd.DataFrame({
        "transaction_id":               tx_ids,
        "transaction_dt":               tx_ts.dt.strftime("%Y-%m-%d"),
        "transaction_shipped_dt":       shipped_str,
        "transaction_delivery_dt":      delivery_arr,
        "transaction_payment_method":   pay_methods,
        "transaction_currency_paid":    currencies,
        "transaction_sales_channel":    channels,
        "transaction_shipment_status":  ship_status,
        "transaction_quantity_sold":    quantities,
        "transaction_discount_pct":     discounts,
        "transaction_sale_amount":      sale_amounts,
    })
    for col in cust_cols:  df[col] = customers_df[col].values[cust_idx]
    for col in prod_cols:  df[col] = products_df[col].values[prod_idx]
    for col in emp_cols:   df[col] = employees_df[col].values[emp_idx]
    for col in ship_cols:  df[col] = shipping_df[col].values[ship_idx]
    return df


# ════════════════════════════════════════════════════════════════
# 4. MAIN
# ════════════════════════════════════════════════════════════════

def main():
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║  Retrograde Collective — Incremental Load            ║")
    print("║  Target: 250 000 offline + 250 000 online            ║")
    print("║  Includes SCD2 change scenarios                      ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    # ── Load reference data from initial run ─────────────────
    print("▶ Loading reference data from initial run...")
    ref = load_reference()
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
    print(f"  ✓ Loaded {len(customers_df):,} customers, {len(products_df)} products")

    # ── Apply SCD2 changes ────────────────────────────────────
    print("\n▶ Applying SCD2 change scenarios...")
    customers_df, _     = apply_customer_changes(customers_df)
    employees_off_df    = apply_employee_changes_offline(employees_off_df)
    employees_onl_df    = apply_employee_changes_online(employees_onl_df)

    # ── Add new entities ──────────────────────────────────────
    print("\n▶ Adding new entities...")
    customers_df = add_new_customers(customers_df, N_NEW_CUSTOMERS, last_cust_id)
    products_df  = add_new_products(products_df, last_prod_id, N_NEW_PRODUCTS)

    # ── Build transactions ────────────────────────────────────
    print("\n▶ Generating transactions...")
    offline_df = build_offline_transactions_incr(
        n=N_OFFLINE,
        customers_df=customers_df,
        products_df=products_df,
        employees_df=employees_off_df,
        tx_id_start=last_off_tx_id + 1,
    )
    online_df = build_online_transactions_incr(
        n=N_ONLINE,
        customers_df=customers_df,
        products_df=products_df,
        employees_df=employees_onl_df,
        shipping_df=shipping_df,
        tx_id_start=last_onl_tx_id + 1,
    )

    # ── Save CSVs ─────────────────────────────────────────────
    print("\n▶ Saving CSVs...")
    offline_path = os.path.join(OUTPUT_INCR, "offline_sales_incremental.csv")
    online_path  = os.path.join(OUTPUT_INCR, "online_sales_incremental.csv")

    offline_df.to_csv(offline_path, index=False)
    online_df.to_csv(online_path,   index=False)

    print(f"  ✓ {offline_path}  ({len(offline_df):,} rows, {os.path.getsize(offline_path)/1e6:.1f} MB)")
    print(f"  ✓ {online_path}   ({len(online_df):,} rows, {os.path.getsize(online_path)/1e6:.1f} MB)")

    # ── Incremental summary ───────────────────────────────────
    print("\n── Incremental Load Summary ──────────────────────────")
    print(f"  Date range:       {START_DATE} → {END_DATE}")
    print(f"  New customers:    {N_NEW_CUSTOMERS}")
    print(f"  Changed customers:{N_CUSTOMERS_CHANGED_CITY + N_CUSTOMERS_CHANGED_CONTACT}")
    print(f"    - city change:      {N_CUSTOMERS_CHANGED_CITY}   (SCD2 new version)")
    print(f"    - contact change:   {N_CUSTOMERS_CHANGED_CONTACT}  (SCD2 new version)")
    print(f"  Promoted employees: {N_EMPLOYEES_PROMOTED} offline (SCD2 new version)")
    print(f"  Raised employees:   {N_EMPLOYEES_RAISE} online  (SCD2 new version)")
    print(f"  New products:       {N_NEW_PRODUCTS}")
    print(f"  Offline TX IDs:     OFF{last_off_tx_id+1:07d} → OFF{last_off_tx_id+N_OFFLINE:07d}")
    print(f"  Online  TX IDs:     ONL{last_onl_tx_id+1:07d} → ONL{last_onl_tx_id+N_ONLINE:07d}")
    print(f"\n  Offline revenue:  ${offline_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Online revenue:   ${online_df['transaction_sale_amount'].sum():,.2f}")
    print("\n✅ Incremental load complete.\n")


if __name__ == "__main__":
    main()