#!/usr/bin/env python3
# ================================================================
# FILE    : generate_initial_load.py
# PURPOSE : Generate 500 000-row initial load CSVs for
#           Retrograde Collective vintage clothing brand.
#
# OUTPUTS:
#   data/initial/offline_sales_initial.csv
#   data/initial/online_sales_initial.csv
#   data/reference/master_data.pkl   ← read by incremental script
#
# USAGE:
#   python generate_initial_load.py
# ================================================================

import os
import sys
import pickle
import numpy as np
import pandas as pd
from tqdm import tqdm

# -- local imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
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
    make_transaction_ids, make_entity_ids, make_short_ids,
    make_emails, make_phones, make_salaries,
    calc_sale_amounts, weighted_choice,
)

# ── Configuration ─────────────────────────────────────────────────
SEED            = 42
N_OFFLINE       = 500_000
N_ONLINE        = 500_000
N_CUSTOMERS     = 14_000     # unique customers (reused across transactions)
N_EMP_OFFLINE   = 150        # ~15 per branch across 10 branches
N_EMP_ONLINE    = 80

START_DATE      = "2020-01-01"
END_DATE        = "2023-12-31"

OUTPUT_INITIAL  = "data/initial"
OUTPUT_REF      = "data/reference"
os.makedirs(OUTPUT_INITIAL, exist_ok=True)
os.makedirs(OUTPUT_REF,     exist_ok=True)

rng = np.random.default_rng(SEED)


# ════════════════════════════════════════════════════════════════
# 1. MASTER ENTITY BUILDERS
# ════════════════════════════════════════════════════════════════

def build_suppliers() -> pd.DataFrame:
    """20 fixed vintage clothing suppliers."""
    rows = []
    for i, (name, email, phone, contact, location) in enumerate(SUPPLIERS, start=1):
        rows.append({
            "supplier_id":               f"SUPP{str(i).zfill(2)}",
            "supplier_name":             name,
            "supplier_email":            email,
            "supplier_number":           phone,
            "supplier_primary_contact":  contact,
            "supplier_location":         location,
        })
    return pd.DataFrame(rows)


def build_products(suppliers_df: pd.DataFrame) -> pd.DataFrame:
    """Build product table from catalog. Each product tied to one supplier."""
    rows = []
    prod_num = 1
    sup_ids = suppliers_df["supplier_id"].tolist()

    for category, items in PRODUCT_CATALOG.items():
        # Assign each category a primary supplier (rotated) for realism
        primary_sup_idx = (prod_num % len(sup_ids))

        for (name, cost, price, warranty) in items:
            # Small chance of secondary supplier for variety
            sup_idx = primary_sup_idx if rng.random() > 0.2 else rng.integers(len(sup_ids))
            rows.append({
                "product_id":               f"PROD{str(prod_num).zfill(3)}",
                "product_category":         category,
                "product_name":             name,
                "product_unit_cost":        cost,
                "product_unit_price":       price,
                "product_warranty_period":  warranty,
                "supplier_id":              sup_ids[sup_idx],
            })
            prod_num += 1

    df = pd.DataFrame(rows)
    df = df.merge(suppliers_df, on="supplier_id", how="left")
    return df


def build_store_branches() -> pd.DataFrame:
    """10 physical store branches."""
    rows = []
    for i, (city, state, phone, days, hours) in enumerate(STORE_BRANCHES, start=1):
        rows.append({
            "store_branch_id":                f"BRANCH{str(i).zfill(2)}",
            "store_branch_city":              city,
            "store_branch_state":             state,
            "store_branch_phone_number":      phone,
            "store_branch_operating_days":    days,
            "store_branch_operating_hours":   hours,
        })
    return pd.DataFrame(rows)


def build_offline_employees(store_branches_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Distribute N employees across store branches proportionally.
    Title determines salary range.
    """
    branch_ids   = store_branches_df["store_branch_id"].tolist()
    # Assign branches: roughly equal distribution
    branch_idx   = rng.integers(0, len(branch_ids), size=n)

    first_names  = rng.choice(FIRST_NAMES, size=n)
    last_names   = rng.choice(LAST_NAMES,  size=n)
    titles       = rng.choice(OFFLINE_EMPLOYEE_TITLES, size=n)
    salaries     = make_salaries(titles.tolist(), OFFLINE_SALARY_RANGES, rng)
    emp_ids      = make_entity_ids(n, "EMPO")

    df = pd.DataFrame({
        "employee_id":           emp_ids,
        "employee_firstname":    first_names,
        "employee_lastname":     last_names,
        "employee_title":        titles,
        "employee_salary":       salaries,
        "store_branch_id":       [branch_ids[i] for i in branch_idx],
    })
    df["employee_email"] = make_emails(
        df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng
    )
    df["employee_phone_number"] = make_phones(n, rng)

    # Join store branch details onto employee
    df = df.merge(store_branches_df, on="store_branch_id", how="left")
    return df


def build_online_employees(n: int) -> pd.DataFrame:
    """
    Online employees (warehouse, CS, fulfillment).
    No physical store branch attached.
    """
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    titles      = rng.choice(ONLINE_EMPLOYEE_TITLES, size=n)
    salaries    = make_salaries(titles.tolist(), ONLINE_SALARY_RANGES, rng)
    emp_ids     = make_entity_ids(n, "EMPW")

    df = pd.DataFrame({
        "employee_id":           emp_ids,
        "employee_firstname":    first_names,
        "employee_lastname":     last_names,
        "employee_title":        titles,
        "employee_salary":       salaries,
    })
    df["employee_email"]        = make_emails(
        df["employee_firstname"], df["employee_lastname"], EMAIL_DOMAINS, rng
    )
    df["employee_phone_number"] = make_phones(n, rng)
    return df


def build_customers(n: int, start_id: int = 1) -> pd.DataFrame:
    """
    Build customer pool. Geography weighted toward US.
    SCD2 start_dt defaults to random date in 2018-2019 (pre-purchase history).
    """
    first_names = rng.choice(FIRST_NAMES, size=n)
    last_names  = rng.choice(LAST_NAMES,  size=n)
    cust_ids    = make_entity_ids(n, "CUST", start=start_id)

    # Geography
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

    df = pd.DataFrame({
        "customer_id":           cust_ids,
        "customer_firstname":    first_names,
        "customer_lastname":     last_names,
        "customer_country":      countries,
        "customer_city":         cities,
    })
    df["customer_email"]        = make_emails(
        df["customer_firstname"], df["customer_lastname"], EMAIL_DOMAINS, rng
    )
    df["customer_phone_number"] = make_phones(n, rng)
    return df


def build_shipping_options() -> pd.DataFrame:
    """20 shipping method + carrier combinations (online only)."""
    rows = []
    for i, (method, carrier) in enumerate(SHIPPING_OPTIONS, start=1):
        rows.append({
            "shipping_id":      f"SHIP{str(i).zfill(2)}",
            "shipping_method":  method,
            "shipping_carrier": carrier,
        })
    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════
# 2. TRANSACTION BUILDERS
# ════════════════════════════════════════════════════════════════

def build_offline_transactions(
    n:                  int,
    customers_df:       pd.DataFrame,
    products_df:        pd.DataFrame,
    employees_df:       pd.DataFrame,   # already has store_branch cols joined
    tx_id_start:        int = 1,
    start_date:         str = START_DATE,
    end_date:           str = END_DATE,
) -> pd.DataFrame:
    """
    Build N offline transactions.
    One row = one denormalized transaction (all entity attributes inlined).
    """
    print(f"  Sampling {n:,} offline transactions...")

    # -- sampling indices
    cust_idx  = rng.choice(len(customers_df),  size=n)   # repeat buyers
    prod_idx  = rng.choice(
        len(products_df),
        size=n,
        # Power-law-ish weights: first products sell more (popular items)
        p=np.array([1/(i+1)**0.5 for i in range(len(products_df))])
         / sum(1/(i+1)**0.5 for i in range(len(products_df)))
    )
    emp_idx   = rng.choice(len(employees_df),  size=n)

    # -- transaction attributes
    tx_ids     = make_transaction_ids(n, "OFF", start=tx_id_start)
    tx_dates   = generate_dates(n, start_date, end_date, rng)
    quantities = rng.choice([1, 1, 1, 2, 2, 3], size=n)  # mostly qty=1
    discounts  = rng.choice(DISCOUNT_VALUES, size=n,
                             p=np.array(DISCOUNT_WEIGHTS))

    # Fetch prices from products
    unit_prices = products_df["product_unit_price"].values[prod_idx]
    sale_amounts = calc_sale_amounts(quantities, unit_prices, discounts)

    pay_methods  = weighted_choice(OFFLINE_PAYMENT_METHODS,  OFFLINE_PAYMENT_WEIGHTS,  n, rng)
    currencies   = weighted_choice(OFFLINE_CURRENCIES,       OFFLINE_CURR_WEIGHTS,     n, rng)
    channels     = np.full(n, "In-Store")
    ship_status  = np.full(n, "Collected In-Store")

    # -- assemble flat row
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
        "transaction_id":             tx_ids,
        "transaction_dt":             pd.to_datetime(tx_dates).strftime("%Y-%m-%d"),
        "transaction_payment_method": pay_methods,
        "transaction_currency_paid":  currencies,
        "transaction_sales_channel":  channels,
        "transaction_shipment_status":ship_status,
        "transaction_quantity_sold":  quantities,
        "transaction_discount_pct":   discounts,
        "transaction_sale_amount":    sale_amounts,
    })

    # Customer columns
    for col in cust_cols:
        df[col] = customers_df[col].values[cust_idx]

    # Product + supplier columns
    for col in prod_cols:
        df[col] = products_df[col].values[prod_idx]

    # Employee + store branch columns
    for col in emp_cols:
        df[col] = employees_df[col].values[emp_idx]

    return df


def build_online_transactions(
    n:              int,
    customers_df:   pd.DataFrame,
    products_df:    pd.DataFrame,
    employees_df:   pd.DataFrame,
    shipping_df:    pd.DataFrame,
    tx_id_start:    int = 1,
    start_date:     str = START_DATE,
    end_date:       str = END_DATE,
) -> pd.DataFrame:
    """
    Build N online transactions.
    Includes shipping columns and shipped/delivery dates.
    No physical store_branch (online fulfillment).
    """
    print(f"  Sampling {n:,} online transactions...")

    cust_idx  = rng.choice(len(customers_df), size=n)
    prod_idx  = rng.choice(
        len(products_df),
        size=n,
        p=np.array([1/(i+1)**0.45 for i in range(len(products_df))])
         / sum(1/(i+1)**0.45 for i in range(len(products_df)))
    )
    emp_idx   = rng.choice(len(employees_df), size=n)
    ship_idx  = rng.choice(len(shipping_df),  size=n)

    tx_ids      = make_transaction_ids(n, "ONL", start=tx_id_start)
    tx_dates    = generate_dates(n, start_date, end_date, rng)
    quantities  = rng.choice([1, 1, 1, 2, 2, 3], size=n)
    discounts   = rng.choice(DISCOUNT_VALUES, size=n, p=np.array(DISCOUNT_WEIGHTS))

    unit_prices  = products_df["product_unit_price"].values[prod_idx]
    sale_amounts = calc_sale_amounts(quantities, unit_prices, discounts)

    pay_methods  = weighted_choice(ONLINE_PAYMENT_METHODS, ONLINE_PAYMENT_WEIGHTS, n, rng)
    currencies   = weighted_choice(ONLINE_CURRENCIES,      ONLINE_CURR_WEIGHTS,    n, rng)
    channels     = weighted_choice(ONLINE_CHANNELS, ONLINE_CHANNEL_WEIGHTS, n, rng)
    ship_status  = weighted_choice(ONLINE_SHIPMENT_STATUSES, ONLINE_SHIPMENT_WEIGHTS, n, rng)

    # Shipped date: 1–3 days after transaction
    tx_ts         = pd.Series(pd.to_datetime(tx_dates))
    shipped_dates = add_business_days(tx_ts, 1, 3, rng)

    # Delivery date: 2–10 days after shipped (NULL if not yet delivered)
    delivery_dates = add_business_days(shipped_dates, 2, 10, rng).dt.strftime("%Y-%m-%d")
    shipped_str    = shipped_dates.dt.strftime("%Y-%m-%d")

    # Pending/Processing orders have no delivery date yet
    mask_no_delivery = pd.Series(ship_status).isin(["Pending", "Processing", "Cancelled"])
    delivery_dates = pd.Series(delivery_dates)
    delivery_dates[mask_no_delivery.values] = None

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
        "transaction_delivery_dt":      delivery_dates.values,
        "transaction_payment_method":   pay_methods,
        "transaction_currency_paid":    currencies,
        "transaction_sales_channel":    channels,
        "transaction_shipment_status":  ship_status,
        "transaction_quantity_sold":    quantities,
        "transaction_discount_pct":     discounts,
        "transaction_sale_amount":      sale_amounts,
    })

    for col in cust_cols:
        df[col] = customers_df[col].values[cust_idx]

    for col in prod_cols:
        df[col] = products_df[col].values[prod_idx]

    for col in emp_cols:
        df[col] = employees_df[col].values[emp_idx]

    for col in ship_cols:
        df[col] = shipping_df[col].values[ship_idx]

    return df


# ════════════════════════════════════════════════════════════════
# 3. MAIN
# ════════════════════════════════════════════════════════════════

def main():
    print("\n╔══════════════════════════════════════════════╗")
    print("║  Retrograde Collective — Initial Load        ║")
    print("║  Target: 500 000 offline + 500 000 online   ║")
    print("╚══════════════════════════════════════════════╝\n")

    # ── Build master entities ─────────────────────────────────
    print("▶ Building master entities...")
    suppliers_df        = build_suppliers()
    products_df         = build_products(suppliers_df)
    store_branches_df   = build_store_branches()
    employees_off_df    = build_offline_employees(store_branches_df, N_EMP_OFFLINE)
    employees_onl_df    = build_online_employees(N_EMP_ONLINE)
    customers_df        = build_customers(N_CUSTOMERS, start_id=1)
    shipping_df         = build_shipping_options()

    print(f"  ✓ {len(suppliers_df)} suppliers")
    print(f"  ✓ {len(products_df)} products across {len(PRODUCT_CATALOG)} categories")
    print(f"  ✓ {len(store_branches_df)} store branches")
    print(f"  ✓ {len(employees_off_df)} offline employees")
    print(f"  ✓ {len(employees_onl_df)} online employees")
    print(f"  ✓ {len(customers_df)} customers")
    print(f"  ✓ {len(shipping_df)} shipping options\n")

    # ── Build transactions ────────────────────────────────────
    print("▶ Generating transactions...")
    offline_df = build_offline_transactions(
        n=N_OFFLINE,
        customers_df=customers_df,
        products_df=products_df,
        employees_df=employees_off_df,
        tx_id_start=1,
    )

    online_df = build_online_transactions(
        n=N_ONLINE,
        customers_df=customers_df,
        products_df=products_df,
        employees_df=employees_onl_df,
        shipping_df=shipping_df,
        tx_id_start=1,
    )

    # ── Save CSVs ─────────────────────────────────────────────
    print("\n▶ Saving CSVs...")
    offline_path = os.path.join(OUTPUT_INITIAL, "offline_sales_initial.csv")
    online_path  = os.path.join(OUTPUT_INITIAL, "online_sales_initial.csv")

    offline_df.to_csv(offline_path, index=False)
    online_df.to_csv(online_path,   index=False)

    print(f"  ✓ {offline_path}  ({len(offline_df):,} rows, {os.path.getsize(offline_path)/1e6:.1f} MB)")
    print(f"  ✓ {online_path}   ({len(online_df):,} rows, {os.path.getsize(online_path)/1e6:.1f} MB)")

    # ── Save reference data for incremental script ────────────
    print("\n▶ Saving reference data for incremental generator...")
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
    }
    ref_path = os.path.join(OUTPUT_REF, "master_data.pkl")
    with open(ref_path, "wb") as f:
        pickle.dump(master, f)
    print(f"  ✓ {ref_path}")

    # ── Quick stats ───────────────────────────────────────────
    print("\n── Offline Summary ──────────────────────────────")
    print(f"  Date range:    {offline_df['transaction_dt'].min()} → {offline_df['transaction_dt'].max()}")
    print(f"  Unique customers:  {offline_df['customer_id'].nunique():,}")
    print(f"  Unique products:   {offline_df['product_id'].nunique():,}")
    print(f"  Total revenue:    ${offline_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Avg sale amount:  ${offline_df['transaction_sale_amount'].mean():.2f}")
    print(f"  Payment method distribution:\n{offline_df['transaction_payment_method'].value_counts().to_string()}")

    print("\n── Online Summary ───────────────────────────────")
    print(f"  Date range:    {online_df['transaction_dt'].min()} → {online_df['transaction_dt'].max()}")
    print(f"  Unique customers:  {online_df['customer_id'].nunique():,}")
    print(f"  Unique products:   {online_df['product_id'].nunique():,}")
    print(f"  Total revenue:    ${online_df['transaction_sale_amount'].sum():,.2f}")
    print(f"  Avg sale amount:  ${online_df['transaction_sale_amount'].mean():.2f}")
    print(f"  Shipment status:\n{online_df['transaction_shipment_status'].value_counts().to_string()}")

    print("\n✅ Initial load complete.\n")


if __name__ == "__main__":
    main()