"""
ETL Procedure Runner
====================
Two responsibilities:
  1. Install (CREATE OR REPLACE) ETL procedures from SQL files in DWH_BUILD
  2. Call the master wrapper procedures that move data through layers

Usage standalone:
    python procedure_runner.py install          # create/replace all ETL procedures
    python procedure_runner.py call             # call all three layer wrappers
    python procedure_runner.py call --layer stg_cl
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("procedure_runner")

# ---------------------------------------------------------------------------
# DB config (same env vars as staging_loader)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))


def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, port=DB_PORT,
    )
    conn.autocommit = True
    return conn



#  PART 1 — INSTALL ETL PROCEDURES  (CREATE OR REPLACE from SQL files)


ETL_INSTALL_PLAN = [
    # ── stg_cl procedures ──────────────────────────────────────────────
    ("STG_CL: load_stg_offline",         "stg_cl/stg_cl_procedure/load_stg_offline.sql"),
    ("STG_CL: load_stg_online",          "stg_cl/stg_cl_procedure/load_stg_online.sql"),
    ("STG_CL: load_stg_all",             "stg_cl/stg_cl_procedure/load_stg_all.sql"),

    # ── bl_3nf functions (used inside bl_3nf procedures) ───────────────
    ("BL_3NF fn: get_suppliers",          "bl_3nf/bl_3nf_function/fn_get_suppliers_data.sql"),
    ("BL_3NF fn: get_shippings",          "bl_3nf/bl_3nf_function/fn_get_shippings_data.sql"),
    ("BL_3NF fn: get_store_branches",     "bl_3nf/bl_3nf_function/fn_get_store_branches_data.sql"),
    ("BL_3NF fn: get_products",           "bl_3nf/bl_3nf_function/fn_get_products_data.sql"),
    ("BL_3NF fn: get_customers_scd",      "bl_3nf/bl_3nf_function/fn_get_new_customers_scd.sql"),
    ("BL_3NF fn: get_employees_scd",      "bl_3nf/bl_3nf_function/fn_get_new_employees_scd.sql"),

    # ── bl_3nf procedures ──────────────────────────────────────────────
    ("BL_3NF: load_ce_suppliers",         "bl_3nf/bl_3nf_procedure/load_ce_suppliers.sql"),
    ("BL_3NF: load_ce_shippings",         "bl_3nf/bl_3nf_procedure/load_ce_shippings.sql"),
    ("BL_3NF: load_ce_store_branches",    "bl_3nf/bl_3nf_procedure/load_ce_store_branches.sql"),
    ("BL_3NF: load_ce_products",          "bl_3nf/bl_3nf_procedure/load_ce_products.sql"),
    ("BL_3NF: load_ce_customers_scd",     "bl_3nf/bl_3nf_procedure/load_ce_customers_scd.sql"),
    ("BL_3NF: load_ce_employees_scd",     "bl_3nf/bl_3nf_procedure/load_ce_employees_scd.sql"),
    ("BL_3NF: load_ce_transactions",      "bl_3nf/bl_3nf_procedure/load_ce_transactions.sql"),
    ("BL_3NF: sp_load_bl_3nf_all",        "bl_3nf/bl_3nf_procedure/sp_load_bl_3nf_all.sql"),

    # ── bl_dm procedures ───────────────────────────────────────────────
    ("BL_DM: load_dim_dates",             "bl_dm/bl_dm_procedure/load_dim_dates.sql"),
    ("BL_DM: load_dm_suppliers",          "bl_dm/bl_dm_procedure/load_dm_suppliers.sql"),
    ("BL_DM: load_dm_shippings",          "bl_dm/bl_dm_procedure/load_dm_shippings.sql"),
    ("BL_DM: load_dm_store_branches",     "bl_dm/bl_dm_procedure/load_dm_store_branches.sql"),
    ("BL_DM: load_dm_products",           "bl_dm/bl_dm_procedure/load_dm_products.sql"),
    ("BL_DM: load_dm_customers_scd",      "bl_dm/bl_dm_procedure/load_dm_customers_scd.sql"),
    ("BL_DM: load_dm_employees_scd",      "bl_dm/bl_dm_procedure/load_dm_employees_scd.sql"),
    ("BL_DM: load_dm_junk_transactions",  "bl_dm/bl_dm_procedure/load_dm_junk_transactions.sql"),
    ("BL_DM: load_fct_transactions_dd",   "bl_dm/bl_dm_procedure/load_fct_transactions_dd.sql"),
    ("BL_DM: sp_load_bl_dm_all",          "bl_dm/bl_dm_procedure/load_bl_dm_all.sql"),
]


def install_etl_procedures(dwh_root: Path, conn=None) -> None:
    """
    Execute every SQL file in ETL_INSTALL_PLAN against the DWH database.
    All files are CREATE OR REPLACE so this is fully idempotent.
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    log.info("━" * 55)
    log.info("INSTALLING ETL PROCEDURES  (%d files)", len(ETL_INSTALL_PLAN))
    log.info("DWH_BUILD root: %s", dwh_root)
    log.info("━" * 55)

    # Pre-flight: check all files exist
    missing = [
        (label, rel) for label, rel in ETL_INSTALL_PLAN
        if not (dwh_root / rel).exists()
    ]
    if missing:
        for label, rel in missing:
            log.error("  MISSING: %-40s  %s", label, rel)
        raise FileNotFoundError(
            f"{len(missing)} SQL file(s) not found under {dwh_root}"
        )

    passed = 0
    for i, (label, rel_path) in enumerate(ETL_INSTALL_PLAN, 1):
        sql_path = dwh_root / rel_path
        t0 = time.perf_counter()
        try:
            sql = sql_path.read_text(encoding="utf-8")
            with conn.cursor() as cur:
                cur.execute(sql)
            elapsed = time.perf_counter() - t0
            log.info("  [%2d/%2d]  ✅  %-40s  (%.2fs)", i, len(ETL_INSTALL_PLAN), label, elapsed)
            passed += 1
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            log.error("  [%2d/%2d]  ❌  %-40s  (%.2fs)", i, len(ETL_INSTALL_PLAN), label, elapsed)
            log.error("           %s", exc)
            if close_conn:
                conn.close()
            raise

    log.info("━" * 55)
    log.info("ETL PROCEDURES INSTALLED: %d/%d", passed, len(ETL_INSTALL_PLAN))
    log.info("━" * 55)

    if close_conn:
        conn.close()



#  PART 2 — CALL ETL PROCEDURES  (execute the loaded wrappers)


ETL_CALL_ORDER = [
    ("stg_cl",  "stg_cl.sp_load_stg_all"),
    ("bl_3nf",  "bl_3nf.sp_load_bl_3nf_all"),
    ("bl_dm",   "bl_dm.sp_load_bl_dm_all"),
]


def call_procedure(conn, procedure_name: str) -> float:
    """CALL a single stored procedure. Returns elapsed seconds."""
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(f"CALL {procedure_name}()")
    elapsed = time.perf_counter() - t0
    return elapsed


def run_etl_layers(layers: list[str] | None = None, conn=None) -> None:
    """
    Call the master wrapper procedure for each requested layer.

    Parameters
    ----------
    layers : list of layer names to run, e.g. ["stg_cl", "bl_3nf", "bl_dm"].
             None means run all in order.
    conn   : existing psycopg2 connection, or None to create one.
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    to_run = ETL_CALL_ORDER
    if layers:
        valid = {name for name, _ in ETL_CALL_ORDER}
        for l in layers:
            if l not in valid:
                raise ValueError(f"Unknown layer '{l}'. Valid: {sorted(valid)}")
        to_run = [(name, proc) for name, proc in ETL_CALL_ORDER if name in layers]

    log.info("━" * 55)
    log.info("CALLING ETL PROCEDURES  (%d layers)", len(to_run))
    log.info("━" * 55)

    try:
        for i, (layer_name, proc_name) in enumerate(to_run, 1):
            log.info("  [%d/%d]  ▶  CALL %s() ...", i, len(to_run), proc_name)
            try:
                elapsed = call_procedure(conn, proc_name)
                log.info("  [%d/%d]  ✅  %s  (%.1fs)", i, len(to_run), layer_name, elapsed)
            except Exception as exc:
                log.error("  [%d/%d]  ❌  %s  FAILED", i, len(to_run), layer_name)
                log.error("           %s", exc)
                raise
    finally:
        if close_conn:
            conn.close()

    log.info("━" * 55)
    log.info("ALL ETL LAYERS COMPLETE")
    log.info("━" * 55)


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="ETL Procedure Runner")
    sub = parser.add_subparsers(dest="command", required=True)

    # -- install ---
    p_install = sub.add_parser("install", help="CREATE OR REPLACE all ETL procedures")
    p_install.add_argument(
        "--dwh-root", default="DWH_BUILD",
        help="Path to DWH_BUILD folder (default: DWH_BUILD)",
    )

    # -- call ---
    p_call = sub.add_parser("call", help="CALL the master ETL wrapper procedures")
    p_call.add_argument(
        "--layer", nargs="*", default=None,
        choices=["stg_cl", "bl_3nf", "bl_dm"],
        help="Which layer(s) to run (default: all in order)",
    )

    args = parser.parse_args()

    if args.command == "install":
        dwh_root = Path(args.dwh_root)
        if not dwh_root.is_dir():
            log.error("DWH_BUILD root not found: %s", dwh_root)
            sys.exit(1)
        install_etl_procedures(dwh_root)

    elif args.command == "call":
        run_etl_layers(layers=args.layer)


if _name_ == "_main_":
    main()