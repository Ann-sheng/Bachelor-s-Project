
# DWH Infrastructure Setup, Executes the structural DDL files from DWH_BUILD in the correct


"""
    python Infrastructure_Setup.py                         
    python Infrastructure_Setup.py --env /path/to/.env     
    python Infrastructure_Setup.py --dry-run               
    python Infrastructure_Setup.py --from-step 14          
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv


# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("dwh_setup")



PLAN = [
    # ── Bootstrap (postgres DB) ────────────────────────────────────────────
    ( 1, "Create database",                  "bootstrap/database.sql",                         "postgres"),
    ( 2, "Create roles & users",             "bootstrap/roles.sql",                            "postgres"),

    # ── Schemas + role settings (inside DnD_SALES) ────────────────────────
    ( 3, "Create schemas",                   "setup/schema.sql",                               "dwh"),
    ( 4, "Role DB settings",                 "setup/role.sql",                                 "dwh"),

    # ── BL_CN — control & logging infrastructure ───────────────────────────
    # Tables first 
    ( 5, "BL_CN: etl_run table",             "bl_cn/bl_cn_table/etl_run.sql",                  "dwh"),
    ( 6, "BL_CN: etl_log table",             "bl_cn/bl_cn_table/etl_log.sql",                  "dwh"),
    ( 7, "BL_CN: etl_run index",             "bl_cn/bl_cn_index/etl_run.sql",                  "dwh"),
    ( 8, "BL_CN: etl_log indexes",           "bl_cn/bl_cn_index/etl_log.sql",                  "dwh"),
    # Logging routines 
    ( 9, "BL_CN: log_start function",        "bl_cn/bl_cn_function/log_start.sql",             "dwh"),
    (10, "BL_CN: log_success procedure",     "bl_cn/bl_cn_procedure/log_success.sql",          "dwh"),
    (11, "BL_CN: log_failure procedure",     "bl_cn/bl_cn_procedure/log_failure.sql",          "dwh"),
    # Monitoring views
    (12, "BL_CN: v_latest_runs view",        "bl_cn/bl_cn_view/v_latest_runs.sql",             "dwh"),
    (13, "BL_CN: v_etl_summary view",        "bl_cn/bl_cn_view/v_etl_summary.sql",             "dwh"),
    (14, "BL_CN: v_failed_runs view",        "bl_cn/bl_cn_view/v_failed_runs.sql",             "dwh"),
    (15, "BL_CN: v_pipeline_runs view",      "bl_cn/bl_cn_view/v_pipeline_runs.sql",           "dwh"),

    # ── SA_AUDIT ───────────────────────────────────────────────────────────
    (16, "SA_AUDIT: file_load_log table",    "sa_audit/sa_audit_table/file_load_log.sql",      "dwh"),
    (17, "SA_AUDIT: file_load_log indexes",  "sa_audit/sa_audit_index/file_load_log.sql",      "dwh"),

    # ── SRC — raw landing tables ───────────────────────────────────────────
    (18, "SRC: sales_offline table",         "src/src_table/sales_offline.sql",                "dwh"),
    (19, "SRC: sales_online table",          "src/src_table/sales_online.sql",                 "dwh"),

    # ── STG_CLN — tables & indexes only (no load procedures) ──────────────
    (20, "STG: reject_sales table",          "stg_cln/stg_cln_table/reject_sales.sql",           "dwh"),
    (21, "STG: sales_offline table",         "stg_cln/stg_cln_table/sales_offline.sql",          "dwh"),
    (22, "STG: sales_online table",          "stg_cln/stg_cln_table/sales_online.sql",           "dwh"),
    (23, "STG: reject_sales index",          "stg_cln/stg_cln_index/reject_sales.sql",           "dwh"),
    (24, "STG: sales_offline indexes",       "stg_cln/stg_cln_index/sales_offline.sql",          "dwh"),
    (25, "STG: sales_online indexes",        "stg_cln/stg_cln_index/sales_online.sql",           "dwh"),

    # ── BL_3NF — sequences → tables (FK order) → indexes → unknown members 
    (26, "BL_3NF: sequences",                "bl_3nf/bl_3nf_sequence/surrogate_keys.sql",      "dwh"),
    (27, "BL_3NF: ce_suppliers table",       "bl_3nf/bl_3nf_tables/ce_suppliers.sql",          "dwh"),
    (28, "BL_3NF: ce_shippings table",       "bl_3nf/bl_3nf_tables/ce_shippings.sql",          "dwh"),
    (29, "BL_3NF: ce_store_branches table",  "bl_3nf/bl_3nf_tables/ce_store_branches.sql",     "dwh"),
    (30, "BL_3NF: ce_products table",        "bl_3nf/bl_3nf_tables/ce_products.sql",           "dwh"),
    (31, "BL_3NF: ce_customers_scd table",   "bl_3nf/bl_3nf_tables/ce_customers_scd.sql",      "dwh"),
    (32, "BL_3NF: ce_employees_scd table",   "bl_3nf/bl_3nf_tables/ce_employees_scd.sql",      "dwh"),
    (33, "BL_3NF: ce_transactions table",    "bl_3nf/bl_3nf_tables/ce_transactions.sql",       "dwh"),
    (34, "BL_3NF: ce_suppliers indexes",     "bl_3nf/bl_3nf_index/ce_suppliers.sql",           "dwh"),
    (35, "BL_3NF: ce_shippings indexes",     "bl_3nf/bl_3nf_index/ce_shippings.sql",           "dwh"),
    (36, "BL_3NF: ce_store_branches index",  "bl_3nf/bl_3nf_index/ce_store_branches.sql",      "dwh"),
    (37, "BL_3NF: ce_products indexes",      "bl_3nf/bl_3nf_index/ce_products.sql",            "dwh"),
    (38, "BL_3NF: ce_customers_scd indexes", "bl_3nf/bl_3nf_index/ce_customers_scd.sql",       "dwh"),
    (39, "BL_3NF: ce_employees_scd indexes", "bl_3nf/bl_3nf_index/ce_employees_scd.sql",       "dwh"),
    (40, "BL_3NF: ce_transactions indexes",  "bl_3nf/bl_3nf_index/ce_transactions.sql",        "dwh"),
    (41, "BL_3NF: unknown members",          "bl_3nf/bl_3nf_unknown_members/insert_unknown_members.sql", "dwh"),

    # ── BL_DM — sequences → tables (FK order) → indexes → unknown members 
    (42, "BL_DM: sequences",                 "bl_dm/bl_dm_table_sequence/surrogate_keys.sql",  "dwh"),
    (43, "BL_DM: dm_suppliers table",        "bl_dm/bl_dm_table/dm_suppliers.sql",             "dwh"),
    (44, "BL_DM: dm_shippings table",        "bl_dm/bl_dm_table/dm_shippings.sql",             "dwh"),
    (45, "BL_DM: dm_store_branches table",   "bl_dm/bl_dm_table/dm_store_branches.sql",        "dwh"),
    (46, "BL_DM: dim_dates table",           "bl_dm/bl_dm_table/dim_dates.sql",                "dwh"),
    (47, "BL_DM: dm_customers_scd table",    "bl_dm/bl_dm_table/dm_customers_scd.sql",         "dwh"),
    (48, "BL_DM: dm_employees_scd table",    "bl_dm/bl_dm_table/dm_employees_scd.sql",         "dwh"),
    (49, "BL_DM: dm_junk_transactions table","bl_dm/bl_dm_table/dm_junk_transactions.sql",     "dwh"),
    (50, "BL_DM: dm_products table",         "bl_dm/bl_dm_table/dm_products.sql",              "dwh"),
    (51, "BL_DM: fct_transactions_dd table", "bl_dm/bl_dm_table/fct_transactions_dd.sql",      "dwh"),
    (52, "BL_DM: dm_suppliers indexes",      "bl_dm/bl_dm_index/dm_suppliers.sql",             "dwh"),
    (53, "BL_DM: dm_shippings indexes",      "bl_dm/bl_dm_index/dm_shippings.sql",             "dwh"),
    (54, "BL_DM: dm_store_branches indexes", "bl_dm/bl_dm_index/dm_store_branches.sql",        "dwh"),
    (55, "BL_DM: dim_dates indexes",         "bl_dm/bl_dm_index/dim_dates.sql",                "dwh"),
    (56, "BL_DM: dm_customers_scd indexes",  "bl_dm/bl_dm_index/dm_customers_scd.sql",         "dwh"),
    (57, "BL_DM: dm_employees_scd indexes",  "bl_dm/bl_dm_index/dm_employees_scd.sql",         "dwh"),
    (58, "BL_DM: dm_junk index",             "bl_dm/bl_dm_index/dm_junk_transactions.sql",     "dwh"),
    (59, "BL_DM: dm_products indexes",       "bl_dm/bl_dm_index/dm_products.sql",              "dwh"),
    (60, "BL_DM: fct_transactions indexes",  "bl_dm/bl_dm_index/fct_transactions_dd.sql",      "dwh"),
    (61, "BL_DM: unknown members",           "bl_dm/bl_dm_unknown_members/insert_unknown_members.sql", "dwh"),

    # ── Permissions
    (62, "Permissions",                      "setup/permission.sql",                           "dwh"),
]



# DB connection helpers

def _build_dsn(env: dict, dbname: str, role: str) -> dict:
    if role == "admin":
        return {
            "host":     env["DB_HOST"],
            "port":     int(env.get("DB_PORT", 5432)),
            "dbname":   dbname,
            "user":     env.get("ADMIN_USER", "postgres"),      
            "password": env.get("ADMIN_PASSWORD", ""),          
        }
    else:
        return {
            "host":     env["DB_HOST"],
            "port":     int(env.get("DB_PORT", 5432)),
            "dbname":   dbname,
            "user":     env["DB_USER"],
            "password": env["DB_PASSWORD"],
        }


def get_connection(env: dict, target: str):
    if target == "postgres":
        dbname = env.get("ADMIN_DB", "postgres")
        role = "admin"
    else:
        dbname = env["DB_NAME"]
        role = "app"

    conn = psycopg2.connect(**_build_dsn(env, dbname, role))
    conn.autocommit = True
    return conn



# SQL execution, Checker

def execute_file(conn: psycopg2.extensions.connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)



# Main runner

def run(
    dwh_root: Path,
    env: dict,
    from_step: int = 1,
    dry_run: bool = False,
) -> None:

    total   = len(PLAN)
    passed  = 0
    failed  = 0
    skipped = 0

    missing = [
        (step, label, rel)
        for step, label, rel, _ in PLAN
        if not (dwh_root / rel).exists()
    ]
    if missing:
        log.error("Missing SQL files — aborting before any DB changes:")
        for step, label, rel in missing:
            log.error("  step %02d  %-45s  %s", step, label, rel)
        sys.exit(1)

    log.info("=" * 60)
    log.info("DWH Infrastructure Setup  —  %d steps  (root: %s)", total, dwh_root)
    if dry_run:
        log.info("DRY-RUN mode — no SQL will be executed")
    if from_step > 1:
        log.info("Resuming from step %d", from_step)
    log.info("=" * 60)

    connections: dict[str, Optional[psycopg2.extensions.connection]] = {
        "postgres": None,
        "dwh":      None,
    }

    try:
        for step, label, rel_path, target in PLAN:

            if step < from_step:
                log.info("  [skip] step %02d — %s", step, label)
                skipped += 1
                continue

            sql_path = dwh_root / rel_path

            if dry_run:
                log.info("  [dry ] step %02d — %s  (%s)", step, label, rel_path)
                continue

            if connections[target] is None:
                log.debug("Opening '%s' connection…", target)
                connections[target] = get_connection(env, target)

            conn = connections[target]
            t0   = time.perf_counter()

            try:
                execute_file(conn, sql_path)
                elapsed = time.perf_counter() - t0
                log.info("  [ ok ] step %02d — %-45s (%.2fs)", step, label, elapsed)
                passed += 1
            
            except psycopg2.errors.DuplicateDatabase:
                elapsed = time.perf_counter() - t0
                log.info("  [skip] step %02d — %-45s already exists (%.2fs)", step, label, elapsed)
                skipped += 1

            except Exception as exc:
                elapsed = time.perf_counter() - t0
                log.error("  [FAIL] step %02d — %s  (%.2fs)", step, label, elapsed)
                log.error("         file : %s", rel_path)
                log.error("         error: %s", exc)
                failed += 1

                try:
                    answer = input("\nStep failed. Continue anyway? [y/N] ").strip().lower()
                except EOFError:
                    answer = "n" 

                if answer != "y":
                    log.error("Aborting setup at step %d.", step)
                    break

    finally:
        for target, conn in connections.items():
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    # Summary
    log.info("=" * 60)
    if dry_run:
        log.info("Dry run complete — %d steps would execute", total)
    else:
        log.info(
            "Setup complete — passed: %d  |  failed: %d  |  skipped: %d",
            passed, failed, skipped,
        )
        if failed:
            log.warning("Re-run with --from-step <N> to retry failed steps.")
    log.info("=" * 60)

    if failed:
        sys.exit(1)



# Entry point

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute DWH_BUILD SQL files in dependency order."
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to .env file (default: .env in current directory)",
    )
    parser.add_argument(
        "--dwh-root",
        default="DWH_BUILD",
        help="Path to the DWH_BUILD root folder (default: ./DWH_BUILD)",
    )
    parser.add_argument(
        "--from-step",
        type=int,
        default=1,
        metavar="N",
        help="Skip steps before N — useful for resuming after a failure",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without connecting to the database",
    )
    args = parser.parse_args()

    root_dir = Path(__file__).resolve().parent  
    dwh_root = root_dir.parent / "DWH_BUILD"  
    scripts_dir = root_dir.parent / "SCRIPTS"  

    dotenv_path = scripts_dir / ".env" 
    load_dotenv(dotenv_path=dotenv_path)

    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    env = {k: os.environ.get(k) for k in required_vars}
    missing_vars = [k for k, v in env.items() if not v]
    if missing_vars:
        log.error("Missing required env vars: %s", missing_vars)
        sys.exit(1)

    # Optional admin override — falls back to postgres defaults if absent
    env["ADMIN_USER"]     = os.environ.get("ADMIN_USER", "postgres")
    env["ADMIN_PASSWORD"] = os.environ.get("ADMIN_PASSWORD", "")
    env["ADMIN_DB"]       = os.environ.get("ADMIN_DB", "postgres")

    log.info("Bootstrap will connect as admin user: %s", env["ADMIN_USER"])

    if not dwh_root.is_dir():
        log.error("DWH_BUILD root not found: %s", dwh_root)
        sys.exit(1)

    run(
        dwh_root=dwh_root,
        env=env,
        from_step=args.from_step,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
