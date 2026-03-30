import io
import os
import sys
import tempfile
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load .env
load_dotenv()

# -----------------------------
# CONFIG FROM ENV
# -----------------------------
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET")
MINIO_SECURE    = os.getenv("MINIO_SECURE", "false").lower() == "true"

DB_HOST     = os.getenv("DB_HOST")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))

LOCAL_TEMP_DIR = os.getenv("LOCAL_TEMP_DIR", tempfile.gettempdir())

# -----------------------------
# IMPORTS AFTER CONFIG
# -----------------------------
import pandas as pd
from minio import Minio
import psycopg2
import psycopg2.extras

log = logging.getLogger("staging_loader")

# =============================================================================
# COLUMN MAPPINGS
#   key   = parquet column name (as produced by the generator)
#   value = target staging table column name
# =============================================================================

# --------------- src.sales_offline ---------------
OFFLINE_COL_MAP = {
    # ── audit / batch ────────────────────────────
    "batch_id":                       "batch_id",
    "load_type":                      "load_type",
    "batch_dt":                       "batch_dt",
    # ── customer ─────────────────────────────────
    "customer_id":                    "customer_id",
    "customer_firstname":             "customer_firstname",
    "customer_lastname":              "customer_lastname",
    "customer_email":                 "customer_email",
    # ── product ──────────────────────────────────
    "product_id":                     "product_id",
    "product_category":               "product_category",
    "product_name":                   "product_name",
    "product_unit_cost":              "unit_cost",
    "product_unit_price":             "unit_price",
    "product_warranty_period":        "warranty_period_months",
    # ── transaction ──────────────────────────────
    "transaction_id":                 "transaction_id",
    "transaction_dt":                 "transaction_date",
    "transaction_quantity_sold":      "quantity_sold",
    "transaction_discount_pct":       "discount_applied",
    "transaction_sale_amount":        "sales_amount",
    "transaction_payment_method":     "payment_method",
    "transaction_currency_paid":      "currency_paid",
    "transaction_sales_channel":      "sales_channel",
    # ── employee ─────────────────────────────────
    "employee_id":                    "employee_id",
    "employee_firstname":             "employee_firstname",
    "employee_lastname":              "employee_lastname",
    "employee_title":                 "employee_title",
    "employee_email":                 "employee_email",
    "employee_phone_number":          "employee_phone_number",
    "employee_salary":                "employee_salary",
    # ── store branch ─────────────────────────────
    "store_branch_id":                "storebranch_id",
    "store_branch_state":             "storebranch_state",
    "store_branch_city":              "storebranch_city",
    "store_branch_phone_number":      "storebranch_phone_number",
    "store_branch_operating_days":    "storebranch_operating_days",
    "store_branch_operating_hours":   "storebranch_operating_hours",
    # ── supplier ─────────────────────────────────
    "supplier_id":                    "supplier_id",
    "supplier_name":                  "supplier_name",
    "supplier_email":                 "supplier_email",
    "supplier_number":                "supplier_number",
    "supplier_primary_contact":       "supplier_primary_contact",
    "supplier_location":              "supplier_location",
}

# --------------- src.sales_online ----------------
ONLINE_COL_MAP = {
    # ── audit / batch ────────────────────────────
    "batch_id":                       "batch_id",
    "load_type":                      "load_type",
    "batch_dt":                       "batch_dt",
    # ── customer ─────────────────────────────────
    "customer_id":                    "customer_id",
    "customer_firstname":             "customer_firstname",
    "customer_lastname":              "customer_lastname",
    "customer_country":               "customer_country",
    "customer_city":                  "customer_city",
    "customer_phone_number":          "customer_phone_number",
    "customer_email":                 "customer_email",
    # ── product ──────────────────────────────────
    "product_id":                     "product_id",
    "product_category":               "product_category",
    "product_name":                   "product_name",
    "product_unit_cost":              "unit_cost",
    "product_unit_price":             "unit_price",
    "product_warranty_period":        "warranty_period_months",
    # ── transaction ──────────────────────────────
    "transaction_id":                 "transaction_id",
    "transaction_dt":                 "transaction_date",
    "transaction_quantity_sold":      "quantity_sold",
    "transaction_discount_pct":       "discount_applied",
    "transaction_sale_amount":        "sales_amount",
    "transaction_payment_method":     "payment_method",
    "transaction_currency_paid":      "currency_paid",
    "transaction_sales_channel":      "sales_channel",
    # ── employee ─────────────────────────────────
    "employee_id":                    "employee_id",
    "employee_firstname":             "employee_firstname",
    "employee_lastname":              "employee_lastname",
    "employee_title":                 "employee_title",
    "employee_email":                 "employee_email",
    "employee_phone_number":          "employee_phone_number",
    "employee_salary":                "employee_salary",
    # ── supplier ─────────────────────────────────
    "supplier_id":                    "supplier_id",
    "supplier_name":                  "supplier_name",
    "supplier_email":                 "supplier_email",
    "supplier_number":                "supplier_number",
    "supplier_primary_contact":       "supplier_primary_contact",
    "supplier_location":              "supplier_location",
    # ── shipping ─────────────────────────────────
    "shipping_id":                    "shipping_id",
    "shipping_method":                "shipping_method",
    "shipping_carrier":               "shipping_carrier",
    "transaction_shipped_dt":         "shipped_date",
    "transaction_delivery_dt":        "delivery_date",
    "transaction_shipment_status":    "shipment_status",
}

# Target table per source prefix
TARGET_TABLE = {
    "offline": "src.sales_offline",
    "online":  "src.sales_online",
}

# Column map per source prefix
COL_MAPS = {
    "offline": OFFLINE_COL_MAP,
    "online":  ONLINE_COL_MAP,
}


# =============================================================================
# CLOUD CLIENT
# =============================================================================
def get_cloud_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


# =============================================================================
# DB CONNECTION
# =============================================================================
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )


# =============================================================================
# IDEMPOTENCY CHECK
# =============================================================================
def _batch_already_loaded(conn, table: str, batch_id: str) -> bool:
    """Return True if ANY row with this batch_id exists in the target table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {table} WHERE batch_id = %s LIMIT 1",
            (batch_id,),
        )
        return cur.fetchone() is not None


def _purge_batch(conn, table: str, batch_id: str) -> int:
    """Delete all rows for a batch_id (used when force=True). Returns deleted count."""
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {table} WHERE batch_id = %s",
            (batch_id,),
        )
        deleted = cur.rowcount
    conn.commit()
    log.info("   🗑  Purged %d existing rows for batch_id=%s from %s", deleted, batch_id, table)
    return deleted


# =============================================================================
# CORE LOADER
# =============================================================================
def load_parquet_to_staging(
    file_key: str,
    source: str,           # "offline" | "online"
    conn,
    cloud: Minio,
    force: bool = False,
    tmp_dir: str = "/tmp",
) -> dict:
    """
    Download one parquet file from MinIO, map its columns to the correct
    staging table, and bulk-load via COPY.

    Returns
    -------
    dict  {"status": "SUCCESS"|"SKIPPED", "rows_loaded": int}
    """
    source = source.lower()
    if source not in TARGET_TABLE:
        raise ValueError(f"Unknown source '{source}'. Expected 'offline' or 'online'.")

    table   = TARGET_TABLE[source]
    col_map = COL_MAPS[source]

    # ── 1. Download parquet to a temp file ───────────────────────────────────
    filename   = Path(file_key).name
    local_path = str(Path(tmp_dir) / filename)

    log.debug("   ⬇  Downloading s3://%s/%s → %s", MINIO_BUCKET, file_key, local_path)
    cloud.fget_object(MINIO_BUCKET, file_key, local_path)

    try:
        # ── 2. Read parquet ───────────────────────────────────────────────────
        df = pd.read_parquet(local_path)

        # ── 3. Idempotency check ──────────────────────────────────────────────
        batch_id = str(df["batch_id"].iloc[0])

        if _batch_already_loaded(conn, table, batch_id):
            if not force:
                log.info("   ⏭  Batch %s already in %s — skipping", batch_id, table)
                return {"status": "SKIPPED", "rows_loaded": 0}
            _purge_batch(conn, table, batch_id)

        # ── 4. Column mapping ─────────────────────────────────────────────────
        # Keep only parquet columns that exist in the map
        parquet_cols_present = [c for c in col_map if c in df.columns]
        df_mapped = df[parquet_cols_present].rename(columns=col_map)

        # ── 5. Inject load-time columns ───────────────────────────────────────
        df_mapped["source_file"] = file_key   # full MinIO object key as lineage

        # Ensure all values are string-safe for COPY (NaN → None/NULL)
        df_mapped = df_mapped.where(pd.notnull(df_mapped), None)

        # ── 6. Build ordered column list (matches what we insert) ─────────────
        target_cols = list(df_mapped.columns)

        # ── 7. Bulk load via COPY FROM STDIN ──────────────────────────────────
        #    We stream a CSV through a StringIO buffer — no temp file, no disk
        #    I/O round-trip, and much faster than execute_many for large frames.
        buf = io.StringIO()
        df_mapped.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        col_list_sql = ", ".join(f'"{c}"' for c in target_cols)
        copy_sql = (
            f"COPY {table} ({col_list_sql}) "
            f"FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
        )

        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, buf)

        conn.commit()
        rows_loaded = len(df_mapped)

        return {"status": "SUCCESS", "rows_loaded": rows_loaded}

    finally:
        # Always clean up the local temp file
        try:
            Path(local_path).unlink(missing_ok=True)
        except Exception:
            pass


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================
def main(load_type: str = "initial", source: str = "both", file_key: str = None, force: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    log.info("🚀 STAGING LOADER STARTED")
    log.info("Load type : %s", load_type)
    log.info("Source    : %s", source)
    log.info("Force load: %s", force)

    cloud = get_cloud_client()
    conn  = get_db_connection()

    total_loaded  = 0
    total_skipped = 0
    total_failed  = 0
    file_list     = []

    try:
        log.info("📦 Collecting files from MinIO...")

        prefixes = {
            "initial":     ["offline/initial/",     "online/initial/"],
            "incremental": ["offline/incremental/",  "online/incremental/"],
            "all": [
                "offline/initial/",     "online/initial/",
                "offline/incremental/", "online/incremental/",
            ],
        }

        # Single-file override (useful for replays / debugging)
        if file_key:
            src = file_key.split("/")[0]   # "offline" or "online"
            file_list = [(src, file_key)]
        else:
            for prefix in prefixes.get(load_type.lower(), []):
                src = prefix.split("/")[0]
                if source.lower() != "both" and src != source.lower():
                    continue

                log.info("🔎 Checking prefix: %s", prefix)
                keys = cloud.list_objects(MINIO_BUCKET, prefix, recursive=True)
                parquet_keys = [k.object_name for k in keys if k.object_name.endswith(".parquet")]
                log.info("   → found %d parquet file(s)", len(parquet_keys))

                for key in parquet_keys:
                    file_list.append((src, key))

        log.info("📊 TOTAL FILES FOUND: %d", len(file_list))
        if not file_list:
            log.error("❌ No files found. Pipeline stopped.")
            raise RuntimeError("No files found in MinIO for given parameters")

        log.info("⚙️  Starting ingestion...")
        for i, (src, key) in enumerate(file_list, 1):
            log.info("➡ [%d/%d] Processing: %s", i, len(file_list), key)
            try:
                result = load_parquet_to_staging(
                    file_key=key,
                    source=src,
                    conn=conn,
                    cloud=cloud,
                    force=force,
                    tmp_dir=LOCAL_TEMP_DIR,
                )

                if result["status"] == "SUCCESS":
                    total_loaded += result["rows_loaded"]
                    log.info("   ✅ SUCCESS — %s rows inserted into %s", result["rows_loaded"], TARGET_TABLE[src])
                elif result["status"] == "SKIPPED":
                    total_skipped += 1
                    log.info("   ⏭  SKIPPED (already loaded; use --force to reload)")
                else:
                    total_failed += 1
                    log.warning("   ⚠  UNKNOWN STATUS: %s", result)

            except Exception:
                total_failed += 1
                log.exception("   ❌ FAILED: %s", key)

        # ── FINAL SUMMARY ─────────────────────────────────────────────────────
        log.info("─" * 55)
        log.info("📌 FINAL SUMMARY")
        log.info("   Rows loaded : %d", total_loaded)
        log.info("   Skipped     : %d", total_skipped)
        log.info("   Failed      : %d", total_failed)

        if total_failed > 0:
            log.error("❌ PIPELINE FINISHED WITH ERRORS")
            sys.exit(1)
        else:
            log.info("✅ PIPELINE COMPLETED SUCCESSFULLY")

    finally:
        conn.close()
        log.info("🔒 DB connection closed")


# =============================================================================
# CLI  —  python staging_loader.py [--load-type all] [--source offline] [--force]
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load parquet files from MinIO into staging tables.")
    parser.add_argument(
        "--load-type",
        default="initial",
        choices=["initial", "incremental", "all"],
        help="Which load-type prefix to scan (default: initial)",
    )
    parser.add_argument(
        "--source",
        default="both",
        choices=["offline", "online", "both"],
        help="Which source to process (default: both)",
    )
    parser.add_argument(
        "--file-key",
        default=None,
        help="Process a single MinIO object key instead of scanning prefixes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-load even if the batch_id already exists in the target table",
    )
    args = parser.parse_args()

    main(
        load_type=args.load_type,
        source=args.source,
        file_key=args.file_key,
        force=args.force,
    )