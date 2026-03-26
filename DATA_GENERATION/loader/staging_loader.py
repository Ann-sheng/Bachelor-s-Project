import os
import sys
import tempfile
import logging
from dotenv import load_dotenv

# Load .env
load_dotenv()

# -----------------------------
# CONFIG FROM ENV
# -----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

LOCAL_TEMP_DIR = os.getenv("LOCAL_TEMP_DIR", tempfile.gettempdir())

# -----------------------------
# IMPORTS AFTER CONFIG
# -----------------------------
from minio import Minio
import psycopg2

# -----------------------------
# CLOUD CLIENT
# -----------------------------
def get_cloud_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

# -----------------------------
# DB CONNECTION
# -----------------------------
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )

# -----------------------------
# MAIN LOADER
# -----------------------------
def main(load_type="initial", source="both", file_key=None, force=False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    log = logging.getLogger("staging_loader")

    log.info("🚀 STAGING LOADER STARTED")
    log.info("Load type : %s", load_type)
    log.info("Source    : %s", source)
    log.info("Force load: %s", force)

    cloud = get_cloud_client()
    conn = get_db_connection()

    total_loaded = 0
    total_skipped = 0
    total_failed = 0
    file_list = []

    try:
        log.info("📦 Collecting files from MinIO...")

        prefixes = {
            "initial": ["offline/initial/", "online/initial/"],
            "incremental": ["offline/incremental/", "online/incremental/"],
            "all": [
                "offline/initial/", "online/initial/",
                "offline/incremental/", "online/incremental/"
            ],
        }

        for prefix in prefixes.get(load_type.lower(), []):
            src = prefix.split("/")[0]
            if source.lower() != "both" and src != source.lower():
                continue

            log.info("🔎 Checking prefix: %s", prefix)
            keys = cloud.list_objects(MINIO_BUCKET, prefix, recursive=True)
            parquet_keys = [k.object_name for k in keys if k.object_name.endswith(".parquet")]
            log.info("   → found %d parquet files", len(parquet_keys))

            for key in parquet_keys:
                file_list.append((src, key))

        log.info("📊 TOTAL FILES FOUND: %d", len(file_list))
        if not file_list:
            log.error("❌ No files found. Pipeline stopped.")
            raise RuntimeError("No files found in MinIO for given parameters")

        log.info("⚙️ Starting ingestion...")
        for i, (src, key) in enumerate(file_list, 1):
            log.info("➡ [%d/%d] Processing: %s", i, len(file_list), key)
            try:
                # Replace with your actual load function
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
                    log.info("   ✅ SUCCESS (%s rows)", result["rows_loaded"])
                elif result["status"] == "SKIPPED":
                    total_skipped += 1
                    log.info("   ⏭ SKIPPED")
                else:
                    total_failed += 1
                    log.warning("   ⚠ UNKNOWN STATUS")

            except Exception:
                total_failed += 1
                log.exception("   ❌ FAILED: %s", key)

        # FINAL SUMMARY
        log.info("📌 FINAL SUMMARY")
        log.info("Rows loaded   : %d", total_loaded)
        log.info("Skipped       : %d", total_skipped)
        log.info("Failed        : %d", total_failed)

        if total_failed > 0:
            log.error("❌ PIPELINE FINISHED WITH ERRORS")
            sys.exit(1)
        else:
            log.info("✅ PIPELINE COMPLETED SUCCESSFULLY")

    finally:
        conn.close()
        log.info("🔒 DB connection closed")


if __name__ == "__main__":
    print("🚀 SCRIPT STARTED")
    main()