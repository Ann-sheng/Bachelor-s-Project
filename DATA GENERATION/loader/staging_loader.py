#Download Parquet files from cloud storage and bulk-load  into PostgreSQL staging tables.

import io
import os
import sys
import logging
import time
import tempfile
from pathlib import Path
from typing import Optional

import click
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(_file_), "..", "src"))
from cloud_storage import get_cloud_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(_name_)


# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )



# Target table mapping

TARGET_TABLES = {
    "offline": "sa_offline_sales.src_offline_sales",
    "online":  "sa_online_sales.src_online_sales",
}

# Columns that exist in the staging tables but NOT in the Parquet files
LOADER_ADDED_COLS = {"src_id", "staged_at"}

# Expected Parquet columns 
REQUIRED_PARQUET_COLS = {
    "batch_id", "load_type", "batch_dt",
    "transaction_id", "transaction_dt", "transaction_sale_amount",
    "customer_id", "product_id", "supplier_id", "employee_id",
}



# Audit log helpers
def is_already_loaded(file_key: str, conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM sa_audit.file_load_log "
            "WHERE file_key = %s AND status = 'SUCCESS'",
            (file_key,)
        )
        return cur.fetchone() is not None


def log_start(file_key: str, source: str, load_type: str, file_size: int,
              batch_id: str, conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sa_audit.file_load_log
                (file_key, source, load_type, batch_id, file_size_bytes, status)
            VALUES (%s, %s, %s, %s, %s, 'STARTED')
            RETURNING log_id
            """,
            (file_key, source.upper(), load_type.upper(), batch_id, file_size)
        )
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id


def log_success(log_id: int, rows_loaded: int, conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sa_audit.file_load_log
            SET status = 'SUCCESS', rows_loaded = %s, finished_at = NOW()
            WHERE log_id = %s
            """,
            (rows_loaded, log_id)
        )
    conn.commit()


def log_failure(log_id: int, error: str, conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sa_audit.file_load_log
            SET status = 'FAILED', error_message = %s, finished_at = NOW()
            WHERE log_id = %s
            """,
            (str(error)[:2000], log_id)
        )
    conn.commit()



# Schema validation

def validate_schema(df: pd.DataFrame, file_key: str) -> None:
    """Raise ValueError if required columns are missing from the Parquet file."""
    missing = REQUIRED_PARQUET_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"Parquet file '{file_key}' is missing required columns: {sorted(missing)}"
        )
    log.info("Schema validation passed (%d columns present)", len(df.columns))



#  bulk COPY to PostgreSQL

def get_staging_columns(table_name: str, conn) -> list:
    """
    Return the list of writable column names from the staging table,
    excluding auto-generated ones (src_id, staged_at).
    """
    schema, tbl = table_name.split(".")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM   information_schema.columns
            WHERE  table_schema = %s
              AND  table_name   = %s
              AND  is_generated = 'NEVER'
              AND  column_default NOT LIKE 'nextval%%'
              AND  column_default NOT LIKE 'now()%%'
              AND  column_default IS NULL
                OR column_name NOT IN ('src_id', 'staged_at')
            ORDER BY ordinal_position
            """,
            (schema, tbl)
        )
        return [row[0] for row in cur.fetchall()
                if row[0] not in LOADER_ADDED_COLS]


def add_source_file_column(df: pd.DataFrame, file_key: str) -> pd.DataFrame:
    df = df.copy()
    df["source_file"] = file_key
    return df


def bulk_copy_to_postgres(df: pd.DataFrame, table_name: str,
                          staging_cols: list, conn) -> int:
    df_aligned = pd.DataFrame(columns=staging_cols)
    for col in staging_cols:
        if col in df.columns:
            df_aligned[col] = df[col].values
        else:
            df_aligned[col] = None  

    buffer = io.StringIO()
    df_aligned.to_csv(
        buffer,
        index=False,
        header=False,
        na_rep=r"\N",  
    )
    buffer.seek(0)

    col_list = ", ".join(f'"{c}"' for c in staging_cols)
    copy_sql = (
        f"COPY {table_name} ({col_list}) "
        f"FROM STDIN WITH (FORMAT CSV, NULL '\\N', QUOTE '\"')"
    )

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buffer)

    conn.commit()
    return len(df_aligned)



# Main load function


def load_parquet_to_staging(
    file_key: str,
    source: str,         
    conn,
    cloud,
    force: bool = False,
    tmp_dir: str = "/tmp",
) -> dict:
    result = {"file_key": file_key, "source": source, "rows_loaded": 0,
              "status": "SKIPPED", "duration_sec": 0}

    if not force and is_already_loaded(file_key, conn):
        log.info("SKIP  %s  (already loaded)", file_key)
        result["status"] = "SKIPPED"
        return result

    t0 = time.time()
    log_id = None
    local_path = Path(tmp_dir) / Path(file_key).name

    try:
        log.info("Downloading  %s", file_key)
        cloud.download(file_key, str(local_path))
        file_size = local_path.stat().st_size

        df = pd.read_parquet(local_path, engine="pyarrow")
        log.info("Read %d rows from %s", len(df), file_key)

        validate_schema(df, file_key)

        batch_id   = df["batch_id"].iloc[0] if "batch_id" in df.columns else "unknown"
        load_type  = df["load_type"].iloc[0] if "load_type" in df.columns else "UNKNOWN"

        log_id = log_start(file_key, source, load_type, file_size, batch_id, conn)

        df = add_source_file_column(df, file_key)

        table_name    = TARGET_TABLES[source.lower()]
        staging_cols  = get_staging_columns(table_name, conn)

        rows_loaded = bulk_copy_to_postgres(df, table_name, staging_cols, conn)

        log_success(log_id, rows_loaded, conn)

        duration = round(time.time() - t0, 2)
        log.info(
            "LOADED  %s  →  %s  |  %d rows  |  %.1f sec  |  %.0f rows/sec",
            file_key, table_name, rows_loaded, duration,
            rows_loaded / duration if duration > 0 else 0,
        )

        result.update({"rows_loaded": rows_loaded, "status": "SUCCESS",
                       "duration_sec": duration})

    except Exception as e:
        log.error("FAILED  %s  :  %s", file_key, str(e))
        if log_id:
            log_failure(log_id, str(e), conn)
        result["status"] = "FAILED"
        raise

    finally:
        if local_path.exists():
            local_path.unlink()

    return result



# CLI

@click.command()
@click.option(
    "--load-type",
    type=click.Choice(["initial", "incremental", "all"], case_sensitive=False),
    default="all",
    help="Which Parquet files to load from cloud storage.",
)
@click.option(
    "--source",
    type=click.Choice(["offline", "online", "both"], case_sensitive=False),
    default="both",
    help="Which source channel to load.",
)
@click.option(
    "--file-key",
    default=None,
    help="Load a specific cloud object key. Overrides --load-type.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-load even if already in audit log (override idempotency).",
)
def main(load_type, source, file_key, force):
    cloud   = get_cloud_client()
    conn    = get_db_connection()
    tmp_dir = os.environ.get("LOCAL_TEMP_DIR", tempfile.gettempdir())

    total_loaded = 0
    total_skipped = 0
    total_failed  = 0


    if file_key:
        src = "online" if file_key.startswith("online") else "offline"
        file_list = [(src, file_key)]
    else:
        file_list = []
        prefixes  = {
            "initial":     ["offline/initial/", "online/initial/"],
            "incremental": ["offline/incremental/", "online/incremental/"],
            "all":         ["offline/initial/", "online/initial/",
                            "offline/incremental/", "online/incremental/"],
        }

        for prefix in prefixes[load_type.lower()]:
            src = "offline" if prefix.startswith("offline") else "online"
            if source.lower() != "both" and src != source.lower():
                continue
            for key in cloud.list_files(prefix):
                if key.endswith(".parquet"):
                    file_list.append((src, key))

    if not file_list:
        log.warning("No Parquet files found for load_type='%s' source='%s'",
                    load_type, source)
        return

    log.info("Files to process: %d", len(file_list))

    for src, key in file_list:
        try:
            result = load_parquet_to_staging(
                file_key=key,
                source=src,
                conn=conn,
                cloud=cloud,
                force=force,
                tmp_dir=tmp_dir,
            )
            if result["status"] == "SUCCESS":
                total_loaded  += result["rows_loaded"]
            elif result["status"] == "SKIPPED":
                total_skipped += 1
        except Exception as e:
            total_failed += 1
            log.error("Continuing after failure on %s", key)

    conn.close()

    print(f"\n{'─'*50}")
    print(f"  Staging load complete")
    print(f"  Files processed:  {len(file_list)}")
    print(f"  Rows loaded:      {total_loaded:,}")
    print(f"  Files skipped:    {total_skipped} (already loaded)")
    print(f"  Files failed:     {total_failed}")
    print(f"{'─'*50}\n")

    if total_failed > 0:
        sys.exit(1)


if _name_ == "_main_":
    main()