
# PURPOSE : Full end-to-end pipeline orchestrator.
#
#  Full pipeline from scratch (initial load):
#   python pipeline/run_pipeline.py --mode initial
#
#  Incremental update (assumes initial already run):
#   python pipeline/run_pipeline.py --mode incremental

import os
import sys
import subprocess
import logging
import time
from pathlib import Path
from datetime import datetime, timezone

import click
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(_name_)


ROOT = Path(_file_).parent.parent


def step(name: str):
    print(f"\n{'═'*60}")
    print(f"  STEP: {name}")
    print(f"{'═'*60}")


def run_python(script: Path, *args) -> None:
    cmd = [sys.executable, str(script)] + list(args)
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=True)
    if result.returncode != 0:
        raise RuntimeError(f"{script.name} exited with code {result.returncode}")


def run_sql_procedure(proc_name: str) -> None:
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            log.info("Calling: CALL %s()", proc_name)
            t0 = time.time()
            cur.execute(f"CALL {proc_name}()")
            duration = time.time() - t0
            log.info("Completed %s in %.1f sec", proc_name, duration)
    finally:
        conn.close()


def run_sql_file(sql_file: Path) -> None:
    import subprocess
    cmd = [
        "psql",
        f"--host={os.environ['PG_HOST']}",
        f"--port={os.environ.get('PG_PORT', '5432')}",
        f"--dbname={os.environ['PG_DATABASE']}",
        f"--username={os.environ['PG_USER']}",
        "--file", str(sql_file),
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = os.environ["PG_PASSWORD"]
    result = subprocess.run(cmd, env=env, check=True)
    if result.returncode != 0:
        raise RuntimeError(f"SQL file {sql_file} failed")


def print_pipeline_log(n_rows: int = 20) -> None:
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT procedure_name, layer, status, rows_inserted,
                       ROUND(EXTRACT(EPOCH FROM (finished_at - started_at)), 1) AS sec,
                       started_at::TIME
                FROM   bl_cn.etl_log
                ORDER  BY started_at DESC
                LIMIT  {n_rows}
            """)
            rows = cur.fetchall()
            if rows:
                print("\n── ETL Log (most recent) ──────────────────────────────")
                print(f"  {'Procedure':<45} {'Layer':<8} {'Status':<10} {'Rows':>8} {'Sec':>6}")
                print(f"  {'-'*45} {'-'*8} {'-'*10} {'-'*8} {'-'*6}")
                for row in rows:
                    proc, layer, status, ri, sec, ts = row
                    ri   = ri   or 0
                    sec  = sec  or 0
                    print(f"  {proc:<45} {layer:<8} {status:<10} {ri:>8,} {sec:>6.1f}")

            cur.execute(f"""
                SELECT file_key, source, load_type, rows_loaded,
                       ROUND(duration_sec, 1) AS sec, status
                FROM   sa_audit.file_load_log
                ORDER  BY started_at DESC
                LIMIT  10
            """)
            rows = cur.fetchall()
            if rows:
                print("\n── Staging File Load Log ──────────────────────────────")
                for row in rows:
                    fk, src, lt, rl, sec, st = row
                    print(f"  [{st}] {src} {lt} | {rl:,} rows | {sec}s | {Path(fk).name}")
    finally:
        conn.close()



@click.command()
@click.option(
    "--mode",
    type=click.Choice(["initial", "incremental"], case_sensitive=False),
    required=True,
    help="initial = full load from scratch | incremental = append new data",
)
@click.option(
    "--stage",
    type=click.Choice(["all", "generate", "staging-only", "db-only"],
                       case_sensitive=False),
    default="all",
    help=(
        "all = full pipeline | "
        "generate = Python generators only | "
        "staging-only = cloud→staging only | "
        "db-only = BL_3NF + BL_DM only (staging must be pre-loaded)"
    ),
)
@click.option(
    "--skip-generate",
    is_flag=True,
    default=False,
    help="Skip data generation (assume Parquet files already in cloud).",
)
def main(mode, stage, skip_generate):

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n╔════════════════════════════════════════════════════╗")
    print(f"║  Retrograde Collective DWH Pipeline                  ║")
    print(f"║  Mode: {mode.upper():<12}        Stage: {stage:<17}  ║")
    print(f"║  Start: {ts:<43}                                     ║")
    print(f"╚══════════════════════════════════════════════════════╝")

    pipeline_t0 = time.time()

    try:
        if stage in ("all", "generate") and not skip_generate:
            if mode == "initial":
                step("Generate Initial Load → Parquet → Cloud")
                run_python(ROOT / "generators" / "generate_initial_load.py")
            else:
                step("Generate Incremental Load → Parquet → Cloud")
                run_python(ROOT / "generators" / "generate_incremental_load.py")

        if stage in ("all", "staging-only") and mode == "initial":
            step("Create Schemas + Staging Tables (initial only)")
            for sql_file in sorted((ROOT / "sql").glob("*.sql")):
                log.info("Executing %s", sql_file.name)
                run_sql_file(sql_file)

        if stage in ("all", "staging-only"):
            step(f"Load Staging from Cloud ({mode})")
            run_python(
                ROOT / "loaders" / "staging_loader.py",
                "--load-type", mode,
                "--source", "both",
            )

        if stage in ("all", "db-only"):
            step("BL_3NF Layer")
            run_sql_procedure("bl_3nf.sp_load_bl_3nf_all")

        if stage in ("all", "db-only"):
            step("BL_DM Layer")
            run_sql_procedure("bl_dm.sp_load_bl_dm_all")

    except Exception as e:
        log.error("Pipeline FAILED: %s", str(e))
        print(f"\n❌ Pipeline failed: {e}\n")
        sys.exit(1)

    duration = time.time() - pipeline_t0
    print(f"\n{'═'*60}")
    print(f"Pipeline complete in {duration:.1f} sec ({duration/60:.1f} min)")
    print(f"{'═'*60}")

    print_pipeline_log()


if _name_ == "_main_":
    main()