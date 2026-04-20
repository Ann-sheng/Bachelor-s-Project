"""
DWH Pipeline Orchestrator
=========================
Single entry point for all pipelines.

Usage:
    python run.py create_dwh                     # infrastructure only
    python run.py install_etl                     # install ETL procedures only
    python run.py initial                         # generate + stage + ETL
    python run.py incremental                     # generate + stage + ETL
    python run.py initial --skip-generate         # stage + ETL (data already in cloud)
    python run.py all                             # everything in order
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
ROOT   = Path(_file_).resolve().parent
SCRIPS = ROOT / "SCRIPS"

sys.path.insert(0, str(ROOT / "PIPELINES"))
sys.path.insert(0, str(SCRIPS / "generator"))
sys.path.insert(0, str(SCRIPS / "helper"))
sys.path.insert(0, str(SCRIPS / "cloud"))
sys.path.insert(0, str(SCRIPS / "loader"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("orchestrator")

DWH_BUILD_ROOT = ROOT / "DWH_BUILD"


# ---------------------------------------------------------------------------
# Pipeline wrappers
# ---------------------------------------------------------------------------

def do_create_dwh(args):
    from Infrastructure_Setup import run as infra_run
    from dotenv import load_dotenv
    import os

    load_dotenv(override=True)

    required_vars = [
        "DB_HOST", "DB_PORT", "DB_NAME",
        "DB_USER", "DB_PASSWORD",
        "ADMIN_USER", "ADMIN_PASSWORD",
    ]
    env = {k: os.environ.get(k) for k in required_vars}
    missing = [k for k, v in env.items() if not v]
    if missing:
        log.error("Missing env vars: %s", missing)
        sys.exit(1)

    infra_run(
        dwh_root=DWH_BUILD_ROOT,
        env=env,
        from_step=getattr(args, "from_step", 1),
        dry_run=getattr(args, "dry_run", False),
    )


def do_install_etl(args):
    from procedure_runner import install_etl_procedures
    install_etl_procedures(DWH_BUILD_ROOT)


def do_initial(args):
    from initial_load import run as initial_run
    initial_run(
        skip_generate=getattr(args, "skip_generate", False),
        skip_install=getattr(args, "skip_install", False),
    )


def do_incremental(args):
    from incremental_load import run as incremental_run
    incremental_run(
        skip_generate=getattr(args, "skip_generate", False),
        skip_install=getattr(args, "skip_install", False),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="DWH Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python run.py all                        # full setup + initial + incremental
  python run.py create_dwh                 # infrastructure only
  python run.py create_dwh --dry-run       # see plan without executing
  python run.py create_dwh --from-step 20  # resume from step 20
  python run.py install_etl                # install ETL procedures only
  python run.py initial                    # initial load pipeline
  python run.py initial --skip-generate    # skip generation (data in cloud)
  python run.py incremental                # incremental load pipeline
        """,
    )

    sub = parser.add_subparsers(dest="pipeline", required=True)

    # -- create_dwh --
    p_dwh = sub.add_parser("create_dwh", help="Run infrastructure setup (DDL)")
    p_dwh.add_argument("--from-step", type=int, default=1)
    p_dwh.add_argument("--dry-run", action="store_true")

    # -- install_etl --
    sub.add_parser("install_etl", help="Install ETL procedures only")

    # -- initial --
    p_init = sub.add_parser("initial", help="Initial load pipeline")
    p_init.add_argument("--skip-generate", action="store_true")
    p_init.add_argument("--skip-install", action="store_true")

    # -- incremental --
    p_incr = sub.add_parser("incremental", help="Incremental load pipeline")
    p_incr.add_argument("--skip-generate", action="store_true")
    p_incr.add_argument("--skip-install", action="store_true")

    # -- all --
    p_all = sub.add_parser("all", help="Run everything in order")
    p_all.add_argument("--skip-generate", action="store_true",
                       help="Skip data generation in both loads")

    args = parser.parse_args()

    # ── Dispatch ──────────────────────────────────────────────────────
    PIPELINE_MAP = {
        "create_dwh":  [("Create DWH",          do_create_dwh)],
        "install_etl": [("Install ETL Procs",   do_install_etl)],
        "initial":     [("Initial Load",         do_initial)],
        "incremental": [("Incremental Load",     do_incremental)],
        "all": [
            ("Create DWH",         do_create_dwh),
            ("Install ETL Procs",  do_install_etl),
            ("Initial Load",       do_initial),
            ("Incremental Load",   do_incremental),
        ],
    }

    steps = PIPELINE_MAP[args.pipeline]

    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║           DWH PIPELINE ORCHESTRATOR             ║")
    log.info("╠══════════════════════════════════════════════════╣")
    log.info("║  Pipeline : %-37s ║", args.pipeline)
    log.info("║  Started  : %-37s ║", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("║  Steps    : %-37s ║", len(steps))
    log.info("╚══════════════════════════════════════════════════╝")

    total_t0 = time.perf_counter()

    for i, (name, func) in enumerate(steps, 1):
        log.info("")
        log.info("═══  [%d/%d]  %s  ═══", i, len(steps), name)
        t0 = time.perf_counter()
        try:
            func(args)
            elapsed = time.perf_counter() - t0
            log.info("═══  [%d/%d]  %s  ✅  (%.1fs)  ═══", i, len(steps), name, elapsed)
        except Exception:
            elapsed = time.perf_counter() - t0
            log.exception("═══  [%d/%d]  %s  ❌  FAILED  (%.1fs)  ═══", i, len(steps), name, elapsed)
            log.error("Pipeline aborted.")
            sys.exit(1)

    total = time.perf_counter() - total_t0
    log.info("")
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║  🎉  ALL PIPELINES COMPLETE  (%.1fs total)      ║", total)
    log.info("╚══════════════════════════════════════════════════╝")


if _name_ == "_main_":
    main()
