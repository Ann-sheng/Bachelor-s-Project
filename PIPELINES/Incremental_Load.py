
# Pipeline: Incremental: Load Install/refresh ETL procedures, Generate incremental data, Load cloud, CALL stg_cl 

"""
        python incremental_load.py
        python incremental_load.py --skip-generate
        python incremental_load.py --skip-install
"""

import argparse
import logging
import sys
import time
from pathlib import Path


# Path setup
ROOT    = Path(__file__).resolve().parent.parent
SCRIPS  = ROOT / "SCRIPTS"

sys.path.insert(0, str(SCRIPS / "generator"))
sys.path.insert(0, str(SCRIPS / "helper"))
sys.path.insert(0, str(SCRIPS / "cloud"))
sys.path.insert(0, str(SCRIPS / "loader"))

log = logging.getLogger("pipeline.incremental")

DWH_BUILD_ROOT = ROOT / "DWH_BUILD"



# Pipeline steps

def step_install_procedures():
    from procedure_runner import install_etl_procedures
    install_etl_procedures(DWH_BUILD_ROOT)


def step_generate():
    from generate_incremental_load import main as generate_incremental
    generate_incremental()


def step_load_staging():
    from staging_loader import main as load_staging
    try:
        load_staging(
            load_type="incremental",
            source="both",
            file_key=None,
            force=False,
        )
    except SystemExit as e:
        if e.code != 0:
            raise RuntimeError("Staging loader failed (see logs above)") from e


def step_run_etl():
    from procedure_runner import run_etl_layers
    run_etl_layers(layers=["stg_cln", "bl_3nf", "bl_dm"])



# Pipeline orchestration

STEPS = [
    ("Install ETL Procedures",       step_install_procedures),
    ("Generate Incremental Data",    step_generate),
    ("Load Cloud → Staging",         step_load_staging),
    ("Run ETL (stg→3nf→dm)",        step_run_etl),
]


def run(skip_generate: bool = False, skip_install: bool = False):
    log.info(" PIPELINE: INCREMENTAL LOAD  ")

    skips = set()
    if skip_generate:
        skips.add("Generate Incremental Data")
        log.info(" --skip-generate: generation step will be skipped")
    if skip_install:
        skips.add("Install ETL Procedures")
        log.info(" --skip-install: procedure install will be skipped")

    pipeline_t0 = time.perf_counter()

    for i, (name, func) in enumerate(STEPS, 1):
        if name in skips:
            log.info("[%d/%d]    SKIP: %s", i, len(STEPS), name)
            continue

        log.info("━" * 55)
        log.info("[%d/%d]    START: %s", i, len(STEPS), name)
        log.info("━" * 55)

        t0 = time.perf_counter()
        try:
            func()
            elapsed = time.perf_counter() - t0
            log.info("[%d/%d]    DONE: %s  (%.1fs)", i, len(STEPS), name, elapsed)
        except Exception:
            elapsed = time.perf_counter() - t0
            log.exception("[%d/%d]    FAILED: %s  (%.1fs)", i, len(STEPS), name, elapsed)
            raise

    total = time.perf_counter() - pipeline_t0
    log.info("═" * 55)
    log.info("  INCREMENTAL LOAD PIPELINE COMPLETE  (%.1fs total)", total)
    log.info("═" * 55)



# CLI
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(description="Incremental Load Pipeline")
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--skip-install", action="store_true")
    args = parser.parse_args()

    run(skip_generate=args.skip_generate, skip_install=args.skip_install)


if __name__ == "__main__":
    main()