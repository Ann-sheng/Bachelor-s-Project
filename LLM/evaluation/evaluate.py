"""
evaluate.py — NL-to-SQL accuracy evaluation script

Usage:
    python evaluate.py                        # saves to results_baseline.json
    python evaluate.py --output results_v2    # saves to results_v2.json
"""

import os
import sys
import json
import time
import argparse
import re
import psycopg2
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sklearn.metrics import accuracy_score

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Load api/.env 
_here = Path(__file__).resolve().parent
_env_path = _here.parent / ".env"

if not _env_path.exists():
    print(f"ERROR: .env not found at {_env_path}")
    sys.exit(1)

load_dotenv(_env_path, override=True)

if not os.getenv("DB_PASSWORD"):
    print(f"ERROR: .env found at {_env_path} but DB_PASSWORD is empty or missing")
    sys.exit(1)


DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", 5432))
DB_NAME     = os.getenv("DB_NAME", "dnd_sales")
DB_USER     = os.getenv("DB_USER", "svc_nlsql")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SCHEMA   = os.getenv("DB_SCHEMA", "bl_dm")
API_URL     = "http://localhost:8000/query"
PAIRS_FILE  = _here / "test_pairs.json"

# ---------------------------------------------------------------------------
# SQL normalisation (for exact match)
# ---------------------------------------------------------------------------

def normalise_sql(sql: str) -> str:
    """
    Lower-cases, collapses all whitespace to single spaces, strips semicolons.
    Two semantically identical queries written in different styles will still
    differ after normalisation — that is expected and why execution match
    exists as a second metric.
    """
    sql = sql.lower()
    sql = re.sub(r"\s+", " ", sql)   # collapse whitespace
    sql = sql.strip().rstrip(";")
    return sql


def exact_match(generated: str, gold: str) -> bool:
    return normalise_sql(generated) == normalise_sql(gold)


# ---------------------------------------------------------------------------
# Postgres helpers (execution match)
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options=f"-c search_path={DB_SCHEMA}",
    )


def run_sql(sql: str):
    """
    Runs sql via svc_nlsql.  Returns (rows, error_message).
    rows is a sorted list of tuples (sortable for order-independent comparison).
    Returns (None, error_msg) on any DB error.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return sorted(rows, key=lambda r: [str(v) for v in r]), None
    except Exception as e:
        return None, str(e)


def execution_match(generated_sql: str, gold_sql: str):
    """
    Returns (match: bool, detail: str)
    match is False if either query errors or row-sets differ.
    """
    gold_rows, gold_err = run_sql(gold_sql)
    if gold_err:
        return False, f"gold_sql_error: {gold_err}"

    gen_rows, gen_err = run_sql(generated_sql)
    if gen_err:
        return False, f"generated_sql_error: {gen_err}"

    if gold_rows == gen_rows:
        return True, "rows_match"
    else:
        return False, (
            f"row_mismatch | gold_rows={len(gold_rows)} "
            f"generated_rows={len(gen_rows)}"
        )


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def call_api(question: str, timeout_s: int = 300):
    """
    POST to /query.  Returns (sql: str | None, error: str | None).
    timeout_s covers the full round-trip including LLM inference (~3 min on CPU).
    """
    try:
        resp = requests.post(
            API_URL,
            json={"question": question, "history": []},
            timeout=timeout_s,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("sql", ""), None
        else:
            return None, f"http_{resp.status_code}: {resp.text[:200]}"
    except requests.exceptions.Timeout:
        return None, "api_timeout"
    except Exception as e:
        return None, str(e)


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

def run_evaluation(pairs: list, output_path: Path):
    print(f"\n{'='*60}")
    print(f"  NL-to-SQL Evaluation — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Pairs: {len(pairs)}   Output: {output_path.name}")
    print(f"  Estimated runtime: ~{len(pairs) * 3 // 60}–{len(pairs) * 4 // 60} hours on CPU")
    print(f"{'='*60}\n")

    results = []
    exact_labels = []
    exec_labels   = []

    eval_start = time.time()

    for i, pair in enumerate(pairs, start=1):
        q_id       = pair["id"]
        category   = pair.get("category", "unknown")
        question   = pair["question"]
        gold_sql   = pair["sql"]

        q_start = time.time()
        elapsed_total = q_start - eval_start

        print(f"[{i:02d}/{len(pairs)}] Q{q_id} ({category})")
        print(f"         Question : {question}")

        # — API call —
        generated_sql, api_err = call_api(question)
        api_time = time.time() - q_start

        if api_err:
            print(f"         API ERROR : {api_err}")
            em = False
            xm, xm_detail = False, f"skipped_api_error: {api_err}"
        else:
            print(f"         Generated : {generated_sql[:120]}{'…' if len(generated_sql) > 120 else ''}")

            # — Exact match —
            em = exact_match(generated_sql, gold_sql)

            # — Execution match —
            xm, xm_detail = execution_match(generated_sql, gold_sql)

        exact_labels.append(1 if em else 0)
        exec_labels.append(1 if xm else 0)

        status_em = "✓ EXACT" if em else "✗ exact"
        status_xm = "✓ EXEC " if xm else "✗ exec "
        print(f"         Result    : {status_em}  |  {status_xm}  ({xm_detail})")
        print(f"         API time  : {api_time:.1f}s   "
              f"Total elapsed: {elapsed_total/60:.1f} min\n")

        results.append({
            "id"            : q_id,
            "category"      : category,
            "question"      : question,
            "gold_sql"      : gold_sql,
            "generated_sql" : generated_sql if not api_err else None,
            "api_error"     : api_err,
            "exact_match"   : em,
            "exec_match"    : xm,
            "exec_detail"   : xm_detail if not api_err else f"skipped: {api_err}",
            "api_time_s"    : round(api_time, 2),
        })

        # Checkpoint save after every question so progress isn't lost
        _save(results, exact_labels, exec_labels, output_path, final=False)

    # Final save with computed metrics
    _save(results, exact_labels, exec_labels, output_path, final=True)

    total_min = (time.time() - eval_start) / 60
    print(f"\n{'='*60}")
    print(f"  DONE in {total_min:.1f} min")
    exact_score = sum(exact_labels) / len(exact_labels)
    print(f"  Exact match     : {exact_score:.0%}  ({sum(exact_labels)}/{len(exact_labels)})")
    exec_score = sum(exec_labels) / len(exec_labels)
    print(f"  Execution match    : {exec_score:.0%}  ({sum(exec_labels)}/{len(exec_labels)})")
    print(f"  Results saved to: {output_path}")
    print(f"{'='*60}\n")


def _save(results, exact_labels, exec_labels, path: Path, final: bool):
    """Save current progress (and final metrics if final=True) to JSON."""
    payload = {
        "meta": {
            "timestamp"   : datetime.now().isoformat(),
            "total_pairs" : len(results),
            "complete"    : final,
        },
        "results": results,
    }
    if final and exact_labels:
        # accuracy_score(y_true, y_pred) — here both are the same list because
        # we built exact_labels/exec_labels as 1/0 ground truth == predicted.
        # For exact match, ground truth is all-1s vs the model's 1/0 outputs.
        payload["summary"] = {
            "exact_match_score"     : round(accuracy_score(
                [1] * len(exact_labels), exact_labels), 4),
            "execution_match_score" : round(accuracy_score(
                [1] * len(exec_labels), exec_labels), 4),
            "exact_match_count"     : f"{sum(exact_labels)}/{len(exact_labels)}",
            "exec_match_count"      : f"{sum(exec_labels)}/{len(exec_labels)}",
            "breakdown_by_category" : _category_breakdown(results),
        }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _category_breakdown(results: list) -> dict:
    """Per-category exact and execution match rates."""
    cats: dict = {}
    for r in results:
        c = r["category"]
        if c not in cats:
            cats[c] = {"exact": [], "exec": []}
        cats[c]["exact"].append(1 if r["exact_match"] else 0)
        cats[c]["exec"].append(1 if r["exec_match"] else 0)

    breakdown = {}
    for cat, data in cats.items():
        n = len(data["exact"])
        breakdown[cat] = {
            "n"           : n,
            "exact_match" : round(sum(data["exact"]) / n, 4),
            "exec_match"  : round(sum(data["exec"])  / n, 4),
        }
    return breakdown


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NL-to-SQL accuracy evaluator")
    parser.add_argument(
        "--output", default="results_baseline",
        help="Output filename without .json (default: results_baseline)"
    )
    parser.add_argument(
        "--pairs", default=str(PAIRS_FILE),
        help="Path to test_pairs.json"
    )
    parser.add_argument(
        "--start-from", type=int, default=1,
        help="Skip pairs with id < this value (resume after a crash)"
    )
    args = parser.parse_args()

    pairs_path = Path(args.pairs)
    if not pairs_path.exists():
        print(f"ERROR: test_pairs.json not found at {pairs_path}")
        sys.exit(1)

    with open(pairs_path, encoding="utf-8") as f:
        all_pairs = json.load(f)

    if args.start_from > 1:
        all_pairs = [p for p in all_pairs if p["id"] >= args.start_from]
        print(f"Resuming from pair id={args.start_from} ({len(all_pairs)} remaining)")

    output_path = _here / f"{args.output}.json"
    run_evaluation(all_pairs, output_path)