# Dapper & Darling — Retail Data Warehouse & NL-to-SQL Analytics Platform

> A full end-to-end data engineering project built on PostgreSQL, Python, and a fine-tuned local LLM — from raw sales data to a conversational SQL interface embedded in Power BI.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Repository Structure](#repository-structure)
4. [Prerequisites](#prerequisites)
5. [Quick Start](#quick-start)
6. [Component Reference](#component-reference)
   - [Data Warehouse Layers](#data-warehouse-layers)
   - [Data Generator](#data-generator)
   - [ETL Pipelines](#etl-pipelines)
   - [NL-to-SQL API](#nl-to-sql-api)
   - [LLM Fine-Tuning](#llm-fine-tuning)
   - [Power BI Custom Visual](#power-bi-custom-visual)
7. [Configuration & Environment Variables](#configuration--environment-variables)
8. [Security Model](#security-model)
9. [Running the Evaluation Suite](#running-the-evaluation-suite)
10. [Troubleshooting](#troubleshooting)
11. [License](#license)
12. [Contributing](#contributing)

---

## Project Overview

This project implements a **retail data warehouse** for the fictional vintage clothing brand *Dapper & Darling*, together with a **natural-language-to-SQL** (NL-to-SQL) query interface that runs entirely on-premises using a locally hosted LLM.

### What it does

| Capability | Details |
|---|---|
| **Synthetic data generation** | Generates 500 k initial + 250 k incremental realistic retail transactions (offline & online) with SCD Type 2 change scenarios |
| **Multi-layer DWH** | Five PostgreSQL schemas: `src` → `stg_cln` → `bl_3nf` → `bl_dm` → `bl_cn` (control/logging) |
| **ETL automation** | Python orchestrator executes stored procedures in dependency order; supports initial and incremental loads |
| **NL-to-SQL API** | FastAPI service converts plain-English questions into SQL via a local SQLCoder 7B model served by Ollama |
| **LLM fine-tuning** | QLoRA fine-tuning notebook (Google Colab / T4) that adapts SQLCoder 7B-2 to the warehouse schema |
| **Power BI integration** | Custom React visual embedded in a `.pbix` report — type a question, get a live table of results |
| **Security layers** | Three-layer defence: input sanitisation → statement-type gate → read-only DB user |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW                                    │
│                                                                      │
│  Generator ──► MinIO (Parquet) ──► SRC ──► STG_CLN ──► BL_3NF        │
│                                                           │          │
│                                                        BL_DM         │
│                                                           │          │
│                                                     Power BI         │
│                                                      │               │
│                                              Custom Visual           │
│                                                      │               │
│                                            FastAPI NL-to-SQL         │
│                                                      │               │
│                                         Ollama + SQLCoder-ft         │
└──────────────────────────────────────────────────────────────────────┘
```

### DWH Layer Progression

```
src         Raw landing tables (VARCHAR everything, no validation)
  │
stg_cln     Cleaned & typed staging (duplicate removal, reject capture)
  │
bl_3nf      3NF core entities (CE_*) — SCD Type 1 & 2
  │
bl_dm       Star schema (fact + dimensions) — optimised for analytics
  │
bl_cn       Control & logging (ETL run tracking, views, error surfacing)
```

---

## Repository Structure

```
ann-sheng-bachelor-s-project/
│
├── DWH_BUILD/                      # All DDL & DML for the warehouse
│   ├── bootstrap/                  # Database & role creation
│   ├── setup/                      # Schemas, permissions, role settings
│   ├── sa_audit/                   # File-load audit log
│   ├── src/                        # Raw source tables
│   ├── stg_cln/                    # Staging tables, procedures, indexes
│   ├── bl_3nf/                     # 3NF entities, functions, procedures
│   ├── bl_dm/                      # Star schema tables, procedures, indexes
│   └── bl_cn/                      # Control layer — logging infrastructure
│
├── SCRIPTS/                        # Python utilities
│   ├── docker-compose.yaml         # PostgreSQL + MinIO containers
│   ├── requirements.txt
│   ├── cloud/                      # Cloud storage abstraction (S3/MinIO)
│   ├── generator/                  # Synthetic data generators
│   │   ├── generate_initial_load.py
│   │   ├── generate_incremental_load.py
│   │   └── vintage_data.py         # Brand-specific reference data
│   ├── helper/                     # Shared utilities
│   └── loader/                     # Staging loader & procedure runner
│
├── PIPELINES/                      # Orchestration entry points
│   ├── Orchestrator.py             # Master CLI — run everything from here
│   ├── Infrastructure_Setup.py     # Step-by-step DDL execution
│   ├── Initial_Load.py             # Full initial pipeline
│   └── Incremental_Load.py         # Incremental pipeline
│
├── LLM/                            # NL-to-SQL system
│   ├── requirements.txt
│   ├── api/
│   │   ├── llm_api.py              # FastAPI application
│   │   ├── schema_prompt.py        # Schema context injected into every prompt
│   │   └── security_test.py        # Security regression suite
│   ├── evaluation/
│   │   ├── evaluate.py             # Exact-match + execution-match evaluator
│   │   ├── test_pairs.json         # 50 gold question–SQL pairs
│   │   ├── results_baseline.json   # Baseline evaluation results
│   │   ├── results_finetuned.json  # Post fine-tune results
│   │   └── results_after_eng.json  # Post prompt-engineering results
│   └── fine_tunning/
│       ├── fine_tunning.ipynb      # QLoRA training notebook (Colab)
│       ├── Modelfile               # Ollama model config for the fine-tuned GGUF
│       └── training_pairs.json     # 176 training question–SQL pairs
│
├── POWER_BI_REPORT/
│   └── cust_visual/
│       └── sqlChatVisual/          # Custom Power BI visual (React + TypeScript)
│           ├── src/
│           │   ├── ChatVisual.tsx  # Main chat UI component
│           │   ├── visual.ts       # PBI visual entry point
│           │   └── settings.ts     # Formatting model
│           └── style/
│               └── visual.less
│
└── DOCUMENTATION/
    └── dimensions data description.docx
```

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.10 | Pipelines, generators, API |
| PostgreSQL | 16 | Data warehouse |
| Docker + Docker Compose | any recent | PostgreSQL + MinIO containers |
| Ollama | latest | LLM inference server |
| Node.js | ≥ 18 | Power BI visual build |
| PBIVIZ CLI | latest | Package the custom visual |
| Google Colab (optional) | — | Fine-tuning (requires T4 GPU) |

---

## Quick Start

### 1 — Start infrastructure

```bash
cd SCRIPTS
cp .env.example .env          # fill in DB and MinIO credentials
docker-compose up -d          # starts PostgreSQL on :5432 and MinIO on :9000
```

### 2 — Build the warehouse

```bash
cd PIPELINES
python Orchestrator.py create_dwh      # runs all DDL (62 steps)
python Orchestrator.py install_etl     # installs stored procedures
```

### 3 — Generate data and run the initial load

```bash
python Orchestrator.py initial         # generates 1M rows, uploads, ETL
```

### 4 — Run an incremental load (optional)

```bash
python Orchestrator.py incremental     # 500 k rows + SCD2 changes
```

### 5 — Start the NL-to-SQL API

```bash
# Pull the base model and load the fine-tuned adapter
ollama create sqlcoder-ft -f LLM/fine_tunning/Modelfile

cd LLM/api
pip install -r ../requirements.txt
uvicorn llm_api:app --reload --port 800
```

### 6 — Test the API

```bash
curl -X POST http://localhost:800/query \
     -H "Content-Type: application/json" \
     -d '{"question": "What is the total revenue by product category?"}'
```

### 7 — Build and install the Power BI visual

```bash
cd POWER_BI_REPORT/cust_visual/sqlChatVisual
npm install
pbiviz package                   # produces sqlChatVisual.pbiviz
```

Import the `.pbiviz` file into your Power BI Desktop report.

---

## Component Reference

### Data Warehouse Layers

#### `bl_cn` — Control & Logging

Every ETL procedure calls `bl_cn.log_start()` on entry and `bl_cn.log_success()` / `bl_cn.log_failure()` on exit. This writes one row to `bl_cn.etl_log` per procedure execution.

| View | Purpose |
|---|---|
| `v_latest_runs` | Most recent execution status per procedure |
| `v_failed_runs` | All FAILED runs + stalled STARTED runs (> 30 min) |
| `v_etl_summary` | Aggregated success rate, avg duration, total rows per procedure |
| `v_pipeline_runs` | Pipeline-level summary grouped by `run_id` |

#### `bl_3nf` — Core Entities

| Entity | SCD Type | Source |
|---|---|---|
| `ce_suppliers` | 1 | Offline + Online |
| `ce_shippings` | 1 | Online only |
| `ce_store_branches` | 1 | Offline only |
| `ce_products` | 1 | Offline + Online |
| `ce_customers_scd` | **2** | Offline + Online |
| `ce_employees_scd` | **2** | Offline + Online |
| `ce_transactions` | Insert-only | Offline + Online |

#### `bl_dm` — Star Schema

The fact table `fct_transactions_dd` links to:
`dim_dates`, `dm_products`, `dm_customers_scd`, `dm_employees_scd`,
`dm_store_branches`, `dm_shippings`, `dm_suppliers`, `dm_junk_transactions`

Pre-computed measures stored in the fact: `transaction_gross_profit`, `transaction_profit_margin`.

---

### Data Generator

Two scripts produce **Parquet files** uploaded directly to MinIO:

| Script | Rows | Date range |
|---|---|---|
| `generate_initial_load.py` | 500 k offline + 500 k online | 2020-01-01 → 2023-12-31 |
| `generate_incremental_load.py` | 250 k offline + 250 k online | 2024-01-01 → 2024-06-30 |

The incremental script also applies **SCD2 change scenarios**:

- 400 customer city relocations
- 200 customer contact updates
- 80 employee promotions (offline)
- 40 employee salary raises (online)
- 1 500 new customers + 15 new products

All entity pools (products, employees, customers, stores, suppliers) are
persisted to MinIO as reference Parquet files so the incremental run can pick
up exactly where the initial load left off.

---

### ETL Pipelines

Run everything through the single entry point:

```bash
python Orchestrator.py <command> [options]

Commands:
  create_dwh      Infrastructure DDL only
  install_etl     (Re-)install stored procedures
  initial         generate + stage + ETL (initial)
  incremental     generate + stage + ETL (incremental)
  all             Everything in order

Options:
  --skip-generate    Skip data generation (data already in cloud)
  --skip-install     Skip procedure installation
  --from-step N      Resume DDL setup from step N
  --dry-run          Print plan without executing (create_dwh only)
```

---

### NL-to-SQL API

**Base URL:** `http://localhost:800`

#### `POST /query`

```json
// Request
{
  "question": "What are the top 5 customers by total spending?",
  "history": []
}

// Response
{
  "sql": "SELECT c.customer_firstname, ...",
  "rows": [{ "customer_firstname": "Alex", ... }, ...]
}
```

Multi-turn conversation is supported via the `history` array (last 4 messages are forwarded to the model).

#### `GET /health`

Returns database reachability and security layer status.

#### Three-Layer Security

```
Layer 3 (input)   sanitise_input()   — length cap, injection pattern block
Layer 2 (SQL)     is_safe_sql()      — SELECT-only gate, forbidden keyword list
Layer 1 (DB)      svc_nlsql user     — read-only, schema-locked, statement_timeout=5min
```

---

### LLM Fine-Tuning

The fine-tuning notebook (`LLM/fine_tunning/fine_tunning.ipynb`) runs on
Google Colab with a T4 GPU and takes approximately 60 minutes.

**Training data:** 176 hand-crafted question–SQL pairs covering 15 query categories
(simple aggregation, date filters, multi-join, SCD2, junk dimension, etc.)

**Process:**

1. Load `defog/sqlcoder-7b-2` in 4-bit quantisation (QLoRA)
2. Attach LoRA adapters to all attention + FFN projection layers (r=16, α=32)
3. Train for 3 epochs with completion-only loss
4. Merge adapter → base model → export to GGUF (Q4_K_M, ≈ 4 GB)
5. Load into Ollama via `Modelfile`

**Evaluation results** (50 gold pairs, execution-match metric):

| Model | Exact match | Execution match |
|---|---|---|
| Baseline SQLCoder-7B-2 | — | ~62% |
| + Prompt engineering | — | ~74% |
| + Fine-tuning | — | ~82% |

---

### Power BI Custom Visual

The `sqlChatVisual` is a React + TypeScript visual that:

- Renders a split-pane chat interface (left: conversation, right: result table)
- Sends questions to `http://localhost:800/query` on user submit
- Persists conversation and last result set in `localStorage` (survives PBI tab switching)
- Provides CSV export via clipboard or modal fallback
- Supports multi-turn conversation by passing history to the API

**Build:**

```bash
cd POWER_BI_REPORT/cust_visual/sqlChatVisual
npm install
pbiviz package          # → dist/sqlChatVisual.pbiviz
```

> **Note:** The FastAPI server must be running locally on port 800 while the report is in use.

---

## Configuration & Environment Variables

Copy `SCRIPTS/.env` and fill in the values:

```dotenv
# Cloud / MinIO
CLOUD_PROVIDER=minio
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_BUCKET=dnd-sales-raw

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dnd_sales
DB_USER=postgres
DB_PASSWORD=your_password
ADMIN_USER=postgres
ADMIN_PASSWORD=your_password

# Generator seeds (change for different datasets)
SEED_INITIAL=42
SEED_INCREMENTAL=99
```

For the LLM API, copy `LLM/.env`:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dnd_sales
DB_SCHEMA=bl_dm
DB_USER=svc_nlsql
DB_PASSWORD=svc_nlsql_password

OLLAMA_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=sqlcoder-ft

MAX_QUESTION_LENGTH=500
```

---

## Security Model

### Database Roles

| Role | Login user | Permissions |
|---|---|---|
| `dwh_admin` | `dba_admin` | Full access, CREATEROLE, CREATEDB |
| `dwh_etl` | `svc_etl` | SELECT/INSERT/UPDATE on all schemas |
| `dwh_analyst` | `analyst_user` | SELECT on `bl_dm` + `bl_cn`; 5-min statement timeout |
| `dwh_reporter` | `svc_bi_tool` | SELECT on `bl_dm` only; 2-min statement timeout |
| `dwh_reporter` | `svc_nlsql` | SELECT on `bl_dm` only; 1-min lock timeout, 5-min statement timeout |

`svc_nlsql` accesses only `bl_dm` and cannot call any DML. Employee salaries are
exposed through `bl_dm.v_employees_public` which returns `NULL` for the `employee_salary`
column — the base table is explicitly revoked from analyst and reporter roles.

### API Security

The security regression suite (`LLM/api/security_test.py`) covers 10 test cases
across all three layers. Run it with:

```bash
cd LLM/api
python security_test.py --output security_results_v2
```

---

## Running the Evaluation Suite

```bash
# Ensure the API is running on :800 and the DB is populated
cd LLM/evaluation
python evaluate.py --output results_myrun

# Resume after a crash
python evaluate.py --output results_myrun --start-from 15
```

The evaluator measures two metrics per question:

- **Exact match** — normalised SQL string comparison
- **Execution match** — row-set equality after running both gold and generated SQL against the live database

Results are saved to JSON with per-category breakdowns.

---

## Troubleshooting

**Ollama returns empty response**
Verify the model is loaded: `ollama list`. The prompt size is ~2 500 tokens; ensure `num_ctx` is set to at least 4 096 in the Modelfile.

**`svc_nlsql` permission denied**
Re-run `DWH_BUILD/setup/permission.sql` as `dba_admin` after any schema change.

**Staging loader skips all files**
The idempotency check matches on `batch_id`. Use `--force` to reload a batch that already exists.

**Infrastructure setup fails at step N**
Resume with `python Orchestrator.py create_dwh --from-step N`.

**Power BI visual shows "Could not reach FastAPI"**
The visual calls `http://localhost:800` from within the Power BI iframe. FastAPI must be running on the same machine where the report is open, and the browser must allow localhost requests.

---

## License

This project is released under the **MIT License**. See [LICENSE](LICENSE) for the full text.

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.


