"""
NL-to-SQL API — schema context prompt wired in, bl_dm search_path fixed.

Run with (from inside the api/ folder):
    uvicorn llm_api:app --reload --port 8000
"""

import os
import re
import requests
import sqlglot
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from schema_prompt import SCHEMA_CONTEXT   # ← Step 7 addition

# ── Load environment ──────────────────────────────────────────────────────────

load_dotenv(dotenv_path=".env", override=False)

# ── Config ────────────────────────────────────────────────────────────────────

OLLAMA_URL   = os.getenv("OLLAMA_URL",   "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "sqlcoder")

DB_CONFIG = {
    "host":            os.getenv("DB_HOST", "localhost"),
    "port":            int(os.getenv("DB_PORT", 5432)),
    "dbname":          os.getenv("DB_NAME"),
    "user":            os.getenv("DB_USER"),
    "password":        os.getenv("DB_PASSWORD"),
    "options":         f"-c search_path={os.getenv('DB_SCHEMA', 'bl_dm')}",  # fixed
    "connect_timeout": 10,
}

MAX_QUESTION_LENGTH = int(os.getenv("MAX_QUESTION_LENGTH", 500))
QUERY_TIMEOUT       = int(os.getenv("QUERY_TIMEOUT_SECONDS", 300))
MAX_ROWS_RETURNED   = 500

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="NL-to-SQL API", version="0.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)

# ── Request / response models ─────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    history: list[str] = []

class QueryResponse(BaseModel):
    sql: str
    rows: list = []

# ── SECURITY LAYER 3 — Input sanitisation ────────────────────────────────────

INJECTION_PATTERNS = [
    r";\s*(drop|delete|truncate|update|insert|alter|create)",
    r"--",
    r"/\*.*\*/",
]

def sanitise_input(question: str) -> str:
    question = question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    if len(question) > MAX_QUESTION_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Question too long ({len(question)} chars). Maximum is {MAX_QUESTION_LENGTH}.",
        )
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, question, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail="Question contains disallowed characters or patterns.",
            )
    return question

# ── Output cleaner ────────────────────────────────────────────────────────────

def strip_sql(raw: str) -> str:
    cleaned = re.sub(r"</?s>", "", raw)
    cleaned = re.sub(r"```(?:sql)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"--.*", "", cleaned)
    return cleaned.strip()

# ── SECURITY LAYER 2 — Statement type gate ───────────────────────────────────

FORBIDDEN_KEYWORDS = [
    "drop", "insert", "update", "delete",
    "truncate", "exec", "execute", "alter",
    "create", "replace", "grant", "revoke",
]

def is_safe_sql(sql: str) -> bool:
    sql_lower = sql.lower()
    if not sql_lower.strip().startswith("select"):
        return False
    if any(kw in sql_lower for kw in FORBIDDEN_KEYWORDS):
        return False
    return True

# ── sqlglot validation ────────────────────────────────────────────────────────

def validate_sql_syntax(sql: str) -> None:
    try:
        parsed = sqlglot.parse(sql, dialect="postgres", error_level=sqlglot.ErrorLevel.RAISE)
        if not parsed:
            raise ValueError("Empty parse result.")
    except sqlglot.errors.ParseError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Generated SQL failed syntax validation: {exc}",
        )

# ── Ollama helper ─────────────────────────────────────────────────────────────

def call_ollama(question: str) -> str:
    prompt = f"""{SCHEMA_CONTEXT}

### Question:
{question}

### SQL query (SELECT only, using bl_dm schema):
"""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "stop": [";", "\n\n"],
        },
    }

    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=QUERY_TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Cannot reach Ollama. Is it running? Try: ollama serve")
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Ollama took too long to respond.")

    raw = response.json().get("response", "").strip()
    if not raw:
        raise HTTPException(status_code=500, detail="Ollama returned an empty response.")
    return raw

# ── SECURITY LAYER 1 — Postgres execution ────────────────────────────────────

def execute_query(sql: str) -> list[dict]:
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"Cannot connect to Postgres: {exc}")

    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchmany(MAX_ROWS_RETURNED)
                return [dict(row) for row in rows]
    except psycopg2.errors.InsufficientPrivilege:
        raise HTTPException(status_code=403, detail="svc_nlsql does not have permission to access that table.")
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Postgres error: {exc}")
    finally:
        conn.close()

# ── Endpoint ──────────────────────────────────────────────────────────────────

@app.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):
    clean_question = sanitise_input(request.question)
    raw_sql        = call_ollama(clean_question)
    sql            = strip_sql(raw_sql)

    if not is_safe_sql(sql):
        raise HTTPException(
            status_code=422,
            detail=f"Generated SQL is not a safe SELECT statement: {sql!r}",
        )

    validate_sql_syntax(sql)
    rows = execute_query(sql)

    return QueryResponse(sql=sql, rows=rows)

# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        db_status = "ok"
    except Exception as exc:
        db_status = f"unreachable — {exc}"

    return {
        "status":          "ok",
        "database":        db_status,
        "schema":          os.getenv("DB_SCHEMA", "bl_dm"),
        "security_layers": ["input_sanitisation", "statement_gate", "sqlglot", "read_only_user"],
    }