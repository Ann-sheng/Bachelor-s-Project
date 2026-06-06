"""
NL-to-SQL API — schema context prompt wired in, bl_dm search_path fixed.

Run with (from inside the api/ folder):
    uvicorn llm_api:app --reload --port 800
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
from pathlib import Path

from schema_prompt import SCHEMA_CONTEXT


# ── Environment loading ───────────────────────────────────────────────────────
# Loads .env from project root so DB + model config can be externalized.
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=False)


# ── Configuration ──────────────────────────────────────────────────────────────
# Ollama LLM endpoint + model used for SQL generation
OLLAMA_URL   = os.getenv("OLLAMA_URL",   "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "sqlcoder")

# PostgreSQL connection config (read-only user expected)
DB_CONFIG = {
    "host":            os.getenv("DB_HOST", "localhost"),
    "port":            int(os.getenv("DB_PORT", 5432)),
    "dbname":          os.getenv("DB_NAME"),
    "user":            os.getenv("DB_USER"),
    "password":        os.getenv("DB_PASSWORD"),

    # Forces schema context (important for multi-schema safety)
    "options":         f"-c search_path={os.getenv('DB_SCHEMA', 'bl_dm')}",

    "connect_timeout": 10,
}

# Safety limits
MAX_QUESTION_LENGTH = int(os.getenv("MAX_QUESTION_LENGTH", 500))
QUERY_TIMEOUT       = int(os.getenv("QUERY_TIMEOUT_SECONDS", 300))
MAX_ROWS_RETURNED   = 500


# ── FastAPI setup ──────────────────────────────────────────────────────────────
app = FastAPI(title="NL-to-SQL API", version="0.6.0")

# Allow Power BI / frontend clients to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response schemas ────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    history: list[dict] = []   # conversation memory for better SQL generation


class QueryResponse(BaseModel):
    sql: str
    rows: list = []


# ── SECURITY LAYER 3: Input sanitisation ──────────────────────────────────────
# Blocks obvious SQL injection attempts at the natural-language stage.

INJECTION_PATTERNS = [
    r";\s*(drop|delete|truncate|update|insert|alter|create)",
    r"--",          # SQL comment injection
    r"/\*.*\*/",    # block comments
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

    # Block injection-like patterns early
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, question, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail="Question contains disallowed characters or patterns.",
            )

    return question


# ── SQL output cleaner ────────────────────────────────────────────────────────
# Removes markdown/code fences + model artefacts before execution.

def strip_sql(raw: str) -> str:
    text = re.sub(r'```sql\s*', '', raw, flags=re.IGNORECASE)
    text = text.replace('```', '')

    # Stop generation artifacts / prompt leakage
    for sep in ['\n---', '\n###', '\nQuestion:', '\n\n\n', '### Input', '### SQL']:
        idx = text.find(sep)
        if idx != -1:
            text = text[:idx]

    # Hard cutoff at first semicolon (prevents multi-statement execution)
    if ';' in text:
        text = text.split(';')[0]

    return text.strip()


# ── SECURITY LAYER 2: SQL statement gate ──────────────────────────────────────
# Ensures only SELECT queries are allowed.

FORBIDDEN_KEYWORDS = [
    "drop", "insert", "update", "delete",
    "truncate", "exec", "execute", "alter",
    "create", "replace", "grant", "revoke",
]

def is_safe_sql(sql: str) -> bool:
    """Quick keyword + structure safety check."""
    sql_lower = sql.lower()

    # Must be a SELECT query
    if not sql_lower.strip().startswith("select"):
        return False

    # Block dangerous keywords anywhere in query
    if any(kw in sql_lower for kw in FORBIDDEN_KEYWORDS):
        return False

    return True


# ── SQL syntax validation (sqlglot) ───────────────────────────────────────────
def validate_sql_syntax(sql: str) -> tuple[bool, str]:
    """
    Returns (is_valid, error_message).
    Does NOT raise — caller decides whether to block or warn.
    """
    try:
        parsed = sqlglot.parse(sql, dialect="postgres", error_level=sqlglot.ErrorLevel.RAISE)
        if not parsed:
            return False, "Empty parse result."
        return True, ""
    except sqlglot.errors.ParseError as exc:
        return False, str(exc)


# ── LLM CALL (Ollama) ─────────────────────────────────────────────────────────
def call_ollama(question: str, history: list[dict] = []) -> str:
    """
    Sends prompt to Ollama SQLCoder model with schema + chat history context.
    """

    # Keep only last 2 turns to avoid prompt bloat
    recent_history = history[-4:] if len(history) > 4 else history

    # Convert history into SQL-style comments so model doesn't treat it as structure
    history_text = ""
    if recent_history:
        history_text = "\n-- Recent context (for reference only):\n"
        for entry in recent_history:
            if isinstance(entry, dict):
                role    = entry.get("role", "")
                content = entry.get("content", "")
                if role == "user":
                    history_text += f"-- Previous question: {content}\n"
                elif role == "assistant":
                    history_text += f"-- Previous SQL: {content}\n"

    # Core prompt format expected by SQLCoder fine-tuned model
    prompt = f"""### Instructions:
Your task is to convert a question into a SQL query, given a Postgres database schema.
Adhere to these rules:
- Output ONLY the SQL query. No explanation, no preamble, no markdown.
- Always qualify table names with the bl_dm schema.
- Always use table aliases and qualify all column references.
- Carefully read the schema notes before writing any query.

### Input:
{SCHEMA_CONTEXT}
{history_text}
Question: {question}

### Response:
"""

    # Debug: estimate tokens 
    print(f"[debug] prompt size: {len(prompt)} chars, ~{len(prompt) // 4} tokens")

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0,
            "num_ctx": 4096,      # increases schema + history capacity
            "num_predict": 512,   # limits output length
            "stop": [";", "###", "```"],  # prevents multi-statement + prompt leakage
        },
    }

    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=QUERY_TIMEOUT)
        response.raise_for_status()

    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="Cannot reach Ollama. Is it running? Try: ollama serve",
        )

    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Ollama took too long to respond.")

    raw = response.json().get("response", "").strip()

    if not raw:
        raise HTTPException(status_code=500, detail="Ollama returned an empty response.")

    return raw

# ── SECURITY LAYER 1 — Postgres execution ────────────────────────────────────

# ── DATABASE EXECUTION LAYER ──────────────────────────────────────────────────
def execute_query(sql: str) -> list[dict]:
    """Executes validated SQL against Postgres (read-only expected user)."""

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"Cannot connect to Postgres: {exc}")

    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)

                # Limit returned dataset size for safety/performance
                rows = cur.fetchmany(MAX_ROWS_RETURNED)
                return [dict(row) for row in rows]

    except psycopg2.errors.InsufficientPrivilege:
        raise HTTPException(
            status_code=403,
            detail="svc_nlsql does not have permission to access that table.",
        )

    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Postgres error: {exc}")

    finally:
        conn.close()


# ── API ENDPOINT ──────────────────────────────────────────────────────────────
@app.post("/query", response_model=QueryResponse)
def query(request: QueryRequest):

    # Step 1: sanitize input
    clean_question = sanitise_input(request.question)

    # Step 2: generate SQL via LLM
    raw_sql = call_ollama(clean_question, request.history)
    sql = strip_sql(raw_sql)

    # Step 3: validate output exists
    if not sql:
        raise HTTPException(
            status_code=422,
            detail="Model did not return SQL — try rephrasing your question.",
        )

    # Step 4: enforce SELECT-only policy
    if not is_safe_sql(sql):
        raise HTTPException(
            status_code=422,
            detail=f"Generated SQL is not a safe SELECT statement: {sql!r}",
        )

    # Step 5: syntax validation (strict on first query, relaxed on follow-ups)
    is_valid, parse_error = validate_sql_syntax(sql)

    if not is_valid:
        if len(request.history) == 0:
            raise HTTPException(
                status_code=422,
                detail=f"Generated SQL failed syntax validation: {parse_error}",
            )
        else:
            print(f"[warn] sqlglot parse warning on follow-up (allowing): {parse_error}")

    # Step 6: execute query
    rows = execute_query(sql)

    return QueryResponse(sql=sql, rows=rows)


# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Checks DB connectivity + config sanity."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        db_status = "ok"
    except Exception as exc:
        db_status = f"unreachable — {exc}"

    return {
        "status": "ok",
        "database": db_status,
        "schema": os.getenv("DB_SCHEMA", "bl_dm"),
        "security_layers": [
            "input_sanitisation",
            "statement_gate",
            "sqlglot",
            "read_only_user",
        ],
    }