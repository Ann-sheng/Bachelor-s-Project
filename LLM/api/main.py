"""
NL-to-SQL API

Run with (from inside the api/ folder):
    uvicorn main:app --reload --port 8000
"""

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from models import QueryRequest, QueryResponse
from security import sanitise_input, is_safe_sql
from llm import generate_sql
from database import execute_query

app = FastAPI(title="NL-to-SQL API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["Content-Type"],
)


@app.post("/query", response_model=QueryResponse)
def query(request: QueryRequest) -> QueryResponse:
    question = sanitise_input(request.question)  
    sql      = generate_sql(question)             

    if not is_safe_sql(sql):                     
        raise HTTPException(
            status_code=422,
            detail=f"Generated SQL is not a safe SELECT statement: {sql!r}",
        )

    rows = execute_query(sql)                   
    return QueryResponse(sql=sql, rows=rows)


@app.get("/health")
def health():
    try:
        conn = psycopg2.connect(**settings.db_config)
        conn.close()
        db_status = "ok"
    except Exception as exc:
        db_status = f"unreachable — {exc}"

    return {
        "status":          "ok",
        "database":        db_status,
        "schema":          settings.db_schema,
        "security_layers": ["input_sanitisation", "statement_gate", "sqlglot", "read_only_user"],
    }