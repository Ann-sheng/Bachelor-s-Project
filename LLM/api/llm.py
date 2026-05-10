"""
If the LLM provider or model changes, this is the only file that changes.
"""

import re
import requests
import sqlglot
from fastapi import HTTPException

from config import settings
from schema_prompt import SCHEMA_CONTEXT

#ollama call
def generate_sql(question: str) -> str:
    raw = _call_ollama(question)
    sql = _clean_output(raw)
    _validate_syntax(sql)
    return sql


# ── private helpers ───────────────────────────────────────────────────────────

def _call_ollama(question: str) -> str:
    prompt = (
        f"{SCHEMA_CONTEXT}\n\n"
        f"### Question:\n{question}\n\n"
        f"### SQL query (SELECT only, using bl_dm schema):\n"
    )
    payload = {
        "model":   settings.ollama_model,
        "prompt":  prompt,
        "stream":  False,
        "options": {"temperature": 0, "stop": [";", "\n\n"]},
    }

    try:
        resp = requests.post(
            settings.ollama_url, json=payload, timeout=settings.query_timeout_seconds
        )
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="Cannot reach Ollama. Is it running? Try: ollama serve",
        )
    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Ollama took too long to respond.")

    raw = resp.json().get("response", "").strip()
    if not raw:
        raise HTTPException(status_code=500, detail="Ollama returned an empty response.")
    return raw


def _clean_output(raw: str) -> str:
    cleaned = re.sub(r"</?s>", "", raw)
    cleaned = re.sub(r"```(?:sql)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"--.*", "", cleaned)
    return cleaned.strip()


def _validate_syntax(sql: str) -> None:
    try:
        parsed = sqlglot.parse(sql, dialect="postgres", error_level=sqlglot.ErrorLevel.RAISE)
        if not parsed:
            raise ValueError("Empty parse result.")
    except sqlglot.errors.ParseError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Generated SQL failed syntax validation: {exc}",
        )