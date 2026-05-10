"""
Layer 3 — sanitise_input: runs before the question reaches the LLM.
Layer 2 — is_safe_sql:    runs after the LLM, before Postgres.

Layer 1 (read-only DB user) lives in database.py.
"""

import re
from fastapi import HTTPException
from config import settings

_INJECTION_PATTERNS = [
    r";\s*(drop|delete|truncate|update|insert|alter|create)",
    r"--",
    r"/\*.*\*/",
]

_FORBIDDEN_KEYWORDS = frozenset([
    "drop", "insert", "update", "delete",
    "truncate", "exec", "execute", "alter",
    "create", "replace", "grant", "revoke",
])


def sanitise_input(question: str) -> str:
    question = question.strip()

    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    if len(question) > settings.max_question_length:
        raise HTTPException(
            status_code=400,
            detail=f"Question too long ({len(question)} chars). Max is {settings.max_question_length}.",
        )

    for pattern in _INJECTION_PATTERNS:
        if re.search(pattern, question, re.IGNORECASE):
            raise HTTPException(
                status_code=400,
                detail="Question contains disallowed characters or patterns.",
            )

    return question


def is_safe_sql(sql: str) -> bool:
    lower = sql.lower()
    if not lower.strip().startswith("select"):
        return False
    if any(kw in lower for kw in _FORBIDDEN_KEYWORDS):
        return False
    return True