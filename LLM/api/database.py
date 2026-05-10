"""
If the database driver or connection strategy changes, this is the only file that changes.

Security Layer 1: nlsql_user is granted SELECT-only on the dm schema at the database level
"""

import psycopg2
import psycopg2.extras
from fastapi import HTTPException

from config import settings


def execute_query(sql: str) -> list[dict]:
    try:
        conn = psycopg2.connect(**settings.db_config)
    except psycopg2.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"Cannot connect to Postgres: {exc}")

    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchmany(settings.max_rows_returned)
                return [dict(row) for row in rows]
    except psycopg2.errors.InsufficientPrivilege:
        raise HTTPException(
            status_code=403,
            detail="nlsql_user does not have permission to access that table.",
        )
    except psycopg2.Error as exc:
        raise HTTPException(status_code=500, detail=f"Postgres error: {exc}")
    finally:
        conn.close()