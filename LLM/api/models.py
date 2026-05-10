"""
models.py — Request and response shapes 
"""

from pydantic import BaseModel


class QueryRequest(BaseModel):
    question: str
    history:  list[str] = []


class QueryResponse(BaseModel):
    sql:  str
    rows: list = []