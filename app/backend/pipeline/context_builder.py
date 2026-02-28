"""
DSR|RIECT — Context Builder
Assembles full context dict for SQL generation and LLM prompting
"""

import json
import logging
from typing import Any

from config import KPI_FORMULAS, JOIN_HINTS, CHAT_HISTORY_WINDOW
from db import get_connection
from pipeline.intent_engine import classify_intent
from pipeline.vectoriser import rank_tables, get_relevant_schema_text

logger = logging.getLogger(__name__)


def build_context(query: str, session_id: str, schema_dict: dict) -> dict:
    """
    Assemble full context dict for pipeline.
    Returns: {intent, relevant_schemas, schema_text, chat_history, kpi_formulas, join_hints}
    """
    intent_result = classify_intent(query)

    # Get top-K relevant tables
    relevant_tables = rank_tables(query, schema_dict, top_k=5)
    schema_text = get_relevant_schema_text(query, schema_dict, top_k=5)

    # Load chat history
    chat_history = _get_chat_history(session_id)

    # Select relevant KPI formulas based on intent
    kpi_types = intent_result.get("kpi_types", [])
    relevant_formulas = {k: v for k, v in KPI_FORMULAS.items() if k in kpi_types} \
        if kpi_types else KPI_FORMULAS

    return {
        "intent": intent_result,
        "relevant_schemas": relevant_tables,
        "schema_text": schema_text,
        "chat_history": chat_history,
        "kpi_formulas": relevant_formulas,
        "join_hints": JOIN_HINTS,
        "query": query,
        "session_id": session_id,
    }


def _get_chat_history(session_id: str) -> list[dict]:
    """Load last N messages for this session."""
    if not session_id:
        return []
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT role, content, created_at FROM messages
               WHERE session_id = ?
               ORDER BY id DESC LIMIT ?""",
            (session_id, CHAT_HISTORY_WINDOW * 2),
        ).fetchall()
        # Reverse to get chronological order
        history = [
            {"role": row["role"], "content": row["content"]}
            for row in reversed(rows)
        ]
        return history
    finally:
        conn.close()


def format_history_for_prompt(history: list[dict]) -> str:
    """Format chat history as prompt text."""
    if not history:
        return ""
    lines = ["--- Chat History ---"]
    for msg in history:
        role = "User" if msg["role"] == "user" else "Assistant"
        content = msg["content"][:600]  # Truncate long messages
        lines.append(f"{role}: {content}")
    return "\n".join(lines)
