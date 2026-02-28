"""
DSR|RIECT — Vectoriser
Embeds user query and scores against schema descriptions for top-K table selection
Uses cosine similarity on keyword overlap (lightweight, no model dependency at runtime)
Falls back to sentence-transformers if available
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Attempt to load sentence-transformers
_embedder = None
_embed_available = False

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
    _embedder = SentenceTransformer("all-MiniLM-L6-v2")
    _embed_available = True
    logger.info("sentence-transformers loaded for vectoriser")
except Exception:
    pass  # Keyword scoring fallback is active — no warning needed


def _keyword_score(query_tokens: set, table_name: str, columns: list) -> float:
    """Lightweight keyword overlap scoring."""
    table_tokens = set(re.split(r'[_\s]+', table_name.lower()))
    col_tokens = set()
    for col in columns:
        col_tokens.update(re.split(r'[_\s]+', col.get("name", "").lower()))

    candidate_tokens = table_tokens | col_tokens
    if not candidate_tokens:
        return 0.0

    overlap = len(query_tokens & candidate_tokens)
    score = overlap / (len(query_tokens) + 1)
    return score


def rank_tables(query: str, schema_dict: dict, top_k: int = 5) -> list[dict]:
    """
    Score all tables against the query and return top-K most relevant.
    Returns: [{"schema": str, "table": str, "columns": list, "score": float}]
    """
    query_tokens = set(re.split(r'[\s_\-,.:;?!]+', query.lower()))
    # Remove stop words
    stop_words = {"the", "a", "an", "of", "for", "in", "at", "by", "to",
                  "is", "are", "was", "were", "show", "me", "get", "what",
                  "how", "many", "all", "any", "this", "that", "with", "and"}
    query_tokens -= stop_words

    candidates = []
    for schema_name, tables in schema_dict.items():
        # Handle both full schema ({table: [cols]}) and summary ({schema: [table_names]})
        if isinstance(tables, list):
            # Summary format — no column info, use table name only for scoring
            for table_name in tables:
                if not isinstance(table_name, str) or table_name.startswith("_"):
                    continue
                score = _keyword_score(query_tokens, table_name, [])
                candidates.append({
                    "schema": schema_name, "table": table_name,
                    "full_name": f"{schema_name}.{table_name}",
                    "columns": [], "score": score,
                })
            continue
        for table_name, columns in tables.items():
            if table_name.startswith("_"):
                continue
            if not isinstance(columns, list):
                continue

            score = _keyword_score(query_tokens, table_name, columns)

            # Bonus for intent keywords — mapped to real table name fragments
            intent_boosts = {
                "sales":      ["pos_transactional", "omni_transactional", "dt_pos"],
                "revenue":    ["pos_transactional", "omni_transactional"],
                "bill":       ["pos_transactional", "omni_transactional"],
                "transaction": ["pos_transactional", "omni_transactional"],
                "inventory":  ["inventory_current"],
                "stock":      ["inventory_current"],
                "soh":        ["inventory_current"],
                "customer":   ["pos_transactional"],
                "store":      ["stores", "pos_transactional"],
                "article":    ["pos_transactional", "inventory_current"],
                "sku":        ["inventory_current"],
                "transfer":   ["dt_pos_ist"],
                "ist":        ["dt_pos_ist"],
            }
            q_lower = query.lower()
            for term, table_hints in intent_boosts.items():
                if term in q_lower:
                    for hint in table_hints:
                        if hint in table_name.lower():
                            score += 0.3
                            break

            candidates.append({
                "schema": schema_name,
                "table": table_name,
                "full_name": f"{schema_name}.{table_name}",
                "columns": columns,
                "score": round(score, 4),
            })

    # Sort by score descending, take top-K
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:top_k]


def get_relevant_schema_text(query: str, schema_dict: dict, top_k: int = 5) -> str:
    """Return relevant schema as text block for LLM prompt injection."""
    top_tables = rank_tables(query, schema_dict, top_k)
    lines = [f"-- Most relevant tables for: '{query}'\n"]
    for item in top_tables:
        if item["score"] > 0 or len(top_tables) <= 3:
            if item["columns"]:
                col_lines = "\n".join(
                    f"    {c['name']} ({c.get('type','String')})"
                    for c in item["columns"]
                )
                lines.append(f"Table: {item['full_name']}\nColumns:\n{col_lines}\n")
            else:
                lines.append(f"Table: {item['full_name']} (no columns)\n")
    return "\n".join(lines)
