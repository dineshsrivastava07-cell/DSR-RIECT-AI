"""
DSR|RIECT — ClickHouse Schema Inspector
Inspects schemas, caches results in SQLite schema_cache table
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from clickhouse.connector import get_client
from db import get_connection
from settings.settings_store import get_clickhouse_config

logger = logging.getLogger(__name__)

# In-memory schema cache (supplement SQLite)
_schema_cache: dict = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def inspect_schemas(schemas: list = None, force_refresh: bool = False) -> dict:
    """
    Inspect ClickHouse schemas, return rich schema dict.
    Checks SQLite cache first; refreshes if force_refresh=True.
    """
    if schemas is None:
        cfg = get_clickhouse_config()
        schemas = cfg.get("schemas", ["vmart_sales", "customers", "vmart_product"])

    result = {}
    client = get_client()

    for schema in schemas:
        schema_tables = {}
        try:
            tables_result = client.query(f"SHOW TABLES FROM `{schema}`")
            tables = [row[0] for row in tables_result.result_rows]

            for table in tables:
                cached = _get_cached_columns(schema, table, force_refresh)
                if cached is not None:
                    schema_tables[table] = cached
                else:
                    columns = _describe_table(client, schema, table)
                    _save_to_cache(schema, table, columns)
                    schema_tables[table] = columns

        except Exception as e:
            logger.error(f"Schema inspection failed for {schema}: {e}")
            schema_tables["_error"] = str(e)

        result[schema] = schema_tables

    _schema_cache.update(result)
    return result


def _describe_table(client, schema: str, table: str) -> list[dict]:
    """Run DESCRIBE TABLE and return column list."""
    try:
        result = client.query(f"DESCRIBE TABLE `{schema}`.`{table}`")
        columns = []
        for row in result.result_rows:
            columns.append({
                "name": row[0],
                "type": row[1],
                "default_type": row[2] if len(row) > 2 else "",
                "default_expression": row[3] if len(row) > 3 else "",
                "comment": row[4] if len(row) > 4 else "",
            })
        return columns
    except Exception as e:
        logger.warning(f"DESCRIBE failed for {schema}.{table}: {e}")
        return []


def _get_cached_columns(schema: str, table: str, force_refresh: bool) -> Any:
    """Check SQLite cache. Returns None if missing or stale."""
    if force_refresh:
        return None
    conn = get_connection()
    try:
        from config import SCHEMA_CACHE_TTL
        row = conn.execute(
            "SELECT columns_json, cached_at FROM schema_cache WHERE schema_name=? AND table_name=?",
            (schema, table),
        ).fetchone()
        if row is None:
            return None
        # Check TTL
        cached_at = datetime.fromisoformat(row["cached_at"])
        age = (datetime.now(timezone.utc) - cached_at).total_seconds()
        if age > SCHEMA_CACHE_TTL:
            return None
        return json.loads(row["columns_json"])
    finally:
        conn.close()


def _save_to_cache(schema: str, table: str, columns: list):
    """Save column list to SQLite cache."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO schema_cache (schema_name, table_name, columns_json, cached_at)
               VALUES (?, ?, ?, ?)""",
            (schema, table, json.dumps(columns), _now()),
        )
        conn.commit()
    finally:
        conn.close()


def get_table_schema(schema: str, table: str) -> list[dict]:
    """Get columns for a specific table (from cache or ClickHouse)."""
    cached = _get_cached_columns(schema, table, False)
    if cached:
        return cached
    client = get_client()
    columns = _describe_table(client, schema, table)
    _save_to_cache(schema, table, columns)
    return columns


def get_schema_summary() -> dict:
    """Return summary dict: {schema: [table_names]} from SQLite cache."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT schema_name, table_name FROM schema_cache ORDER BY schema_name, table_name"
        ).fetchall()
        result = {}
        for row in rows:
            s, t = row["schema_name"], row["table_name"]
            result.setdefault(s, []).append(t)
        return result
    finally:
        conn.close()


def get_schema_as_text(schemas: list = None) -> str:
    """Return schema as text block for LLM prompts."""
    schema_dict = inspect_schemas(schemas)
    lines = []
    for schema_name, tables in schema_dict.items():
        lines.append(f"\n-- Schema: {schema_name}")
        for table_name, columns in tables.items():
            if table_name.startswith("_"):
                continue
            col_list = ", ".join(
                f"{c['name']} {c['type']}" for c in columns
            ) if columns else "(no columns)"
            lines.append(f"  {schema_name}.{table_name}({col_list})")
    return "\n".join(lines)
