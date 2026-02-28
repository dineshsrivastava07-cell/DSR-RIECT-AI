"""
DSR|RIECT — ClickHouse Query Runner
Execute SQL and return structured result dict
"""

import logging
import time
from typing import Any

import pandas as pd

from clickhouse.connector import get_client

logger = logging.getLogger(__name__)


def run_query(sql: str) -> dict:
    """
    Execute a ClickHouse SQL query.
    Returns: {data, columns, row_count, execution_time_ms, sql_used} or {error, sql_used}
    """
    t_start = time.perf_counter()
    try:
        client = get_client()
        result = client.query(sql)
        elapsed_ms = int((time.perf_counter() - t_start) * 1000)

        columns = list(result.column_names)
        rows = result.result_rows  # list of tuples

        # Convert to list of dicts
        data = [dict(zip(columns, row)) for row in rows]

        # Sanitise values for JSON serialisation
        data = _sanitise_data(data)

        return {
            "data": data,
            "columns": columns,
            "row_count": len(data),
            "execution_time_ms": elapsed_ms,
            "sql_used": sql,
        }
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - t_start) * 1000)
        logger.error(f"Query failed ({elapsed_ms}ms): {e}\nSQL: {sql[:500]}")
        return {
            "error": str(e),
            "sql_used": sql,
            "execution_time_ms": elapsed_ms,
        }


def run_query_df(sql: str) -> tuple[pd.DataFrame, dict]:
    """
    Execute query and return (DataFrame, meta_dict).
    DataFrame is empty on error.
    """
    result = run_query(sql)
    if "error" in result:
        return pd.DataFrame(), result
    df = pd.DataFrame(result["data"])
    return df, result


def _sanitise_data(data: list[dict]) -> list[dict]:
    """Convert non-JSON-serialisable types to safe equivalents."""
    import math
    from datetime import date, datetime

    sanitised = []
    for row in data:
        clean = {}
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                clean[k] = v.isoformat()
            elif isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    clean[k] = None
                else:
                    clean[k] = v
            elif hasattr(v, "item"):  # numpy scalar
                clean[k] = v.item()
            else:
                clean[k] = v
        sanitised.append(clean)
    return sanitised
