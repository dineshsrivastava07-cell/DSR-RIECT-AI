"""
DSR|RIECT — KPI Controller
Orchestrates all KPI engines, graceful degradation if data missing
"""

import logging
import pandas as pd
from typing import Any

from riect.kpi_engine import spsf_engine, sell_thru_engine, doi_engine, mbq_engine
from riect.kpi_engine.anomaly_engine import detect_anomalies

logger = logging.getLogger(__name__)


class KPIController:
    """
    Runs all available KPI engines on query result DataFrames.
    Returns combined KPI results with summaries and breach records.
    """

    def run_all(self, query_result: dict) -> dict:
        """
        Run all applicable KPI engines on query result.
        Returns: {spsf, sell_thru, doi, mbq, combined_breaches, summaries}
        """
        if not query_result or "data" not in query_result or not query_result["data"]:
            return _empty_result()

        df = pd.DataFrame(query_result["data"])
        df.columns = [c.lower().strip() for c in df.columns]

        results = {}

        # Run each engine (graceful degradation)
        results["spsf"] = self._run_engine("SPSF", spsf_engine, df)
        results["sell_thru"] = self._run_engine("SELL_THRU", sell_thru_engine, df)
        results["doi"] = self._run_engine("DOI", doi_engine, df)
        results["mbq"] = self._run_engine("MBQ", mbq_engine, df)

        # Collect all breach records
        combined_breaches = []
        for kpi_name, engine_result in results.items():
            if engine_result.get("breaches") is not None:
                breaches_df = engine_result["breaches"]
                if not breaches_df.empty:
                    breach_records = breaches_df.to_dict(orient="records")
                    for record in breach_records:
                        record["kpi_type"] = kpi_name.upper()
                    combined_breaches.extend(breach_records)

        # Sort combined by priority
        priority_order = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
        combined_breaches.sort(key=lambda x: priority_order.get(x.get("priority", "P4"), 3))

        # Run anomaly detection across the full result DataFrame
        try:
            anomaly_result = detect_anomalies(df)
        except Exception as e:
            logger.warning(f"Anomaly detection skipped: {e}")
            anomaly_result = {"anomalies": [], "total_anomalies": 0}

        return {
            "spsf": results.get("spsf", {}),
            "sell_thru": results.get("sell_thru", {}),
            "doi": results.get("doi", {}),
            "mbq": results.get("mbq", {}),
            "anomalies": anomaly_result,
            "combined_breaches": combined_breaches,
            "total_p1": sum(1 for b in combined_breaches if b.get("priority") == "P1"),
            "total_p2": sum(1 for b in combined_breaches if b.get("priority") == "P2"),
            "total_p3": sum(1 for b in combined_breaches if b.get("priority") == "P3"),
        }

    def _run_engine(self, kpi_name: str, engine_module, df: pd.DataFrame) -> dict:
        """Run a single KPI engine, return result or empty dict on failure."""
        try:
            compute_fn = {
                "SPSF": spsf_engine.compute_spsf,
                "SELL_THRU": sell_thru_engine.compute_sell_thru,
                "DOI": doi_engine.compute_doi,
                "MBQ": mbq_engine.compute_mbq,
            }.get(kpi_name)

            summary_fn = {
                "SPSF": spsf_engine.get_spsf_summary,
                "SELL_THRU": sell_thru_engine.get_sell_thru_summary,
                "DOI": doi_engine.get_doi_summary,
                "MBQ": mbq_engine.get_mbq_summary,
            }.get(kpi_name)

            breach_fn = {
                "SPSF": spsf_engine.get_breach_rows,
                "SELL_THRU": sell_thru_engine.get_breach_rows,
                "DOI": doi_engine.get_breach_rows,
                "MBQ": mbq_engine.get_breach_rows,
            }.get(kpi_name)

            result_df = compute_fn(df)
            summary = summary_fn(result_df) if summary_fn else {}
            breaches = breach_fn(result_df) if breach_fn else pd.DataFrame()

            logger.debug(f"{kpi_name}: {len(result_df)} rows, {len(breaches)} breaches")

            return {
                "available": bool(summary),
                "summary": summary,
                "breaches": breaches,
                "data": result_df,
            }
        except Exception as e:
            logger.warning(f"{kpi_name} engine skipped (data mismatch): {e}")
            return {"available": False, "summary": {}, "breaches": pd.DataFrame()}


def _empty_result() -> dict:
    return {
        "spsf": {"available": False},
        "sell_thru": {"available": False},
        "doi": {"available": False},
        "mbq": {"available": False},
        "anomalies": {"anomalies": [], "total_anomalies": 0},
        "combined_breaches": [],
        "total_p1": 0,
        "total_p2": 0,
        "total_p3": 0,
    }
