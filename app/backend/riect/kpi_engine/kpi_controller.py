"""
DSR|RIECT — KPI Controller
Orchestrates all KPI engines, graceful degradation if data missing
"""

import logging
import pandas as pd
from typing import Any

from riect.kpi_engine import spsf_engine, sell_thru_engine, doi_engine, mbq_engine
from riect.kpi_engine import extended_kpi_engine
from riect.kpi_engine.anomaly_engine import detect_anomalies
from pipeline.kpi_alignment import detect_available_kpis, get_available_categories

logger = logging.getLogger(__name__)


class KPIController:
    """
    Runs all available KPI engines on query result DataFrames.
    Returns combined KPI results with summaries and breach records.
    """

    def run_all(self, query_result: dict) -> dict:
        """
        Run all applicable KPI engines on query result.
        Returns: {spsf, sell_thru, doi, mbq, atv, upt, discount_rate, ...
                  anomalies, combined_breaches, kpi_availability, available_categories}
        """
        if not query_result or "data" not in query_result or not query_result["data"]:
            return _empty_result()

        df = pd.DataFrame(query_result["data"])
        df.columns = [c.lower().strip() for c in df.columns]

        # Detect available KPIs based on column presence
        availability = detect_available_kpis(df.columns.tolist())

        results = {}

        # ── Core engines (existing) ────────────────────────────────────────────
        results["spsf"]      = self._run_engine("SPSF",      spsf_engine,      df)
        results["sell_thru"] = self._run_engine("SELL_THRU", sell_thru_engine, df)
        results["doi"]       = self._run_engine("DOI",        doi_engine,       df)
        results["mbq"]       = self._run_engine("MBQ",        mbq_engine,       df)

        # ── Extended engines ───────────────────────────────────────────────────
        ext_results = self.run_extended(df, availability)
        results.update(ext_results)

        # ── Collect all breach records ─────────────────────────────────────────
        combined_breaches = []
        for kpi_name, engine_result in results.items():
            if engine_result.get("breaches") is not None:
                breaches_df = engine_result["breaches"]
                if isinstance(breaches_df, pd.DataFrame) and not breaches_df.empty:
                    breach_records = breaches_df.to_dict(orient="records")
                    for record in breach_records:
                        record["kpi_type"] = kpi_name.upper()
                    combined_breaches.extend(breach_records)

        # Sort combined by priority
        priority_order = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
        combined_breaches.sort(key=lambda x: priority_order.get(x.get("priority", "P4"), 3))

        # ── Anomaly detection ──────────────────────────────────────────────────
        try:
            anomaly_result = detect_anomalies(df)
        except Exception as e:
            logger.warning(f"Anomaly detection skipped: {e}")
            anomaly_result = {"anomalies": [], "total_anomalies": 0}

        return {
            # Core engines
            "spsf":            results.get("spsf", {"available": False}),
            "sell_thru":       results.get("sell_thru", {"available": False}),
            "doi":             results.get("doi", {"available": False}),
            "mbq":             results.get("mbq", {"available": False}),
            # Extended engines
            "atv":             results.get("atv",             {"available": False}),
            "upt":             results.get("upt",             {"available": False}),
            "discount_rate":   results.get("discount_rate",   {"available": False}),
            "non_promo_disc":  results.get("non_promo_disc",  {"available": False}),
            "gross_margin":    results.get("gross_margin",    {"available": False}),
            "mobile_pct":      results.get("mobile_pct",      {"available": False}),
            "bill_integrity":  results.get("bill_integrity",  {"available": False}),
            "soh_health":      results.get("soh_health",      {"available": False}),
            "git_coverage":    results.get("git_coverage",    {"available": False}),
            "mbq_shortfall_amt": results.get("mbq_shortfall_amt", {"available": False}),
            "aop_vs_actual":   results.get("aop_vs_actual",   {"available": False}),
            # Aggregates
            "anomalies":          anomaly_result,
            "combined_breaches":  combined_breaches,
            "total_p1": sum(1 for b in combined_breaches if b.get("priority") == "P1"),
            "total_p2": sum(1 for b in combined_breaches if b.get("priority") == "P2"),
            "total_p3": sum(1 for b in combined_breaches if b.get("priority") == "P3"),
            # Availability map for prompt builder
            "kpi_availability":   availability,
            "available_categories": get_available_categories(availability),
        }

    def run_extended(self, df: pd.DataFrame, availability: dict) -> dict:
        """Run extended KPI engines based on detected column availability."""
        ext = {}

        ENGINE_MAP = {
            "atv": (
                "ATV",
                extended_kpi_engine.compute_atv,
                extended_kpi_engine.get_atv_summary,
                extended_kpi_engine.get_atv_breach_rows,
            ),
            "upt": (
                "UPT",
                extended_kpi_engine.compute_upt,
                extended_kpi_engine.get_upt_summary,
                extended_kpi_engine.get_upt_breach_rows,
            ),
            "discount_rate": (
                "DISC",
                extended_kpi_engine.compute_discount_rate,
                extended_kpi_engine.get_discount_rate_summary,
                extended_kpi_engine.get_discount_rate_breach_rows,
            ),
            "non_promo_disc": (
                "NPDISC",
                extended_kpi_engine.compute_non_promo_disc,
                extended_kpi_engine.get_non_promo_disc_summary,
                extended_kpi_engine.get_non_promo_disc_breach_rows,
            ),
            "gross_margin": (
                "GM",
                extended_kpi_engine.compute_gross_margin,
                extended_kpi_engine.get_gross_margin_summary,
                extended_kpi_engine.get_gross_margin_breach_rows,
            ),
            "mobile_pct": (
                "MOBPCT",
                extended_kpi_engine.compute_mobile_pct,
                extended_kpi_engine.get_mobile_pct_summary,
                extended_kpi_engine.get_mobile_pct_breach_rows,
            ),
            "bill_integrity": (
                "BILLINT",
                extended_kpi_engine.compute_bill_integrity,
                extended_kpi_engine.get_bill_integrity_summary,
                extended_kpi_engine.get_bill_integrity_breach_rows,
            ),
            "soh_health": (
                "SOHH",
                extended_kpi_engine.compute_soh_health,
                extended_kpi_engine.get_soh_health_summary,
                None,  # No breach_rows for SOH health (classification only)
            ),
            "git_coverage": (
                "GIT",
                extended_kpi_engine.compute_git_coverage,
                extended_kpi_engine.get_git_coverage_summary,
                None,
            ),
            "mbq_shortfall_amt": (
                "MBQAMT",
                extended_kpi_engine.compute_mbq_shortfall_amt,
                extended_kpi_engine.get_mbq_shortfall_amt_summary,
                extended_kpi_engine.get_mbq_shortfall_amt_breach_rows,
            ),
            "aop_vs_actual": (
                "AOP",
                extended_kpi_engine.compute_aop_vs_actual,
                extended_kpi_engine.get_aop_summary,
                extended_kpi_engine.get_aop_breach_rows,
            ),
        }

        for kpi_key, (tag, compute_fn, summary_fn, breach_fn) in ENGINE_MAP.items():
            # Check availability using the kpi_alignment registry key
            # Note: mobile_pct maps to "mobile_penetration" in registry
            registry_key = kpi_key if kpi_key != "mobile_pct" else "mobile_penetration"
            git_key = kpi_key if kpi_key != "git_coverage" else "git_coverage"

            if not availability.get(registry_key, availability.get(kpi_key, False)):
                ext[kpi_key] = {"available": False, "reason": "required columns not in result"}
                continue

            try:
                result_df = compute_fn(df)
                summary   = summary_fn(result_df) if summary_fn else {}
                breaches  = breach_fn(result_df)  if breach_fn  else pd.DataFrame()

                ext[kpi_key] = {
                    "available": bool(summary),
                    "summary":   summary,
                    "breaches":  breaches,
                    "data":      result_df,
                }
                logger.debug(f"{tag}: computed, summary keys={list(summary.keys())}")
            except Exception as e:
                logger.warning(f"{tag} engine skipped: {e}")
                ext[kpi_key] = {"available": False, "reason": str(e)}

        return ext

    def _run_engine(self, kpi_name: str, engine_module, df: pd.DataFrame) -> dict:
        """Run a single core KPI engine, return result or empty dict on failure."""
        try:
            compute_fn = {
                "SPSF":      spsf_engine.compute_spsf,
                "SELL_THRU": sell_thru_engine.compute_sell_thru,
                "DOI":       doi_engine.compute_doi,
                "MBQ":       mbq_engine.compute_mbq,
            }.get(kpi_name)

            summary_fn = {
                "SPSF":      spsf_engine.get_spsf_summary,
                "SELL_THRU": sell_thru_engine.get_sell_thru_summary,
                "DOI":       doi_engine.get_doi_summary,
                "MBQ":       mbq_engine.get_mbq_summary,
            }.get(kpi_name)

            breach_fn = {
                "SPSF":      spsf_engine.get_breach_rows,
                "SELL_THRU": sell_thru_engine.get_breach_rows,
                "DOI":       doi_engine.get_breach_rows,
                "MBQ":       mbq_engine.get_breach_rows,
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
        # Core
        "spsf":            {"available": False},
        "sell_thru":       {"available": False},
        "doi":             {"available": False},
        "mbq":             {"available": False},
        # Extended
        "atv":             {"available": False},
        "upt":             {"available": False},
        "discount_rate":   {"available": False},
        "non_promo_disc":  {"available": False},
        "gross_margin":    {"available": False},
        "mobile_pct":      {"available": False},
        "bill_integrity":  {"available": False},
        "soh_health":      {"available": False},
        "git_coverage":    {"available": False},
        "mbq_shortfall_amt": {"available": False},
        "aop_vs_actual":   {"available": False},
        # Aggregates
        "anomalies":          {"anomalies": [], "total_anomalies": 0},
        "combined_breaches":  [],
        "total_p1": 0,
        "total_p2": 0,
        "total_p3": 0,
        "kpi_availability":   {},
        "available_categories": [],
    }
