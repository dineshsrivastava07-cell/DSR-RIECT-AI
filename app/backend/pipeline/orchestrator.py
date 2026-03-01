"""
DSR|RIECT — Decision Intelligence + Orchestration Layer

The brain of the analytical pipeline.
Classifies every query → decides exactly which stages to run → executes → streams.

Route taxonomy:
  GREETING        – hi, hello, what can you do
  GENERAL_CHAT    – retail conversation, no live data needed
  ALERT_REVIEW    – show alerts / P1 / exceptions from DB
  SCHEMA_BROWSE   – what tables exist, describe schema
  DATA_QUERY      – needs SQL + ClickHouse + LLM narrative
  KPI_ANALYSIS    – needs SQL + KPI engines + P1-P4 alerts + LLM
  TREND_ANALYSIS  – multi-period SQL + trend LLM
  VENDOR_ANALYSIS – PO/GR SQL + supply chain LLM
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

import pandas as pd

logger = logging.getLogger(__name__)

# ─── Route taxonomy ───────────────────────────────────────────────────────────

class Route:
    GREETING        = "GREETING"
    GENERAL_CHAT    = "GENERAL_CHAT"
    ALERT_REVIEW    = "ALERT_REVIEW"
    SCHEMA_BROWSE   = "SCHEMA_BROWSE"
    DATA_QUERY      = "DATA_QUERY"
    KPI_ANALYSIS    = "KPI_ANALYSIS"
    TREND_ANALYSIS  = "TREND_ANALYSIS"
    VENDOR_ANALYSIS = "VENDOR_ANALYSIS"
    PEAK_HOURS      = "PEAK_HOURS"


# ─── Decision dataclass ───────────────────────────────────────────────────────

@dataclass
class PipelineDecision:
    route: str
    intent: dict = field(default_factory=dict)

    # Stage flags
    needs_llm:          bool = True
    needs_clickhouse:   bool = False
    needs_schema:       bool = False
    needs_sql:          bool = False
    needs_kpi:          bool = False
    needs_alerts:       bool = False
    needs_trend:        bool = False

    # System capabilities (filled by orchestrator)
    llm_available:      bool = False
    llm_model:          str  = ""
    ch_available:       bool = False
    schema_available:   bool = False

    # Normalized query (post spell-correction, abbreviation expansion)
    normalized_query:   str  = ""

    # Execution go/no-go
    can_proceed:        bool = True
    blockers:           list = field(default_factory=list)
    guidance:           str  = ""

    @property
    def stages(self) -> list[str]:
        """Ordered list of pipeline stages to run."""
        s = ["context_build"]
        if self.needs_schema and self.ch_available:
            s.append("schema_load")
        if self.needs_sql and self.ch_available and self.schema_available:
            s.append("sql_generate")
            s.append("sql_execute")
        if self.needs_kpi:
            s.append("kpi_analyse")
        if self.needs_alerts:
            s.append("alert_generate")
        s.append("prompt_build")
        if self.needs_llm and self.llm_available:
            s.append("llm_stream")
        return s

    def describe(self) -> str:
        flags = []
        if self.needs_sql:       flags.append("SQL")
        if self.needs_kpi:       flags.append("KPI")
        if self.needs_alerts:    flags.append("Alerts")
        if self.needs_trend:     flags.append("Trend")
        return f"{self.route} [{', '.join(flags) or 'LLM only'}]"


# ─── Pattern rules for route classification ───────────────────────────────────

GREETING_PATTERNS = re.compile(
    r"^(hi|hello|hey|howdy|yo|good (morning|afternoon|evening)|what('s| is) up|"
    r"what can you do|help me|how are you|introduce yourself|"
    r"what are you|who are you|tell me about yourself)[\s?!.]*$",
    re.IGNORECASE,
)

ALERT_PATTERNS = re.compile(
    r"\b(alert\w*|exception\w*|p1|p2|p3|critical|urgent|breach\w*|"
    r"open issue|action needed|flag\w*|escalat\w*|resolv\w*|inbox)\b",
    re.IGNORECASE,
)

SCHEMA_PATTERNS = re.compile(
    r"\b(what tables?|list tables?|show tables?|describe tables?|schema|"
    r"what data|what fields?|columns?|available data|what do you have|"
    r"what can you (show|query|access)|what.s available)\b",
    re.IGNORECASE,
)

TREND_PATTERNS = re.compile(
    r"\b(trend|over time|week.on.week|month.on.month|yoy|year.on.year|"
    r"historical|last \d+ (day|week|month)|time series|sparkline|growth)\b",
    re.IGNORECASE,
)

VENDOR_PATTERNS = re.compile(
    r"\b(vendor|supplier|purchase order|po |grn|goods receipt|delivery|"
    r"lead time|fill rate|inbound|replenish)\b",
    re.IGNORECASE,
)

KPI_PATTERNS = re.compile(
    r"\b(spsf|sales per square foot|floor sqft|sell.through|sell.thru|"
    r"doi|days of inventory|mbq|minimum baseline|kpi|"
    r"upt|units per transaction|performance|target|scorecard|compliance|baseline)\b",
    re.IGNORECASE,
)

PEAK_HOURS_PATTERNS = re.compile(
    r"\b(peak hour\w*|peak time\w*|rush hour\w*|busy hour\w*|busiest hour\w*|"
    r"busiest time\w*|hourly sale\w*|hourly revenue|hourly traffic|hourly performance|"
    r"foot traffic hour\w*|store timing\w*|high peak|by hour|per hour)\b",
    re.IGNORECASE,
)

DATA_PATTERNS = re.compile(
    r"\b(show|top|bottom|list|which|best|worst|compare|rank|highest|lowest|"
    r"sales|revenue|stock|inventory|customer|footfall|store|category|sku|"
    r"discount|pilferage|shrinkage|returns|return|refund|loss|leakage|"
    r"markdown|fraud|bill integrity|pilfrage|theft|stolen|clearance|"
    r"report|analysis|analyse|analytics|unauthorized|non.promo)\b",
    re.IGNORECASE,
)


# ─── Orchestrator ─────────────────────────────────────────────────────────────

class PipelineOrchestrator:
    """
    Decision Intelligence + Orchestration for DSR|RIECT analytical pipeline.

    Usage:
        orchestrator = PipelineOrchestrator()
        decision = await orchestrator.decide(query, session_id)
        result = await orchestrator.execute(decision, query, session_id, ws_callback)
    """

    # ── Decision stage ─────────────────────────────────────────────────────

    async def decide(self, query: str, session_id: str = "", preferred_llm: str = None) -> PipelineDecision:
        """Classify query → build routing decision with capability checks."""

        # 0. Normalize query — fix spelling, expand abbreviations, apply aliases
        from pipeline.query_normalizer import normalize_query, correction_summary
        norm_result = normalize_query(query)
        normalized  = norm_result["normalized"]
        norm_flags  = norm_result["flags"]
        if norm_result["corrections"]:
            logger.info(f"Query normalized: {correction_summary(norm_result)}")

        # Use normalized text for all downstream classification
        query = normalized

        # 1. Classify route (using normalized query)
        decision = self._classify_route(query)
        decision.intent = self._classify_intent(query)
        decision.normalized_query = normalized   # carry forward for execute()

        # Attach normalization metadata to intent for prompt/response use
        decision.intent["norm_corrections"] = correction_summary(norm_result)
        decision.intent["norm_flags"]       = norm_flags
        decision.intent["target_date"]      = norm_result.get("target_date", "")

        # 2. Check LLM capability — honour user's preferred model first
        from llm.ollama_client import is_available as ollama_check
        from settings.settings_store import get_llm_key
        from llm.qwen_client import is_configured as qwen_is_configured, get_model as qwen_get_model

        CLOUD_PROVIDERS = {"claude", "gemini", "openai"}
        QWEN_IDS = {"qwen", "qwen3.5-plus", "qwen3.5-flash", "qwen3-max"}
        llm_ok    = False
        llm_model = ""

        if preferred_llm and preferred_llm in QWEN_IDS:
            # User selected a Qwen cloud model
            if qwen_is_configured():
                llm_ok    = True
                llm_model = preferred_llm if preferred_llm != "qwen" else qwen_get_model()
                decision.llm_available = True
                decision.llm_model     = llm_model
            else:
                logger.warning("preferred_llm=qwen but Qwen not connected — falling back to auto")
                preferred_llm = None

        if not preferred_llm or (preferred_llm not in CLOUD_PROVIDERS and preferred_llm not in QWEN_IDS):
            # Auto-select: Qwen first (if configured), then Ollama, then other cloud
            if qwen_is_configured():
                llm_ok    = True
                llm_model = qwen_get_model()   # "qwen3.5-plus" (stored preference)
            else:
                llm_ok, llm_model = await ollama_check()
                if preferred_llm and preferred_llm not in CLOUD_PROVIDERS:
                    # User picked a specific Ollama model — use it directly
                    llm_ok_specific, resolved = await ollama_check(preferred_llm)
                    if llm_ok_specific:
                        llm_model = resolved
                        llm_ok    = True
                if not llm_ok:
                    llm_ok    = bool(get_llm_key("claude") or get_llm_key("gemini") or get_llm_key("openai"))
                    llm_model = "cloud" if llm_ok else ""
            decision.llm_available = llm_ok
            decision.llm_model     = llm_model

        elif preferred_llm and preferred_llm in CLOUD_PROVIDERS:
            # User explicitly chose Claude / Gemini / OpenAI
            if get_llm_key(preferred_llm):
                llm_ok    = True
                llm_model = preferred_llm
                decision.llm_available = True
                decision.llm_model     = preferred_llm
            else:
                logger.warning(f"preferred_llm={preferred_llm} but no key found — falling back to auto")
                preferred_llm = None
                if qwen_is_configured():
                    llm_ok    = True
                    llm_model = qwen_get_model()
                    decision.llm_available = True
                    decision.llm_model     = llm_model

        # 3. Check ClickHouse capability
        from settings.settings_store import is_clickhouse_configured
        ch_ok = is_clickhouse_configured()
        decision.ch_available = ch_ok

        # 4. Check schema cache
        if ch_ok:
            from clickhouse.schema_inspector import get_schema_summary
            try:
                summary = get_schema_summary()
                decision.schema_available = bool(summary)
            except Exception:
                decision.schema_available = False

        # 5. Evaluate blockers
        if decision.needs_llm and not llm_ok:
            decision.blockers.append("no_llm")
            decision.guidance = (
                "No LLM is configured.\n"
                "• Start Ollama locally: `ollama pull llama3.1`\n"
                "• Or connect Claude/Gemini/ChatGPT in ⚙ Settings"
            )

        if decision.needs_clickhouse and not ch_ok:
            decision.blockers.append("clickhouse_not_configured")
            decision.guidance = (
                "ClickHouse is not connected.\n"
                "Go to ⚙ Settings → ClickHouse Connection, enter your credentials and click Test Connection."
            )
        elif decision.needs_sql and ch_ok and not decision.schema_available:
            decision.blockers.append("schema_not_cached")
            decision.guidance = (
                "ClickHouse is connected but schema hasn't been loaded yet.\n"
                "Go to ⚙ Settings → Test Connection to load the schema."
            )

        decision.can_proceed = len(decision.blockers) == 0 or (
            # Can still give partial response (guidance only) without data
            "no_llm" not in decision.blockers and
            decision.route in (Route.GREETING, Route.GENERAL_CHAT, Route.ALERT_REVIEW)
        )

        logger.info(f"Decision: {decision.describe()} | stages={decision.stages} | blockers={decision.blockers}")
        return decision

    # ── Route classifier ────────────────────────────────────────────────────

    def _classify_route(self, query: str) -> PipelineDecision:
        q = query.strip()

        # GREETING — short, no keywords, or explicit greeting pattern
        if len(q.split()) <= 4 and GREETING_PATTERNS.match(q):
            return PipelineDecision(
                route=Route.GREETING,
                needs_llm=True,
                needs_clickhouse=False, needs_schema=False,
                needs_sql=False, needs_kpi=False,
            )

        # ALERT_REVIEW — "show alerts", "P1 exceptions", "open issues"
        # Route here if alert intent is dominant and no specific KPI metric requested
        if ALERT_PATTERNS.search(q) and not KPI_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.ALERT_REVIEW,
                needs_llm=True,
                needs_clickhouse=False, needs_schema=False,
                needs_sql=False, needs_kpi=False, needs_alerts=True,
            )

        # SCHEMA_BROWSE — "what tables do you have"
        if SCHEMA_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.SCHEMA_BROWSE,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=False, needs_kpi=False,
            )

        # VENDOR_ANALYSIS — supply chain queries
        if VENDOR_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.VENDOR_ANALYSIS,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=True, needs_kpi=False,
            )

        # KPI_ANALYSIS — explicit KPI terms with data request
        if KPI_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.KPI_ANALYSIS,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=True, needs_kpi=True, needs_alerts=True,
            )

        # PEAK_HOURS — hourly store traffic / sales analysis
        if PEAK_HOURS_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.PEAK_HOURS,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=True, needs_kpi=False,
            )

        # TREND_ANALYSIS — time-series / comparative queries
        if TREND_PATTERNS.search(q) and DATA_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.TREND_ANALYSIS,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=True, needs_kpi=False, needs_trend=True,
            )

        # DATA_QUERY — general data request
        if DATA_PATTERNS.search(q):
            return PipelineDecision(
                route=Route.DATA_QUERY,
                needs_llm=True,
                needs_clickhouse=True, needs_schema=True,
                needs_sql=True, needs_kpi=False,
            )

        # GENERAL_CHAT — retail knowledge, no live data
        return PipelineDecision(
            route=Route.GENERAL_CHAT,
            needs_llm=True,
            needs_clickhouse=False, needs_schema=False,
            needs_sql=False, needs_kpi=False,
        )

    def _classify_intent(self, query: str) -> dict:
        from pipeline.intent_engine import classify_intent
        return classify_intent(query)

    # ── Execution stage ─────────────────────────────────────────────────────

    async def execute(
        self,
        decision: PipelineDecision,
        query: str,
        session_id: str,
        ws_send: Callable[[dict], Awaitable[None]],
    ) -> dict:
        """
        Execute the decided pipeline stages.
        Streams progress via ws_send callback.
        Returns final response_blocks dict.
        """
        from llm.llm_router import get_router
        from settings.settings_store import get_default_llm

        # Always use the normalized query — falls back to raw if normalization wasn't run
        query = decision.normalized_query or query

        router = get_router(decision.llm_model if decision.llm_model not in ("", "cloud") else None)

        # ── Blocker: cannot proceed at all ─────────────────────────────
        if not decision.can_proceed:
            guidance = decision.guidance or "Cannot process this request. Check Settings."
            await ws_send({"type": "token", "content": guidance})
            from pipeline.response_formatter import format_response
            return format_response(guidance, {}, {})

        # ── Signal route + any query corrections to frontend ───────────
        await ws_send({"type": "route", "route": decision.route, "stages": decision.stages})
        correction_msg = decision.intent.get("norm_corrections", "")
        if correction_msg:
            await ws_send({"type": "query_normalized", "message": correction_msg})

        # State carried across stages
        schema_dict   = {}
        query_result  = {}
        sql_info      = {}
        kpi_results   = {}
        alerts        = []

        # ─── STAGE: context_build ──────────────────────────────────────
        await ws_send({"type": "stage", "stage": "context_build"})
        from pipeline.context_builder import build_context
        context = build_context(query, session_id, schema_dict)

        # ─── STAGE: schema_load ────────────────────────────────────────
        if "schema_load" in decision.stages:
            await ws_send({"type": "stage", "stage": "schema_load"})
            try:
                from clickhouse.schema_inspector import inspect_schemas
                from settings.settings_store import get_clickhouse_config
                cfg = get_clickhouse_config()
                schema_dict = inspect_schemas(cfg["schemas"])
                # Rebuild context with real schema
                context = build_context(query, session_id, schema_dict)
                table_count = sum(len(t) for t in schema_dict.values())
                await ws_send({"type": "schema_ready", "tables": table_count})
            except Exception as e:
                logger.warning(f"Schema load failed: {e}")
                await ws_send({"type": "stage_warn", "stage": "schema_load", "msg": str(e)})

        # ─── STAGE: data_freshness ─────────────────────────────────────
        # Detect the most recent COMPLETE sales date from ClickHouse.
        # Today's date may have partial data (data pipeline lag) — we skip
        # dates with fewer than 10,000 bills to avoid using an incomplete day.
        if decision.needs_sql and decision.ch_available:
            try:
                from clickhouse.connector import get_client
                ch = get_client()
                r1 = ch.query(
                    "SELECT toDate(BILLDATE) AS dt "
                    "FROM vmart_sales.pos_transactional_data "
                    "GROUP BY dt "
                    "HAVING COUNT(DISTINCT BILLNO) >= 10000 "
                    "ORDER BY dt DESC LIMIT 1"
                )
                context["latest_sales_date"] = str(r1.result_rows[0][0]) if r1.result_rows else ""
                await ws_send({"type": "data_freshness",
                               "latest_sales": context["latest_sales_date"]})
                logger.info(f"Data freshness: sales={context['latest_sales_date']}")
            except Exception as e:
                logger.warning(f"Data freshness check failed: {e}")

            # Inject user-specified target_date regardless of freshness check success
            target_date = decision.intent.get("target_date", "")
            context["target_date"] = target_date
            if target_date:
                logger.info(f"target_date={target_date} — SQL will filter on this specific date")

        # ─── STAGE: sql_generate ───────────────────────────────────────
        if "sql_generate" in decision.stages and schema_dict:
            await ws_send({"type": "stage", "stage": "sql_generate"})
            # Enrich prompt for route-specific SQL
            enriched_context = self._enrich_context_for_route(context, decision)
            from pipeline.sql_generator import generate_sql, validate_sql_basic
            sql_info = await generate_sql(query, enriched_context, router)
            sql = sql_info.get("sql", "")
            if sql:
                valid, reason = validate_sql_basic(sql)
                if valid:
                    await ws_send({"type": "sql_generated", "sql": sql})
                else:
                    logger.warning(f"SQL validation failed: {reason}")
                    sql_info = {}

        # ─── STAGE: sql_execute ────────────────────────────────────────
        if "sql_execute" in decision.stages and sql_info.get("sql"):
            await ws_send({"type": "stage", "stage": "sql_execute"})
            from clickhouse.query_runner import run_query
            from pipeline.sql_generator import generate_sql, validate_sql_basic
            query_result = run_query(sql_info["sql"])
            if "error" not in query_result:
                await ws_send({
                    "type": "data_ready",
                    "rows": query_result.get("row_count", 0),
                    "columns": query_result.get("columns", []),
                })
            else:
                sql_error_msg = query_result["error"]
                await ws_send({"type": "sql_error", "message": sql_error_msg})
                # ── Retry: feed the error back to LLM for self-correction ──
                await ws_send({"type": "stage", "stage": "sql_retry"})
                retry_context = dict(enriched_context)
                # Extract ClickHouse "Maybe you meant" suggestion if present
                maybe_hint = ""
                import re as _re
                m = _re.search(r"Maybe you meant[:\s]+(\[.*?\])", sql_error_msg)
                if m:
                    maybe_hint = f"\nClickHouse suggested: {m.group(1)}"
                _retry_latest = enriched_context.get("latest_sales_date", "")
                retry_context["sql_hints"] = (
                    f"YOUR PREVIOUS SQL FAILED.\n"
                    f"Error: {sql_error_msg[:500]}{maybe_hint}\n"
                    f"Failed SQL: {sql_info['sql'][:800]}\n\n"
                    "RULES TO FIX:\n"
                    "- SHRTNAME is in pos_transactional_data, NOT in stores. Do not use s.SHRTNAME.\n"
                    "- stores has STORE_NAME (not SHRTNAME). Only join stores if you need AREA_TIER.\n"
                    f"- Date filter: toDate(BILLDATE) = toDate('{_retry_latest}') — use this EXACT date. NEVER use today() or today()-1.\n"
                    "- Use ONLY columns that exist in the table you select them from.\n"
                    "Fix the SQL and return only the corrected query."
                )
                sql_info = await generate_sql(query, retry_context, router)
                sql = sql_info.get("sql", "")
                if sql:
                    valid, reason = validate_sql_basic(sql)
                    if valid:
                        await ws_send({"type": "sql_generated", "sql": sql})
                        query_result = run_query(sql)
                        if "error" not in query_result:
                            await ws_send({
                                "type": "data_ready",
                                "rows": query_result.get("row_count", 0),
                                "columns": query_result.get("columns", []),
                            })
                        else:
                            await ws_send({"type": "sql_error", "message": query_result["error"]})
                            query_result = {}
                    else:
                        query_result = {}
                else:
                    query_result = {}

        # ─── STAGE: alias_normalise ────────────────────────────────────
        # Rename LLM-generated column aliases to pipeline-canonical names
        # This ensures enrichment, KPI engines, and chain summaries all work
        # even if the LLM deviated from mandatory alias rules.
        if query_result.get("data"):
            query_result = self._normalise_column_aliases(query_result)

        # ─── STAGE: sqft_enrich ────────────────────────────────────────
        # Add floor_sqft column to query results for authentic SPSF computation
        if query_result.get("data"):
            query_result = self._enrich_with_sqft(query_result)

        # ─── STAGE: supplementary_queries ─────────────────────────────
        # Runs 4 pre-built queries for dept, articles, peak hours, top MRP.
        # Triggered only when: main query succeeded + KPI/DATA route + ≥3 rows returned.
        # These feed Sections 4, 5, 7 of the analytical prompt (always present now).
        supplementary_data: dict = {}
        _supp_routes = (Route.KPI_ANALYSIS, Route.DATA_QUERY, Route.TREND_ANALYSIS)
        if (
            decision.route in _supp_routes
            and decision.ch_available
            and context.get("latest_sales_date")
            and len(query_result.get("data", [])) >= 3
        ):
            await ws_send({"type": "stage", "stage": "supplementary_queries"})
            supplementary_data = await self._run_supplementary_queries(context)

        # ─── STAGE: kpi_analyse ────────────────────────────────────────
        if "kpi_analyse" in decision.stages and query_result.get("data"):
            await ws_send({"type": "stage", "stage": "kpi_analyse"})
            from riect.kpi_engine.kpi_controller import KPIController
            controller = KPIController()
            kpi_results = controller.run_all(query_result)
            anomaly_summary = kpi_results.get("anomalies", {})
            await ws_send({
                "type": "kpi_done",
                "p1": kpi_results.get("total_p1", 0),
                "p2": kpi_results.get("total_p2", 0),
                "p3": kpi_results.get("total_p3", 0),
                "anomalies": anomaly_summary.get("total_anomalies", 0),
            })
        elif query_result.get("data"):
            # Run anomaly detection even on non-KPI routes (DATA_QUERY, TREND, etc.)
            try:
                import pandas as pd
                from riect.kpi_engine.anomaly_engine import detect_anomalies
                df_tmp = pd.DataFrame(query_result["data"])
                df_tmp.columns = [c.lower().strip() for c in df_tmp.columns]
                kpi_results = {"anomalies": detect_anomalies(df_tmp)}
            except Exception:
                pass

        # ─── Merge store_inventory anomalies (ST%, DOI) into kpi_results ──
        # Runs anomaly detection on store-level inventory data to surface
        # sell-through and DOI outliers that aren't in the main sales query.
        if supplementary_data.get("store_inventory", {}).get("data"):
            try:
                import pandas as pd
                from riect.kpi_engine.anomaly_engine import detect_anomalies
                inv_df = pd.DataFrame(supplementary_data["store_inventory"]["data"])
                inv_df.columns = [c.lower().strip() for c in inv_df.columns]
                inv_anom = detect_anomalies(inv_df)
                if inv_anom.get("anomalies"):
                    existing_anom = kpi_results.get("anomalies", {})
                    prev_list = existing_anom.get("anomalies", [])
                    merged = prev_list + inv_anom["anomalies"]
                    merged.sort(key=lambda x: (
                        0 if x.get("severity") == "P1" else 1,
                        -abs(x.get("z_score", 0))
                    ))
                    kpi_results.setdefault("anomalies", {})
                    kpi_results["anomalies"]["anomalies"]       = merged
                    kpi_results["anomalies"]["total_anomalies"] = len(merged)
                    kpi_results["anomalies"]["p1_anomalies"]    = sum(1 for a in merged if a.get("severity") == "P1")
                    kpi_results["anomalies"]["p2_anomalies"]    = sum(1 for a in merged if a.get("severity") == "P2")
                    logger.info(f"Inventory anomalies merged: {len(inv_anom['anomalies'])} new findings")
            except Exception as e:
                logger.warning(f"Store inventory anomaly detection failed: {e}")

        # ─── STAGE: alert_generate ─────────────────────────────────────
        if "alert_generate" in decision.stages:
            await ws_send({"type": "stage", "stage": "alert_generate"})
            if decision.route == Route.ALERT_REVIEW:
                # Load from DB
                from riect.alert_engine.alert_store import get_alerts
                db_alerts = get_alerts(limit=20)
                alerts = db_alerts
            elif kpi_results:
                from riect.alert_engine.alert_generator import generate_alerts
                from riect.alert_engine.action_recommender import enrich_alerts_with_actions
                from riect.alert_engine.alert_store import save_alerts
                alerts = generate_alerts(kpi_results, session_id=session_id)
                alerts = enrich_alerts_with_actions(alerts)
                save_alerts(alerts)

            if alerts:
                await ws_send({
                    "type": "alerts_ready",
                    "count": len(alerts),
                    "p1": sum(1 for a in alerts if (a.get("priority") if isinstance(a, dict) else a) == "P1"),
                })

        # ─── STAGE: prompt_build ───────────────────────────────────────
        await ws_send({"type": "stage", "stage": "prompt_build"})
        system_prompt, user_prompt = self._build_prompt(
            query, decision, context, query_result, schema_dict, alerts, kpi_results,
            supplementary_data=supplementary_data,
        )

        # ─── STAGE: llm_stream ─────────────────────────────────────────
        # Token budget: 6000 tokens works well for both:
        #   - qwen3-coder:480b-cloud via Ollama (3000 tok/s, cloud-backed) → very fast
        #   - Qwen3.5-plus via chat.qwen.ai (cloud) → fast
        #   - Local Ollama 7B → ~180s max (acceptable given streaming starts immediately)
        _MAX_TOKENS = 6000
        narrative_parts = []
        if "llm_stream" in decision.stages:
            await ws_send({"type": "stage", "stage": "llm_stream"})
            try:
                async for chunk in router.stream(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=_MAX_TOKENS,
                    temperature=0.3,  # Lower = more factual, consistent retail analytics
                ):
                    narrative_parts.append(chunk)
                    await ws_send({"type": "token", "content": chunk})
            except Exception as e:
                logger.error(f"LLM stream failed: {e}")
                try:
                    text = await router.generate(system_prompt, user_prompt, max_tokens=_MAX_TOKENS)
                    narrative_parts = [text]
                    await ws_send({"type": "token", "content": text})
                except Exception as e2:
                    fallback = self._fallback_narrative(query, decision, query_result, kpi_results)
                    narrative_parts = [fallback]
                    await ws_send({"type": "token", "content": fallback})

        narrative = "".join(narrative_parts)

        # ─── Format final response blocks ──────────────────────────────
        from pipeline.response_formatter import format_response
        blocks = format_response(
            narrative=narrative,
            query_result=query_result,
            sql_info=sql_info,
            alerts=alerts,
            intent=decision.intent,
        )

        return blocks, narrative, kpi_results, alerts

    # ── Prompt construction per route ───────────────────────────────────────

    def _build_prompt(
        self,
        query: str,
        decision: PipelineDecision,
        context: dict,
        query_result: dict,
        schema_dict: dict,
        alerts: list,
        kpi_results: dict = None,
        supplementary_data: dict = None,
    ) -> tuple[str, str]:
        from config import RIECT_SYSTEM_PROMPT, KPI_FORMULAS, JOIN_HINTS

        route = decision.route
        system = RIECT_SYSTEM_PROMPT

        # Route-specific system additions
        if route == Route.GREETING:
            system += (
                "\nThe user is greeting you. Introduce yourself as DSR|RIECT — "
                "a Retail Intelligence AI assistant for SPSF, Sell-Through, DOI, MBQ analysis, "
                "exceptions management, and store performance. Be concise and inviting."
            )
            return system, f"User said: '{query}'\n\nRespond with a brief, friendly introduction."

        if route == Route.GENERAL_CHAT:
            system += "\nAnswer using retail intelligence expertise. Be concise and practical."
            return system, f"User question: {query}"

        if route == Route.ALERT_REVIEW:
            alert_text = self._format_alerts_for_prompt(alerts)
            return system, (
                f"User request: {query}\n\n"
                f"Current open alerts:\n{alert_text}\n\n"
                "Summarise the most critical issues and recommended actions. "
                "Lead with P1s, then P2s. Be specific and actionable."
            )

        if route == Route.SCHEMA_BROWSE:
            schema_text = self._schema_summary_text(schema_dict)
            return system, (
                f"User request: {query}\n\n"
                f"Available ClickHouse schema:\n{schema_text}\n\n"
                "Describe what data is available and what insights can be derived from it."
            )

        # DATA_QUERY / KPI_ANALYSIS / TREND / VENDOR
        from pipeline.prompt_builder import build_analysis_prompt
        return build_analysis_prompt(query, context, query_result, kpi_results,
                                     supplementary_data=supplementary_data)

    def _enrich_context_for_route(self, context: dict, decision: PipelineDecision) -> dict:
        """Add route-specific hints to context for SQL generation."""
        from config import KPI_FORMULAS, JOIN_HINTS
        ctx = dict(context)

        latest_sales = ctx.get("latest_sales_date", "")

        if decision.route == Route.KPI_ANALYSIS:
            kpi_types = decision.intent.get("kpi_types", [])
            needs_inventory = any(k in kpi_types for k in ("SELL_THRU", "DOI", "MBQ"))

            if needs_inventory:
                # Comprehensive query: sales MTD + inventory join for Sell-Through / DOI
                # MTD sales are required because SPSF/Sell-Thru thresholds are MONTHLY metrics
                # Inventory: use vmart_product.inventory_current (live snapshot, no date filter)
                ctx["sql_hints"] = (
                    "COMPREHENSIVE KPI QUERY — MTD (month-to-date) sales + inventory: "
                    "SELECT p.STORE_ID, p.SHRTNAME, p.ZONE, p.REGION, "
                    "  SUM(p.NETAMT) AS net_sales_amount, SUM(p.QTY) AS total_qty, "
                    "  COUNT(DISTINCT p.BILLNO) AS bill_count, "
                    "  anyLast(inv.total_soh) AS stock_on_hand "
                    "FROM vmart_sales.pos_transactional_data p "
                    "LEFT JOIN ("
                    "  SELECT STORE_CODE, SUM(<stock_qty_col>) AS total_soh "
                    "  FROM vmart_product.inventory_current "
                    "  GROUP BY STORE_CODE"
                    ") inv ON toString(p.STORE_ID) = inv.STORE_CODE "
                    f"WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{latest_sales}')) "
                    f"  AND toDate(p.BILLDATE) <= toDate('{latest_sales}') "
                    "GROUP BY p.STORE_ID, p.SHRTNAME, p.ZONE, p.REGION "
                    "ORDER BY net_sales_amount DESC LIMIT 200. "
                    "IMPORTANT: Replace <stock_qty_col> with the actual stock quantity column from Schema Context. "
                    "IMPORTANT: Use MTD date range (toStartOfMonth to latest_date) — NOT single day. "
                    "SPSF and Sell-Through thresholds are MONTHLY metrics (P1<₹500/sqft/month). "
                    "NEVER use s.SHRTNAME — stores table does NOT have SHRTNAME. "
                    "NEVER use data_science tables — inventory is vmart_product.inventory_current only. "
                    "LABEL RULES: "
                    "(1) If articles/SKUs are involved: include ARTICLENAME, DIVISION, SECTION, DEPARTMENT. "
                    "(2) Always include STORE_ID and SHRTNAME so the pipeline can enrich with floor_sqft for SPSF."
                )
            else:
                # SPSF-only or general KPI: pure sales MTD query — NO inventory join
                # CRITICAL: SPSF threshold is MONTHLY (P1<₹500/sqft/month, target=₹1,000/sqft/month)
                # Must use month-to-date data so SPSF values are comparable to monthly thresholds
                # Pipeline auto-enriches with floor_sqft from SQLite store master
                ctx["sql_hints"] = (
                    "SPSF / SALES KPI QUERY — MTD (month-to-date) sales, NO inventory JOIN. "
                    "CRITICAL: SPSF thresholds are MONTHLY (P1<₹500/sqft/month, Target=₹1,000/sqft/month). "
                    "Use MTD date range so SPSF is comparable to monthly benchmarks. "
                    "CRITICAL: Do NOT join inventory tables for SPSF — floor_sqft comes from the pipeline, NOT ClickHouse. "
                    "Correct SQL: "
                    "SELECT p.STORE_ID, p.SHRTNAME, p.ZONE, p.REGION, "
                    "  SUM(p.NETAMT) AS net_sales_amount, "
                    "  SUM(p.QTY) AS total_qty, "
                    "  COUNT(DISTINCT p.BILLNO) AS bill_count, "
                    "  SUM(p.GROSSAMT) AS total_gross, "
                    "  SUM(p.DISCOUNTAMT) AS total_discount "
                    "FROM vmart_sales.pos_transactional_data p "
                    f"WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{latest_sales}')) "
                    f"  AND toDate(p.BILLDATE) <= toDate('{latest_sales}') "
                    "GROUP BY p.STORE_ID, p.SHRTNAME, p.ZONE, p.REGION "
                    "ORDER BY net_sales_amount DESC LIMIT 200. "
                    "NEVER use s.SHRTNAME — stores table does NOT have SHRTNAME. "
                    "NEVER join inventory tables for SPSF — that will corrupt net_sales_amount. "
                    "The pipeline adds floor_sqft and computes spsf automatically after the query. "
                    "LABEL RULES: "
                    "(1) If articles/SKUs involved: include ARTICLENAME, DIVISION, SECTION, DEPARTMENT. "
                    "(2) Always include STORE_ID and SHRTNAME in SELECT."
                )
        elif decision.route == Route.DATA_QUERY:
            # Check norm flags for specialised queries first
            norm_flags = decision.intent.get("norm_flags", {})
            if norm_flags.get("has_peak_hours"):
                ctx["sql_hints"] = (
                    "PEAK HOURS ANALYSIS — extract hour from BILLDATE using toHour(). "
                    "SELECT STORE_ID, SHRTNAME, toHour(BILLDATE) AS hour, "
                    "  COUNT(DISTINCT BILLNO) AS txn_count, "
                    "  SUM(NETAMT) AS revenue, "
                    "  SUM(QTY) AS qty "
                    "FROM vmart_sales.pos_transactional_data "
                    f"WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                    "GROUP BY STORE_ID, SHRTNAME, hour "
                    "ORDER BY SHRTNAME, txn_count DESC LIMIT 500. "
                    "NEVER join inventory. NEVER use today(). NEVER use toDateTime()."
                )
            elif norm_flags.get("has_pilferage"):
                ctx["sql_hints"] = (
                    "PILFERAGE ANALYSIS: Use pos_transactional_data columns: "
                    "NETAMT, GROSSAMT, DISCOUNTAMT, PROMOAMT, QTY, BILLNO, SHRTNAME, BILLDATE. "
                    "Bill Integrity = NETAMT / (GROSSAMT - DISCOUNTAMT - PROMOAMT). "
                    "SELECT SHRTNAME, SUM(NETAMT) net, SUM(GROSSAMT) gross, "
                    "  SUM(DISCOUNTAMT) disc, SUM(PROMOAMT) promo, "
                    "  round(SUM(NETAMT)/nullIf(SUM(GROSSAMT)-SUM(DISCOUNTAMT)-SUM(PROMOAMT),0),3) AS bill_integrity "
                    f"FROM vmart_sales.pos_transactional_data WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                    "GROUP BY SHRTNAME ORDER BY bill_integrity ASC LIMIT 200."
                )
            elif norm_flags.get("has_discount"):
                ctx["sql_hints"] = (
                    "DISCOUNT ANALYSIS: Use GROSSAMT, DISCOUNTAMT, PROMOAMT, NETAMT, SHRTNAME. "
                    "Non-promo discount = DISCOUNTAMT - PROMOAMT (positive = unauthorized markdown). "
                    "SELECT SHRTNAME, SUM(GROSSAMT) gross, SUM(DISCOUNTAMT) disc, SUM(PROMOAMT) promo, "
                    "  SUM(DISCOUNTAMT-PROMOAMT) non_promo_disc, "
                    "  round(SUM(DISCOUNTAMT)/nullIf(SUM(GROSSAMT),0)*100,2) disc_rate_pct "
                    f"FROM vmart_sales.pos_transactional_data WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                    "GROUP BY SHRTNAME ORDER BY disc_rate_pct DESC LIMIT 200."
                )
            elif norm_flags.get("has_returns"):
                ctx["sql_hints"] = (
                    "SALES RETURNS: negative NETAMT or QTY = return transaction. "
                    "SELECT SHRTNAME, DIVISION, "
                    "  SUM(CASE WHEN NETAMT > 0 THEN NETAMT ELSE 0 END) gross_sales, "
                    "  SUM(CASE WHEN NETAMT < 0 THEN abs(NETAMT) ELSE 0 END) return_amt, "
                    "  SUM(CASE WHEN QTY < 0 THEN abs(QTY) ELSE 0 END) return_qty, "
                    "  SUM(CASE WHEN QTY > 0 THEN QTY ELSE 0 END) sale_qty, "
                    "  round(SUM(CASE WHEN NETAMT<0 THEN abs(NETAMT) ELSE 0 END)/"
                    "    nullIf(SUM(CASE WHEN NETAMT>0 THEN NETAMT ELSE 0 END),0)*100,2) return_rate_pct "
                    f"FROM vmart_sales.pos_transactional_data WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                    "GROUP BY SHRTNAME, DIVISION ORDER BY return_rate_pct DESC LIMIT 200."
                )
            else:
                ctx["sql_hints"] = (
                    "GENERAL DATA QUERY — follow all rules below precisely. "

                    "COLUMN ALIASES (MANDATORY — never deviate): "
                    "  SUM(NETAMT) AS net_sales_amount, "
                    "  SUM(QTY) AS total_qty, "
                    "  COUNT(DISTINCT BILLNO) AS bill_count, "
                    "  SUM(GROSSAMT) AS total_gross, "
                    "  SUM(DISCOUNTAMT) AS total_discount. "

                    "GROUPING RULE (CRITICAL): "
                    "  For store-level queries (sales, revenue, performance): "
                    f"  SELECT STORE_ID, SHRTNAME, ZONE, REGION, "
                    "  SUM(NETAMT) AS net_sales_amount, SUM(QTY) AS total_qty, COUNT(DISTINCT BILLNO) AS bill_count "
                    f"  FROM vmart_sales.pos_transactional_data "
                    f"  WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                    "  GROUP BY STORE_ID, SHRTNAME, ZONE, REGION "
                    "  ORDER BY net_sales_amount DESC LIMIT 200. "
                    "  !! DO NOT add DIVISION, SECTION, DEPARTMENT to GROUP BY for store queries. "
                    "  !! Adding category columns splits each store into 10s of rows — wrong totals. "
                    "  !! ONLY add category cols to GROUP BY if user explicitly says 'by division'/'by section'/'category-wise'. "

                    "TABLE RULES: "
                    "  SHRTNAME is in pos_transactional_data — NOT in stores. Do NOT select s.SHRTNAME. "
                    "  Do NOT join stores unless you need AREA_TIER or STORE_TYPE. "

                    "INVENTORY: "
                    "  Use ONLY vmart_product.inventory_current (live snapshot — no date filter needed). "
                    "  Key columns: ICODE (String), STORE_CODE (String = STORE_ID in sales). "
                    "  Join: toString(p.STORE_ID) = inv.STORE_CODE. "
                    "  Stock query: SELECT STORE_CODE, SUM(<stock_col>) AS total_stock "
                    "    FROM vmart_product.inventory_current GROUP BY STORE_CODE ORDER BY total_stock DESC LIMIT 200. "
                    "  Use actual stock column name from Schema Context. "
                    "  NEVER use data_science.inv_31JAN2026 or data_science.inventory_monthly_movements_opt. "

                    "LABEL RULES: "
                    "  (1) Always include SHRTNAME in store-level sales queries. "
                    "  (2) If articles/SKUs queried: include ARTICLENAME, DIVISION, SECTION, DEPARTMENT. "
                    "  (3) If CUSTOMER_MOBILE selected: JOIN customers.customer_master cm "
                    "      ON p.CUSTOMER_MOBILE = cm.CUSTOMER_MOBILE and SELECT cm.CUSTOMER_NAME."
                )

        elif decision.route == Route.PEAK_HOURS:
            ctx["sql_hints"] = (
                "PEAK HOURS ANALYSIS — extract hour from BILLDATE. "
                "Required SQL: "
                "SELECT STORE_ID, SHRTNAME, toHour(BILLDATE) AS hour, "
                "  COUNT(DISTINCT BILLNO) AS txn_count, "
                "  SUM(NETAMT) AS revenue, "
                "  SUM(QTY) AS qty "
                "FROM vmart_sales.pos_transactional_data "
                f"WHERE toDate(BILLDATE) = toDate('{latest_sales}') "
                "GROUP BY STORE_ID, SHRTNAME, hour "
                "ORDER BY SHRTNAME, txn_count DESC "
                "LIMIT 500. "
                "This returns one row per (store, hour) — the pipeline summarises peak hours per store. "
                "If user asks store-wise: add SHRTNAME to GROUP BY. "
                "If user asks chain-wide only: remove SHRTNAME from GROUP BY and SELECT. "
                "NEVER join inventory tables for peak hours. "
                "NEVER use toDateTime() — always toDate() for date filters. "
                "NEVER use today() — always use toDate('{latest_sales}')."
            )

        elif decision.route == Route.TREND_ANALYSIS:
            ctx["sql_hints"] = (
                "Group by a time dimension: toStartOfWeek(BILLDATE) or toStartOfMonth(BILLDATE). "
                "Use NETAMT for revenue, QTY for units. "
                "Include ORDER BY the time dimension ASC."
            )

        elif decision.route == Route.VENDOR_ANALYSIS:
            ctx["sql_hints"] = (
                "Use vmart_sales.dt_pos_ist for inter-store transfers (SOURCE_STORE_CODE, DEST_STORE_CODE, ICODE, QTY, TRANSFER_DATE). "
                "Use vmart_product.inventory_current for current stock levels (ICODE, STORE_CODE, + stock columns from Schema Context). "
                "No vendor/PO tables exist — focus on transfer and inventory data. "
                "NEVER use data_science.inventory_monthly_movements_opt or data_science.inv_31JAN2026."
            )

        return ctx

    # ── Column alias normalisation ───────────────────────────────────────────

    def _normalise_column_aliases(self, query_result: dict) -> dict:
        """
        Rename LLM-generated column aliases to pipeline-canonical names.
        Ensures SPSF enrichment, KPI engines, and chain summaries receive
        correctly named columns regardless of what alias the LLM chose.

        Canonical aliases (what the pipeline expects):
          net_sales_amount  ← SUM(NETAMT)
          total_qty         ← SUM(QTY)
          bill_count        ← COUNT(DISTINCT BILLNO)
          total_gross       ← SUM(GROSSAMT)
          total_discount    ← SUM(DISCOUNTAMT)
          total_promo       ← SUM(PROMOAMT)
          total_mrp         ← SUM(MRPAMT)
        """
        # Map: any lowercase variant → canonical name
        ALIAS_MAP = {
            # Net sales amount
            "yesterday_revenue":  "net_sales_amount",
            "revenue":            "net_sales_amount",
            "total_revenue":      "net_sales_amount",
            "net_sales":          "net_sales_amount",
            "total_sales":        "net_sales_amount",
            "sales_amount":       "net_sales_amount",
            "total_netamt":       "net_sales_amount",
            "netamt":             "net_sales_amount",
            "total_net_sales":    "net_sales_amount",
            "sum(netamt)":        "net_sales_amount",
            # Qty
            "units_sold":         "total_qty",
            "qty_sold":           "total_qty",
            "total_units":        "total_qty",
            "units":              "total_qty",
            "quantity":           "total_qty",
            "qty":                "total_qty",
            # Bill count
            "total_bills":        "bill_count",
            "bill_no":            "bill_count",
            "bills":              "bill_count",
            "num_bills":          "bill_count",
            "transactions":       "bill_count",
            "total_transactions": "bill_count",
            "bills_count":        "bill_count",
            # Gross / discount / promo
            "grossamt":           "total_gross",
            "sum(grossamt)":      "total_gross",
            "discountamt":        "total_discount",
            "sum(discountamt)":   "total_discount",
            "promoamt":           "total_promo",
            "sum(promoamt)":      "total_promo",
            "mrpamt":             "total_mrp",
            "sum(mrpamt)":        "total_mrp",
        }

        data = query_result.get("data", [])
        columns = list(query_result.get("columns", []))
        if not data or not columns:
            return query_result

        # Build rename map: only rename columns that need it
        rename = {}
        new_columns = []
        for col in columns:
            canonical = ALIAS_MAP.get(col.lower())
            if canonical and col.lower() != canonical:
                rename[col] = canonical
                new_columns.append(canonical)
            else:
                new_columns.append(col)

        if not rename:
            return query_result  # Nothing to rename

        # Rename keys in every data row
        new_data = []
        for row in data:
            new_row = {}
            for k, v in row.items():
                new_row[rename.get(k, k)] = v
            new_data.append(new_row)

        result = dict(query_result)
        result["data"] = new_data
        result["columns"] = new_columns
        logger.info(f"Column alias normalised: {list(rename.items())}")
        return result

    # ── SqFt enrichment ─────────────────────────────────────────────────────

    def _enrich_with_sqft(self, query_result: dict) -> dict:
        """
        Enrich query_result rows with floor_sqft from SQLite store_sqft table.
        Joins on STORE_ID (int) or SHRTNAME (str — uppercase match).
        Adds 'floor_sqft' column AND pre-computes 'spsf' (₹/sqft) so the LLM
        reports authentic pre-calculated values rather than doing its own arithmetic.
        """
        data = query_result.get("data", [])
        if not data:
            return query_result

        # Detect available join key in first row (case-insensitive)
        first_row = {k.lower(): v for k, v in data[0].items()}

        # SHRTNAME column aliases the LLM commonly generates
        SHRTNAME_ALIASES = {"shrtname", "store_name", "store_short_name", "short_name",
                            "storename", "shrt_name", "store"}
        # Net sales column aliases the LLM commonly generates
        NET_SALES_ALIASES = {"net_sales_amount", "netamt", "net_sales", "total_sales",
                             "sales_amount", "total_net_sales", "total_netamt", "sum(netamt)"}

        # Store code aliases: inventory tables use 'store_code', 'code', 'admsite_code'
        STORE_CODE_ALIASES = {"store_code", "code", "admsite_code"}

        has_store_id = "store_id" in first_row
        shrtname_col = next((k for k in first_row if k in SHRTNAME_ALIASES), None)
        net_col = next((k for k in first_row if k in NET_SALES_ALIASES), None)
        store_code_col = next((k for k in first_row if k in STORE_CODE_ALIASES), None)
        has_shrtname = shrtname_col is not None
        has_store_code = store_code_col is not None and not has_store_id

        if not has_store_id and not has_shrtname and not has_store_code:
            return query_result  # No join key available

        try:
            from settings.store_sqft_store import (
                get_sqft_lookup_by_store_id,
                get_sqft_lookup_by_shrtname,
                get_store_label_lookup,
            )
            id_lookup: dict = get_sqft_lookup_by_store_id() if (has_store_id or has_store_code) else {}
            shrt_lookup: dict = get_sqft_lookup_by_shrtname() if has_shrtname else {}
            label_lookup: dict = get_store_label_lookup()  # {store_id: {shrtname, store_name, city_name}}

            if not id_lookup and not shrt_lookup:
                return query_result  # No sqft data loaded yet

            enriched = []
            spsf_computed = False
            label_added = False
            for row in data:
                row = dict(row)
                row_lower = {k.lower(): v for k, v in row.items()}
                sqft = 0
                resolved_sid = None

                # Resolve store_id from STORE_ID column
                if has_store_id and id_lookup:
                    try:
                        sid = int(row_lower.get("store_id", 0))
                        sqft = id_lookup.get(sid, 0)
                        resolved_sid = sid
                    except (ValueError, TypeError):
                        pass

                # Resolve store_id from store_code/code/admsite_code (inventory tables)
                if sqft == 0 and has_store_code and id_lookup:
                    try:
                        sid = int(float(row_lower.get(store_code_col, 0) or 0))
                        sqft = id_lookup.get(sid, 0)
                        resolved_sid = sid
                    except (ValueError, TypeError):
                        pass

                # Resolve via SHRTNAME
                if sqft == 0 and has_shrtname and shrt_lookup:
                    shrt = str(row_lower.get(shrtname_col, "")).strip().upper()
                    sqft = shrt_lookup.get(shrt, 0)

                if sqft > 0:
                    row["floor_sqft"] = sqft
                    # Pre-compute SPSF if net sales column is present
                    if net_col:
                        try:
                            net_val = float(row_lower.get(net_col, 0) or 0)
                            if net_val > 0:
                                row["spsf"] = round(net_val / sqft, 2)
                                spsf_computed = True
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass

                # ── Store label enrichment ─────────────────────────────────
                # Add shrtname/store_name if missing but store_id is resolved
                if resolved_sid and label_lookup.get(resolved_sid):
                    lbl = label_lookup[resolved_sid]
                    # Add shrtname if no SHRTNAME-family column present
                    if not has_shrtname and lbl.get("shrtname"):
                        row["shrtname"] = lbl["shrtname"]
                        label_added = True
                    # Add store_name if not already in result
                    if "store_name" not in row_lower and lbl.get("store_name"):
                        row["store_name"] = lbl["store_name"]
                        label_added = True

                enriched.append(row)

            result = dict(query_result)
            result["data"] = enriched

            # ── Sync columns list to match actual lowercase keys in enriched data ──
            # _enrich_with_sqft lowercases all DataFrame columns, so data rows have
            # lowercase keys. Normalise result["columns"] to lowercase to match,
            # preventing row.get("NETAMT") from returning None when key is "netamt".
            cols = list(result.get("columns", []))
            if enriched:
                actual_keys = set(enriched[0].keys())
                actual_lower_map = {k.lower(): k for k in actual_keys}
                # Map original column names → their actual lowercase key in data
                cols = [actual_lower_map.get(c.lower(), c.lower()) for c in cols]

            # Add new enrichment columns if not already present
            for new_col in ("shrtname", "store_name", "floor_sqft", "spsf"):
                if enriched and new_col in enriched[0] and new_col not in cols:
                    cols.append(new_col)
            result["columns"] = cols
            matched = sum(1 for r in enriched if r.get("floor_sqft", 0) > 0)
            logger.info(
                f"store enrichment: {matched}/{len(enriched)} sqft matched"
                + (f", spsf pre-computed ('{net_col}')" if spsf_computed else "")
                + (", store labels added" if label_added else "")
            )
            return result

        except Exception as e:
            logger.warning(f"sqft enrichment failed: {e}")
            return query_result

    # ── Helpers ─────────────────────────────────────────────────────────────

    async def _run_supplementary_queries(self, context: dict) -> dict:
        """
        Run 6 supplementary ClickHouse queries IN PARALLEL using asyncio.gather.
        Returns: {store_inventory, dept, articles, articles_bottom, peak_hours, top_mrp}
        Any individual failure returns {} for that key — never raises.
        Parallel execution cuts total time from ~20-30s (sequential) to ~5-8s (parallel).
        """
        date = context.get("latest_sales_date", "")
        if not date:
            return {}

        from datetime import datetime as _dt
        try:
            days_elapsed = max(_dt.strptime(date, "%Y-%m-%d").day, 1)
        except Exception:
            days_elapsed = 1

        STORE_F   = "STORE_ID NOT IN (SELECT CODE FROM `vmart_sales`.`stores` WHERE CLOSING_DATE IS NOT NULL)"
        STORE_F_P = "p.STORE_ID NOT IN (SELECT CODE FROM `vmart_sales`.`stores` WHERE CLOSING_DATE IS NOT NULL)"
        INV_SUB   = "(SELECT ICODE, SUM(toFloat64OrZero(SOH)) AS SOH FROM `vmart_product`.`inventory_current` GROUP BY ICODE)"

        from clickhouse.query_runner import run_query

        # Build all 6 SQL strings upfront
        sql_store_inventory = f"""
            WITH mtd AS (
                SELECT STORE_ID, ICODE, SUM(QTY) AS mtd_qty
                FROM `vmart_sales`.`pos_transactional_data`
                WHERE toDate(BILLDATE) >= toStartOfMonth(toDate('{date}'))
                  AND toDate(BILLDATE) <= toDate('{date}')
                  AND QTY > 0
                  AND {STORE_F}
                GROUP BY STORE_ID, ICODE
            )
            SELECT
                ms.STORE_ID,
                anyLast(st.SHRTNAME)  AS store_name,
                anyLast(st.ZONE)      AS zone,
                anyLast(st.REGION)    AS region,
                SUM(ms.mtd_qty)       AS mtd_qty,
                SUM(toFloat64OrZero(inv.SOH)) AS total_soh,
                round(SUM(ms.mtd_qty) / nullIf(SUM(ms.mtd_qty) + SUM(toFloat64OrZero(inv.SOH)), 0) * 100, 1)
                    AS sell_thru_pct,
                round(SUM(toFloat64OrZero(inv.SOH)) / nullIf(SUM(ms.mtd_qty) / {days_elapsed}, 0), 0)
                    AS doi
            FROM mtd ms
            LEFT JOIN `vmart_product`.`inventory_current` inv
                ON ms.STORE_ID = inv.STORE_ID AND ms.ICODE = inv.ICODE
            LEFT JOIN `vmart_sales`.`stores` st ON ms.STORE_ID = st.CODE
            WHERE st.CLOSING_DATE IS NULL
            GROUP BY ms.STORE_ID
            HAVING SUM(ms.mtd_qty) > 0
            ORDER BY sell_thru_pct ASC
            LIMIT 100
        """

        sql_dept = f"""
            SELECT
                p.DIVISION, p.SECTION, p.DEPARTMENT,
                SUM(p.NETAMT)                AS net_sales_amount,
                SUM(p.QTY)                   AS total_qty,
                COUNT(DISTINCT p.BILLNO)     AS bill_count,
                SUM(p.GROSSAMT)              AS total_gross,
                SUM(p.DISCOUNTAMT)           AS total_discount,
                round(SUM(p.DISCOUNTAMT) / nullIf(SUM(p.GROSSAMT), 0) * 100, 1) AS discount_pct,
                COUNT(DISTINCT p.ARTICLECODE) AS article_count,
                SUM(toFloat64OrZero(inv.SOH)) AS total_soh,
                round(SUM(p.QTY) / nullIf(SUM(p.QTY) + SUM(toFloat64OrZero(inv.SOH)), 0) * 100, 1)
                    AS sell_thru_pct,
                round(SUM(toFloat64OrZero(inv.SOH)) / nullIf(SUM(p.QTY) / {days_elapsed}, 0), 0)
                    AS doi
            FROM `vmart_sales`.`pos_transactional_data` p
            LEFT JOIN {INV_SUB} inv ON p.ICODE = inv.ICODE
            WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{date}'))
              AND toDate(p.BILLDATE) <= toDate('{date}')
              AND p.QTY > 0
              AND {STORE_F_P}
            GROUP BY p.DIVISION, p.SECTION, p.DEPARTMENT
            ORDER BY net_sales_amount DESC
            LIMIT 25
        """

        sql_articles = f"""
            SELECT
                p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT,
                anyLast(p.STYLE_OR_PATTERN) AS STYLE_OR_PATTERN,
                anyLast(p.SIZE)             AS SIZE,
                anyLast(p.COLOR)            AS COLOR,
                SUM(p.NETAMT)               AS net_sales_amount,
                SUM(p.QTY)                  AS total_qty,
                COUNT(DISTINCT p.BILLNO)    AS bill_count,
                round(SUM(p.MRPAMT) / nullIf(SUM(p.QTY), 0), 0) AS avg_mrp,
                SUM(toFloat64OrZero(inv.SOH)) AS total_soh,
                round(SUM(p.QTY) / nullIf(SUM(p.QTY) + SUM(toFloat64OrZero(inv.SOH)), 0) * 100, 1)
                    AS sell_thru_pct,
                round(SUM(toFloat64OrZero(inv.SOH)) / nullIf(SUM(p.QTY) / {days_elapsed}, 0), 0)
                    AS doi
            FROM `vmart_sales`.`pos_transactional_data` p
            LEFT JOIN {INV_SUB} inv ON p.ICODE = inv.ICODE
            WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{date}'))
              AND toDate(p.BILLDATE) <= toDate('{date}')
              AND p.QTY > 0
              AND {STORE_F_P}
            GROUP BY p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT
            ORDER BY net_sales_amount DESC
            LIMIT 25
        """

        sql_articles_bottom = f"""
            SELECT
                p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT,
                anyLast(p.STYLE_OR_PATTERN) AS STYLE_OR_PATTERN,
                anyLast(p.SIZE)             AS SIZE,
                anyLast(p.COLOR)            AS COLOR,
                SUM(p.NETAMT)               AS net_sales_amount,
                SUM(p.QTY)                  AS total_qty,
                COUNT(DISTINCT p.BILLNO)    AS bill_count,
                round(SUM(p.MRPAMT) / nullIf(SUM(p.QTY), 0), 0) AS avg_mrp,
                SUM(toFloat64OrZero(inv.SOH)) AS total_soh,
                round(SUM(p.QTY) / nullIf(SUM(p.QTY) + SUM(toFloat64OrZero(inv.SOH)), 0) * 100, 1)
                    AS sell_thru_pct,
                round(SUM(toFloat64OrZero(inv.SOH)) / nullIf(SUM(p.QTY) / {days_elapsed}, 0), 0)
                    AS doi
            FROM `vmart_sales`.`pos_transactional_data` p
            LEFT JOIN {INV_SUB} inv ON p.ICODE = inv.ICODE
            WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{date}'))
              AND toDate(p.BILLDATE) <= toDate('{date}')
              AND p.QTY > 0
              AND {STORE_F_P}
            GROUP BY p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT
            ORDER BY net_sales_amount ASC
            LIMIT 25
        """

        sql_peak_hours = f"""
            SELECT STORE_ID, SHRTNAME, ZONE, REGION,
                   toHour(BILLDATE)                                    AS hour,
                   COUNT(DISTINCT BILLNO)                              AS txn_count,
                   countIf(CUSTOMER_MOBILE != '' AND CUSTOMER_MOBILE IS NOT NULL,
                           CUSTOMER_MOBILE)                            AS unique_customers,
                   SUM(NETAMT)                                         AS net_sales_amount,
                   SUM(QTY)                                            AS total_qty
            FROM `vmart_sales`.`pos_transactional_data`
            WHERE toDate(BILLDATE) = toDate('{date}')
              AND {STORE_F}
            GROUP BY STORE_ID, SHRTNAME, ZONE, REGION, hour
            ORDER BY STORE_ID, txn_count DESC
            LIMIT 500
        """

        sql_peak_hours_fallback = f"""
            SELECT STORE_ID, SHRTNAME, ZONE, REGION,
                   toHour(BILLDATE)               AS hour,
                   COUNT(DISTINCT BILLNO)         AS txn_count,
                   COUNT(DISTINCT CUSTOMER_MOBILE) AS unique_customers,
                   SUM(NETAMT)                    AS net_sales_amount,
                   SUM(QTY)                       AS total_qty
            FROM `vmart_sales`.`pos_transactional_data`
            WHERE toDate(BILLDATE) = toDate('{date}')
              AND {STORE_F}
            GROUP BY STORE_ID, SHRTNAME, ZONE, REGION, hour
            ORDER BY STORE_ID, txn_count DESC
            LIMIT 500
        """

        sql_top_mrp = f"""
            SELECT p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT,
                   anyLast(p.STYLE_OR_PATTERN) AS STYLE_OR_PATTERN,
                   anyLast(p.SIZE)             AS SIZE,
                   anyLast(p.COLOR)            AS COLOR,
                   anyLast(toFloat64OrNull(v.MRP)) AS unit_mrp,
                   SUM(p.NETAMT)               AS net_sales_amount,
                   SUM(p.QTY)                  AS total_qty,
                   COUNT(DISTINCT p.BILLNO)    AS bill_count,
                   SUM(toFloat64OrZero(inv.SOH)) AS total_soh,
                   round(SUM(p.QTY) / nullIf(SUM(p.QTY) + SUM(toFloat64OrZero(inv.SOH)), 0) * 100, 1)
                       AS sell_thru_pct,
                   round(SUM(toFloat64OrZero(inv.SOH)) / nullIf(SUM(p.QTY) / {days_elapsed}, 0), 0)
                       AS doi
            FROM `vmart_sales`.`pos_transactional_data` p
            LEFT JOIN (
                SELECT ICODE, anyLast(toFloat64OrNull(MRP)) AS MRP
                FROM `vmart_product`.`vitem_data`
                WHERE toFloat64OrNull(MRP) > 0
                GROUP BY ICODE
            ) v ON p.ICODE = v.ICODE
            LEFT JOIN {INV_SUB} inv ON p.ICODE = inv.ICODE
            WHERE toDate(p.BILLDATE) >= toStartOfMonth(toDate('{date}'))
              AND toDate(p.BILLDATE) <= toDate('{date}')
              AND p.QTY > 0
              AND {STORE_F_P}
            GROUP BY p.ICODE, p.ARTICLENAME, p.DIVISION, p.SECTION, p.DEPARTMENT
            ORDER BY unit_mrp DESC
            LIMIT 7
        """

        # Helper: run a single sync query in a thread, return result or {}
        async def _run(name: str, sql: str) -> tuple[str, dict]:
            try:
                result = await asyncio.to_thread(run_query, sql)
                return name, result
            except Exception as e:
                logger.warning(f"Supp {name} query failed: {e}")
                return name, {}

        async def _run_peak() -> tuple[str, dict]:
            """Peak hours with countIf, fallback to COUNT DISTINCT if unsupported."""
            try:
                result = await asyncio.to_thread(run_query, sql_peak_hours)
                return "peak_hours", result
            except Exception:
                try:
                    result = await asyncio.to_thread(run_query, sql_peak_hours_fallback)
                    return "peak_hours", result
                except Exception as e2:
                    logger.warning(f"Supp peak_hours query failed: {e2}")
                    return "peak_hours", {}

        # Run all 6 queries in parallel — saves ~15-25 seconds vs sequential
        tasks = [
            _run("store_inventory", sql_store_inventory),
            _run("dept", sql_dept),
            _run("articles", sql_articles),
            _run("articles_bottom", sql_articles_bottom),
            _run_peak(),
            _run("top_mrp", sql_top_mrp),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        supp: dict = {}
        for item in results:
            if isinstance(item, Exception):
                logger.warning(f"Supplementary gather exception: {item}")
            elif isinstance(item, tuple) and len(item) == 2:
                name, data = item
                supp[name] = data

        succeeded = sum(1 for v in supp.values() if v.get("data"))
        logger.info(f"Supplementary queries (parallel): {succeeded}/{len(supp)} succeeded")
        return supp

    def _format_alerts_for_prompt(self, alerts: list) -> str:
        if not alerts:
            return "No open alerts in the system."
        lines = []
        for a in alerts[:15]:
            if isinstance(a, dict):
                lines.append(
                    f"[{a.get('priority','?')}] {a.get('kpi_type','')} | "
                    f"{a.get('dimension_value','')} | {a.get('exception_text','')}"
                )
        return "\n".join(lines) or "No alerts."

    def _schema_summary_text(self, schema_dict: dict) -> str:
        if not schema_dict:
            return "No schema loaded."
        lines = []
        for schema, tables in schema_dict.items():
            lines.append(f"\n{schema}:")
            for table, cols in tables.items():
                if isinstance(cols, list):
                    col_names = ", ".join(c["name"] for c in cols[:8])
                    lines.append(f"  {table} ({col_names}{'...' if len(cols) > 8 else ''})")
        return "\n".join(lines)

    def _fallback_narrative(
        self, query: str, decision: PipelineDecision,
        query_result: dict, kpi_results: dict
    ) -> str:
        rows = query_result.get("row_count", 0)
        p1 = kpi_results.get("total_p1", 0)
        p2 = kpi_results.get("total_p2", 0)
        parts = [f"Query '{query}' processed via {decision.route} pipeline."]
        if rows:
            parts.append(f"{rows} records retrieved from ClickHouse.")
        if p1 + p2:
            parts.append(f"KPI analysis found {p1} P1 critical and {p2} P2 high priority alerts.")
        if decision.blockers:
            parts.append(f"\nNote: {decision.guidance}")
        return " ".join(parts)
