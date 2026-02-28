"""
DSR|RIECT — Intent Engine
Classifies user query intent using regex + keyword matching
"""

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

INTENTS = {
    "sales_analytics": {
        "keywords": ["sales", "revenue", "turnover", "spsf", "sell-thru", "sell thru",
                     "net sales", "top stores", "bottom stores", "store performance",
                     "category sales", "daily sales", "weekly sales", "monthly sales"],
        "kpi_types": ["SPSF", "SELL_THRU"],
    },
    "inventory_analysis": {
        "keywords": ["stock", "inventory", "doi", "days of inventory", "overstock",
                     "understock", "stockout", "soh", "git", "goods in transit",
                     "days cover", "days of cover", "replenish"],
        "kpi_types": ["DOI", "MBQ"],
    },
    "peak_hours": {
        "keywords": ["peak hours", "peak hour", "peak time", "rush hour", "busy hour",
                     "hourly sales", "hourly revenue", "hourly traffic", "busiest hour",
                     "foot traffic hour", "by hour", "store timing", "high peak",
                     "hourly performance", "per hour", "busiest time"],
        "kpi_types": ["PEAK_HOURS"],
    },
    "customer_intelligence": {
        "keywords": ["customer", "footfall", "catchment", "visit",
                     "foot traffic", "conversion", "basket", "loyalty", "segment"],
        "kpi_types": [],
    },
    "kpi_dashboard": {
        "keywords": ["kpi", "performance", "target", "dashboard", "scorecard",
                     "spsf", "sell through", "doi", "mbq", "breach", "health"],
        "kpi_types": ["SPSF", "SELL_THRU", "DOI", "MBQ"],
    },
    "vendor_supply": {
        "keywords": ["vendor", "po", "purchase order", "delivery", "gr", "grn",
                     "goods receipt", "supply", "supplier", "lead time", "fill rate"],
        "kpi_types": [],
    },
    "alert_exceptions": {
        "keywords": ["alert", "exception", "p1", "p2", "p3", "critical", "breach",
                     "urgent", "priority", "action needed", "flag"],
        "kpi_types": ["SPSF", "SELL_THRU", "DOI", "MBQ"],
    },
    "loss_pilferage": {
        "keywords": ["pilferage", "shrinkage", "theft", "stolen", "leakage",
                     "bill integrity", "fraud", "cashier", "unauthorized discount",
                     "pilfer", "shrink", "unexplained loss", "unaccounted",
                     "stock shrinkage", "stock loss"],
        "kpi_types": ["PILFERAGE"],
    },
    "discount_analysis": {
        "keywords": ["discount", "markdown", "clearance", "promo discount",
                     "non-promo", "unauthorized markdown", "discount rate",
                     "discount analysis", "discountamt", "promoamt",
                     "manual discount", "extra discount"],
        "kpi_types": ["DISCOUNT"],
    },
    "sales_returns": {
        "keywords": ["returns", "return", "refund", "credit note", "sales return",
                     "exchange", "return rate", "product return",
                     "negative sales", "reversal"],
        "kpi_types": ["RETURNS"],
    },
    "general_retail": {
        "keywords": ["compare", "trend", "forecast", "recommendation", "insight",
                     "analyse", "analysis", "report", "summary", "overview"],
        "kpi_types": [],
    },
}

# Intents that require SQL queries
SQL_REQUIRED_INTENTS = {
    "sales_analytics", "inventory_analysis", "customer_intelligence",
    "kpi_dashboard", "vendor_supply",
    "loss_pilferage", "discount_analysis", "sales_returns",
    "peak_hours",
}


def classify_intent(query: str) -> dict:
    """
    Classify user query intent.
    Returns: {intent, confidence, requires_sql, kpi_types}
    """
    q_lower = query.lower()
    scores: dict[str, int] = {}

    for intent, config in INTENTS.items():
        score = 0
        for keyword in config["keywords"]:
            if keyword in q_lower:
                # Exact word boundary match scores higher
                if re.search(r'\b' + re.escape(keyword) + r'\b', q_lower):
                    score += 2
                else:
                    score += 1
        scores[intent] = score

    # Pick highest score
    best_intent = max(scores, key=lambda k: scores[k])
    best_score = scores[best_intent]

    # No match → general
    if best_score == 0:
        best_intent = "general_retail"

    # Confidence: normalise against max possible
    total_keywords = len(INTENTS[best_intent]["keywords"])
    max_score = total_keywords * 2
    confidence = min(1.0, best_score / max(max_score, 1))

    kpi_types = INTENTS[best_intent]["kpi_types"]
    requires_sql = best_intent in SQL_REQUIRED_INTENTS or best_score >= 2

    result = {
        "intent": best_intent,
        "confidence": round(confidence, 3),
        "requires_sql": requires_sql,
        "kpi_types": kpi_types,
        "scores": scores,
    }
    logger.debug(f"Intent classified: {best_intent} (conf={confidence:.2f})")
    return result
