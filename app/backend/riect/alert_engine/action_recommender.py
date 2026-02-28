"""
DSR|RIECT — Action Recommender
Maps (kpi_type, priority) → action / owner / timeline / expected_impact
"""

ACTION_PLAYBOOK = {
    ("SPSF", "P1"): {
        "recommended_action": "Immediate store visit + floor reset required. Review planogram compliance. Activate promotional activity.",
        "action_owner": "Regional Manager",
        "response_timeline": "24 hours",
        "expected_impact": "SPSF improvement of 15-25% within 1 week with targeted intervention",
    },
    ("SPSF", "P2"): {
        "recommended_action": "Review category performance. Identify slow-movers for markdown. Improve product density.",
        "action_owner": "Store Manager",
        "response_timeline": "48 hours",
        "expected_impact": "SPSF improvement of 8-12% within 2 weeks",
    },
    ("SPSF", "P3"): {
        "recommended_action": "Monitor weekly trend. Adjust visual merchandising and cross-sell opportunities.",
        "action_owner": "Store Manager",
        "response_timeline": "1 week",
        "expected_impact": "SPSF improvement of 3-5% within 1 month",
    },
    ("SPSF", "P4"): {
        "recommended_action": "Continue monitoring. No immediate action required.",
        "action_owner": "Store Manager",
        "response_timeline": "Monthly review",
        "expected_impact": "Maintain current performance",
    },
    ("SELL_THRU", "P1"): {
        "recommended_action": "Immediate markdown required. Raise markdown ticket to pricing team. Review OTB plan.",
        "action_owner": "Buying Team + Store Manager",
        "response_timeline": "24-48 hours",
        "expected_impact": "Sell-through increase of 20%+ within 2 weeks with markdown",
    },
    ("SELL_THRU", "P2"): {
        "recommended_action": "Initiate markdown review. Increase visibility with end-cap display. Consider buy-one-get-one promotions.",
        "action_owner": "Category Manager",
        "response_timeline": "3-5 days",
        "expected_impact": "Sell-through improvement to above 80% within 3 weeks",
    },
    ("SELL_THRU", "P3"): {
        "recommended_action": "Targeted promotion or bundle offer. Review pricing vs competitors.",
        "action_owner": "Store Manager",
        "response_timeline": "1 week",
        "expected_impact": "Sell-through improvement to above 95% within 1 month",
    },
    ("SELL_THRU", "P4"): {
        "recommended_action": "No action needed. Monitor for trend changes.",
        "action_owner": "Store Manager",
        "response_timeline": "Monthly review",
        "expected_impact": "Sustain current sell-through rate",
    },
    ("DOI", "P1"): {
        "recommended_action": "Critical overstock. Immediate inter-store transfer or markdown. Pause replenishment orders.",
        "action_owner": "Supply Chain + Regional Manager",
        "response_timeline": "48 hours",
        "expected_impact": "DOI reduction to below 60 days within 3 weeks",
    },
    ("DOI", "P2"): {
        "recommended_action": "Reduce replenishment order quantities. Plan clearance event. Review demand forecast.",
        "action_owner": "Supply Chain Manager",
        "response_timeline": "1 week",
        "expected_impact": "DOI reduction to below 30 days within 4-6 weeks",
    },
    ("DOI", "P3"): {
        "recommended_action": "Adjust replenishment frequency. Review min-max settings for this SKU.",
        "action_owner": "Store Manager",
        "response_timeline": "2 weeks",
        "expected_impact": "DOI normalisation within 2 months",
    },
    ("DOI", "P4"): {
        "recommended_action": "Monitor inventory levels. No immediate action.",
        "action_owner": "Store Manager",
        "response_timeline": "Monthly review",
        "expected_impact": "Maintain healthy DOI levels",
    },
    ("MBQ", "P1"): {
        "recommended_action": "Urgent replenishment order required. Raise emergency PO. Check supplier availability.",
        "action_owner": "Procurement + Store Manager",
        "response_timeline": "Immediate (same day)",
        "expected_impact": "MBQ compliance restored within 24-72 hours",
    },
    ("MBQ", "P2"): {
        "recommended_action": "Raise standard replenishment order. Prioritise in inbound queue.",
        "action_owner": "Procurement",
        "response_timeline": "24-48 hours",
        "expected_impact": "MBQ compliance restored within 3-5 days",
    },
    ("MBQ", "P3"): {
        "recommended_action": "Schedule replenishment in next order cycle. Verify MBQ targets are still appropriate.",
        "action_owner": "Store Manager",
        "response_timeline": "Next order cycle",
        "expected_impact": "MBQ compliance restored within 1 week",
    },
    ("MBQ", "P4"): {
        "recommended_action": "Within tolerance. Monitor for next cycle.",
        "action_owner": "Store Manager",
        "response_timeline": "Next review cycle",
        "expected_impact": "Maintain MBQ compliance",
    },
}


def get_action(kpi_type: str, priority: str) -> dict:
    """Return action recommendation for a KPI breach."""
    key = (kpi_type.upper(), priority.upper())
    return ACTION_PLAYBOOK.get(key, {
        "recommended_action": "Review data and investigate root cause.",
        "action_owner": "Store Manager",
        "response_timeline": "1 week",
        "expected_impact": "To be determined",
    })


def enrich_alerts_with_actions(alerts: list) -> list:
    """Enrich alert dicts with recommended actions in-place."""
    for alert in alerts:
        kpi_type = alert.get("kpi_type", "")
        priority = alert.get("priority", "P4")
        action = get_action(kpi_type, priority)
        alert.update({
            "recommended_action": action.get("recommended_action", ""),
            "action_owner": action.get("action_owner", ""),
            "response_timeline": action.get("response_timeline", ""),
            "expected_impact": action.get("expected_impact", ""),
        })
    return alerts
