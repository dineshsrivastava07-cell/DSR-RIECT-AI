"""
DSR|RIECT — Alerts REST API
GET/POST /api/alerts, PATCH /api/alerts/{id}/resolve
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from riect.alert_engine.alert_store import get_alerts, resolve_alert, get_alert_summary, save_alerts, clear_scan_alerts
from riect.alert_engine.alert_generator import generate_alerts
from riect.alert_engine.action_recommender import enrich_alerts_with_actions
from riect.kpi_engine.kpi_controller import KPIController

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("")
async def list_alerts(
    priority: Optional[str] = Query(None, description="Filter by priority: P1|P2|P3|P4"),
    resolved: bool = Query(False, description="Include resolved alerts"),
    session_id: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
):
    """Get ranked P1→P4 alerts."""
    alerts = get_alerts(priority=priority, resolved=resolved, session_id=session_id, limit=limit)
    summary = get_alert_summary()
    return {"alerts": alerts, "summary": summary, "total": len(alerts)}


class RunAlertRequest(BaseModel):
    query_result: dict
    session_id: str = ""


@router.post("/run")
async def run_alert_engine(request: RunAlertRequest):
    """Trigger alert engine on provided query result data."""
    controller = KPIController()
    kpi_results = controller.run_all(request.query_result)

    alerts = generate_alerts(kpi_results, session_id=request.session_id)
    alerts = enrich_alerts_with_actions(alerts)
    saved_count = save_alerts(alerts)

    return {
        "alerts_generated": len(alerts),
        "alerts_saved": saved_count,
        "alerts": alerts,
        "kpi_summary": {
            "total_p1": kpi_results.get("total_p1", 0),
            "total_p2": kpi_results.get("total_p2", 0),
            "total_p3": kpi_results.get("total_p3", 0),
        },
    }


@router.post("/scan")
async def scan_live_kpis():
    """
    Trigger a full live KPI scan from ClickHouse.
    Queries store-level SPSF, Sell-Through, and DOI for all active stores.
    Clears previous scan alerts and replaces with fresh ranked P1-P4 results.
    Returns scan summary: alerts_generated, alerts_saved, p1/p2/p3 counts, scan_date.
    """
    import asyncio
    from riect.alert_engine.live_scanner import run_live_scan
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, run_live_scan)
    return result


@router.patch("/{alert_id}/resolve")
async def resolve_alert_endpoint(alert_id: str):
    """Mark an alert as resolved."""
    success = resolve_alert(alert_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return {"status": "resolved", "alert_id": alert_id}


@router.get("/summary")
async def alert_summary():
    """Return alert counts by priority."""
    return get_alert_summary()
