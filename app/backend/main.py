"""
DSR|RIECT — FastAPI Application
Main entry point: REST APIs + WebSocket /ws/chat + OAuth
"""

import hashlib
import json
import logging
import os
import secrets
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode, quote

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(__file__))

from config import APP_NAME, APP_VERSION
from db import init_db, get_connection
from settings.settings_store import (
    save_clickhouse_config, get_clickhouse_config,
    save_llm_key, get_llm_key, set_default_llm, get_default_llm,
    set_setting, get_setting,
)
from settings.riect_plan_store import (
    get_kpi_targets, set_kpi_targets, get_all_plan_targets,
    delete_kpi_target, get_plan_summary,
)
from settings.store_sqft_store import (
    import_from_csv as import_sqft_csv,
    get_store_sqft_count, get_all_stores,
)
from clickhouse.connector import test_connection, reset_client
from clickhouse.schema_inspector import inspect_schemas, get_table_schema, get_schema_summary
from llm.ollama_client import get_available_models, get_best_model
from riect.alert_engine.alert_store import get_alert_summary
from riect.api.alerts_api import router as alerts_router
from riect.api.kpi_api import router as kpi_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── App Initialisation ───────────────────────────────────────────────────────

app = FastAPI(title=APP_NAME, version=APP_VERSION)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)
app.include_router(alerts_router)
app.include_router(kpi_router)


SQFT_CSV_PATH = "/Users/dsr-ai-lab/untitled folder/Store Detail SQR Feet Area.csv"


async def _run_initial_alert_scan():
    """Background task: run live KPI scan 10 s after startup."""
    import asyncio as _asyncio
    await _asyncio.sleep(10)
    try:
        from riect.alert_engine.live_scanner import run_live_scan
        loop = _asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_live_scan)
        logger.info(f"Startup alert scan: {result}")
    except Exception as e:
        logger.warning(f"Startup alert scan failed (non-critical): {e}")


@app.on_event("startup")
async def startup():
    init_db()
    # Auto-load store sqft master if not yet imported
    sqft_count = get_store_sqft_count()
    if sqft_count == 0:
        import os
        if os.path.isfile(SQFT_CSV_PATH):
            result = import_sqft_csv(SQFT_CSV_PATH)
            logger.info(f"Auto-imported store sqft: {result}")
        else:
            logger.warning(f"store_sqft: CSV not found at {SQFT_CSV_PATH}")
    else:
        logger.info(f"store_sqft: {sqft_count} stores loaded for SPSF")
    # Restore and lock Qwen as permanent default LLM on every boot
    from llm.qwen_client import (
        is_configured as qwen_is_configured,
        has_saved_credentials as qwen_has_creds,
        keep_alive as qwen_keep_alive,
        auto_relogin as qwen_auto_relogin,
        start_heartbeat as qwen_start_heartbeat,
        get_email as qwen_get_email,
    )
    _qwen_active = False
    if qwen_is_configured():
        # Token in SQLite — verify and auto-relogin from Keychain if expired
        _qwen_active = await qwen_keep_alive()
    elif qwen_has_creds():
        # Token missing from SQLite but Keychain has credentials — restore silently
        logger.info("Qwen: token missing — restoring from Keychain credentials")
        _qwen_active = await qwen_auto_relogin()
    if _qwen_active:
        set_default_llm("qwen")
        qwen_start_heartbeat()
        creds = qwen_has_creds()
        logger.info(f"Qwen: active as default LLM "
                    f"({'auto-relogin ON' if creds else 'token-only — re-paste if server offline long'})")
    elif qwen_get_email():
        # Email known but session could not be restored (network down, SSO token expired)
        # Still start heartbeat — it will keep retrying auto-relogin every 5 min
        set_default_llm("qwen")
        qwen_start_heartbeat()
        logger.warning("Qwen: session restore failed — heartbeat will retry every 5 min")
    else:
        logger.info("Qwen: not configured — connect via Settings")
    logger.info(f"{APP_NAME} v{APP_VERSION} started")
    # Trigger initial alert scan in background after ClickHouse warms up
    import asyncio
    asyncio.create_task(_run_initial_alert_scan())


# ─── Static Files ─────────────────────────────────────────────────────────────

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    async def serve_frontend():
        idx = os.path.join(FRONTEND_DIR, "index.html")
        return FileResponse(idx) if os.path.isfile(idx) else {"status": f"{APP_NAME} running"}
else:
    @app.get("/")
    async def root():
        return {"status": f"{APP_NAME} running", "version": APP_VERSION}


# ─── Health / Status ──────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "app": APP_NAME, "version": APP_VERSION}


@app.get("/api/status")
async def get_status():
    """Full system status: Ollama models, ClickHouse, LLM providers."""
    # Ollama
    ollama_models = await get_available_models()
    best_model = await get_best_model()

    # ClickHouse
    ch_configured = bool(get_setting("clickhouse_password"))
    ch_status = "configured" if ch_configured else "not_configured"

    # LLM providers
    return {
        "ollama": {
            "available": len(ollama_models) > 0,
            "models": ollama_models,
            "best_model": best_model,
        },
        "clickhouse": {
            "status": ch_status,
            "host": get_setting("clickhouse_host", ""),
        },
        "llm_providers": {
            "default": get_default_llm(),
            "qwen":   bool(__import__("llm.qwen_client", fromlist=["is_configured"]).is_configured()),
            "claude": bool(get_llm_key("claude")),
            "gemini": bool(get_llm_key("gemini")),
            "openai": bool(get_llm_key("openai")),
        },
    }


# ─── Pydantic Models ─────────────────────────────────────────────────────────

class ClickHouseConfig(BaseModel):
    host: str
    port: int = 8443
    user: str
    password: str
    secure: bool = True
    schemas: list = ["vmart_sales", "customers", "data_science"]


class LLMKeyPayload(BaseModel):
    key: str


class QwenLoginPayload(BaseModel):
    email: str
    password: str


class QwenTokenPayload(BaseModel):
    token: str
    email: Optional[str] = ""


class QwenModelPayload(BaseModel):
    model: str


class DefaultLLMPayload(BaseModel):
    model: str


class NewSession(BaseModel):
    title: Optional[str] = "New Chat"
    role: Optional[str] = "HQ"


class RiectPlanTarget(BaseModel):
    kpi_type: str                          # SPSF, SELL_THRU, DOI, MBQ, UPT
    p1: float
    p2: float
    p3: float
    target: float
    dimension: Optional[str] = "global"   # global, store, category, division
    dimension_value: Optional[str] = ""   # store code / category name
    period: Optional[str] = ""
    notes: Optional[str] = ""


# ─── Settings Endpoints ───────────────────────────────────────────────────────

@app.post("/api/settings/clickhouse")
async def save_ch_config(config: ClickHouseConfig):
    save_clickhouse_config(**config.dict())
    reset_client()
    return {"status": "saved"}


@app.get("/api/settings/clickhouse")
async def get_ch_config():
    from settings.settings_store import is_clickhouse_configured
    if not is_clickhouse_configured():
        # Not configured yet — return blanks so form does not pre-fill with placeholder host
        from config import CLICKHOUSE_DEFAULTS
        return {
            "host": "",
            "port": CLICKHOUSE_DEFAULTS["port"],
            "user": "",
            "password": "",
            "secure": True,
            "schemas": CLICKHOUSE_DEFAULTS["schemas"],
            "configured": False,
        }
    cfg = get_clickhouse_config()
    cfg["password"] = "••••••" if cfg.get("password") else ""
    cfg["configured"] = True
    return cfg


@app.delete("/api/settings/clickhouse")
async def clear_ch_config():
    """Clear stored ClickHouse credentials and reset to unconfigured state."""
    from settings.settings_store import set_setting
    for key in ["clickhouse_host", "clickhouse_port", "clickhouse_user",
                "clickhouse_password", "clickhouse_secure", "clickhouse_schemas",
                "clickhouse_configured"]:
        set_setting(key, "")
    reset_client()
    return {"status": "cleared"}


@app.post("/api/settings/clickhouse/test")
async def test_ch_connection(config: ClickHouseConfig):
    result = test_connection(config.dict())
    if result["status"] == "connected":
        save_clickhouse_config(**config.dict())
        reset_client()
        try:
            inspect_schemas(config.schemas, force_refresh=True)
        except Exception as e:
            logger.warning(f"Schema refresh after test failed: {e}")
    return result


@app.post("/api/settings/llm/{provider}")
async def save_llm_key_endpoint(provider: str, payload: LLMKeyPayload):
    if provider not in ["claude", "gemini", "openai"]:
        raise HTTPException(400, f"Unknown provider: {provider}")
    save_llm_key(provider, payload.key)
    return {"status": "saved", "provider": provider}


@app.delete("/api/settings/llm/{provider}")
async def remove_llm_key(provider: str):
    """Disconnect a cloud LLM provider."""
    if provider not in ["claude", "gemini", "openai"]:
        raise HTTPException(400, f"Unknown provider: {provider}")
    set_setting(f"llm_{provider}_key", "")
    return {"status": "removed", "provider": provider}


@app.get("/api/settings/llm")
async def get_llm_settings():
    ollama_models = await get_available_models()
    best = await get_best_model()
    return {
        "default_model": get_default_llm(),
        "ollama_models": ollama_models,
        "best_ollama_model": best,
        "claude_configured": bool(get_llm_key("claude")),
        "gemini_configured": bool(get_llm_key("gemini")),
        "openai_configured": bool(get_llm_key("openai")),
    }


@app.post("/api/settings/llm/default")
async def set_default_llm_endpoint(payload: DefaultLLMPayload):
    set_default_llm(payload.model)
    return {"status": "saved", "model": payload.model}


# ─── Qwen Endpoints ───────────────────────────────────────────────────────────

@app.post("/api/settings/qwen/google")
async def qwen_google_login(payload: GoogleOAuthExchange):
    """
    Exchange a Google OAuth code for a Qwen session token.
    Flow: Google OAuth popup → code → this endpoint → Google token exchange
          → id_token → Qwen /api/v2/auth/google → Qwen session token → SQLite.
    """
    import httpx
    from llm.qwen_client import save_token as qwen_store_token, _HEADERS_BASE

    # 1. Exchange code with Google to get id_token
    token_data = {
        "client_id":    payload.client_id,
        "code":         payload.code,
        "code_verifier": payload.code_verifier,
        "grant_type":   "authorization_code",
        "redirect_uri": payload.redirect_uri,
    }
    if payload.client_secret:
        token_data["client_secret"] = payload.client_secret

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            g_resp = await client.post(GOOGLE_TOKEN_URL, data=token_data)
            g_resp.raise_for_status()
            g_tokens = g_resp.json()
    except Exception as e:
        raise HTTPException(400, f"Google token exchange failed: {e}")

    id_token = g_tokens.get("id_token", "")
    if not id_token:
        raise HTTPException(400, "Google did not return an id_token — ensure 'openid' scope is included")

    # Resolve email from Google userinfo
    email = ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            ui = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {g_tokens.get('access_token', '')}"},
            )
            if ui.status_code == 200:
                email = ui.json().get("email", "")
    except Exception:
        pass

    # 2. Send id_token to Qwen → get Qwen session token
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            q_resp = await client.post(
                "https://chat.qwen.ai/api/v2/auth/google",
                json={"id_token": id_token},
                headers=_HEADERS_BASE,
            )
            q_data = q_resp.json() if q_resp.headers.get("content-type", "").startswith("application/json") else {}
    except Exception as e:
        raise HTTPException(502, f"Qwen Google auth failed: {e}")

    inner = q_data.get("data", {}) if isinstance(q_data.get("data"), dict) else {}
    qwen_token = (
        inner.get("token") or inner.get("access_token")
        or q_data.get("token") or q_data.get("access_token")
    )

    if q_data.get("success") and qwen_token:
        resolved_email = inner.get("email") or email or "Google Account"
        result = await qwen_store_token(qwen_token, resolved_email)
        if result["success"]:
            set_default_llm("qwen")
            return {"status": "connected", "email": result["email"],
                    "message": f"Connected as {result['email']}"}

    # Qwen rejected the token — surface error
    err_code = inner.get("code", "")
    err_msg  = inner.get("details") or inner.get("message") or err_code or "Qwen rejected the Google token"
    raise HTTPException(401, detail=f"Qwen auth failed: {err_msg}")


@app.post("/api/settings/qwen/login")
async def qwen_login(payload: QwenLoginPayload):
    """Authenticate with chat.qwen.ai using email + password."""
    from llm.qwen_client import login as qwen_auth, start_heartbeat as qwen_start_heartbeat
    result = await qwen_auth(payload.email, payload.password)
    if result["success"]:
        set_default_llm("qwen")
        qwen_start_heartbeat()
        return {"status": "connected", "email": result["email"],
                "message": result["message"]}
    raise HTTPException(401, detail=result["message"])


@app.post("/api/settings/qwen/token")
async def qwen_save_token(payload: QwenTokenPayload):
    """Save a session token pasted by the user (for Google/SSO-linked accounts)."""
    from llm.qwen_client import save_token as qwen_store_token, start_heartbeat as qwen_start_heartbeat
    result = await qwen_store_token(payload.token, payload.email)
    if result["success"]:
        set_default_llm("qwen")
        qwen_start_heartbeat()
        return {"status": "connected", "email": result["email"], "message": result["message"]}
    raise HTTPException(401, detail=result["message"])


@app.post("/api/settings/qwen/capture")
async def qwen_capture_token(payload: QwenTokenPayload):
    """Called by bookmarklet/snippet from chat.qwen.ai — CORS open so cross-origin POST works."""
    from llm.qwen_client import save_token as qwen_store_token, start_heartbeat as qwen_start_heartbeat
    result = await qwen_store_token(payload.token, payload.email)
    if result["success"]:
        set_default_llm("qwen")
        qwen_start_heartbeat()
        return {"status": "connected", "email": result["email"]}
    raise HTTPException(401, detail=result["message"])


@app.get("/qwen-connect")
async def qwen_connect_page(token: str = None):
    """
    Bookmarklet redirect target.
    If 'token' query param is present, save it and show success/error HTML.
    The bookmarklet on chat.qwen.ai reads localStorage and navigates here.
    """
    _CSS = """*{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#111;color:#eee;display:flex;align-items:center;
         justify-content:center;min-height:100vh;flex-direction:column;gap:16px}
    .icon{font-size:48px} h1{font-size:22px;color:#00dc82}
    p{font-size:13px;color:#888;text-align:center}
    .email{color:#f5c518;font-weight:600}
    .btn{margin-top:12px;background:#00dc82;color:#000;border:none;
         padding:10px 24px;border-radius:8px;font-size:13px;font-weight:700;cursor:pointer}
    .err{color:#f55}"""

    if token:
        from llm.qwen_client import save_token as qwen_store_token, start_heartbeat as qwen_start_heartbeat
        result = await qwen_store_token(token.strip())
        if result["success"]:
            set_default_llm("qwen")
            qwen_start_heartbeat()
            email = result.get("email", "Qwen Account")
            body = f"""<div class="icon">✦</div>
  <h1>Qwen Connected!</h1>
  <p>Signed in as<br><span class="email">{email}</span></p>
  <p style="margin-top:4px">DSR|RIECT is now using Qwen as the default AI.</p>
  <button class="btn" onclick="window.close()">Close &amp; Return to DSR|RIECT</button>
  <script>setTimeout(()=>window.close(),4000)</script>"""
        else:
            msg = result.get("message", "Token rejected")
            body = f"""<div class="icon err" style="font-size:36px">✗</div>
  <h1 class="err">Connection Failed</h1>
  <p class="err">{msg}</p>
  <p style="margin-top:8px;font-size:11px">Ensure you are signed in to chat.qwen.ai<br>then click the bookmark again.</p>
  <button class="btn" style="background:#333;color:#eee" onclick="window.close()">Close</button>"""
    else:
        # Legacy path (no token in URL) — generic success page
        body = """<div class="icon">✦</div>
  <h1>Qwen Connected!</h1>
  <p>You are signed in as<br><span class="email" id="em">your Qwen account</span></p>
  <p style="margin-top:4px">DSR|RIECT is now using Qwen as the default AI.</p>
  <button class="btn" onclick="window.close()">Close &amp; Return to DSR|RIECT</button>
  <script>
    const p=new URLSearchParams(location.search);
    const em=p.get('email');if(em)document.getElementById('em').textContent=em;
    setTimeout(()=>window.close(),4000);
  </script>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DSR|RIECT — Qwen</title>
  <style>{_CSS}</style>
</head>
<body>{body}</body>
</html>"""
    return HTMLResponse(html)


@app.post("/api/settings/qwen/model")
async def qwen_set_model(payload: QwenModelPayload):
    """Set the active Qwen model (qwen3.5-plus | qwen3.5-flash | qwen3-max)."""
    from llm.qwen_client import set_model, QWEN_MODELS
    if payload.model not in QWEN_MODELS:
        raise HTTPException(400, f"Unknown Qwen model: {payload.model}. Valid: {list(QWEN_MODELS)}")
    set_model(payload.model)
    return {"status": "saved", "model": payload.model, "label": QWEN_MODELS[payload.model]}


@app.get("/api/settings/qwen/status")
async def qwen_status():
    """Return Qwen connection status."""
    from llm.qwen_client import is_configured, get_email, get_model, QWEN_MODELS
    configured = is_configured()
    return {
        "connected": configured,
        "email":     get_email() if configured else "",
        "model":     get_model(),
        "model_label": QWEN_MODELS.get(get_model(), get_model()),
        "models":    [{"id": k, "label": v} for k, v in QWEN_MODELS.items()],
    }


@app.delete("/api/settings/qwen")
async def qwen_disconnect():
    """Disconnect Qwen account."""
    from llm.qwen_client import disconnect
    disconnect()
    return {"status": "disconnected"}


# ─── OAuth Endpoints (Google / Gemini) ───────────────────────────────────────

# In-memory state store for OAuth PKCE (session_id → state)
_oauth_states: dict = {}

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
GEMINI_SCOPE = "https://www.googleapis.com/auth/generativelanguage openid email profile"


class GoogleOAuthStart(BaseModel):
    client_id: str
    redirect_uri: str


class GoogleOAuthExchange(BaseModel):
    code: str
    code_verifier: str
    client_id: str
    client_secret: Optional[str] = None
    redirect_uri: str


@app.post("/api/auth/google/start")
async def google_oauth_start(payload: GoogleOAuthStart):
    """
    Start Google OAuth2 PKCE flow.
    Returns auth_url for frontend to open in popup.
    """
    state = secrets.token_urlsafe(32)
    _oauth_states[state] = {"client_id": payload.client_id, "redirect_uri": payload.redirect_uri}

    params = {
        "client_id": payload.client_id,
        "redirect_uri": payload.redirect_uri,
        "response_type": "code",
        "scope": GEMINI_SCOPE,
        "state": state,
        "access_type": "offline",
        "prompt": "consent",
    }
    auth_url = f"{GOOGLE_AUTH_URL}?{urlencode(params)}"
    return {"auth_url": auth_url, "state": state}


@app.post("/api/auth/google/exchange")
async def google_oauth_exchange(payload: GoogleOAuthExchange):
    """Exchange OAuth2 code for access token."""
    import httpx

    token_data = {
        "client_id": payload.client_id,
        "code": payload.code,
        "code_verifier": payload.code_verifier,
        "grant_type": "authorization_code",
        "redirect_uri": payload.redirect_uri,
    }
    if payload.client_secret:
        token_data["client_secret"] = payload.client_secret

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(GOOGLE_TOKEN_URL, data=token_data)
            resp.raise_for_status()
            tokens = resp.json()

        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

        # Get user email
        email = ""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                ui_resp = await client.get(
                    GOOGLE_USERINFO_URL,
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                if ui_resp.status_code == 200:
                    email = ui_resp.json().get("email", "")
        except Exception:
            pass

        # Save token as "gemini" API key
        if access_token:
            set_setting("llm_gemini_key", access_token)
            set_setting("llm_gemini_refresh_token", refresh_token)
            set_setting("llm_gemini_email", email)
            set_setting("llm_gemini_auth_type", "oauth")

        return {
            "status": "connected",
            "email": email,
            "provider": "google",
        }
    except Exception as e:
        raise HTTPException(400, f"OAuth exchange failed: {e}")


@app.get("/api/auth/google/status")
async def google_auth_status():
    auth_type = get_setting("llm_gemini_auth_type", "key")
    email = get_setting("llm_gemini_email", "")
    has_token = bool(get_llm_key("gemini"))
    return {
        "connected": has_token,
        "auth_type": auth_type,
        "email": email,
    }


@app.delete("/api/auth/google")
async def google_auth_revoke():
    set_setting("llm_gemini_key", "")
    set_setting("llm_gemini_refresh_token", "")
    set_setting("llm_gemini_email", "")
    set_setting("llm_gemini_auth_type", "")
    return {"status": "disconnected"}


@app.get("/api/auth/google/callback")
async def google_oauth_callback(code: str = None, state: str = None, error: str = None):
    """
    OAuth2 redirect callback page.
    Returns HTML that posts the auth code back to the opener window via postMessage.
    """
    import json as _json

    if error:
        payload = _json.dumps({"type": "oauth_error", "error": error})
    elif code:
        payload = _json.dumps({"type": "oauth_code", "code": code, "state": state or ""})
    else:
        payload = _json.dumps({"type": "oauth_error", "error": "no_code_returned"})

    html = (
        "<!DOCTYPE html><html><head><title>DSR|RIECT Authentication</title>"
        "<style>body{font-family:sans-serif;text-align:center;padding:60px 20px;"
        "background:#0d1117;color:#8b949e;}h2{color:#e6edf3}p{margin-top:12px}</style></head>"
        "<body><h2>DSR|RIECT</h2><p>Authenticating... this window will close automatically.</p>"
        "<script>"
        "(function(){"
        "var p=" + payload + ";"
        "if(window.opener){"
        "window.opener.postMessage(p,window.location.origin);"
        "setTimeout(function(){window.close();},800);"
        "}else{"
        "document.querySelector('p').textContent="
        "p.type==='oauth_code'?'Authentication successful. You may close this window.'"
        ":'Authentication failed: '+p.error;"
        "}"
        "})();"
        "</script></body></html>"
    )
    return HTMLResponse(html)


class GoogleClientIdPayload(BaseModel):
    client_id: str


@app.post("/api/auth/google/client_id")
async def set_google_client_id(payload: GoogleClientIdPayload):
    """Store Google OAuth2 Client ID for Gemini sign-in."""
    set_setting("google_oauth_client_id", payload.client_id)
    return {"status": "saved"}


@app.get("/api/auth/google/client_id")
async def get_google_client_id():
    cid = get_setting("google_oauth_client_id", "")
    return {"client_id": cid, "configured": bool(cid)}


@app.get("/api/auth/providers")
async def get_provider_status():
    """Full status of all LLM provider connections (DB key or env var)."""
    import os

    def _auth_source(provider: str) -> str:
        """Return 'account' if key comes from env var, 'api_key' if from settings."""
        env_map = {
            "claude": ["ANTHROPIC_API_KEY", "CLAUDE_API_KEY"],
            "gemini": ["GOOGLE_API_KEY", "GEMINI_API_KEY"],
            "openai": ["OPENAI_API_KEY"],
        }
        # If stored in SQLite → api_key
        from settings.settings_store import LLM_CLAUDE_KEY, LLM_GEMINI_KEY, LLM_OPENAI_KEY
        db_key_map = {"claude": LLM_CLAUDE_KEY, "gemini": LLM_GEMINI_KEY, "openai": LLM_OPENAI_KEY}
        if get_setting(db_key_map.get(provider, "")):
            return "api_key"
        # If from env var → account
        for ev in env_map.get(provider, []):
            if os.environ.get(ev):
                return "account"
        return "api_key"

    gemini_key = get_llm_key("gemini")
    return {
        "claude": {
            "connected": bool(get_llm_key("claude")),
            "auth_type": _auth_source("claude"),
        },
        "gemini": {
            "connected": bool(gemini_key),
            "auth_type": get_setting("llm_gemini_auth_type", _auth_source("gemini")),
            "email": get_setting("llm_gemini_email", ""),
        },
        "openai": {
            "connected": bool(get_llm_key("openai")),
            "auth_type": _auth_source("openai"),
        },
        "google_oauth_client_id_configured": bool(get_setting("google_oauth_client_id", "")),
    }


# ─── Store SqFt Endpoints ─────────────────────────────────────────────────────

@app.get("/api/store-sqft/status")
async def store_sqft_status():
    """Return status of store sqft master data."""
    count = get_store_sqft_count()
    return {
        "stores_loaded": count,
        "spsf_ready": count > 0,
        "message": f"{count} stores with valid sqft data loaded" if count > 0 else "No sqft data — upload CSV to enable SPSF",
    }


@app.get("/api/store-sqft")
async def list_store_sqft(limit: int = 100, offset: int = 0):
    """List store sqft records."""
    all_rows = get_all_stores()
    total = len(all_rows)
    return {
        "total": total,
        "stores": all_rows[offset:offset + limit],
    }


@app.post("/api/store-sqft/import")
async def import_store_sqft(csv_path: str = "/Users/dsr-ai-lab/untitled folder/Store Detail SQR Feet Area.csv"):
    """Import store sqft from CSV file on server. Returns import stats."""
    import os
    if not os.path.isfile(csv_path):
        raise HTTPException(404, f"File not found: {csv_path}")
    result = import_sqft_csv(csv_path)
    if "error" in result:
        raise HTTPException(500, result["error"])
    return {
        "status": "imported",
        **result,
        "stores_with_sqft": get_store_sqft_count(),
    }


# ─── RIECT-Plan Endpoints ─────────────────────────────────────────────────────

@app.get("/api/riect-plan")
async def get_riect_plan():
    """
    Return all configured RIECT-Plan KPI targets with comparison to config defaults.
    """
    return {
        "plan": get_all_plan_targets(),
        "summary": get_plan_summary(),
    }


@app.get("/api/riect-plan/{kpi_type}")
async def get_riect_plan_kpi(
    kpi_type: str,
    dimension: str = "global",
    dimension_value: str = "",
):
    """Return thresholds for a specific KPI (e.g. SPSF, SELL_THRU, DOI)."""
    targets = get_kpi_targets(kpi_type.upper(), dimension, dimension_value)
    if not targets:
        raise HTTPException(404, f"No targets found for {kpi_type}")
    return targets


@app.post("/api/riect-plan")
async def set_riect_plan(payload: RiectPlanTarget):
    """
    Set or update a KPI target.
    Examples:
      {"kpi_type":"SELL_THRU","p1":0.55,"p2":0.75,"p3":0.90,"target":1.0}
      {"kpi_type":"SPSF","p1":400,"p2":600,"p3":900,"target":1100}
    """
    ok = set_kpi_targets(
        kpi_type=payload.kpi_type,
        p1=payload.p1,
        p2=payload.p2,
        p3=payload.p3,
        target=payload.target,
        dimension=payload.dimension or "global",
        dimension_value=payload.dimension_value or "",
        period=payload.period or "",
        notes=payload.notes or "",
    )
    if not ok:
        raise HTTPException(500, "Failed to save target")
    return {
        "status": "saved",
        "kpi_type": payload.kpi_type.upper(),
        "dimension": payload.dimension,
        "targets": get_kpi_targets(payload.kpi_type.upper(), payload.dimension or "global", payload.dimension_value or ""),
    }


@app.delete("/api/riect-plan/{kpi_type}")
async def delete_riect_plan_kpi(
    kpi_type: str,
    dimension: str = "global",
    dimension_value: str = "",
):
    """Remove a specific KPI target override (reverts to config.py defaults)."""
    ok = delete_kpi_target(kpi_type.upper(), dimension, dimension_value)
    if not ok:
        raise HTTPException(500, "Failed to delete target")
    return {"status": "deleted", "kpi_type": kpi_type.upper(), "reverted_to": "config_default"}


# ─── Schema Endpoints ─────────────────────────────────────────────────────────

@app.get("/api/schema/tables")
async def list_tables():
    try:
        summary = get_schema_summary()
        if not summary:
            cfg = get_clickhouse_config()
            schema_dict = inspect_schemas(cfg["schemas"])
            summary = {s: list(t.keys()) for s, t in schema_dict.items()}
        return {"schemas": summary}
    except Exception as e:
        raise HTTPException(503, f"ClickHouse unavailable: {e}")


@app.get("/api/schema/{schema_name}/{table_name}")
async def describe_table(schema_name: str, table_name: str):
    try:
        columns = get_table_schema(schema_name, table_name)
        return {"schema": schema_name, "table": table_name, "columns": columns}
    except Exception as e:
        raise HTTPException(503, str(e))


@app.post("/api/schema/refresh")
async def refresh_schemas():
    try:
        cfg = get_clickhouse_config()
        schema_dict = inspect_schemas(cfg["schemas"], force_refresh=True)
        total = sum(len(t) for t in schema_dict.values())
        return {"status": "refreshed", "total_tables": total}
    except Exception as e:
        raise HTTPException(503, str(e))


# ─── Session Endpoints ────────────────────────────────────────────────────────

@app.get("/api/sessions")
async def list_sessions(limit: int = 30):
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT s.session_id, s.created_at, s.title, s.role,
                  (SELECT content FROM messages WHERE session_id=s.session_id
                   ORDER BY id DESC LIMIT 1) as last_message,
                  (SELECT COUNT(*) FROM messages WHERE session_id=s.session_id) as msg_count
               FROM sessions s ORDER BY s.created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.post("/api/sessions")
async def create_session(payload: NewSession):
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO sessions (session_id, created_at, title, role) VALUES (?,?,?,?)",
            (session_id, now, payload.title, payload.role),
        )
        conn.commit()
    finally:
        conn.close()
    return {"session_id": session_id, "created_at": now, "title": payload.title}


@app.patch("/api/sessions/{session_id}")
async def update_session(session_id: str, payload: dict):
    conn = get_connection()
    try:
        if "title" in payload:
            conn.execute(
                "UPDATE sessions SET title=? WHERE session_id=?",
                (payload["title"], session_id),
            )
            conn.commit()
    finally:
        conn.close()
    return {"status": "updated"}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    conn = get_connection()
    try:
        conn.execute("DELETE FROM messages WHERE session_id=?", (session_id,))
        conn.execute("DELETE FROM sessions WHERE session_id=?", (session_id,))
        conn.commit()
    finally:
        conn.close()
    return {"status": "deleted"}


@app.get("/api/sessions/{session_id}/messages")
async def get_session_messages(session_id: str, limit: int = 100):
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, role, content, created_at FROM messages WHERE session_id=? ORDER BY id LIMIT ?",
            (session_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _save_message(session_id: str, role: str, content: str):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO messages (session_id, role, content, created_at) VALUES (?,?,?,?)",
            (session_id, role, content, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_session(session_id: str):
    conn = get_connection()
    try:
        exists = conn.execute("SELECT 1 FROM sessions WHERE session_id=?", (session_id,)).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO sessions (session_id, created_at, title, role) VALUES (?,?,?,?)",
                (session_id, datetime.now(timezone.utc).isoformat(), "Chat", "HQ"),
            )
            conn.commit()
    finally:
        conn.close()


# ─── WebSocket Chat ───────────────────────────────────────────────────────────

@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connected")
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await _ws_send(websocket, {"type": "error", "message": "Invalid JSON"})
                continue

            session_id = msg.get("session_id", "")
            query = msg.get("message", "").strip()
            preferred_llm = msg.get("llm")

            if not query:
                continue

            if session_id:
                _ensure_session(session_id)
                _save_message(session_id, "user", query)

            await _handle_chat(websocket, query, session_id, preferred_llm)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await _ws_send(websocket, {"type": "error", "message": str(e)})
        except Exception:
            pass


async def _handle_chat(websocket: WebSocket, query: str, session_id: str, preferred_llm: str = None):
    """Decision Intelligence + Orchestration pipeline for a single chat turn."""
    from pipeline.orchestrator import PipelineOrchestrator

    try:
        orchestrator = PipelineOrchestrator()

        # ── Decision: classify route + check capabilities ─────────────────
        decision = await orchestrator.decide(query, session_id, preferred_llm=preferred_llm)

        await _ws_send(websocket, {
            "type": "decision",
            "route": decision.route,
            "stages": decision.stages,
        })
        if decision.llm_model:
            await _ws_send(websocket, {"type": "model", "model": decision.llm_model})

        # ── Execute pipeline — streams progress via callback ──────────────
        async def ws_callback(data: dict):
            await _ws_send(websocket, data)

        result = await orchestrator.execute(decision, query, session_id, ws_callback)
        blocks, narrative, kpi_results, alerts = result

        # ── Persist to DB ─────────────────────────────────────────────────
        # Use normalized query for session title so it reads clean in history
        display_query = decision.normalized_query or query
        if session_id:
            _save_message(session_id, "assistant", narrative)
            _update_session_title(session_id, display_query)

        # ── Final done event ──────────────────────────────────────────────
        await _ws_send(websocket, {
            "type": "done",
            "blocks": blocks,
            "kpi_summary": {
                "total_p1": kpi_results.get("total_p1", 0),
                "total_p2": kpi_results.get("total_p2", 0),
                "total_p3": kpi_results.get("total_p3", 0),
            } if kpi_results else {},
        })

    except Exception as e:
        logger.error(f"Chat pipeline error: {e}", exc_info=True)
        await _ws_send(websocket, {"type": "error", "message": str(e)})


def _update_session_title(session_id: str, query: str):
    """Auto-title session from first query if still 'New Chat' / 'Chat'."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT title FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if row and row["title"] in ("New Chat", "Chat", ""):
            title = query[:50] + ("..." if len(query) > 50 else "")
            conn.execute(
                "UPDATE sessions SET title=? WHERE session_id=?", (title, session_id)
            )
            conn.commit()
    finally:
        conn.close()


async def _ws_send(websocket: WebSocket, data: dict):
    try:
        await websocket.send_text(json.dumps(data))
    except Exception:
        pass
