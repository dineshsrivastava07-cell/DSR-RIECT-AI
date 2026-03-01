"""
DSR|RIECT — Qwen Client
Email-authenticated access to chat.qwen.ai (no DashScope API key).

Auth flow:
  email+password → token → SQLite (token) + macOS Keychain (password)
  Google/SSO     → paste token → SQLite (token only)

Persistence:
  - Token stored in SQLite survives RIECT restarts
  - Password stored in macOS Keychain — enables auto-relogin if token expires
  - Background heartbeat pings every 5 min to keep session alive
  - On heartbeat failure: auto-relogin if Keychain credentials exist
  - On 401 during generate: auto-relogin + retry once

Models: Qwen3.5-Plus (default), Qwen3.5 Flash, Qwen3-Max
"""

import asyncio
import json
import logging
import subprocess
from typing import AsyncIterator

import httpx

from settings.settings_store import get_setting, set_setting

logger = logging.getLogger(__name__)

# ─── Model Registry ───────────────────────────────────────────────────────────

QWEN_MODELS = {
    "qwen3.5-plus":  "Qwen3.5-Plus",
    "qwen3.5-flash": "Qwen3.5 Flash",
    "qwen3-max":     "Qwen3-Max",
}
QWEN_DEFAULT_MODEL = "qwen3.5-plus"

# ─── Endpoints ────────────────────────────────────────────────────────────────

_BASE   = "https://chat.qwen.ai"
_LOGIN  = f"{_BASE}/api/v2/user/email/login"
_CHAT   = f"{_BASE}/api/chat/completions"
_VERIFY = f"{_BASE}/api/v2/user/info"

_HEADERS_BASE = {
    "Content-Type":  "application/json",
    "Accept":        "application/json",
    "Origin":        _BASE,
    "Referer":       f"{_BASE}/",
    "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/124.0.0.0 Safari/537.36",
}

# ─── Circuit breaker — skip Qwen for 5 minutes after a 5xx failure ──────────
import time as _time
_cb_failure_time: float = 0.0   # epoch seconds of last 5xx failure
_CB_COOLDOWN = 300              # 5 minutes

def _cb_tripped() -> bool:
    """Return True if circuit breaker is open (Qwen recently failed with 5xx)."""
    return (_time.time() - _cb_failure_time) < _CB_COOLDOWN

def _cb_record_failure():
    global _cb_failure_time
    _cb_failure_time = _time.time()
    logger.warning(f"Qwen circuit breaker opened — skipping for {_CB_COOLDOWN}s")

def _cb_reset():
    global _cb_failure_time
    _cb_failure_time = 0.0

# ─── Settings keys ────────────────────────────────────────────────────────────

QWEN_EMAIL_KEY = "qwen_email"
QWEN_TOKEN_KEY = "qwen_token"
QWEN_MODEL_KEY = "qwen_model"

# ─── macOS Keychain ───────────────────────────────────────────────────────────

_KEYCHAIN_SERVICE = "DSR-RIECT-Qwen"


def _keychain_store(email: str, password: str):
    """Save Qwen password in macOS Keychain (update if exists)."""
    try:
        subprocess.run(
            ["security", "add-generic-password",
             "-s", _KEYCHAIN_SERVICE, "-a", email, "-w", password, "-U"],
            capture_output=True, check=False,
        )
        logger.info(f"Qwen: password stored in macOS Keychain for {email}")
    except Exception as e:
        logger.warning(f"Qwen: Keychain store failed: {e}")


def _keychain_get(email: str) -> str:
    """Retrieve Qwen password from macOS Keychain. Returns '' if not found."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password",
             "-s", _KEYCHAIN_SERVICE, "-a", email, "-w"],
            capture_output=True, text=True, check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _keychain_delete(email: str):
    """Remove Qwen credentials from macOS Keychain on disconnect."""
    try:
        subprocess.run(
            ["security", "delete-generic-password",
             "-s", _KEYCHAIN_SERVICE, "-a", email],
            capture_output=True, check=False,
        )
    except Exception:
        pass


def _keychain_find_email() -> str:
    """
    Find any stored Qwen email in Keychain (fallback when SQLite lost the email).
    Uses `security find-generic-password` without -a to get first match.
    Returns email string or '' if nothing found.
    """
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", _KEYCHAIN_SERVICE],
            capture_output=True, text=True, check=False,
        )
        for line in result.stderr.splitlines():
            if '"acct"' in line and "<blob>" in line:
                # line format:  "acct"<blob>="user@example.com"
                email = line.split('=')[-1].strip().strip('"')
                return email
        return ""
    except Exception:
        return ""


def has_saved_credentials() -> bool:
    """True if Keychain has a password for the stored email (enables auto-relogin)."""
    email = get_email() or _keychain_find_email()
    return bool(email and _keychain_get(email))


# ─── Auth ─────────────────────────────────────────────────────────────────────

async def login(email: str, password: str) -> dict:
    """
    Authenticate with chat.qwen.ai using email + password.
    On success: token saved to SQLite, password saved to macOS Keychain.
    Returns: {success, token, email, message}
    """
    payload = {"email": email, "password": password}

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        try:
            resp = await client.post(_LOGIN, json=payload, headers=_HEADERS_BASE)
            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}

            inner = data.get("data", {}) if isinstance(data.get("data"), dict) else {}
            token = (
                inner.get("token")
                or inner.get("access_token")
                or data.get("token")
                or data.get("access_token")
            )

            if resp.status_code == 200 and data.get("success") and token:
                set_setting(QWEN_EMAIL_KEY, email)
                set_setting(QWEN_TOKEN_KEY, token)
                _keychain_store(email, password)   # persist for auto-relogin
                logger.info(f"Qwen login success for {email}")
                return {"success": True, "token": token, "email": email,
                        "message": f"Connected as {email}"}

            code = (inner.get("code") or "").lower()
            raw  = inner.get("details") or inner.get("message") or data.get("message") or ""
            _CODE_MAP = {
                "not found":           "Account not found — check email or register at chat.qwen.ai",
                "invalid password":    "Incorrect password — please try again",
                "wrong password":      "Incorrect password — please try again",
                "invalid credentials": "Invalid email or password",
                "unauthorized":        "Unauthorised — check credentials",
                "too many requests":   "Too many login attempts — wait a moment and retry",
            }
            msg = _CODE_MAP.get(code) or _CODE_MAP.get(raw.lower()) or raw or "Login failed — check email and password"
            logger.warning(f"Qwen login failed for {email}: code={code!r} raw={raw!r}")
            return {"success": False, "message": msg}

        except httpx.ConnectError:
            return {"success": False, "message": "Cannot reach chat.qwen.ai — check network connection."}
        except Exception as e:
            logger.error(f"Qwen login error: {e}")
            return {"success": False, "message": str(e)}


async def auto_relogin() -> bool:
    """
    Silently re-authenticate using credentials in macOS Keychain.
    Called when: heartbeat fails, 401 received, or token missing on startup.
    Returns True if relogin succeeded and new token is stored.
    """
    email = get_email()
    if not email:
        # Try to find any saved Qwen credentials in Keychain
        email = _keychain_find_email()
        if not email:
            return False
    password = _keychain_get(email)
    if not password:
        logger.info("Qwen auto-relogin: no Keychain password — manual token re-paste needed")
        return False

    logger.info(f"Qwen auto-relogin: authenticating as {email}")
    result = await login(email, password)
    if result["success"]:
        logger.info("Qwen auto-relogin: success — session restored")
        return True
    logger.warning(f"Qwen auto-relogin failed: {result['message']}")
    return False


async def verify_token(token: str) -> bool:
    """Verify an existing session token is still valid."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _VERIFY,
                headers={**_HEADERS_BASE, "Authorization": f"Bearer {token}"},
            )
            return resp.status_code == 200
    except Exception:
        return False


async def keep_alive() -> bool:
    """
    Ping Qwen session. On failure: auto-relogin from Keychain.
    If token is missing entirely: try auto-relogin immediately.
    Returns True if session is active after the call.
    """
    token = get_token()
    if not token:
        # Token missing — try Keychain restore immediately
        return await auto_relogin()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                _VERIFY,
                headers={**_HEADERS_BASE, "Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 200:
                logger.debug("Qwen keep-alive OK")
                return True
            logger.warning(f"Qwen keep-alive: HTTP {resp.status_code} — attempting auto-relogin")
            return await auto_relogin()
    except Exception as e:
        logger.warning(f"Qwen keep-alive error: {e}")
        return False


# ─── Heartbeat ────────────────────────────────────────────────────────────────

_heartbeat_task: asyncio.Task = None
_HEARTBEAT_INTERVAL = 300   # 5 minutes


def start_heartbeat():
    """
    Start background keep-alive loop (idempotent — safe to call multiple times).
    Pings Qwen every 5 min; auto-relogins from Keychain if session expires.
    Call after every successful login/token-save and on server startup.
    """
    global _heartbeat_task
    if _heartbeat_task and not _heartbeat_task.done():
        return  # already running

    async def _loop():
        logger.info("Qwen heartbeat started — pinging every 5 min")
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL)
            # Only stop if user explicitly disconnected (no email = intentional logout)
            if not get_email():
                logger.info("Qwen heartbeat: account disconnected — stopped")
                break
            # keep_alive handles: valid token, expired token, missing token — all via Keychain
            await keep_alive()

    try:
        loop = asyncio.get_event_loop()
        _heartbeat_task = loop.create_task(_loop())
    except RuntimeError:
        pass   # no event loop yet — will be started from startup()


# ─── Settings helpers ─────────────────────────────────────────────────────────

def get_token() -> str:
    return get_setting(QWEN_TOKEN_KEY, "")


def get_email() -> str:
    return get_setting(QWEN_EMAIL_KEY, "")


def get_model() -> str:
    return get_setting(QWEN_MODEL_KEY, QWEN_DEFAULT_MODEL)


def set_model(model_id: str):
    if model_id not in QWEN_MODELS:
        raise ValueError(f"Unknown Qwen model: {model_id}. Valid: {list(QWEN_MODELS)}")
    set_setting(QWEN_MODEL_KEY, model_id)


def is_configured() -> bool:
    return bool(get_token() and get_email())


def disconnect():
    email = get_email()
    set_setting(QWEN_TOKEN_KEY, "")
    set_setting(QWEN_EMAIL_KEY, "")
    if email:
        _keychain_delete(email)


async def save_token(token: str, email: str = "") -> dict:
    """
    Store a session token for Google/SSO-linked accounts (no Keychain — no password available).
    Returns: {success, email, message}
    """
    token = token.strip()
    if not token or len(token) < 20:
        return {"success": False, "message": "Token too short — copy the full value from Local Storage"}

    try:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get(
                _VERIFY,
                headers={**_HEADERS_BASE, "Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 200:
                try:
                    data  = resp.json()
                    inner = data.get("data", data) if isinstance(data, dict) else {}
                    resolved_email = (
                        inner.get("email") or inner.get("username")
                        or email or "Qwen Account"
                    )
                except Exception:
                    resolved_email = email or "Qwen Account"
                set_setting(QWEN_TOKEN_KEY, token)
                set_setting(QWEN_EMAIL_KEY, resolved_email)
                logger.info(f"Qwen token saved for {resolved_email}")
                return {"success": True, "email": resolved_email,
                        "message": f"Connected as {resolved_email}"}
            return {"success": False,
                    "message": f"Token rejected (HTTP {resp.status_code}) — ensure you are logged in to chat.qwen.ai and copied the full 'token' value from Application → Local Storage"}
    except httpx.ConnectError:
        return {"success": False, "message": "Cannot reach chat.qwen.ai — check network"}
    except Exception as e:
        return {"success": False, "message": f"Verification error: {e}"}


# ─── Generation ───────────────────────────────────────────────────────────────

def _auth_headers(token: str) -> dict:
    return {**_HEADERS_BASE, "Authorization": f"Bearer {token}"}


def _build_payload(system_prompt: str, user_prompt: str,
                   model: str, max_tokens: int, temperature: float,
                   stream: bool = False) -> dict:
    return {
        "model":       model,
        "messages":    [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "max_tokens":  max_tokens,
        "temperature": temperature,
        "stream":      stream,
    }


async def generate(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
    model: str = None,
) -> str:
    """Generate response via chat.qwen.ai session token (non-streaming).
    On 401: auto-relogins from Keychain and retries once.
    Circuit breaker: skips immediately for 5 min after a 5xx failure."""
    if _cb_tripped():
        raise RuntimeError("Qwen circuit breaker open — skipping to fallback")
    token = get_token()
    if not token:
        raise ValueError("Qwen not connected — please login in Settings.")

    model = model or get_model()
    payload = _build_payload(system_prompt, user_prompt, model, max_tokens, temperature, False)

    async with httpx.AsyncClient(timeout=15) as client:  # 15s max — fast fail
        resp = await client.post(_CHAT, json=payload, headers=_auth_headers(token))

        if resp.status_code == 401:
            logger.warning("Qwen 401 on generate — attempting auto-relogin")
            if await auto_relogin():
                token = get_token()
                resp = await client.post(_CHAT, json=payload, headers=_auth_headers(token))
            else:
                raise ValueError("Qwen session expired — re-paste token in Settings.")

        if resp.status_code >= 500:
            _cb_record_failure()
        resp.raise_for_status()
        data = resp.json()
        content = (
            data.get("choices", [{}])[0].get("message", {}).get("content")
            or data.get("output", {}).get("text")
            or data.get("result", "")
        )
        if not content:
            raise RuntimeError(f"Empty Qwen response: {data}")
        _cb_reset()
        logger.info(f"Qwen generate OK — model={model}")
        return content


async def generate_stream(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
    model: str = None,
) -> AsyncIterator[str]:
    """Stream response via chat.qwen.ai session token.
    On 401: auto-relogins from Keychain and retries once.
    Circuit breaker: skips immediately for 5 min after a 5xx failure."""
    if _cb_tripped():
        raise RuntimeError("Qwen circuit breaker open — skipping to fallback")
    token = get_token()
    if not token:
        raise ValueError("Qwen not connected — please login in Settings.")

    model = model or get_model()
    payload = _build_payload(system_prompt, user_prompt, model, max_tokens, temperature, True)

    async def _stream_with_token(tok: str):
        async with httpx.AsyncClient(timeout=90) as client:
            async with client.stream(
                "POST", _CHAT, json=payload, headers=_auth_headers(tok)
            ) as resp:
                if resp.status_code == 401:
                    raise ValueError("__401__")
                if resp.status_code >= 500:
                    _cb_record_failure()
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line or not line.startswith("data:"):
                        continue
                    chunk = line[5:].strip()
                    if chunk == "[DONE]":
                        return
                    try:
                        data = json.loads(chunk)
                        delta = (
                            data.get("choices", [{}])[0].get("delta", {}).get("content")
                            or data.get("output", {}).get("text", "")
                        )
                        if delta:
                            yield delta
                    except Exception:
                        continue

    try:
        async for chunk in _stream_with_token(token):
            yield chunk
        _cb_reset()
    except ValueError as e:
        if "__401__" in str(e):
            logger.warning("Qwen 401 on stream — attempting auto-relogin")
            if await auto_relogin():
                async for chunk in _stream_with_token(get_token()):
                    yield chunk
                _cb_reset()
            else:
                raise ValueError("Qwen session expired — re-paste token in Settings.")
        else:
            raise
