"""
DSR|RIECT — Settings Store
CRUD for ClickHouse config + LLM keys in SQLite settings table
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from db import get_connection

logger = logging.getLogger(__name__)

# Settings keys
CH_HOST_KEY = "clickhouse_host"
CH_PORT_KEY = "clickhouse_port"
CH_USER_KEY = "clickhouse_user"
CH_PASS_KEY = "clickhouse_password"
CH_SECURE_KEY = "clickhouse_secure"
CH_SCHEMAS_KEY = "clickhouse_schemas"

LLM_DEFAULT_KEY = "llm_default"
LLM_CLAUDE_KEY = "llm_claude_key"
LLM_GEMINI_KEY = "llm_gemini_key"
LLM_OPENAI_KEY = "llm_openai_key"
LLM_QWEN_EMAIL_KEY = "qwen_email"
LLM_QWEN_TOKEN_KEY = "qwen_token"
LLM_QWEN_MODEL_KEY = "qwen_model"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def set_setting(key: str, value: str):
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default
    finally:
        conn.close()


def _normalise_host(host: str, port: int, secure: bool) -> tuple[str, int, bool]:
    """Strip protocol/path from a URL-style host input."""
    from urllib.parse import urlparse
    h = (host or "").strip()
    if h.startswith("http://") or h.startswith("https://"):
        parsed = urlparse(h)
        scheme  = parsed.scheme
        hostname = parsed.hostname or h
        secure   = scheme == "https"
        port     = parsed.port or (443 if secure else 80)
        return hostname, port, secure
    return h, port, secure


def save_clickhouse_config(host: str, port: int, user: str, password: str,
                            secure: bool = True,
                            schemas: list = None):
    if schemas is None:
        schemas = ["vmart_sales", "customers", "data_science"]
    # Normalise host — strip http://.../path if user pasted a full URL
    host, port, secure = _normalise_host(host, port, secure)
    set_setting(CH_HOST_KEY, host)
    set_setting(CH_PORT_KEY, str(port))
    set_setting(CH_USER_KEY, user)
    set_setting(CH_PASS_KEY, password)
    set_setting(CH_SECURE_KEY, "true" if secure else "false")
    set_setting(CH_SCHEMAS_KEY, json.dumps(schemas))
    # Mark as configured only when user explicitly saved real credentials
    if host and user and password:
        set_setting("clickhouse_configured", "true")
    logger.info(f"ClickHouse config saved: {user}@{host}:{port}")


def is_clickhouse_configured() -> bool:
    """True only after user has explicitly saved valid ClickHouse credentials."""
    # Primary flag (set by save_clickhouse_config)
    if get_setting("clickhouse_configured") == "true":
        return True
    # Fallback: credentials present in settings but flag not yet written
    host = get_setting(CH_HOST_KEY, "")
    user = get_setting(CH_USER_KEY, "")
    password = get_setting(CH_PASS_KEY, "")
    return bool(host and user and password)


def get_clickhouse_config() -> dict:
    from config import CLICKHOUSE_DEFAULTS
    return {
        "host": get_setting(CH_HOST_KEY, CLICKHOUSE_DEFAULTS["host"]),
        "port": int(get_setting(CH_PORT_KEY, str(CLICKHOUSE_DEFAULTS["port"]))),
        "user": get_setting(CH_USER_KEY, CLICKHOUSE_DEFAULTS["user"]),
        "password": get_setting(CH_PASS_KEY, ""),
        "secure": get_setting(CH_SECURE_KEY, "true") == "true",
        "schemas": json.loads(
            get_setting(CH_SCHEMAS_KEY, json.dumps(CLICKHOUSE_DEFAULTS["schemas"]))
        ),
    }


def save_llm_key(provider: str, key: str):
    """Save LLM API key. Provider: claude | gemini | openai"""
    key_map = {
        "claude": LLM_CLAUDE_KEY,
        "gemini": LLM_GEMINI_KEY,
        "openai": LLM_OPENAI_KEY,
    }
    if provider not in key_map:
        raise ValueError(f"Unknown provider: {provider}")
    set_setting(key_map[provider], key)
    logger.info(f"LLM key saved for provider: {provider}")


def get_llm_key(provider: str) -> Optional[str]:
    """
    Return API key for provider.
    Priority: 1) SQLite settings  2) Environment variable
    This means pre-configured env vars (ANTHROPIC_API_KEY etc.) work without
    the user entering anything in the UI.
    """
    import os
    key_map = {
        "claude": LLM_CLAUDE_KEY,
        "gemini": LLM_GEMINI_KEY,
        "openai": LLM_OPENAI_KEY,
    }
    env_map = {
        "claude": ["ANTHROPIC_API_KEY", "CLAUDE_API_KEY"],
        "gemini": ["GOOGLE_API_KEY", "GEMINI_API_KEY"],
        "openai": ["OPENAI_API_KEY"],
    }
    # 1. Check SQLite settings first (user-configured via UI)
    stored = get_setting(key_map.get(provider, ""))
    if stored:
        return stored
    # 2. Fall back to environment variables (system-level config)
    for env_var in env_map.get(provider, []):
        val = os.environ.get(env_var)
        if val:
            return val
    return None


def set_default_llm(model: str):
    set_setting(LLM_DEFAULT_KEY, model)


def get_default_llm() -> str:
    return get_setting(LLM_DEFAULT_KEY, "qwen")


def is_qwen_configured() -> bool:
    from llm.qwen_client import is_configured
    return is_configured()
