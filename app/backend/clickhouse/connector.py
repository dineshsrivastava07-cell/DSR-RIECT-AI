"""
DSR|RIECT — ClickHouse Connector
Manages connection to ClickHouse using clickhouse-connect library
"""

import logging
from typing import Optional
from urllib.parse import urlparse

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from settings.settings_store import get_clickhouse_config

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def _parse_host_input(host_input: str, port_input: int, secure_input: bool) -> tuple[str, int, bool]:
    """
    Normalise host input — handles full URLs like http://chn1.vmart-tools.com/play.
    Returns (hostname, port, secure).
    """
    h = host_input.strip()
    if h.startswith("http://") or h.startswith("https://"):
        parsed = urlparse(h)
        hostname = parsed.hostname or h
        secure   = parsed.scheme == "https"
        # Use explicit port from URL; fall back to scheme default (443/80)
        port     = parsed.port or (443 if secure else 80)
        return hostname, port, secure
    # Plain hostname — trust the form values
    return h, port_input, secure_input


def get_client(force_refresh: bool = False) -> Client:
    """Get or create ClickHouse client. Uses cached connection unless force_refresh."""
    global _client
    if _client is not None and not force_refresh:
        try:
            _client.ping()
            return _client
        except Exception:
            logger.warning("ClickHouse ping failed, reconnecting...")
            _client = None

    cfg = get_clickhouse_config()
    _client = _connect(cfg)
    return _client


def _connect(cfg: dict) -> Client:
    """Create new ClickHouse client from config dict."""
    host, port, secure = _parse_host_input(
        cfg["host"],
        cfg.get("port", 8443),
        cfg.get("secure", True),
    )
    kwargs = {
        "host":    host,
        "port":    port,
        "username": cfg["user"],
        "password": cfg.get("password", ""),
        "secure":   secure,
        "verify":   False,          # Allow self-signed certs on dev
        "connect_timeout": 15,
        "send_receive_timeout": 60,
    }
    try:
        client = clickhouse_connect.get_client(**kwargs)
        logger.info(f"ClickHouse connected: {cfg['user']}@{host}:{port} secure={secure}")
        return client
    except Exception as e:
        logger.error(f"ClickHouse connection failed: {e}")
        raise


def test_connection(cfg: dict) -> dict:
    """Test a ClickHouse connection and return status + schemas found."""
    raw_host = cfg.get("host", "").strip()
    user     = cfg.get("user", "").strip()

    if not raw_host or not user:
        return {
            "status": "failed",
            "error": "Host and username are required. Please fill in all fields.",
        }

    # Resolve host/port/secure from whatever the user typed
    host, port, secure = _parse_host_input(raw_host, cfg.get("port", 8443), cfg.get("secure", True))

    # Build a normalised config for the actual connection attempt
    norm_cfg = dict(cfg)
    norm_cfg["host"]   = host
    norm_cfg["port"]   = port
    norm_cfg["secure"] = secure

    try:
        client  = _connect(norm_cfg)
        schemas = cfg.get("schemas", ["vmart_sales", "customers", "data_science"])
        table_counts = {}
        total_tables = 0
        for schema in schemas:
            try:
                result = client.query(f"SHOW TABLES FROM `{schema}`")
                count  = len(result.result_rows)
                table_counts[schema] = count
                total_tables += count
            except Exception as e:
                table_counts[schema] = f"Error: {e}"

        return {
            "status":       "connected",
            "host":         host,
            "port":         port,
            "secure":       secure,
            "user":         user,
            "schemas":      table_counts,
            "total_tables": total_tables,
        }
    except Exception as e:
        err = str(e)
        # Human-readable diagnosis
        if "Max retries exceeded" in err or "Connection refused" in err or "Failed to establish" in err:
            hint = f"Cannot reach {host}:{port} — check host/port and network. Try port 80 (HTTP) or 8123."
        elif "Authentication failed" in err or "AUTHENTICATION_FAILED" in err or "password" in err.lower():
            hint = "Authentication failed — wrong username or password."
        elif "SSL" in err or "certificate" in err.lower() or "WRONG_VERSION_NUMBER" in err:
            hint = "SSL/TLS error — uncheck 'Secure (HTTPS)' if your server uses plain HTTP."
        elif "timeout" in err.lower():
            hint = f"Connection timed out to {host}:{port} — server may be unreachable."
        elif "Name or service not known" in err or "getaddrinfo" in err:
            hint = f"Hostname '{host}' could not be resolved — check spelling."
        else:
            hint = err
        return {
            "status":    "failed",
            "error":     hint,
            "raw_error": err,
            "resolved":  {"host": host, "port": port, "secure": secure},
        }


def reset_client():
    """Force client reset (call after settings change)."""
    global _client
    _client = None
    logger.info("ClickHouse client reset")
