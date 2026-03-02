"""
DSR|RIECT — SQLite Database Initialisation
Creates all required tables on startup
"""

import sqlite3
import logging
from config import SQLITE_DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.executescript("""
            -- User/API settings (ClickHouse + LLM keys)
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT
            );

            -- Chat sessions
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at TEXT,
                title TEXT,
                role TEXT DEFAULT 'HQ'
            );

            -- Chat messages
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                role TEXT,
                content TEXT,
                created_at TEXT,
                FOREIGN KEY (session_id) REFERENCES sessions(session_id)
            );

            -- ClickHouse schema cache
            CREATE TABLE IF NOT EXISTS schema_cache (
                schema_name TEXT,
                table_name TEXT,
                columns_json TEXT,
                cached_at TEXT,
                PRIMARY KEY (schema_name, table_name)
            );

            -- Store master: floor sqft for SPSF calculation
            CREATE TABLE IF NOT EXISTS store_sqft (
                store_id INTEGER PRIMARY KEY,
                store_name TEXT,
                shrtname TEXT,
                sitetype TEXT,
                floor_sqft INTEGER DEFAULT 0,
                city_name TEXT,
                updated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_store_sqft_shrtname ON store_sqft(shrtname);

            -- RIECT Plan: KPI targets (overrides config.py defaults)
            CREATE TABLE IF NOT EXISTS riect_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kpi_type TEXT NOT NULL,
                dimension TEXT DEFAULT 'global',
                dimension_value TEXT DEFAULT '',
                p1_threshold REAL,
                p2_threshold REAL,
                p3_threshold REAL,
                target REAL,
                period TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                updated_at TEXT,
                UNIQUE(kpi_type, dimension, dimension_value)
            );

            -- Product Alignment cache (ClickHouse → SQLite for fast lookups)
            CREATE TABLE IF NOT EXISTS product_alignment (
                icode              TEXT PRIMARY KEY,
                article_code       TEXT,
                article_name       TEXT,
                division           TEXT,
                section            TEXT,
                department         TEXT,
                option_code        TEXT,
                cost_price         REAL,
                mrp                REAL,
                item_description   TEXT,
                supplier_name      TEXT,
                style_or_pattern   TEXT,
                size               TEXT,
                color              TEXT,
                cached_at          TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pa_division   ON product_alignment(division);
            CREATE INDEX IF NOT EXISTS idx_pa_section    ON product_alignment(section);
            CREATE INDEX IF NOT EXISTS idx_pa_dept       ON product_alignment(department);

            -- RIECT alerts
            CREATE TABLE IF NOT EXISTS riect_alerts (
                alert_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                session_id TEXT,
                priority TEXT NOT NULL,
                kpi_type TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                dimension TEXT NOT NULL,
                dimension_value TEXT NOT NULL,
                kpi_value REAL,
                threshold REAL,
                gap REAL,
                status TEXT,
                exception_text TEXT,
                recommended_action TEXT,
                action_owner TEXT,
                response_timeline TEXT,
                expected_impact TEXT,
                resolved INTEGER DEFAULT 0,
                resolved_at TEXT
            );
        """)

        conn.commit()
        logger.info("DSR|RIECT database initialised successfully")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise
    finally:
        conn.close()
