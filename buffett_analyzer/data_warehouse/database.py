"""
SQLite 数据库管理
"""

import sqlite3
from pathlib import Path
from typing import Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "stock_cache.db"


class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_reports (
                    stock_code TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    roe REAL,
                    roic REAL,
                    revenue REAL,
                    net_profit REAL,
                    deduct_net_profit REAL,
                    parent_net_profit REAL,
                    gross_margin REAL,
                    net_margin REAL,
                    debt_ratio REAL,
                    operating_cash_flow REAL,
                    fcf REAL,
                    capex REAL,
                    updated_at TEXT,
                    PRIMARY KEY (stock_code, report_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS valuation_metrics (
                    stock_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    close_price REAL,
                    pe_ttm REAL,
                    pb REAL,
                    ps_ttm REAL,
                    pe_percentile_5y REAL,
                    pb_percentile_5y REAL,
                    ps_percentile_5y REAL,
                    updated_at TEXT,
                    PRIMARY KEY (stock_code, trade_date)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta_cache (
                    stock_code TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    last_update TEXT,
                    record_count INTEGER DEFAULT 0,
                    PRIMARY KEY (stock_code, table_name)
                )
                """
            )
            conn.commit()
        self._migrate()

    def execute(self, sql: str, parameters=(), many: bool = False):
        with self._connect() as conn:
            if many:
                cursor = conn.executemany(sql, parameters)
            else:
                cursor = conn.execute(sql, parameters)
            conn.commit()
            return cursor

    def fetchall(self, sql: str, parameters=()):
        with self._connect() as conn:
            cursor = conn.execute(sql, parameters)
            return cursor.fetchall()

    def _migrate(self):
        """简单的列迁移：为已存在的数据表增加新列"""
        with self._connect() as conn:
            # financial_reports 表增加 operating_cash_flow 列
            cols = [r[1] for r in conn.execute("PRAGMA table_info(financial_reports)")]
            if "operating_cash_flow" not in cols:
                conn.execute("ALTER TABLE financial_reports ADD COLUMN operating_cash_flow REAL")
                conn.commit()

            # valuation_metrics 表增加行业估值与溯源字段
            v_cols = [r[1] for r in conn.execute("PRAGMA table_info(valuation_metrics)")]
            new_valuation_cols = {
                "industry_pe": "REAL",
                "industry_pb": "REAL",
                "industry_ps": "REAL",
                "pe_vs_industry": "REAL",
                "pb_vs_industry": "REAL",
                "ps_vs_industry": "REAL",
                "data_source": "TEXT",
                "note": "TEXT",
            }
            for col_name, col_type in new_valuation_cols.items():
                if col_name not in v_cols:
                    conn.execute(f"ALTER TABLE valuation_metrics ADD COLUMN {col_name} {col_type}")
                    conn.commit()

            # 新增 enrichment_log 表
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS valuation_enrichment_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT,
                    field_name TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    source TEXT,
                    filled_at TEXT
                )
                """
            )
            conn.commit()

    def fetchone(self, sql: str, parameters=()):
        with self._connect() as conn:
            cursor = conn.execute(sql, parameters)
            return cursor.fetchone()
