"""
缓存管理器：负责 SQLite 的读写、缓存命中检查
"""

import datetime
from typing import List, Optional, Dict, Any
import pandas as pd
from .database import Database



class CacheManager:
    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------------
    # 财务数据缓存
    # ------------------------------------------------------------------
    def has_seven_year_financials(self, stock_code: str) -> bool:
        """检查数据库中是否已有该股票近7年的完整年报数据。"""
        row = self.db.fetchone(
            "SELECT record_count FROM meta_cache WHERE stock_code=? AND table_name='financial_reports'",
            (stock_code,)
        )
        if row and row["record_count"] >= 7:
            # 进一步检查是否包含近7个年报日期（允许当年年报未出的情况）
            current_year = datetime.date.today().year
            expected_years = [f"{y}-12-31" for y in range(current_year - 7, current_year)]
            rows = self.db.fetchall(
                "SELECT report_date FROM financial_reports WHERE stock_code=?",
                (stock_code,)
            )
            cached_dates = {r["report_date"] for r in rows}
            # 至少要有7条且包含最近6年的数据
            return len(cached_dates) >= 7
        return False

    def read_financial_reports(self, stock_code: str) -> pd.DataFrame:
        rows = self.db.fetchall(
            """
            SELECT report_date, roe, roic, revenue, net_profit,
                   deduct_net_profit, parent_net_profit, gross_margin,
                   net_margin, debt_ratio, operating_cash_flow, fcf, capex
            FROM financial_reports
            WHERE stock_code=?
            ORDER BY report_date
            """,
            (stock_code,)
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        return df

    def write_financial_reports(self, stock_code: str, df: pd.DataFrame):
        if df.empty:
            return
        now = datetime.datetime.now().isoformat()
        records = []
        for _, row in df.iterrows():
            records.append((
                stock_code,
                str(row.get('report_date', ''))[:10],
                float(row['roe']) if pd.notna(row.get('roe')) else None,
                float(row['roic']) if pd.notna(row.get('roic')) else None,
                float(row['revenue']) if pd.notna(row.get('revenue')) else None,
                float(row['net_profit']) if pd.notna(row.get('net_profit')) else None,
                float(row['deduct_net_profit']) if pd.notna(row.get('deduct_net_profit')) else None,
                float(row['parent_net_profit']) if pd.notna(row.get('parent_net_profit')) else None,
                float(row['gross_margin']) if pd.notna(row.get('gross_margin')) else None,
                float(row['net_margin']) if pd.notna(row.get('net_margin')) else None,
                float(row['debt_ratio']) if pd.notna(row.get('debt_ratio')) else None,
                float(row['operating_cash_flow']) if pd.notna(row.get('operating_cash_flow')) else None,
                float(row['fcf']) if pd.notna(row.get('fcf')) else None,
                float(row['capex']) if pd.notna(row.get('capex')) else None,
                now
            ))

        self.db.execute(
            """
            INSERT OR REPLACE INTO financial_reports
            (stock_code, report_date, roe, roic, revenue, net_profit,
             deduct_net_profit, parent_net_profit, gross_margin, net_margin,
             debt_ratio, operating_cash_flow, fcf, capex, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            # 使用 executemany
            many=True,
            parameters=records
        )
        # 注意：sqlite3 的 execute 不直接支持 many 参数，需要改 Database.execute

    def update_meta(self, stock_code: str, table_name: str, count: int):
        now = datetime.datetime.now().isoformat()
        self.db.execute(
            """
            INSERT OR REPLACE INTO meta_cache
            (stock_code, table_name, last_update, record_count)
            VALUES (?, ?, ?, ?)
            """,
            (stock_code, table_name, now, count)
        )

    # ------------------------------------------------------------------
    # 估值数据缓存
    # ------------------------------------------------------------------
    def has_latest_valuation(self, stock_code: str) -> bool:
        """检查是否已有最近交易日的估值数据（以当天日期为准）。"""
        row = self.db.fetchone(
            "SELECT trade_date FROM valuation_metrics WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (stock_code,)
        )
        if not row:
            return False
        latest_date = row["trade_date"]
        today = datetime.date.today().strftime('%Y-%m-%d')
        # 简单策略：如果缓存的最新日期就是今天或昨天，认为有效（股市非每天交易）
        return latest_date == today or (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') == latest_date

    def read_valuation(self, stock_code: str) -> Optional[Dict[str, Any]]:
        row = self.db.fetchone(
            """
            SELECT trade_date, close_price, pe_ttm, pb, ps_ttm,
                   pe_percentile_5y, pb_percentile_5y, ps_percentile_5y,
                   industry_pe, industry_pb, industry_ps,
                   pe_vs_industry, pb_vs_industry, ps_vs_industry,
                   data_source, note
            FROM valuation_metrics
            WHERE stock_code=?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (stock_code,)
        )
        if not row:
            return None
        return {k: row[k] for k in row.keys()}

    def write_valuation(self, stock_code: str, data: Dict[str, Any]):
        if not data or not data.get("trade_date"):
            return
        now = datetime.datetime.now().isoformat()
        self.db.execute(
            """
            INSERT OR REPLACE INTO valuation_metrics
            (stock_code, trade_date, close_price, pe_ttm, pb, ps_ttm,
             pe_percentile_5y, pb_percentile_5y, ps_percentile_5y,
             industry_pe, industry_pb, industry_ps,
             pe_vs_industry, pb_vs_industry, ps_vs_industry,
             data_source, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stock_code,
                data["trade_date"],
                data.get("close_price"),
                data.get("pe_ttm"),
                data.get("pb"),
                data.get("ps_ttm"),
                data.get("pe_percentile_5y"),
                data.get("pb_percentile_5y"),
                data.get("ps_percentile_5y"),
                data.get("industry_pe"),
                data.get("industry_pb"),
                data.get("industry_ps"),
                data.get("pe_vs_industry"),
                data.get("pb_vs_industry"),
                data.get("ps_vs_industry"),
                data.get("data_source"),
                data.get("note"),
                now
            )
        )

    def log_enrichment(self, stock_code: str, field_name: str, old_value: Any, new_value: Any, source: str):
        now = datetime.datetime.now().isoformat()
        self.db.execute(
            """
            INSERT INTO valuation_enrichment_log
            (stock_code, field_name, old_value, new_value, source, filled_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (stock_code, field_name, str(old_value) if old_value is not None else None,
             str(new_value) if new_value is not None else None, source, now)
        )

    # ------------------------------------------------------------------
    # 定性分析结果缓存
    # ------------------------------------------------------------------
    def has_fresh_qualitative(self, stock_code: str, analysis_type: str, hours: int = 24) -> bool:
        """检查定性分析结果是否在指定时间内。"""
        row = self.db.fetchone(
            """
            SELECT created_at FROM qualitative_results
            WHERE stock_code=? AND analysis_type=?
            AND created_at > datetime('now', '-{} hours')
            """.format(hours),
            (stock_code, analysis_type)
        )
        return row is not None

    def read_qualitative_result(self, stock_code: str, analysis_type: str) -> Optional[Dict[str, Any]]:
        """读取定性分析结果。"""
        row = self.db.fetchone(
            "SELECT result_json FROM qualitative_results WHERE stock_code=? AND analysis_type=?",
            (stock_code, analysis_type)
        )
        if not row or not row["result_json"]:
            return None
        import json
        try:
            return json.loads(row["result_json"])
        except json.JSONDecodeError:
            return None

    def write_qualitative_result(self, stock_code: str, analysis_type: str, result: Dict[str, Any]):
        """写入定性分析结果。"""
        import json
        now = datetime.datetime.now().isoformat()
        self.db.execute(
            """
            INSERT OR REPLACE INTO qualitative_results
            (stock_code, analysis_type, result_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (stock_code, analysis_type, json.dumps(result, ensure_ascii=False), now)
        )
