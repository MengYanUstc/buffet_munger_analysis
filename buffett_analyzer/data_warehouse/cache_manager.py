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
            # 金额字段统一转换为万元存储（roe/roic/gross_margin/net_margin/debt_ratio 为比率，不转换）
            records.append((
                stock_code,
                str(row.get('report_date', ''))[:10],
                float(row['roe']) if pd.notna(row.get('roe')) else None,
                float(row['roic']) if pd.notna(row.get('roic')) else None,
                float(row['revenue']) / 10000.0 if pd.notna(row.get('revenue')) else None,
                float(row['net_profit']) / 10000.0 if pd.notna(row.get('net_profit')) else None,
                float(row['deduct_net_profit']) / 10000.0 if pd.notna(row.get('deduct_net_profit')) else None,
                float(row['parent_net_profit']) / 10000.0 if pd.notna(row.get('parent_net_profit')) else None,
                float(row['gross_margin']) if pd.notna(row.get('gross_margin')) else None,
                float(row['net_margin']) if pd.notna(row.get('net_margin')) else None,
                float(row['debt_ratio']) if pd.notna(row.get('debt_ratio')) else None,
                float(row['operating_cash_flow']) / 10000.0 if pd.notna(row.get('operating_cash_flow')) else None,
                float(row['fcf']) / 10000.0 if pd.notna(row.get('fcf')) else None,
                float(row['capex']) / 10000.0 if pd.notna(row.get('capex')) else None,
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
                   total_share,
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
             total_share,
             industry_pe, industry_pb, industry_ps,
             pe_vs_industry, pb_vs_industry, ps_vs_industry,
             data_source, note, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                data.get("total_share"),
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

    # ------------------------------------------------------------------
    # 股价数据缓存
    # ------------------------------------------------------------------
    def has_price_data(self, stock_code: str, table: str, min_records: int = 1, max_age_days: int = None) -> bool:
        """检查是否已有指定数量的股价数据，并可选检查数据是否过期。
        table: 'stock_daily_prices' 或 'stock_weekly_prices'
        max_age_days: 如果指定，检查最新数据日期距离今天是否超过指定天数
        """
        row = self.db.fetchone(
            f"SELECT COUNT(*) as cnt FROM {table} WHERE stock_code=?",
            (stock_code,)
        )
        if row is None or row["cnt"] < min_records:
            return False

        if max_age_days is not None:
            latest = self.get_latest_price_date(stock_code, table)
            if latest is None:
                return False
            age = (datetime.date.today() - latest).days
            if age >= max_age_days:
                return False

        return True

    def get_latest_price_date(self, stock_code: str, table: str) -> Optional[datetime.date]:
        """获取某股票某价格表中的最新日期。"""
        row = self.db.fetchone(
            f"SELECT MAX(trade_date) as latest FROM {table} WHERE stock_code=?",
            (stock_code,)
        )
        if not row or row["latest"] is None:
            return None
        return datetime.datetime.strptime(str(row["latest"])[:10], "%Y-%m-%d").date()

    def read_prices(self, stock_code: str, table: str) -> pd.DataFrame:
        """读取股价数据。使用 SELECT * 兼容 stock_daily_prices 和 stock_weekly_prices 不同列结构。"""
        rows = self.db.fetchall(
            f"SELECT * FROM {table} WHERE stock_code=? ORDER BY trade_date",
            (stock_code,)
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        # 去掉系统列
        for drop_col in ["updated_at"]:
            if drop_col in df.columns:
                df = df.drop(columns=[drop_col])
        return df

    def write_prices(self, stock_code: str, table: str, df: pd.DataFrame):
        """批量写入股价数据。根据 DataFrame 实际列动态生成 INSERT SQL，兼容不同表结构。"""
        if df.empty:
            return
        now = datetime.datetime.now().isoformat()

        # 基础列（所有价格表共有）
        base_cols = ["trade_date", "open", "high", "low", "close",
                     "volume", "amount", "amplitude", "change_pct", "turnover"]
        # 可选列（stock_daily_prices 特有）
        optional_cols = ["pe_ttm", "pb", "ps_ttm"]

        # 确定实际要写入的列
        data_cols = [c for c in base_cols if c in df.columns]
        data_cols += [c for c in optional_cols if c in df.columns]

        # 构建 INSERT SQL
        all_cols = ["stock_code"] + data_cols + ["updated_at"]
        placeholders = ",".join(["?"] * len(all_cols))
        col_names = ",".join(all_cols)

        records = []
        for _, row in df.iterrows():
            record = [stock_code]
            for col in data_cols:
                val = row.get(col)
                if col == "trade_date":
                    # 日期列保持字符串，不转 float
                    record.append(str(val) if pd.notna(val) else None)
                else:
                    record.append(float(val) if pd.notna(val) else None)
            record.append(now)
            records.append(tuple(record))

        self.db.execute(
            f"""
            INSERT OR REPLACE INTO {table}
            ({col_names})
            VALUES ({placeholders})
            """,
            many=True,
            parameters=records,
        )

    # ------------------------------------------------------------------
    # 月度价格缓存（从日K按月 resample）
    # ------------------------------------------------------------------
    def has_monthly_prices(self, stock_code: str, min_records: int = 6) -> bool:
        row = self.db.fetchone(
            "SELECT COUNT(*) as cnt FROM stock_monthly_prices WHERE stock_code=?",
            (stock_code,)
        )
        return row is not None and row["cnt"] >= min_records

    def read_monthly_prices(self, stock_code: str) -> pd.DataFrame:
        rows = self.db.fetchall(
            """
            SELECT stock_code, trade_date, open, high, low, close, volume, amount
            FROM stock_monthly_prices
            WHERE stock_code=?
            ORDER BY trade_date
            """,
            (stock_code,)
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    def write_monthly_prices(self, stock_code: str, df: pd.DataFrame):
        if df.empty:
            return
        now = datetime.datetime.now().isoformat()
        records = []
        for _, row in df.iterrows():
            records.append((
                stock_code,
                str(row.get("trade_date", ""))[:10],
                float(row["open"]) if pd.notna(row.get("open")) else None,
                float(row["high"]) if pd.notna(row.get("high")) else None,
                float(row["low"]) if pd.notna(row.get("low")) else None,
                float(row["close"]) if pd.notna(row.get("close")) else None,
                float(row["volume"]) if pd.notna(row.get("volume")) else None,
                float(row["amount"]) if pd.notna(row.get("amount")) else None,
                now,
            ))
        self.db.execute(
            """
            INSERT OR REPLACE INTO stock_monthly_prices
            (stock_code, trade_date, open, high, low, close, volume, amount, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            many=True,
            parameters=records,
        )
        self.update_meta(stock_code, "stock_monthly_prices", len(records))

    # ------------------------------------------------------------------
    # 指数价格缓存（量化交易专用，与报告流程独立）
    # ------------------------------------------------------------------
    def has_index_prices(self, index_code: str, table: str, min_records: int = 100, max_age_days: int = 1) -> bool:
        """检查指数价格缓存是否有效且未过期。"""
        row = self.db.fetchone(
            f"SELECT COUNT(*) as cnt, MAX(updated_at) as latest FROM {table} WHERE index_code=?",
            (index_code,)
        )
        if row is None or row["cnt"] < min_records:
            return False
        if row["latest"]:
            latest_dt = datetime.datetime.fromisoformat(row["latest"])
            if (datetime.datetime.now() - latest_dt).days > max_age_days:
                return False
        return True

    def read_index_prices(self, index_code: str, table: str) -> pd.DataFrame:
        rows = self.db.fetchall(
            f"""
            SELECT index_code, trade_date, open, high, low, close, volume, amount
            FROM {table} WHERE index_code=? ORDER BY trade_date
            """,
            (index_code,)
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    def write_index_prices(self, index_code: str, table: str, df: pd.DataFrame):
        if df.empty:
            return
        now = datetime.datetime.now().isoformat()
        records = []
        for _, row in df.iterrows():
            records.append((
                index_code,
                str(row.get("trade_date", ""))[:10],
                float(row["open"]) if pd.notna(row.get("open")) else None,
                float(row["high"]) if pd.notna(row.get("high")) else None,
                float(row["low"]) if pd.notna(row.get("low")) else None,
                float(row["close"]) if pd.notna(row.get("close")) else None,
                float(row["volume"]) if pd.notna(row.get("volume")) else None,
                float(row["amount"]) if pd.notna(row.get("amount")) else None,
                now,
            ))
        self.db.execute(
            f"""
            INSERT OR REPLACE INTO {table}
            (index_code, trade_date, open, high, low, close, volume, amount, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            many=True,
            parameters=records,
        )

    def get_latest_index_date(self, index_code: str, table: str) -> Optional[datetime.date]:
        row = self.db.fetchone(
            f"SELECT MAX(trade_date) as latest FROM {table} WHERE index_code=?",
            (index_code,)
        )
        if row and row["latest"]:
            return datetime.datetime.strptime(str(row["latest"])[:10], "%Y-%m-%d").date()
        return None

    # ------------------------------------------------------------------
    # 季度财务数据缓存
    # ------------------------------------------------------------------
    def has_quarterly_financials(self, stock_code: str, min_records: int = 8) -> bool:
        row = self.db.fetchone(
            "SELECT COUNT(*) as cnt FROM financial_reports_quarterly WHERE stock_code=?",
            (stock_code,)
        )
        return row is not None and row["cnt"] >= min_records

    def read_quarterly_financials(self, stock_code: str) -> pd.DataFrame:
        rows = self.db.fetchall(
            """
            SELECT report_date, roe, roic, revenue, net_profit,
                   deduct_net_profit, parent_net_profit, gross_margin,
                   net_margin, debt_ratio, operating_cash_flow, fcf, capex
            FROM financial_reports_quarterly
            WHERE stock_code=?
            ORDER BY report_date
            """,
            (stock_code,)
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    def write_quarterly_financials(self, stock_code: str, df: pd.DataFrame):
        if df.empty:
            return
        now = datetime.datetime.now().isoformat()
        records = []
        for _, row in df.iterrows():
            records.append((
                stock_code,
                str(row.get("report_date", ""))[:10],
                float(row["roe"]) if pd.notna(row.get("roe")) else None,
                float(row["roic"]) if pd.notna(row.get("roic")) else None,
                float(row["revenue"]) / 10000.0 if pd.notna(row.get("revenue")) else None,
                float(row["net_profit"]) / 10000.0 if pd.notna(row.get("net_profit")) else None,
                float(row["deduct_net_profit"]) / 10000.0 if pd.notna(row.get("deduct_net_profit")) else None,
                float(row["parent_net_profit"]) / 10000.0 if pd.notna(row.get("parent_net_profit")) else None,
                float(row["gross_margin"]) if pd.notna(row.get("gross_margin")) else None,
                float(row["net_margin"]) if pd.notna(row.get("net_margin")) else None,
                float(row["debt_ratio"]) if pd.notna(row.get("debt_ratio")) else None,
                float(row["operating_cash_flow"]) / 10000.0 if pd.notna(row.get("operating_cash_flow")) else None,
                float(row["fcf"]) / 10000.0 if pd.notna(row.get("fcf")) else None,
                float(row["capex"]) / 10000.0 if pd.notna(row.get("capex")) else None,
                now,
            ))
        self.db.execute(
            """
            INSERT OR REPLACE INTO financial_reports_quarterly
            (stock_code, report_date, roe, roic, revenue, net_profit,
             deduct_net_profit, parent_net_profit, gross_margin, net_margin,
             debt_ratio, operating_cash_flow, fcf, capex, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            many=True,
            parameters=records,
        )
        self.update_meta(stock_code, "financial_reports_quarterly", len(records))
