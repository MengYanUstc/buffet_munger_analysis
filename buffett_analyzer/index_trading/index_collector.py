# -*- coding: utf-8 -*-
"""
指数数据收集器（量化交易专用）
与报告流程完全独立，用于获取主要A股指数的日K/周K数据，
后续用于开发指数点位买卖判断策略。

数据来源：baostock（稳定、不受代理影响）
"""

import datetime
from typing import Dict, Any, List
import pandas as pd
import baostock as bs

from ..data_warehouse.database import Database
from ..data_warehouse.cache_manager import CacheManager


# 核心关注指数列表
INDEX_CONFIG: List[Dict[str, str]] = [
    {"code": "sh.000001", "name": "上证指数"},
    {"code": "sz.399001", "name": "深证成指"},
    {"code": "sh.000300", "name": "沪深300"},
    {"code": "sh.000016", "name": "上证50"},
    {"code": "sh.000905", "name": "中证500"},
    {"code": "sz.399006", "name": "创业板指"},
]

INDEX_CODES = [cfg["code"] for cfg in INDEX_CONFIG]
INDEX_NAME_MAP = {cfg["code"]: cfg["name"] for cfg in INDEX_CONFIG}


class IndexCollector:
    """
    指数K线数据收集器。
    支持全量获取和增量更新，数据存入独立的 index_daily_prices / index_weekly_prices 表。
    """

    def __init__(self, db_path: str = None):
        self.db = Database(db_path)
        self.cache = CacheManager(self.db)
        self._logged_in = False

    # ------------------------------------------------------------------
    # BaoStock 会话管理
    # ------------------------------------------------------------------
    def _ensure_login(self):
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")
            self._logged_in = True

    def logout(self):
        if self._logged_in:
            bs.logout()
            self._logged_in = False

    def __del__(self):
        self.logout()

    # ------------------------------------------------------------------
    # 核心获取方法
    # ------------------------------------------------------------------
    def collect_all(self, daily_years: int = 5, weekly_years: int = 5) -> Dict[str, Any]:
        """
        批量获取所有关注指数的日K和周K。

        Returns:
            {
                "sh.000001": {
                    "daily": {"df": pd.DataFrame, "source": "cache|incremental|full"},
                    "weekly": {"df": pd.DataFrame, "source": "cache|incremental|full"},
                },
                ...
            }
        """
        results = {}
        self._ensure_login()
        for cfg in INDEX_CONFIG:
            code = cfg["code"]
            name = cfg["name"]
            print(f"[IndexCollector] 收集 {name}({code})...")
            results[code] = {
                "daily": self._collect_single(code, "index_daily_prices", "d", daily_years),
                "weekly": self._collect_single(code, "index_weekly_prices", "w", weekly_years),
            }
        return results

    def collect_single(self, index_code: str, period: str = "daily", years: int = 5) -> Dict[str, Any]:
        """
        获取单个指数的K线数据。

        Args:
            index_code: 指数代码，如 "sh.000300"
            period: "daily" 或 "weekly"
            years: 获取近N年数据
        """
        self._ensure_login()
        table = "index_daily_prices" if period == "daily" else "index_weekly_prices"
        freq = "d" if period == "daily" else "w"
        return self._collect_single(index_code, table, freq, years)

    def _collect_single(
        self, index_code: str, table: str, freq: str, years: int
    ) -> Dict[str, Any]:
        """内部统一收集逻辑，支持增量更新。"""
        max_age = 1 if freq == "d" else 7
        min_records = 200 if freq == "d" else 50

        # 1. 缓存有效且未过期 → 直接返回
        if self.cache.has_index_prices(index_code, table, min_records, max_age):
            df = self.cache.read_index_prices(index_code, table)
            return {"df": df, "source": "cache", "rows": len(df)}

        # 2. 有旧缓存 → 增量更新
        latest_date = self.cache.get_latest_index_date(index_code, table)
        if latest_date is not None:
            start_date = latest_date + datetime.timedelta(days=1)
            today = datetime.date.today()
            if start_date > today:
                df = self.cache.read_index_prices(index_code, table)
                return {"df": df, "source": "cache", "rows": len(df)}

            df_new = self._fetch_from_baostock(index_code, freq, start_date, today)
            if not df_new.empty:
                self.cache.write_index_prices(index_code, table, df_new)
                df_full = self.cache.read_index_prices(index_code, table)
                return {"df": df_full, "source": "incremental", "rows": len(df_full)}
            # 增量为空（如周末），回退到旧缓存
            df = self.cache.read_index_prices(index_code, table)
            return {"df": df, "source": "cache", "rows": len(df)}

        # 3. 无缓存 → 全量拉取
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        df = self._fetch_from_baostock(index_code, freq, start_date, end_date)
        if not df.empty:
            self.cache.write_index_prices(index_code, table, df)
            return {"df": df, "source": "full", "rows": len(df)}
        return {"df": pd.DataFrame(), "source": "failed", "rows": 0}

    def _fetch_from_baostock(
        self, index_code: str, freq: str,
        start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """通过 baostock 拉取指数K线。"""
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        rs = bs.query_history_k_data_plus(
            index_code,
            "date,open,high,low,close,volume,amount",
            start_date=start_str,
            end_date=end_str,
            frequency=freq,
            adjustflag="2",  # 前复权（指数一般不需要，但统一参数）
        )

        if rs.error_code != "0":
            print(f"[IndexCollector] baostock 查询失败 ({index_code}): {rs.error_msg}")
            return pd.DataFrame()

        data = []
        while rs.error_code == "0" and rs.next():
            data.append(rs.get_row_data())

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=rs.fields)
        df = df.rename(columns={"date": "trade_date"})
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["trade_date", "close"])
        return df

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------
    def read_daily(self, index_code: str) -> pd.DataFrame:
        """读取指定指数的日K缓存。"""
        return self.cache.read_index_prices(index_code, "index_daily_prices")

    def read_weekly(self, index_code: str) -> pd.DataFrame:
        """读取指定指数的周K缓存。"""
        return self.cache.read_index_prices(index_code, "index_weekly_prices")

    def get_index_summary(self, index_code: str) -> Dict[str, Any]:
        """获取指定指数的数据摘要。"""
        df_d = self.read_daily(index_code)
        df_w = self.read_weekly(index_code)
        name = INDEX_NAME_MAP.get(index_code, index_code)
        return {
            "index_code": index_code,
            "name": name,
            "daily_rows": len(df_d),
            "weekly_rows": len(df_w),
            "daily_range": f"{df_d['trade_date'].min()} ~ {df_d['trade_date'].max()}" if not df_d.empty else None,
            "weekly_range": f"{df_w['trade_date'].min()} ~ {df_w['trade_date'].max()}" if not df_w.empty else None,
            "latest_close": float(df_d["close"].iloc[-1]) if not df_d.empty else None,
        }
