"""
BaoStock 数据获取封装
从 stock_daily_prices 缓存日K计算PE/PB/PS历史分位，缓存缺失时通过baostock拉取补充。
"""

import datetime
import numpy as np
import pandas as pd
import baostock as bs
from typing import Dict, Any, Optional


class BaoStockFetcher:
    def __init__(self):
        self._logged_in = False

    def _ensure_login(self):
        if not self._logged_in:
            lg = bs.login()
            if lg.error_code != '0':
                raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")
            self._logged_in = True

    def logout(self):
        if self._logged_in:
            bs.logout()
            self._logged_in = False

    @staticmethod
    def _to_baostock_code(stock_code: str) -> str:
        """转换为 baostock 代码格式 sh.600519 / sz.000001"""
        if stock_code.startswith(('60', '68', '69')):
            return f"sh.{stock_code}"
        elif stock_code.startswith(('00', '30')):
            return f"sz.{stock_code}"
        elif stock_code.startswith(('4', '8')):
            return f"bj.{stock_code}"
        return f"sh.{stock_code}"

    @staticmethod
    def _percentile(series: pd.Series, current_val: float) -> Optional[float]:
        """计算当前值在历史分布中的百分位 (0-100)。"""
        clean = series.dropna()
        clean = clean[clean > 0]
        if len(clean) == 0 or pd.isna(current_val):
            return None
        return float(np.sum(clean <= current_val) / len(clean) * 100)

    @staticmethod
    def _compute_valuation_from_df(df: pd.DataFrame) -> Dict[str, Any]:
        """从日K DataFrame 计算估值指标和历史分位。"""
        if df.empty:
            return {"valuation_df": pd.DataFrame(), "latest": {}}

        # 标准化列名（兼容 stock_daily_prices 和 baostock 原始列名）
        col_map = {}
        for src, dst in [("peTTM", "pe_ttm"), ("pbMRQ", "pb"), ("psTTM", "ps_ttm"),
                         ("date", "trade_date")]:
            if src in df.columns:
                col_map[src] = dst
        if col_map:
            df = df.rename(columns=col_map)

        # 确保数值类型
        for col in ["close", "pe_ttm", "pb", "ps_ttm"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 过滤无效值
        df = df.replace('', np.nan)
        df = df.dropna(subset=["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)

        if df.empty:
            return {"valuation_df": pd.DataFrame(), "latest": {}}

        # 提取每月末一个采样点（5年≈60个点），避免日线数据过度稀释
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["year_month"] = df["trade_date"].dt.to_period("M")
        monthly_df = df.groupby("year_month").tail(1).reset_index(drop=True)

        # 取最近一条有有效估值数据的记录作为 latest（避免当天数据延迟导致PE为None）
        valid_df = df.dropna(subset=["pe_ttm", "pb", "ps_ttm"])
        if valid_df.empty:
            latest_row = df.iloc[-1]
        else:
            latest_row = valid_df.iloc[-1]
        pe_val = latest_row.get("pe_ttm")
        pb_val = latest_row.get("pb")
        ps_val = latest_row.get("ps_ttm")

        return {
            "valuation_df": monthly_df,
            "latest": {
                "trade_date": str(latest_row.get("trade_date", ""))[:10],
                "close_price": float(latest_row.get("close")) if pd.notna(latest_row.get("close")) else None,
                "pe_ttm": float(pe_val) if pd.notna(pe_val) else None,
                "pb": float(pb_val) if pd.notna(pb_val) else None,
                "ps_ttm": float(ps_val) if pd.notna(ps_val) else None,
                "pe_percentile_5y": BaoStockFetcher._percentile(monthly_df["pe_ttm"], pe_val),
                "pb_percentile_5y": BaoStockFetcher._percentile(monthly_df["pb"], pb_val),
                "ps_percentile_5y": BaoStockFetcher._percentile(monthly_df["ps_ttm"], ps_val),
            }
        }

    def _fetch_from_baostock(self, stock_code: str) -> pd.DataFrame:
        """通过 baostock API 拉取近5年日K（含PE/PB/PS）。"""
        self._ensure_login()
        code = self._to_baostock_code(stock_code)
        end_date = datetime.date.today().strftime('%Y-%m-%d')
        start_date = (datetime.date.today() - datetime.timedelta(days=5*365+30)).strftime('%Y-%m-%d')

        fields = "date,code,close,peTTM,pbMRQ,psTTM"
        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",  # 前复权（与 price_fetcher 统一）
        )

        if rs.error_code != '0':
            raise RuntimeError(f"BaoStock 查询失败 ({stock_code}): {rs.error_msg}")

        data_list = []
        while rs.error_code == '0' and rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)
        for col in ['close', 'peTTM', 'pbMRQ', 'psTTM']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.replace('', np.nan)
        df = df.dropna(subset=['date'])
        # 标准化列名，与 stock_daily_prices 对齐
        df = df.rename(columns={
            "date": "trade_date",
            "peTTM": "pe_ttm",
            "pbMRQ": "pb",
            "psTTM": "ps_ttm",
        })
        return df

    def fetch_valuation(self, stock_code: str, daily_df: pd.DataFrame = None) -> Dict[str, Any]:
        """
        获取指定股票估值数据。
        优先使用传入的 daily_df（来自 stock_daily_prices 缓存），
        缓存缺失或缺少PE/PB/PS时，通过 baostock API 拉取补充。

        Args:
            stock_code: 股票代码
            daily_df: 可选，stock_daily_prices 缓存的日K DataFrame

        Returns:
            {
                "valuation_df": pd.DataFrame,  # 月末采样点
                "latest": {trade_date, close_price, pe_ttm, pb, ps_ttm,
                           pe_percentile_5y, pb_percentile_5y, ps_percentile_5y},
                "daily_df": pd.DataFrame,       # 完整的日K数据（供写入缓存）
            }
        """
        # 1. 尝试使用缓存数据
        if daily_df is not None and not daily_df.empty:
            # 检查是否有有效的PE/PB/PS数据
            has_pe = "pe_ttm" in daily_df.columns and daily_df["pe_ttm"].notna().any()
            has_pb = "pb" in daily_df.columns and daily_df["pb"].notna().any()
            if has_pe and has_pb:
                result = self._compute_valuation_from_df(daily_df.copy())
                result["daily_df"] = daily_df
                return result

        # 2. 缓存缺失或数据不完整，通过 baostock 拉取
        df = self._fetch_from_baostock(stock_code)
        if df.empty:
            return {"valuation_df": pd.DataFrame(), "latest": {}, "daily_df": pd.DataFrame()}

        result = self._compute_valuation_from_df(df)
        result["daily_df"] = df
        return result

    def __del__(self):
        self.logout()
