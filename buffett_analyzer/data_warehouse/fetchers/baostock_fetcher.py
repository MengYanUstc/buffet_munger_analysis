"""
BaoStock 数据获取封装
获取近7年日K线数据（含PE、PB、PS），并计算最新值的历史分位。
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

    def fetch_valuation(self, stock_code: str) -> Dict[str, Any]:
        """
        获取指定股票近7年估值数据，返回：
        {
            "valuation_df": pd.DataFrame,  # 原始日K数据
            "latest": {
                "trade_date": str,
                "close_price": float,
                "pe_ttm": float,
                "pb": float,
                "ps_ttm": float,
                "pe_percentile_5y": float,
                "pb_percentile_5y": float,
                "ps_percentile_5y": float
            }
        }
        """
        self._ensure_login()
        code = self._to_baostock_code(stock_code)
        end_date = datetime.date.today().strftime('%Y-%m-%d')
        start_date = (datetime.date.today() - datetime.timedelta(days=7*365+30)).strftime('%Y-%m-%d')

        fields = "date,code,close,peTTM,pbMRQ,psTTM"
        rs = bs.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # 后复权
        )

        if rs.error_code != '0':
            raise RuntimeError(f"BaoStock 查询失败 ({stock_code}): {rs.error_msg}")

        data_list = []
        while rs.error_code == '0' and rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return {"valuation_df": pd.DataFrame(), "latest": {}}

        df = pd.DataFrame(data_list, columns=rs.fields)
        for col in ['close', 'peTTM', 'pbMRQ', 'psTTM']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 过滤无效值
        df = df.replace('', np.nan)
        df = df.dropna(subset=['date'])

        # 计算分位数
        def _percentile(series: pd.Series, current_val: float) -> Optional[float]:
            clean = series.dropna()
            clean = clean[clean > 0]
            if len(clean) == 0 or pd.isna(current_val):
                return None
            # 计算当前值在历史分布中的百分位 (0-100)
            return float(np.sum(clean <= current_val) / len(clean) * 100)

        latest_row = df.iloc[-1]
        pe_val = latest_row.get('peTTM')
        pb_val = latest_row.get('pbMRQ')
        ps_val = latest_row.get('psTTM')

        result = {
            "valuation_df": df,
            "latest": {
                "trade_date": str(latest_row.get('date', '')),
                "close_price": float(latest_row.get('close')) if pd.notna(latest_row.get('close')) else None,
                "pe_ttm": float(pe_val) if pd.notna(pe_val) else None,
                "pb": float(pb_val) if pd.notna(pb_val) else None,
                "ps_ttm": float(ps_val) if pd.notna(ps_val) else None,
                "pe_percentile_5y": _percentile(df['peTTM'], pe_val),
                "pb_percentile_5y": _percentile(df['pbMRQ'], pb_val),
                "ps_percentile_5y": _percentile(df['psTTM'], ps_val),
            }
        }
        return result

    def __del__(self):
        self.logout()
