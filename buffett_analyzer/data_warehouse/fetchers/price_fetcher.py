"""
股价数据获取器
拉取近N年日K线和近M年周K线（前复权），返回标准化 DataFrame。
"""

import datetime
from typing import Optional
import pandas as pd
import akshare as ak

from ...utils import is_hk_stock


class PriceFetcher:
    """统一股价数据获取入口，自动识别 A股/港股。"""

    def fetch_daily(self, stock_code: str, years: int = 1) -> pd.DataFrame:
        """拉取近 N 年日K线（前复权）。"""
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        return self._fetch(stock_code, period="daily", start_date=start_date, end_date=end_date)

    def fetch_weekly(self, stock_code: str, years: int = 5) -> pd.DataFrame:
        """拉取近 N 年周K线（前复权）。"""
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        return self._fetch(stock_code, period="weekly", start_date=start_date, end_date=end_date)

    def _fetch(
        self,
        stock_code: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> pd.DataFrame:
        """内部统一拉取逻辑。"""
        is_hk = is_hk_stock(stock_code)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        df = pd.DataFrame()
        last_error = None
        for attempt in range(3):
            try:
                if is_hk:
                    df = ak.stock_hk_hist(
                        symbol=stock_code,
                        period=period,
                        start_date=start_str,
                        end_date=end_str,
                        adjust="qfq",
                    )
                else:
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        period=period,
                        start_date=start_str,
                        end_date=end_str,
                        adjust="qfq",
                    )
                if not df.empty:
                    break
            except Exception as e:
                last_error = e
                import time
                time.sleep(1 + attempt)
        if df.empty:
            print(f"[PriceFetcher] 拉取 {stock_code} {period}K 失败: {last_error}")
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        return self._normalize(df, stock_code, is_hk)

    @staticmethod
    def _normalize(df: pd.DataFrame, stock_code: str, is_hk: bool) -> pd.DataFrame:
        """将 akshare 返回的列名标准化为统一格式。"""
        # 统一列名映射（akshare 返回中文列名）
        col_map = {
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "change_pct",
            "涨跌额": "change_amount",
            "换手率": "turnover",
        }

        # A股有"股票代码"列，港股没有
        rename_dict = {}
        for cn, en in col_map.items():
            if cn in df.columns:
                rename_dict[cn] = en

        df = df.rename(columns=rename_dict)

        # 确保 stock_code 列存在（港股需补充）
        if "stock_code" not in df.columns:
            df["stock_code"] = stock_code

        # 日期格式统一
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

        # 选择需要的列并保持顺序
        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "amplitude", "change_pct", "turnover",
        ]
        available = [c for c in keep_cols if c in df.columns]
        df = df[available].copy()

        # 数值类型转换
        for col in ["open", "high", "low", "close", "volume", "amount", "amplitude", "change_pct", "turnover"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
