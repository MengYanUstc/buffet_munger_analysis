"""
股价数据获取器
A股: baostock -> akshare（混合源）
港股: tushare -> akshare
返回标准化 DataFrame，列统一为：
  stock_code, trade_date, open, high, low, close,
  volume, amount, amplitude, change_pct, turnover
"""

import datetime
import time
from typing import Optional
import pandas as pd
import akshare as ak
import baostock as bs

from ...utils import is_hk_stock
from .tushare_fetcher import TushareFetcher
from .baostock_fetcher import BaoStockFetcher


class PriceFetcher:
    """统一股价数据获取入口。
    A股: baostock -> akshare
    港股: tushare -> akshare
    """

    def __init__(self):
        self.ts_fetcher = TushareFetcher()
        self.bs_fetcher = BaoStockFetcher()
        self.last_source = None

    def fetch_daily(self, stock_code: str, years: int = 1, start_date: datetime.date = None) -> pd.DataFrame:
        """拉取近 N 年日K线（前复权），或从指定 start_date 开始拉取。"""
        end_date = datetime.date.today()
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        return self._fetch(stock_code, period="daily", start_date=start_date, end_date=end_date)

    def fetch_weekly(self, stock_code: str, years: int = 5, start_date: datetime.date = None) -> pd.DataFrame:
        """拉取近 N 年周K线（前复权），或从指定 start_date 开始拉取。"""
        end_date = datetime.date.today()
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        return self._fetch(stock_code, period="weekly", start_date=start_date, end_date=end_date)

    def fetch_monthly(self, stock_code: str, years: int = 3) -> pd.DataFrame:
        """拉取近 N 年月K线（从日K按月 resample）。"""
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))
        # 先获取日K
        df_daily = self._fetch(stock_code, period="daily", start_date=start_date, end_date=end_date)
        if df_daily.empty:
            return pd.DataFrame()
        return self._resample_monthly(df_daily)

    @staticmethod
    def _resample_monthly(df_daily: pd.DataFrame) -> pd.DataFrame:
        """将日K resample 为月K（每月最后一个交易日）。"""
        df = df_daily.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").set_index("trade_date")

        monthly = pd.DataFrame()
        monthly["open"] = df["open"].resample("ME").first()
        monthly["high"] = df["high"].resample("ME").max()
        monthly["low"] = df["low"].resample("ME").min()
        monthly["close"] = df["close"].resample("ME").last()
        monthly["volume"] = df["volume"].resample("ME").sum()
        monthly["amount"] = df["amount"].resample("ME").sum()
        monthly = monthly.dropna().reset_index()
        monthly["trade_date"] = monthly["trade_date"].dt.strftime("%Y-%m-%d")
        monthly["stock_code"] = df_daily["stock_code"].iloc[0]

        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount",
        ]
        return monthly[keep_cols].copy()

    def _fetch(
        self,
        stock_code: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> pd.DataFrame:
        """内部统一拉取逻辑。A股: baostock -> akshare；港股: tushare -> akshare。"""
        is_hk = is_hk_stock(stock_code)

        if not is_hk:
            # A股：baostock -> akshare
            df = self._fetch_baostock(stock_code, period, start_date, end_date)
            if not df.empty:
                self.last_source = "baostock"
                return df
            print(f"[PriceFetcher] baostock 获取 {stock_code} {period}K 失败，回退 akshare")
            df = self._fetch_akshare(stock_code, period, start_date, end_date)
            if not df.empty:
                self.last_source = "akshare"
                return df
        else:
            # 港股：tushare -> akshare
            df = self._fetch_tushare(stock_code, period, start_date, end_date)
            if not df.empty:
                self.last_source = "tushare"
                return df
            df = self._fetch_akshare(stock_code, period, start_date, end_date)
            if not df.empty:
                self.last_source = "akshare"
                return df

        return pd.DataFrame()

    # ------------------------------------------------------------------
    # baostock (A股)
    # ------------------------------------------------------------------
    def _fetch_baostock(
        self, stock_code: str, period: str,
        start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """使用 baostock 拉取 A股 K线。复用 BaoStockFetcher 的会话管理。"""
        bs_code = BaoStockFetcher._to_baostock_code(stock_code)
        if not bs_code:
            return pd.DataFrame()

        freq = "d" if period == "daily" else "w"
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        # 周K不支持 peTTM/pbMRQ/psTTM 字段
        fields = ("date,open,high,low,close,volume,amount,peTTM,pbMRQ,psTTM"
                  if period == "daily" else
                  "date,open,high,low,close,volume,amount")

        try:
            self.bs_fetcher._ensure_login()
            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_str,
                end_date=end_str,
                frequency=freq,
                adjustflag="2",
            )
            data = []
            while rs.error_code == "0" and rs.next():
                data.append(rs.get_row_data())
            df = pd.DataFrame(data, columns=rs.fields)
        except Exception as e:
            print(f"[PriceFetcher] baostock {stock_code} {period}K 失败: {e}")
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        return self._normalize_baostock(df, stock_code)

    @staticmethod
    def _normalize_baostock(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化 baostock 返回的数据。"""
        df = df.rename(columns={
            "date": "trade_date",
        })
        df["stock_code"] = stock_code
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

        # baostock 不返回振幅/涨跌幅/换手率，留空
        df["amplitude"] = None
        df["change_pct"] = None
        df["turnover"] = None

        for col in ["open", "high", "low", "close", "volume", "amount", "peTTM", "pbMRQ", "psTTM"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 标准化列名
        df = df.rename(columns={
            "peTTM": "pe_ttm",
            "pbMRQ": "pb",
            "psTTM": "ps_ttm",
        })

        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "amplitude", "change_pct", "turnover",
            "pe_ttm", "pb", "ps_ttm",
        ]
        available = [c for c in keep_cols if c in df.columns]
        return df[available].copy()

    # ------------------------------------------------------------------
    # tushare (港股)
    # ------------------------------------------------------------------
    def _fetch_tushare(
        self, stock_code: str, period: str,
        start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """使用 tushare 拉取港股 K线。"""
        return self.ts_fetcher._fetch(stock_code, period, start_date, end_date)

    # ------------------------------------------------------------------
    # akshare (A股 + 港股)
    # ------------------------------------------------------------------
    def _fetch_akshare(
        self, stock_code: str, period: str,
        start_date: datetime.date, end_date: datetime.date
    ) -> pd.DataFrame:
        """使用 akshare 拉取 K线，带 3 次重试。"""
        is_hk = is_hk_stock(stock_code)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        freq_map = {"daily": "daily", "weekly": "weekly"}
        freq = freq_map.get(period, "daily")

        df = pd.DataFrame()
        last_error = None
        for attempt in range(3):
            try:
                if is_hk:
                    df = ak.stock_hk_hist(
                        symbol=stock_code, period=freq,
                        start_date=start_str, end_date=end_str, adjust="qfq",
                    )
                else:
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code, period=freq,
                        start_date=start_str, end_date=end_str, adjust="qfq",
                    )
                if not df.empty:
                    break
            except Exception as e:
                last_error = e
                time.sleep(1 + attempt)

        if df.empty:
            print(f"[PriceFetcher] akshare {stock_code} {period}K 失败: {last_error}")
            return pd.DataFrame()

        return self._normalize_akshare(df, stock_code, is_hk)

    @staticmethod
    def _normalize_akshare(df: pd.DataFrame, stock_code: str, is_hk: bool) -> pd.DataFrame:
        """标准化 akshare 返回的数据。"""
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
        rename_dict = {cn: en for cn, en in col_map.items() if cn in df.columns}
        df = df.rename(columns=rename_dict)

        if "stock_code" not in df.columns:
            df["stock_code"] = stock_code

        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")

        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "amplitude", "change_pct", "turnover",
        ]
        available = [c for c in keep_cols if c in df.columns]
        df = df[available].copy()

        for col in ["open", "high", "low", "close", "volume", "amount", "amplitude", "change_pct", "turnover"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
