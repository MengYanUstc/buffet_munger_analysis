"""
Yahoo Finance 数据获取封装
港股优先使用 yahoo，A股作为备用源。
返回标准化 DataFrame，列统一为：
  stock_code, trade_date, open, high, low, close,
  volume, amount, amplitude, change_pct, turnover
"""

from typing import Optional
import pandas as pd
import yfinance as yf

from ...utils import is_hk_stock


class YahooFetcher:
    """Yahoo Finance 股价数据获取器。港股优先，A股备用。"""

    @staticmethod
    def _to_yahoo_code(stock_code: str) -> Optional[str]:
        """将代码转换为 yahoo 格式：
        A股：600519.SS (上海), 000001.SZ (深圳)
        港股：0700.HK (去除前导零)
        """
        if is_hk_stock(stock_code):
            hk_num = stock_code.lstrip("0")
            return f"{hk_num}.HK"
        if len(stock_code) != 6 or not stock_code.isdigit():
            return None
        first = stock_code[0]
        if first in ("6", "9"):
            return f"{stock_code}.SS"
        elif first in ("0", "3"):
            return f"{stock_code}.SZ"
        return None

    def fetch_daily(self, stock_code: str, years: int = 1) -> pd.DataFrame:
        """拉取近 N 年日K线。"""
        return self._fetch(stock_code, period="daily", years=years)

    def fetch_weekly(self, stock_code: str, years: int = 5) -> pd.DataFrame:
        """拉取近 N 年周K线。"""
        return self._fetch(stock_code, period="weekly", years=years)

    def _fetch(self, stock_code: str, period: str, years: int) -> pd.DataFrame:
        """内部统一拉取逻辑。"""
        yf_code = self._to_yahoo_code(stock_code)
        if not yf_code:
            return pd.DataFrame()

        interval = "1d" if period == "daily" else "1wk"
        yf_period = f"{years}y"

        try:
            ticker = yf.Ticker(yf_code)
            df = ticker.history(period=yf_period, interval=interval)
        except Exception as e:
            print(f"[YahooFetcher] {stock_code}({yf_code}) {period}K 失败: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        return self._normalize(df, stock_code)

    @staticmethod
    def _normalize(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化 yfinance 返回的数据。"""
        df = df.reset_index()

        col_map = {
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
        rename_dict = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename_dict)

        df["stock_code"] = stock_code
        # yfinance Date 可能带时区信息，先去掉时区
        df["trade_date"] = (
            pd.to_datetime(df["trade_date"])
            .dt.tz_localize(None)
            .dt.strftime("%Y-%m-%d")
        )

        # yfinance 不返回 amount/amplitude/change_pct/turnover
        df["amount"] = None
        df["amplitude"] = None
        df["change_pct"] = None
        df["turnover"] = None

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "amplitude", "change_pct", "turnover",
        ]
        available = [c for c in keep_cols if c in df.columns]
        return df[available].copy()
