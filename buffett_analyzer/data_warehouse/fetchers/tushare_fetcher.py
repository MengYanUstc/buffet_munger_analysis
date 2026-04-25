"""
Tushare 数据获取封装
主要服务港股日周K线；A股前复权作为备用方案。
支持双 Token 回退：主 Token 失败时自动切换到备用 Token。
港股带 30 秒全局节流控制，规避 hk_daily 2次/分钟限频。
A股前复权策略：pro_bar 优先，失败时自算（daily/weekly + adj_factor）。
返回标准化 DataFrame，列统一为：
  stock_code, trade_date, open, high, low, close,
  volume, amount, amplitude, change_pct, turnover
"""

import os
import time
import datetime
from typing import Optional
import pandas as pd
import tushare as ts

from ...utils import is_hk_stock


class TushareFetcher:
    """Tushare 股价数据获取器。港股优先，A股备用（当前策略下主要服务港股）。支持双 Token 回退。"""

    # 类级别：港股 hk_daily 上次调用时间戳，用于全局 30s 节流
    _hk_last_call: float = 0.0

    def __init__(
        self,
        token: Optional[str] = None,
        fallback_token: Optional[str] = None,
    ):
        self.token = token or os.getenv("TUSHARE_TOKEN")
        self.fallback_token = fallback_token or os.getenv("TUSHARE_TOKEN_FALLBACK")

        # 使用 pro_api(token=...) 创建独立实例，避免全局 set_token 冲突
        self.pro = ts.pro_api(token=self.token) if self.token else None
        self.pro_fallback = ts.pro_api(token=self.fallback_token) if self.fallback_token else None

    def _ensure_api(self) -> bool:
        if self.pro is None and self.pro_fallback is None:
            print("[TushareFetcher] 无可用 TUSHARE_TOKEN")
            return False
        return True

    @staticmethod
    def _to_ts_code(stock_code: str) -> Optional[str]:
        """将代码转换为 tushare 格式：600519.SH / 000001.SZ / 00700.HK"""
        if is_hk_stock(stock_code):
            return f"{stock_code}.HK"
        if len(stock_code) != 6 or not stock_code.isdigit():
            return None
        first = stock_code[0]
        if first in ("6", "9"):
            return f"{stock_code}.SH"
        elif first in ("0", "3"):
            return f"{stock_code}.SZ"
        return None

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

    def _fetch(
        self,
        stock_code: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> pd.DataFrame:
        """内部统一拉取逻辑。"""
        if not self._ensure_api():
            return pd.DataFrame()

        ts_code = self._to_ts_code(stock_code)
        if not ts_code:
            return pd.DataFrame()

        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        freq = "D" if period == "daily" else "W"

        try:
            if is_hk_stock(stock_code):
                df = self._fetch_hk(ts_code, start_str, end_str, period)
            else:
                df = self._fetch_a_share(ts_code, freq, start_str, end_str)
        except Exception as e:
            print(f"[TushareFetcher] {stock_code} {period}K 失败: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        return self._normalize(df, stock_code)

    # ------------------------------------------------------------------
    # A股：pro_bar -> 自算前复权 (daily/weekly + adj_factor)
    # ------------------------------------------------------------------
    def _fetch_a_share(self, ts_code: str, freq: str, start_str: str, end_str: str) -> Optional[pd.DataFrame]:
        """获取 A股 K线，pro_bar 优先，失败时自算前复权，支持双 Token 回退。"""
        # 1) 尝试 pro_bar（主 Token -> 备用 Token）
        for pro_instance, label in [(self.pro, "main"), (self.pro_fallback, "fallback")]:
            if pro_instance is None:
                continue
            try:
                df = ts.pro_bar(
                    ts_code=ts_code, adj="qfq", freq=freq,
                    start_date=start_str, end_date=end_str,
                    api=pro_instance,
                )
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                print(f"[TushareFetcher] pro_bar ({label}) 失败: {e}")

        # 2) 自算前复权（主 Token -> 备用 Token）
        for pro_instance, label in [(self.pro, "main"), (self.pro_fallback, "fallback")]:
            if pro_instance is None:
                continue
            df = self._fetch_a_share_qfq(ts_code, freq, start_str, end_str, pro_instance)
            if df is not None and not df.empty:
                return df

        return None

    def _fetch_a_share_qfq(
        self,
        ts_code: str,
        freq: str,
        start_str: str,
        end_str: str,
        pro_instance,
    ) -> Optional[pd.DataFrame]:
        """用 daily/weekly + adj_factor 自算前复权价格。"""
        try:
            # 拉原始 K线
            if freq == "D":
                df = pro_instance.daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
            else:
                df = pro_instance.weekly(ts_code=ts_code, start_date=start_str, end_date=end_str)

            if df is None or df.empty:
                return None

            # 拉复权因子
            adj = pro_instance.adj_factor(ts_code=ts_code, start_date=start_str, end_date=end_str)
            if adj is None or adj.empty:
                return None

            # 合并
            df = df.merge(adj[["trade_date", "adj_factor"]], on="trade_date", how="left")
            df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")

            # 按日期排序；adj_factor 接口返回的数据可能比 daily 多一天
            # 因此用 adj 全局最新值作为基准，而非 df 最后一行
            df = df.sort_values("trade_date").reset_index(drop=True)
            latest_adj = float(adj["adj_factor"].iloc[0])

            # 计算前复权价格
            for col in ["open", "high", "low", "close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    df[col] = df[col] * df["adj_factor"] / latest_adj

            # 清理辅助列
            df = df.drop(columns=["adj_factor"], errors="ignore")
            return df

        except Exception as e:
            print(f"[TushareFetcher] 自算前复权失败: {e}")
            return None

    # ------------------------------------------------------------------
    # 港股：hk_daily，支持双 Token 回退
    # ------------------------------------------------------------------
    def _fetch_hk(
        self, ts_code: str, start_str: str, end_str: str, period: str
    ) -> Optional[pd.DataFrame]:
        """获取港股 K线，支持双 Token 回退，带 30 秒节流控制。"""
        # 全局 30 秒节流（类级别，所有实例共享）
        elapsed = time.time() - TushareFetcher._hk_last_call
        if elapsed < 30:
            sleep_sec = 30 - elapsed
            print(f"[TushareFetcher] 港股限频节流，等待 {sleep_sec:.1f} 秒...")
            time.sleep(sleep_sec)

        for pro_instance, label in [(self.pro, "main"), (self.pro_fallback, "fallback")]:
            if pro_instance is None:
                continue
            try:
                df = pro_instance.hk_daily(ts_code=ts_code, start_date=start_str, end_date=end_str)
                TushareFetcher._hk_last_call = time.time()
                if df is not None and not df.empty:
                    if period == "weekly":
                        df = self._resample_to_weekly(df)
                    return df
            except Exception as e:
                print(f"[TushareFetcher] hk_daily ({label}) 失败: {e}")
        return None

    @staticmethod
    def _resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
        """将日K resample 为周K（周五为周期结束）。"""
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").set_index("trade_date")

        weekly = pd.DataFrame()
        weekly["open"] = df["open"].resample("W-FRI").first()
        weekly["high"] = df["high"].resample("W-FRI").max()
        weekly["low"] = df["low"].resample("W-FRI").min()
        weekly["close"] = df["close"].resample("W-FRI").last()
        weekly["vol"] = df["vol"].resample("W-FRI").sum()
        weekly["amount"] = df["amount"].resample("W-FRI").sum()
        weekly = weekly.dropna().reset_index()
        weekly["trade_date"] = weekly["trade_date"].dt.strftime("%Y%m%d")
        return weekly

    @staticmethod
    def _normalize(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化 tushare 返回的数据。"""
        col_map = {
            "vol": "volume",
            "pct_chg": "change_pct",
        }
        rename_dict = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename_dict)

        df["stock_code"] = stock_code
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("trade_date").reset_index(drop=True)

        # tushare 不返回振幅/换手率
        df["amplitude"] = None
        df["turnover"] = None

        for col in ["open", "high", "low", "close", "volume", "amount", "change_pct"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        keep_cols = [
            "stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "amplitude", "change_pct", "turnover",
        ]
        available = [c for c in keep_cols if c in df.columns]
        return df[available].copy()
