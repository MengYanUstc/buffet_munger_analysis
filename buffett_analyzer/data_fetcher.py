"""
数据获取模块
默认使用 akshare 获取A股财务报表数据（近7年年报）。
自动处理不同 API 对股票代码前缀/后缀的要求。
"""

import sys
# 静默 akshare 内部的 tqdm 进度条
_old_stdout = sys.stdout
from tqdm import tqdm

class SilentTqdm(tqdm):
    def __init__(self, *args, **kwargs):
        kwargs["disable"] = True
        super().__init__(*args, **kwargs)

# 替换 tqdm 模块内的 tqdm 类
import tqdm as _tqdm_module
_tqdm_module.tqdm = SilentTqdm
sys.stdout = _old_stdout

import pandas as pd
import numpy as np
import akshare as ak


class DataFetcher:
    def __init__(self, source: str = 'akshare'):
        self.source = source.lower()

    @staticmethod
    def _get_exchange(code: str) -> str:
        """根据股票代码判断交易所。"""
        if code.startswith(('60', '68', '69')):
            return 'SH'
        elif code.startswith(('00', '30')):
            return 'SZ'
        elif code.startswith(('4', '8')):
            return 'BJ'
        return 'SH'

    def _format_symbol_indicator(self, code: str) -> str:
        """财务分析指标 API 需要 600519.SH 格式。"""
        return f"{code}.{self._get_exchange(code)}"

    def _format_symbol_report(self, code: str) -> str:
        """三大报表 API 需要 SH600519 格式。"""
        return f"{self._get_exchange(code)}{code}"

    @staticmethod
    def _normalize_date_col(df: pd.DataFrame, col_name: str = '报告期') -> pd.DataFrame:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name])
        return df

    @staticmethod
    def _filter_annual(df: pd.DataFrame, col_name: str = '报告期') -> pd.DataFrame:
        """筛选年报数据（12月），并按报告期升序排列。"""
        if col_name not in df.columns:
            return df
        return df[df[col_name].dt.month == 12].sort_values(col_name).reset_index(drop=True)

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        """将 '--'、'—' 等占位符转为 NaN，并转为数值类型。"""
        return pd.to_numeric(
            series.replace({'--': np.nan, '—': np.nan, '': np.nan}),
            errors='coerce'
        )

    def fetch_indicator_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取财务分析指标（ROE、ROIC 等）。
        使用 akshare.stock_financial_analysis_indicator_em。
        """
        if self.source != 'akshare':
            raise NotImplementedError(f"暂不支持数据源: {self.source}")

        try:
            symbol = self._format_symbol_indicator(stock_code)
            df = ak.stock_financial_analysis_indicator_em(symbol=symbol)
            df = self._normalize_date_col(df, 'REPORT_DATE')
            df = self._filter_annual(df, 'REPORT_DATE')

            for col in ['ROEJQ', 'ROIC']:
                if col in df.columns:
                    df[col] = self._to_numeric(df[col])

            return df.tail(7)
        except Exception as e:
            print(f"[DataFetcher] 获取财务指标数据失败 ({stock_code}): {e}")
            return pd.DataFrame()

