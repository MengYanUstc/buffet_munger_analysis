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

    def fetch_profit_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取利润表（营业总收入、扣非净利润、归母净利润）。
        使用 akshare.stock_profit_sheet_by_report_em。
        """
        if self.source != 'akshare':
            raise NotImplementedError(f"暂不支持数据源: {self.source}")

        try:
            symbol = self._format_symbol_report(stock_code)
            df = ak.stock_profit_sheet_by_report_em(symbol=symbol)
            df = self._normalize_date_col(df, 'REPORT_DATE')
            df = self._filter_annual(df, 'REPORT_DATE')

            for col in ['TOTAL_OPERATE_INCOME', 'DEDUCT_PARENT_NETPROFIT', 'PARENT_NETPROFIT']:
                if col in df.columns:
                    df[col] = self._to_numeric(df[col])

            return df.tail(7)
        except Exception as e:
            print(f"[DataFetcher] 获取利润表数据失败 ({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_balance_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取资产负债表，并计算资产负债率。
        使用 akshare.stock_balance_sheet_by_report_em。
        """
        if self.source != 'akshare':
            raise NotImplementedError(f"暂不支持数据源: {self.source}")

        try:
            symbol = self._format_symbol_report(stock_code)
            df = ak.stock_balance_sheet_by_report_em(symbol=symbol)
            df = self._normalize_date_col(df, 'REPORT_DATE')
            df = self._filter_annual(df, 'REPORT_DATE')

            for col in ['TOTAL_ASSETS', 'TOTAL_LIABILITIES']:
                if col in df.columns:
                    df[col] = self._to_numeric(df[col])

            if 'TOTAL_ASSETS' in df.columns and 'TOTAL_LIABILITIES' in df.columns:
                df['资产负债率'] = (df['TOTAL_LIABILITIES'] / df['TOTAL_ASSETS']) * 100.0

            return df.tail(7)
        except Exception as e:
            print(f"[DataFetcher] 获取资产负债表数据失败 ({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_gross_margin_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取近5年毛利率数据。
        从 stock_financial_analysis_indicator_em 提取 MLR（毛利）和 TOTALOPERATEREVE（营业总收入）。
        """
        if self.source != 'akshare':
            raise NotImplementedError(f"暂不支持数据源: {self.source}")

        try:
            symbol = self._format_symbol_indicator(stock_code)
            df = ak.stock_financial_analysis_indicator_em(symbol=symbol)
            df = self._normalize_date_col(df, 'REPORT_DATE')
            df = self._filter_annual(df, 'REPORT_DATE')

            for col in ['MLR', 'TOTALOPERATEREVE']:
                if col in df.columns:
                    df[col] = self._to_numeric(df[col])

            if 'MLR' in df.columns and 'TOTALOPERATEREVE' in df.columns:
                df['毛利率'] = (df['MLR'] / df['TOTALOPERATEREVE']) * 100.0

            return df.tail(7)
        except Exception as e:
            print(f"[DataFetcher] 获取毛利率数据失败 ({stock_code}): {e}")
            return pd.DataFrame()

    def fetch_capex_and_profit_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取近5年资本开支和净利润数据。
        资本开支 = 固定资产 + 无形资产 + 在建工程 的年度变化（资产负债表近似）
        净利润 = 归属于母公司股东的净利润
        """
        if self.source != 'akshare':
            raise NotImplementedError(f"暂不支持数据源: {self.source}")

        try:
            # 1. 资产负债表：获取固定资产、无形资产、在建工程
            symbol_bs = self._format_symbol_report(stock_code)
            df_bs = ak.stock_balance_sheet_by_report_em(symbol=symbol_bs)
            df_bs = self._normalize_date_col(df_bs, 'REPORT_DATE')
            df_bs = self._filter_annual(df_bs, 'REPORT_DATE')
            df_bs = df_bs.sort_values('REPORT_DATE').reset_index(drop=True)

            for col in ['FIXED_ASSET', 'INTANGIBLE_ASSET', 'CIP']:
                if col in df_bs.columns:
                    df_bs[col] = self._to_numeric(df_bs[col])

            # 计算长期资产合计
            cip = df_bs['CIP'] if 'CIP' in df_bs.columns else pd.Series(0.0, index=df_bs.index)
            df_bs['长期资产合计'] = (
                df_bs['FIXED_ASSET'].fillna(0).values +
                df_bs['INTANGIBLE_ASSET'].fillna(0).values +
                cip.fillna(0).values
            )
            # 资本开支 ≈ 长期资产年度变化（取正值，因为资产减少不是扩张）
            df_bs['资本开支'] = df_bs['长期资产合计'].diff().clip(lower=0)

            # 2. 利润表：获取归母净利润
            symbol_pl = self._format_symbol_report(stock_code)
            df_pl = ak.stock_profit_sheet_by_report_em(symbol=symbol_pl)
            df_pl = self._normalize_date_col(df_pl, 'REPORT_DATE')
            df_pl = self._filter_annual(df_pl, 'REPORT_DATE')
            df_pl = df_pl.sort_values('REPORT_DATE').reset_index(drop=True)

            for col in ['PARENT_NETPROFIT', 'PARENTNETPROFIT']:
                if col in df_pl.columns:
                    df_pl['PARENTNETPROFIT'] = self._to_numeric(df_pl[col])
                    break
            else:
                # 尝试从财务分析指标表获取
                symbol_ind = self._format_symbol_indicator(stock_code)
                df_ind = ak.stock_financial_analysis_indicator_em(symbol=symbol_ind)
                df_ind = self._normalize_date_col(df_ind, 'REPORT_DATE')
                df_ind = self._filter_annual(df_ind, 'REPORT_DATE')
                df_ind = df_ind.sort_values('REPORT_DATE').reset_index(drop=True)
                if 'PARENTNETPROFIT' in df_ind.columns:
                    df_ind['PARENTNETPROFIT'] = self._to_numeric(df_ind['PARENTNETPROFIT'])
                    df_pl = df_ind[['REPORT_DATE', 'PARENTNETPROFIT']]
                else:
                    df_pl = pd.DataFrame()

            # 3. 合并（取最近6年资产负债表用于计算5年变化）
            df_bs_sel = df_bs[['REPORT_DATE', '资本开支', '长期资产合计']].tail(6)
            df_bs_sel = df_bs_sel.iloc[1:]  # 去掉第一行（没有上一年对比）

            if 'PARENTNETPROFIT' in df_pl.columns:
                df_pl_sel = df_pl[['REPORT_DATE', 'PARENTNETPROFIT']].tail(5)
            else:
                df_pl_sel = pd.DataFrame()

            if df_pl_sel.empty:
                return df_bs_sel.tail(5)

            # 合并
            merged = pd.merge(df_bs_sel, df_pl_sel, on='REPORT_DATE', how='inner')
            merged = merged.sort_values('REPORT_DATE').reset_index(drop=True)
            return merged.tail(5)
        except Exception as e:
            print(f"[DataFetcher] 获取资本开支数据失败 ({stock_code}): {e}")
            return pd.DataFrame()
