"""
AkShare 数据获取封装
获取财务指标、利润表、现金流量表数据，统一对齐到年报维度。
自动识别 A 股与港股代码。
"""

import sys
# 静默 akshare 内部的 tqdm
_old_stdout = sys.stdout
from tqdm import tqdm
class SilentTqdm(tqdm):
    def __init__(self, *args, **kwargs):
        kwargs["disable"] = True
        super().__init__(*args, **kwargs)
import tqdm as _tqdm_module
_tqdm_module.tqdm = SilentTqdm
sys.stdout = _old_stdout

import pandas as pd
import numpy as np
import akshare as ak
from typing import Dict, Any

from ...utils import is_hk_stock


class AkShareFetcher:

    @staticmethod
    def _get_exchange(code: str) -> str:
        if code.startswith(('60', '68', '69')):
            return 'SH'
        elif code.startswith(('00', '30')):
            return 'SZ'
        elif code.startswith(('4', '8')):
            return 'BJ'
        return 'SH'

    def _fmt_indicator(self, code: str) -> str:
        return f"{code}.{self._get_exchange(code)}"

    def _fmt_report(self, code: str) -> str:
        return f"{self._get_exchange(code)}{code}"

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.replace({'--': np.nan, '—': np.nan, '': np.nan}), errors='coerce')

    @staticmethod
    def _norm_date(df: pd.DataFrame, col: str = 'REPORT_DATE') -> pd.DataFrame:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
        return df

    @staticmethod
    def _filter_annual(df: pd.DataFrame, col: str = 'REPORT_DATE') -> pd.DataFrame:
        if col not in df.columns:
            return df
        return df[df[col].dt.month == 12].sort_values(col).reset_index(drop=True)

    # ------------------------------------------------------------------
    # A股财务数据获取
    # ------------------------------------------------------------------
    def fetch_a_share_financial_data(self, stock_code: str) -> Dict[str, Any]:
        df_ind = pd.DataFrame()
        try:
            df_ind = ak.stock_financial_analysis_indicator_em(symbol=self._fmt_indicator(stock_code))
            df_ind = self._norm_date(df_ind)
            df_ind = self._filter_annual(df_ind)
            for col in ['ROEJQ', 'ROIC', 'TOTALOPERATEREVE', 'XSMLL', 'XSJLL', 'ZCFZL', 'PARENTNETPROFIT', 'KCFJCXSYJLR']:
                if col in df_ind.columns:
                    df_ind[col] = self._to_numeric(df_ind[col])
            df_ind = df_ind[['REPORT_DATE', 'ROEJQ', 'ROIC', 'TOTALOPERATEREVE', 'XSMLL', 'XSJLL', 'ZCFZL', 'PARENTNETPROFIT', 'KCFJCXSYJLR']]
        except Exception as e:
            print(f"[AkShareFetcher] A股财务指标获取失败 ({stock_code}): {e}")

        df_profit = pd.DataFrame()
        try:
            df_profit = ak.stock_profit_sheet_by_report_em(symbol=self._fmt_report(stock_code))
            df_profit = self._norm_date(df_profit)
            df_profit = self._filter_annual(df_profit)
            revenue_col = None
            if 'TOTAL_OPERATE_INCOME' in df_profit.columns:
                revenue_col = 'TOTAL_OPERATE_INCOME'
            elif 'OPERATE_INCOME' in df_profit.columns:
                revenue_col = 'OPERATE_INCOME'
            desired_cols = ['REPORT_DATE', 'NETPROFIT', 'DEDUCT_PARENT_NETPROFIT', 'PARENT_NETPROFIT']
            if revenue_col:
                desired_cols.insert(1, revenue_col)
            existing_cols = [c for c in desired_cols if c in df_profit.columns]
            for col in existing_cols:
                if col != 'REPORT_DATE':
                    df_profit[col] = self._to_numeric(df_profit[col])
            df_profit = df_profit[existing_cols]
        except Exception as e:
            print(f"[AkShareFetcher] A股利润表获取失败 ({stock_code}): {e}")

        df_cf = pd.DataFrame()
        try:
            df_cf = ak.stock_cash_flow_sheet_by_report_em(symbol=self._fmt_report(stock_code))
            df_cf = self._norm_date(df_cf)
            df_cf = self._filter_annual(df_cf)
            for col in ['NETCASH_OPERATE', 'CONSTRUCT_LONG_ASSET']:
                if col in df_cf.columns:
                    df_cf[col] = self._to_numeric(df_cf[col])
            df_cf = df_cf[['REPORT_DATE', 'NETCASH_OPERATE', 'CONSTRUCT_LONG_ASSET']]
        except Exception as e:
            print(f"[AkShareFetcher] A股现金流量表获取失败 ({stock_code}): {e}")

        return self._merge_and_clean(df_ind, df_profit, df_cf)

    # ------------------------------------------------------------------
    # 港股财务数据获取
    # ------------------------------------------------------------------
    def fetch_hk_financial_data(self, stock_code: str) -> Dict[str, Any]:
        df_ind = pd.DataFrame()
        try:
            df_ind = ak.stock_financial_hk_analysis_indicator_em(symbol=stock_code)
            df_ind = self._norm_date(df_ind)
            df_ind = self._filter_annual(df_ind)
            for col in ['ROE_YEARLY', 'ROIC_YEARLY', 'OPERATE_INCOME', 'GROSS_PROFIT',
                        'GROSS_PROFIT_RATIO', 'NET_PROFIT_RATIO', 'HOLDER_PROFIT',
                        'DEBT_ASSET_RATIO', 'PER_NETCASH_OPERATE', 'BASIC_EPS']:
                if col in df_ind.columns:
                    df_ind[col] = self._to_numeric(df_ind[col])
            df_ind = df_ind[['REPORT_DATE', 'ROE_YEARLY', 'ROIC_YEARLY', 'OPERATE_INCOME',
                               'GROSS_PROFIT', 'GROSS_PROFIT_RATIO', 'NET_PROFIT_RATIO',
                               'HOLDER_PROFIT', 'DEBT_ASSET_RATIO', 'PER_NETCASH_OPERATE', 'BASIC_EPS']]
        except Exception as e:
            print(f"[AkShareFetcher] 港股财务指标获取失败 ({stock_code}): {e}")

        df_cf = pd.DataFrame()
        try:
            df_cf_long = ak.stock_financial_hk_report_em(stock=stock_code, symbol='现金流量表', indicator='年报')
            df_cf_long = self._norm_date(df_cf_long)
            df_cf_long = self._filter_annual(df_cf_long)
            if not df_cf_long.empty:
                df_cf_long['AMOUNT'] = self._to_numeric(df_cf_long['AMOUNT'])
                names = df_cf_long['STD_ITEM_NAME'].unique().tolist()

                def _match_ocf(name):
                    return '经营' in name and '现金净额' in name and '投资' not in name and '融资' not in name
                def _match_capex(name):
                    return '购建' in name and '资产' in name

                ocf_candidates = [n for n in names if _match_ocf(n)]
                capex_candidates = [n for n in names if _match_capex(n)]
                if not ocf_candidates:
                    for n in names:
                        if '经营业务现金净额' in n or '经营活动产生的现金流量净额' in n:
                            ocf_candidates.append(n)
                if not capex_candidates:
                    for n in names:
                        if '购建固定资产、无形资产和其他长期资产支付的现金' in n or '购建无形资产及其他资产' in n:
                            capex_candidates.append(n)

                pivot_data = {}
                for date, group in df_cf_long.groupby('REPORT_DATE'):
                    row = {'REPORT_DATE': date}
                    if ocf_candidates:
                        for cand in ocf_candidates:
                            val = group[group['STD_ITEM_NAME'] == cand]['AMOUNT'].values
                            if len(val) > 0 and pd.notna(val[0]):
                                row['NETCASH_OPERATE'] = float(val[0])
                                break
                    if capex_candidates:
                        total_capex = 0.0
                        has_val = False
                        for cand in capex_candidates:
                            val = group[group['STD_ITEM_NAME'] == cand]['AMOUNT'].values
                            if len(val) > 0 and pd.notna(val[0]):
                                total_capex += float(val[0])
                                has_val = True
                        row['CONSTRUCT_LONG_ASSET'] = total_capex if has_val else np.nan
                    pivot_data[date] = row
                df_cf = pd.DataFrame.from_dict(pivot_data, orient='index').reset_index(drop=True)
        except Exception as e:
            print(f"[AkShareFetcher] 港股现金流量表获取失败 ({stock_code}): {e}")

        return self._merge_and_clean_hk(df_ind, df_cf)

    # ------------------------------------------------------------------
    # 合并与清洗
    # ------------------------------------------------------------------
    @staticmethod
    def _merge_and_clean(df_ind: pd.DataFrame, df_profit: pd.DataFrame, df_cf: pd.DataFrame) -> Dict[str, Any]:
        merged = pd.DataFrame()
        if not df_ind.empty:
            merged = df_ind.copy()
        if not df_profit.empty:
            if merged.empty:
                merged = df_profit.copy()
            else:
                merged = pd.merge(merged, df_profit, on='REPORT_DATE', how='outer')
        if not df_cf.empty:
            if merged.empty:
                merged = df_cf.copy()
            else:
                merged = pd.merge(merged, df_cf, on='REPORT_DATE', how='outer')

        if not merged.empty:
            merged = merged.sort_values('REPORT_DATE').tail(7)
            rename_map = {
                'REPORT_DATE': 'report_date',
                'ROEJQ': 'roe',
                'ROIC': 'roic',
                'TOTALOPERATEREVE': 'revenue_ind',
                'TOTAL_OPERATE_INCOME': 'revenue_profit',
                'OPERATE_INCOME': 'revenue_profit',
                'NETPROFIT': 'net_profit',
                'DEDUCT_PARENT_NETPROFIT': 'deduct_net_profit',
                'PARENTNETPROFIT': 'parent_net_profit_ind',
                'PARENT_NETPROFIT': 'parent_net_profit',
                'XSMLL': 'gross_margin',
                'XSJLL': 'net_margin',
                'ZCFZL': 'debt_ratio',
                'NETCASH_OPERATE': 'operating_cash_flow',
                'CONSTRUCT_LONG_ASSET': 'capex',
                'KCFJCXSYJLR': 'deduct_net_profit_ind'
            }
            actual_rename = {k: v for k, v in rename_map.items() if k in merged.columns}
            merged.rename(columns=actual_rename, inplace=True)

            if 'revenue_profit' in merged.columns and 'revenue_ind' in merged.columns:
                merged['revenue'] = merged['revenue_profit'].fillna(merged['revenue_ind'])
            elif 'revenue_profit' in merged.columns:
                merged['revenue'] = merged['revenue_profit']
            elif 'revenue_ind' in merged.columns:
                merged['revenue'] = merged['revenue_ind']

            if 'parent_net_profit' in merged.columns and 'parent_net_profit_ind' in merged.columns:
                merged['parent_net_profit'] = merged['parent_net_profit'].fillna(merged['parent_net_profit_ind'])
            elif 'parent_net_profit_ind' in merged.columns:
                merged['parent_net_profit'] = merged['parent_net_profit_ind']

            if 'deduct_net_profit' in merged.columns and 'deduct_net_profit_ind' in merged.columns:
                merged['deduct_net_profit'] = merged['deduct_net_profit'].fillna(merged['deduct_net_profit_ind'])
            elif 'deduct_net_profit_ind' in merged.columns:
                merged['deduct_net_profit'] = merged['deduct_net_profit_ind']

            if 'operating_cash_flow' in merged.columns and 'capex' in merged.columns:
                merged['fcf'] = merged['operating_cash_flow'] - merged['capex']
            elif 'operating_cash_flow' in merged.columns:
                merged['fcf'] = merged['operating_cash_flow']
            else:
                merged['fcf'] = np.nan

            drop_cols = [c for c in ['revenue_profit', 'revenue_ind', 'parent_net_profit_ind', 'deduct_net_profit_ind'] if c in merged.columns]
            merged = merged.drop(columns=drop_cols, errors='ignore')

        return {
            "financial_reports": merged,
            "indicator_count": len(df_ind),
            "profit_count": len(df_profit),
            "cashflow_count": len(df_cf)
        }

    @staticmethod
    def _merge_and_clean_hk(df_ind: pd.DataFrame, df_cf: pd.DataFrame) -> Dict[str, Any]:
        merged = pd.DataFrame()
        if not df_ind.empty:
            merged = df_ind.copy()
        if not df_cf.empty:
            if merged.empty:
                merged = df_cf.copy()
            else:
                merged = pd.merge(merged, df_cf, on='REPORT_DATE', how='outer')

        if not merged.empty:
            merged = merged.sort_values('REPORT_DATE').tail(7)
            rename_map = {
                'REPORT_DATE': 'report_date',
                'ROE_YEARLY': 'roe',
                'ROIC_YEARLY': 'roic',
                'OPERATE_INCOME': 'revenue',
                'GROSS_PROFIT_RATIO': 'gross_margin',
                'NET_PROFIT_RATIO': 'net_margin',
                'HOLDER_PROFIT': 'parent_net_profit',
                'DEBT_ASSET_RATIO': 'debt_ratio',
                'NETCASH_OPERATE': 'operating_cash_flow',
                'CONSTRUCT_LONG_ASSET': 'capex',
            }
            actual_rename = {k: v for k, v in rename_map.items() if k in merged.columns}
            merged.rename(columns=actual_rename, inplace=True)

            # 港股不披露扣非净利润，用归母净利润填充
            if 'parent_net_profit' in merged.columns:
                merged['net_profit'] = merged['parent_net_profit']
                merged['deduct_net_profit'] = merged['parent_net_profit']

            if 'operating_cash_flow' in merged.columns and 'capex' in merged.columns:
                merged['fcf'] = merged['operating_cash_flow'] - merged['capex']
            elif 'operating_cash_flow' in merged.columns:
                merged['fcf'] = merged['operating_cash_flow']
            else:
                merged['fcf'] = np.nan

            # 确保百分比格式
            for col in ['gross_margin', 'net_margin', 'debt_ratio']:
                if col in merged.columns:
                    max_val = merged[col].abs().max()
                    if pd.notna(max_val) and max_val <= 1.0:
                        merged[col] = merged[col] * 100.0

        return {
            "financial_reports": merged,
            "indicator_count": len(df_ind),
            "profit_count": 0,
            "cashflow_count": len(df_cf)
        }

    def fetch_financial_data(self, stock_code: str) -> Dict[str, Any]:
        """获取指定股票近7年年报数据，自动识别A股/港股。"""
        if is_hk_stock(stock_code):
            return self.fetch_hk_financial_data(stock_code)
        return self.fetch_a_share_financial_data(stock_code)

    # ------------------------------------------------------------------
    # 季度财务数据获取（A股）
    # ------------------------------------------------------------------
    def fetch_quarterly_financial_data(self, stock_code: str) -> Dict[str, Any]:
        """获取 A股 季度财务数据，不过滤年报，保留所有季度报告。"""
        df_ind = pd.DataFrame()
        try:
            df_ind = ak.stock_financial_analysis_indicator_em(symbol=self._fmt_indicator(stock_code))
            df_ind = self._norm_date(df_ind)
            # 不过滤年报，保留所有季度
            for col in ['ROEJQ', 'ROIC', 'TOTALOPERATEREVE', 'XSMLL', 'XSJLL', 'ZCFZL', 'PARENTNETPROFIT', 'KCFJCXSYJLR']:
                if col in df_ind.columns:
                    df_ind[col] = self._to_numeric(df_ind[col])
            df_ind = df_ind[['REPORT_DATE', 'ROEJQ', 'ROIC', 'TOTALOPERATEREVE', 'XSMLL', 'XSJLL', 'ZCFZL', 'PARENTNETPROFIT', 'KCFJCXSYJLR']]
        except Exception as e:
            print(f"[AkShareFetcher] A股季度财务指标获取失败 ({stock_code}): {e}")

        df_profit = pd.DataFrame()
        try:
            df_profit = ak.stock_profit_sheet_by_report_em(symbol=self._fmt_report(stock_code))
            df_profit = self._norm_date(df_profit)
            # 不过滤年报
            revenue_col = None
            if 'TOTAL_OPERATE_INCOME' in df_profit.columns:
                revenue_col = 'TOTAL_OPERATE_INCOME'
            elif 'OPERATE_INCOME' in df_profit.columns:
                revenue_col = 'OPERATE_INCOME'
            desired_cols = ['REPORT_DATE', 'NETPROFIT', 'DEDUCT_PARENT_NETPROFIT', 'PARENT_NETPROFIT']
            if revenue_col:
                desired_cols.insert(1, revenue_col)
            existing_cols = [c for c in desired_cols if c in df_profit.columns]
            for col in existing_cols:
                if col != 'REPORT_DATE':
                    df_profit[col] = self._to_numeric(df_profit[col])
            df_profit = df_profit[existing_cols]
        except Exception as e:
            print(f"[AkShareFetcher] A股季度利润表获取失败 ({stock_code}): {e}")

        df_cf = pd.DataFrame()
        try:
            df_cf = ak.stock_cash_flow_sheet_by_report_em(symbol=self._fmt_report(stock_code))
            df_cf = self._norm_date(df_cf)
            # 不过滤年报
            for col in ['NETCASH_OPERATE', 'CONSTRUCT_LONG_ASSET']:
                if col in df_cf.columns:
                    df_cf[col] = self._to_numeric(df_cf[col])
            df_cf = df_cf[['REPORT_DATE', 'NETCASH_OPERATE', 'CONSTRUCT_LONG_ASSET']]
        except Exception as e:
            print(f"[AkShareFetcher] A股季度现金流量表获取失败 ({stock_code}): {e}")

        result = self._merge_and_clean(df_ind, df_profit, df_cf)
        # 季度数据保留近 12 个季度（3 年）
        if not result["financial_reports"].empty:
            result["financial_reports"] = result["financial_reports"].sort_values("report_date").tail(12)
        return result
