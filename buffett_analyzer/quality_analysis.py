"""
企业质量分析主模块
整合数据获取、定量计算、评分输出。
"""

from typing import Dict, Any
import numpy as np
from .data_fetcher import DataFetcher
from .scorer import (
    score_roe, score_roic, score_growth,
    calculate_cagr, analyze_roe_stability, analyze_debt_ratio
)


class QualityAnalyzer:
    def __init__(self, stock_code: str, industry_type: str = 'general', source: str = 'akshare'):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.fetcher = DataFetcher(source=source)

    def run(self) -> Dict[str, Any]:
        """
        执行完整的企业质量分析流程，返回结构化评分结果。
        """
        # 1. 获取数据
        df_ind = self.fetcher.fetch_indicator_data(self.stock_code)
        df_profit = self.fetcher.fetch_profit_data(self.stock_code)
        df_balance = self.fetcher.fetch_balance_data(self.stock_code)

        # 2. ROE 评分
        roe_values = []
        avg_roe = None
        roe_score = 0.0
        if not df_ind.empty and 'ROEJQ' in df_ind.columns:
            roe_values = df_ind['ROEJQ'].dropna().tolist()
            if roe_values:
                avg_roe = float(np.mean(roe_values))
                roe_score = score_roe(avg_roe)

        # 3. ROE 稳定性（AI 定性参考）
        roe_stability = (
            analyze_roe_stability(roe_values)
            if len(roe_values) >= 4
            else {
                "error": "ROE 数据不足 4 年，无法进行稳定性分析",
                "roe_values": [round(x, 2) for x in roe_values]
            }
        )

        # 4. ROIC 评分
        avg_roic = None
        roic_score = 0.0
        roic_values = []
        if not df_ind.empty and 'ROIC' in df_ind.columns:
            roic_values = df_ind['ROIC'].dropna().tolist()
            if roic_values:
                avg_roic = float(np.mean(roic_values))
                roic_score = score_roic(avg_roic)

        # 5. 营收增长评分
        revenue_cagr = None
        revenue_score = 0.0
        if not df_profit.empty and 'TOTAL_OPERATE_INCOME' in df_profit.columns:
            rev_series = df_profit['TOTAL_OPERATE_INCOME'].dropna()
            if len(rev_series) >= 2:
                revenue_cagr = calculate_cagr(rev_series.tolist())
                if revenue_cagr is not None:
                    revenue_score = score_growth(revenue_cagr)

        # 6. 利润增长评分（优先扣非，备选归母）
        profit_cagr = None
        profit_score = 0.0
        used_profit_metric = None
        if not df_profit.empty:
            profit_series = None
            if 'DEDUCT_PARENT_NETPROFIT' in df_profit.columns:
                ps = df_profit['DEDUCT_PARENT_NETPROFIT'].dropna()
                if len(ps) >= 2:
                    profit_series = ps
                    used_profit_metric = '扣除非经常性损益后的净利润'

            if profit_series is None and 'PARENT_NETPROFIT' in df_profit.columns:
                ps = df_profit['PARENT_NETPROFIT'].dropna()
                if len(ps) >= 2:
                    profit_series = ps
                    used_profit_metric = '归属于母公司股东的净利润'

            if profit_series is not None and len(profit_series) >= 2:
                profit_cagr = calculate_cagr(profit_series.tolist())
                if profit_cagr is not None:
                    profit_score = score_growth(profit_cagr)

        # 7. 资产负债率分析（AI 定性参考）
        debt_analysis = {}
        if not df_balance.empty and '资产负债率' in df_balance.columns:
            latest_ratio_series = df_balance['资产负债率'].dropna()
            if len(latest_ratio_series) > 0:
                debt_analysis = analyze_debt_ratio(float(latest_ratio_series.iloc[-1]), self.industry_type)

        # 8. 汇总
        script_score = round(roe_score + roic_score + revenue_score + profit_score, 2)
        current_total = script_score  # AI 尚未介入时的总分

        result = {
            "stock_code": self.stock_code,
            "data_years": len(roe_values),
            "roe": {
                "avg_roe": round(avg_roe, 2) if avg_roe is not None else None,
                "yearly_values": [round(x, 2) for x in roe_values],
                "score": roe_score,
                "max_score": 4.0
            },
            "roe_stability": roe_stability,
            "roic": {
                "avg_roic": round(avg_roic, 2) if avg_roic is not None else None,
                "yearly_values": [round(x, 2) for x in roic_values],
                "score": roic_score,
                "max_score": 6.0
            },
            "revenue_growth": {
                "cagr": round(revenue_cagr, 2) if revenue_cagr is not None else None,
                "score": revenue_score,
                "max_score": 3.0
            },
            "profit_growth": {
                "cagr": round(profit_cagr, 2) if profit_cagr is not None else None,
                "used_metric": used_profit_metric,
                "score": profit_score,
                "max_score": 3.0
            },
            "debt_ratio": debt_analysis,
            "scoring_summary": {
                "script_calculated_score": script_score,
                "ai_qualitative_pending": 4.0,
                "current_total": current_total,
                "max_possible_total": current_total + 4.0,
                "full_score": 20.0
            },
            "rating_reference": {
                ">=17": "顶级公司",
                "14-17": "优秀公司",
                "11-14": "中等",
                "8-11": "一般",
                "<8": "较差"
            }
        }
        return result
