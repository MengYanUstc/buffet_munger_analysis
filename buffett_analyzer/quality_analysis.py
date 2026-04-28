"""
企业质量分析主模块（已适配 AnalyzerBase 统一接口）
整合数据获取、定量计算输出。
"""

from typing import Dict, Any
import numpy as np

from .scorer import calculate_cagr, analyze_roe_stability, analyze_debt_ratio
from .quality_scoring import get_default_plugins
from .quality_scoring.plugin_base import ScoringResult
from .data_warehouse.collector import DataCollector
from .core import AnalyzerBase, AnalysisReport


class QualityAnalyzer(AnalyzerBase):
    module_id = "quality"
    module_name = "企业质量分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.collector = DataCollector()

    def run(self) -> AnalysisReport:
        """执行完整的企业质量分析流程，返回标准化报告。"""
        # 0. 确保财务数据已入库（缓存优先策略）
        self.collector.collect(self.stock_code)

        # 1. 从数据库读取财务数据
        df = self.collector.cache.read_financial_reports(self.stock_code)

        # 2. 提取核心指标（仅最近 5 个完整年度）
        roe_values = []
        avg_roe = None
        if not df.empty and "roe" in df.columns:
            roe_values = df["roe"].dropna().tail(5).tolist()
            if roe_values:
                avg_roe = float(np.mean(roe_values))

        roic_values = []
        avg_roic = None
        if not df.empty and "roic" in df.columns:
            roic_values = df["roic"].dropna().tail(5).tolist()
            if roic_values:
                avg_roic = float(np.mean(roic_values))

        revenue_cagr = None
        if not df.empty and "revenue" in df.columns:
            rev_series = df["revenue"].dropna().tail(5)
            if len(rev_series) >= 2:
                revenue_cagr = calculate_cagr(rev_series.tolist())

        profit_cagr = None
        used_profit_metric = None
        if not df.empty:
            profit_series = None
            if "parent_net_profit" in df.columns:
                ps = df["parent_net_profit"].dropna().tail(5)
                if len(ps) >= 2:
                    profit_series = ps
                    used_profit_metric = "归属于母公司股东的净利润"
            if profit_series is None and "deduct_net_profit" in df.columns:
                ps = df["deduct_net_profit"].dropna().tail(5)
                if len(ps) >= 2:
                    profit_series = ps
                    used_profit_metric = "扣除非经常性损益后的净利润"
            if profit_series is None and "net_profit" in df.columns:
                ps = df["net_profit"].dropna().tail(5)
                if len(ps) >= 2:
                    profit_series = ps
                    used_profit_metric = "净利润"
            if profit_series is not None and len(profit_series) >= 2:
                profit_cagr = calculate_cagr(profit_series.tolist())

        debt_ratio = None
        if not df.empty and "debt_ratio" in df.columns:
            latest_ratio_series = df["debt_ratio"].dropna()
            if len(latest_ratio_series) > 0:
                debt_ratio = float(latest_ratio_series.iloc[-1])

        # 3. 构建上下文并运行评分
        context = {
            "stock_code": self.stock_code,
            "industry_type": self.industry_type,
            "avg_roe": avg_roe,
            "roe_values": roe_values,
            "avg_roic": avg_roic,
            "roic_values": roic_values,
            "revenue_cagr": revenue_cagr,
            "profit_cagr": profit_cagr,
            "used_profit_metric": used_profit_metric,
            "debt_ratio": debt_ratio,
        }

        plugins = get_default_plugins()
        scoring_results: Dict[str, ScoringResult] = {}
        for plugin in plugins:
            try:
                scoring_results[plugin.dimension_id] = plugin.compute(context)
            except Exception as e:
                scoring_results[plugin.dimension_id] = ScoringResult(
                    dimension_id=plugin.dimension_id,
                    name=plugin.name,
                    score=0.0,
                    max_score=plugin.max_score,
                    error=f"compute error: {e}",
                )

        roe_res = scoring_results.get("roe")
        roic_res = scoring_results.get("roic")
        revenue_res = scoring_results.get("revenue_growth")
        profit_res = scoring_results.get("profit_growth")
        roe_stab_res = scoring_results.get("roe_stability")
        debt_res = scoring_results.get("debt_ratio")

        script_score = round(
            (roe_res.score if roe_res else 0.0)
            + (roic_res.score if roic_res else 0.0)
            + (revenue_res.score if revenue_res else 0.0)
            + (profit_res.score if profit_res else 0.0),
            2,
        )
        total_score = round(
            (roe_res.score if roe_res else 0.0)
            + (roic_res.score if roic_res else 0.0)
            + (revenue_res.score if revenue_res else 0.0)
            + (profit_res.score if profit_res else 0.0)
            + (roe_stab_res.score if roe_stab_res else 0.0)
            + (debt_res.score if debt_res else 0.0),
            2,
        )

        # roe_stability 和 debt_ratio 为完全定量评分
        roe_stability_output = {}
        if len(roe_values) >= 4:
            roe_stability_output = analyze_roe_stability(roe_values)
        else:
            roe_stability_output = {
                "error": "ROE 数据不足 4 年，无法进行稳定性分析",
                "roe_values": [round(x, 2) for x in roe_values]
            }
        if roe_stab_res:
            roe_stability_output["score"] = roe_stab_res.score
        else:
            roe_stability_output["score"] = roe_stability_output.get("penalty_score", 0.0)

        debt_analysis_output = {}
        if debt_ratio is not None:
            debt_analysis_output = analyze_debt_ratio(debt_ratio, self.industry_type)
        if debt_res:
            debt_analysis_output["score"] = debt_res.score
        else:
            debt_analysis_output["score"] = debt_analysis_output.get("suggested_base_score", 0.0)

        dimensions = {
            "roe": {
                "avg_roe": round(avg_roe, 2) if avg_roe is not None else None,
                "yearly_values": [round(x, 2) for x in roe_values],
                "score": roe_res.score if roe_res else 0.0,
                "max_score": 4.0,
            },
            "roic": {
                "avg_roic": round(avg_roic, 2) if avg_roic is not None else None,
                "yearly_values": [round(x, 2) for x in roic_values],
                "score": roic_res.score if roic_res else 0.0,
                "max_score": 6.0,
            },
            "revenue_growth": {
                "cagr": round(revenue_cagr, 2) if revenue_cagr is not None else None,
                "score": revenue_res.score if revenue_res else 0.0,
                "max_score": 3.0,
            },
            "profit_growth": {
                "cagr": round(profit_cagr, 2) if profit_cagr is not None else None,
                "used_metric": used_profit_metric,
                "score": profit_res.score if profit_res else 0.0,
                "max_score": 3.0,
            },
            "roe_stability": roe_stability_output,
            "debt_ratio": debt_analysis_output,
        }

        summary = {
            "total_score": total_score,
            "full_score": 20.0,
        }

        raw_data = {
            "data_years": len(roe_values),
            "dimension_scores": {k: v.to_dict() for k, v in scoring_results.items()},
            "rating_reference": {
                ">=17": "顶级公司",
                "14-17": "优秀公司",
                "11-14": "中等",
                "8-11": "一般",
                "<8": "较差",
            },
        }

        return AnalysisReport(
            module_id=self.module_id,
            module_name=self.module_name,
            stock_code=self.stock_code,
            total_score=total_score,
            max_score=20.0,
            rating=self._rating(total_score),
            dimensions=dimensions,
            summary=summary,
            raw_data=raw_data,
        )

    @staticmethod
    def _rating(total: float) -> str:
        if total >= 17:
            return "顶级公司"
        elif total >= 14:
            return "优秀公司"
        elif total >= 11:
            return "中等"
        elif total >= 8:
            return "一般"
        else:
            return "较差"
