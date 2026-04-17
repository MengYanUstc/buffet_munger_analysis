"""
资产负债率评分插件 —— 定量基础分 + AI 定性调整
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult, ScoringType
from ...scorer import analyze_debt_ratio


class DebtRatioPlugin(ScoringPlugin):
    dimension_id = "debt_ratio"
    name = "资产负债率"
    max_score = 2.0
    score_type = ScoringType.AI_BASED
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        debt_ratio = context.get("debt_ratio")
        industry_type = context.get("industry_type", "general")
        analysis = analyze_debt_ratio(debt_ratio, industry_type) if debt_ratio is not None else {}
        base_score = analysis.get("suggested_base_score", 0.0)
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=base_score,
            max_score=self.max_score,
            score_type=self.score_type,
            base_score=base_score,
            facts=analysis,
        )

    def get_facts(self, context: Dict[str, Any]) -> Dict[str, Any]:
        debt_ratio = context.get("debt_ratio")
        industry_type = context.get("industry_type", "general")
        analysis = analyze_debt_ratio(debt_ratio, industry_type) if debt_ratio is not None else {}
        return {
            "debt_ratio": analysis.get("debt_ratio"),
            "industry_type": analysis.get("industry_type"),
            "debt_level": analysis.get("debt_level"),
            "suggested_base_score": analysis.get("suggested_base_score"),
        }

    def get_rubric(self) -> str:
        return (
            "基础分由负债率区间决定（通用行业）：≤30% 为 2.0 分，≤50% 为 1.5 分，≤70% 为 1.0 分，>70% 为 0 分。\n"
            "AI 在此基础上根据有息负债占比、现金流覆盖能力、行业特性进行 ±0.5 分的调整，步长 0.5。"
        )
