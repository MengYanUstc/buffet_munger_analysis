"""
资产负债率评分插件 —— 完全定量评分
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult
from ...scorer import analyze_debt_ratio


class DebtRatioPlugin(ScoringPlugin):
    dimension_id = "debt_ratio"
    name = "资产负债率"
    max_score = 2.0
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
            details=analysis,
        )
