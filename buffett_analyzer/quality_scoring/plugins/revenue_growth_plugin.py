"""
营收增长评分插件 —— 完全定量
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult, ScoringType
from ...scorer import score_growth


class RevenueGrowthPlugin(ScoringPlugin):
    dimension_id = "revenue_growth"
    name = "营收增长"
    max_score = 3.0
    score_type = ScoringType.QUANTITATIVE_ONLY
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        cagr = context.get("revenue_cagr")
        score = score_growth(cagr) if cagr is not None else 0.0
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=score,
            max_score=self.max_score,
            score_type=self.score_type,
            facts={"cagr": cagr},
        )

    def get_facts(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return {"cagr": context.get("revenue_cagr")}
