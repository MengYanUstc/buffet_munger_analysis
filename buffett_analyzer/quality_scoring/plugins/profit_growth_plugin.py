"""
利润增长评分插件 —— 完全定量
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult
from ...scorer import score_growth


class ProfitGrowthPlugin(ScoringPlugin):
    dimension_id = "profit_growth"
    name = "利润增长"
    max_score = 3.0
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        cagr = context.get("profit_cagr")
        used_metric = context.get("used_profit_metric")
        score = score_growth(cagr) if cagr is not None else 0.0
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=score,
            max_score=self.max_score,
            facts={"cagr": cagr, "used_metric": used_metric},
        )
