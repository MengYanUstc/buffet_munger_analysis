"""
ROE 评分插件 —— 完全定量
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult
from ...scorer import score_roe


class RoePlugin(ScoringPlugin):
    dimension_id = "roe"
    name = "ROE"
    max_score = 4.0
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        avg_roe = context.get("avg_roe")
        score = score_roe(avg_roe) if avg_roe is not None else 0.0
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=score,
            max_score=self.max_score,
            facts={"avg_roe": avg_roe},
        )
