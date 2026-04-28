"""
ROIC 评分插件 —— 完全定量
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult
from ...scorer import score_roic


class RoicPlugin(ScoringPlugin):
    dimension_id = "roic"
    name = "ROIC"
    max_score = 6.0
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        avg_roic = context.get("avg_roic")
        score = score_roic(avg_roic) if avg_roic is not None else 0.0
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=score,
            max_score=self.max_score,
            facts={"avg_roic": avg_roic},
        )
