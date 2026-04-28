"""
ROE 稳定性评分插件 —— 完全定量评分
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult
from ...scorer import analyze_roe_stability


class RoeStabilityPlugin(ScoringPlugin):
    dimension_id = "roe_stability"
    name = "ROE稳定性"
    max_score = 2.0
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        roe_values = context.get("roe_values", [])
        analysis = analyze_roe_stability(roe_values) if len(roe_values) >= 4 else {}
        penalty_score = analysis.get("penalty_score", analysis.get("suggested_base_score", 0.0))
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=penalty_score,
            max_score=self.max_score,
            details=analysis,
        )
