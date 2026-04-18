"""
ROE 稳定性评分插件 —— 完全定量评分
"""

from typing import Dict, Any

from ..plugin_base import ScoringPlugin, ScoringResult, ScoringType
from ...scorer import analyze_roe_stability


class RoeStabilityPlugin(ScoringPlugin):
    dimension_id = "roe_stability"
    name = "ROE稳定性"
    max_score = 2.0
    score_type = ScoringType.QUANTITATIVE_ONLY
    step = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        roe_values = context.get("roe_values", [])
        analysis = analyze_roe_stability(roe_values) if len(roe_values) >= 4 else {}
        base_score = analysis.get("suggested_base_score", 0.0)
        penalty_score = analysis.get("penalty_score", base_score)
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=penalty_score,
            max_score=self.max_score,
            score_type=self.score_type,
            base_score=base_score,
            penalty_score=penalty_score,
            facts=analysis,
        )

    def get_facts(self, context: Dict[str, Any]) -> Dict[str, Any]:
        roe_values = context.get("roe_values", [])
        analysis = analyze_roe_stability(roe_values) if len(roe_values) >= 4 else {}
        return {
            "roe_values": analysis.get("roe_values"),
            "roe_std": analysis.get("roe_std"),
            "roe_mean": analysis.get("roe_mean"),
            "trend_direction": analysis.get("trend_direction"),
            "trend_diff": analysis.get("trend_diff"),
            "suggested_base_score": analysis.get("suggested_base_score"),
            "trend_penalty": analysis.get("trend_penalty"),
            "penalty_score": analysis.get("penalty_score"),
        }

    def get_rubric(self) -> str:
        return (
            "基础分由标准差决定：σ≤3 为 2.0 分，σ≤5 为 1.5 分，σ≤7 为 1.0 分，σ≤9 为 0.5 分，>9 为 0 分。\n"
            "趋势对称调整：明显上升 +1.0，温和上升 +0.5，基本稳定 0，温和下降 -0.5，明显下降 -1.0。\n"
            "最终得分 = 基础分 + 趋势调整，范围 [0, 2]，步长 0.5。"
        )
