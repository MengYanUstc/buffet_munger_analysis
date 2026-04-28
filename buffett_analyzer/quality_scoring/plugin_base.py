"""
评分插件基类与结果定义
企业质量分析采用完全定量评分，无 AI 微调。
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class ScoringResult:
    dimension_id: str
    name: str
    score: float
    max_score: float
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    facts: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension_id": self.dimension_id,
            "name": self.name,
            "score": self.score,
            "max_score": self.max_score,
            "reason": self.reason,
            "details": self.details,
            "facts": self.facts,
            "error": self.error,
        }


class ScoringPlugin:
    """评分插件基类"""

    dimension_id: str
    name: str
    max_score: float
    step: float = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        """计算评分。"""
        raise NotImplementedError
