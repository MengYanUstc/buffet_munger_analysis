"""
评分插件基类与类型定义
支持三种评分模式：
1. QUANTITATIVE_ONLY: 完全定量（脚本直接计算）
2. AI_BASED: 定量基础分 + AI 定性调整
3. QUALITATIVE_ONLY: 完全定性（AI 直接打分）
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional


class ScoringType(Enum):
    QUANTITATIVE_ONLY = "quantitative_only"
    AI_BASED = "ai_based"
    QUALITATIVE_ONLY = "qualitative_only"


@dataclass
class ScoringResult:
    dimension_id: str
    name: str
    score: float
    max_score: float
    score_type: ScoringType
    base_score: Optional[float] = None          # 仅 AI_BASED 使用
    penalty_score: Optional[float] = None       # 应用强制规则后的基准分（如趋势惩罚）
    ai_adjustment: Optional[float] = None       # 仅 AI_BASED 使用
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None     # 存储子维度分析文本
    facts: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension_id": self.dimension_id,
            "name": self.name,
            "score": self.score,
            "max_score": self.max_score,
            "score_type": self.score_type.value,
            "base_score": self.base_score,
            "penalty_score": self.penalty_score,
            "ai_adjustment": self.ai_adjustment,
            "reason": self.reason,
            "facts": self.facts,
            "error": self.error,
        }


class ScoringPlugin:
    """评分插件基类"""

    dimension_id: str
    name: str
    max_score: float
    score_type: ScoringType
    step: float = 0.5

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        """
        计算评分。对于 QUANTITATIVE_ONLY 类型，直接返回结果；
        对于 AI_BASED / QUALITATIVE_ONLY，可先返回基础分/占位，由 Engine 统一调用 LLM 后回填。
        """
        raise NotImplementedError

    def get_facts(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        提取该维度需要传给 LLM 的事实数据。
        QUANTITATIVE_ONLY 类型可返回 None。
        """
        return None

    def get_rubric(self) -> str:
        """返回该维度的评分标准文本（用于 Prompt）。"""
        return ""

    def get_output_schema(self) -> Dict[str, Any]:
        """
        返回期望 LLM 输出的 JSON Schema（用于 Prompt 说明和解析约束）。
        必须包含 'score' 和 'reason' 字段，可扩展其他子维度字段。
        """
        return {
            "score": "最终得分数字",
            "reason": "30字以内评分理由"
        }
