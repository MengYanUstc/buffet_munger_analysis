"""
质量评分引擎与插件
"""

from .engine import AiScoringEngine
from .llm_client import LLMClient
from .plugin_base import ScoringPlugin, ScoringResult, ScoringType
from .plugins import (
    RoePlugin,
    RoicPlugin,
    RevenueGrowthPlugin,
    ProfitGrowthPlugin,
    RoeStabilityPlugin,
    DebtRatioPlugin,
)


def get_default_plugins() -> list:
    """返回默认启用的评分插件列表。"""
    return [
        RoePlugin(),
        RoicPlugin(),
        RevenueGrowthPlugin(),
        ProfitGrowthPlugin(),
        RoeStabilityPlugin(),
        DebtRatioPlugin(),
    ]


__all__ = [
    "AiScoringEngine",
    "LLMClient",
    "ScoringPlugin",
    "ScoringResult",
    "ScoringType",
    "get_default_plugins",
]
