"""
质量评分插件
"""

from .plugin_base import ScoringPlugin, ScoringResult
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
    "ScoringPlugin",
    "ScoringResult",
    "get_default_plugins",
]
