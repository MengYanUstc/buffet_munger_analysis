"""
评分插件集合
"""

from .roe_plugin import RoePlugin
from .roic_plugin import RoicPlugin
from .revenue_growth_plugin import RevenueGrowthPlugin
from .profit_growth_plugin import ProfitGrowthPlugin
from .roe_stability_plugin import RoeStabilityPlugin
from .debt_ratio_plugin import DebtRatioPlugin

__all__ = [
    "RoePlugin",
    "RoicPlugin",
    "RevenueGrowthPlugin",
    "ProfitGrowthPlugin",
    "RoeStabilityPlugin",
    "DebtRatioPlugin",
]
