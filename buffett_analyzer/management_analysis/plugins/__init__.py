"""
管理层分析评分插件
"""

from .capital_allocation_plugin import CapitalAllocationPlugin
from .integrity_plugin import IntegrityPlugin

__all__ = [
    "CapitalAllocationPlugin",
    "IntegrityPlugin",
]
