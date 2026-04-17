"""
管理层定性分析模块
"""

from .management_analyzer import ManagementAnalyzer
from .plugins import CapitalAllocationPlugin, IntegrityPlugin

__all__ = [
    "ManagementAnalyzer",
    "CapitalAllocationPlugin",
    "IntegrityPlugin",
]
