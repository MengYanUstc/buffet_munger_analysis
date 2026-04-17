"""
核心基础设施：分析器基类与注册表
为后续 3+ 个模块预留统一扩展接口
"""

from .analyzer_base import AnalyzerBase, AnalysisReport
from .registry import AnalyzerRegistry

__all__ = ["AnalyzerBase", "AnalysisReport", "AnalyzerRegistry"]
