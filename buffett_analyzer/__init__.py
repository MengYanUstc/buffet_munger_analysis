from .quality_analysis import QualityAnalyzer
from .management_analysis import ManagementAnalyzer
from .moat_analysis import MoatAnalyzer
from .scorer import *
from .data_fetcher import DataFetcher
from .core import AnalyzerBase, AnalysisReport, AnalyzerRegistry

# 自动注册内置分析模块，避免 CLI 外部忘记调用 register_analyzers()
AnalyzerRegistry.register(QualityAnalyzer)
AnalyzerRegistry.register(ManagementAnalyzer)
AnalyzerRegistry.register(MoatAnalyzer)

__version__ = "0.3.0"
