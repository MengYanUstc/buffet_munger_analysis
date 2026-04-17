"""
商业模式分析模块（Module 4）
总分 20 分 = 定性 10 分（Coze LLM）+ 定量 10 分（资本开支 4 分 + FCF 6 分）

增长确定性（3分）已移至估值模块，由估值模块复用本模块的定性缓存。
"""

from .business_model_analyzer import BusinessModelAnalyzer
from .capex_scorer import compute_capex_score

__all__ = ["BusinessModelAnalyzer", "compute_capex_score"]
