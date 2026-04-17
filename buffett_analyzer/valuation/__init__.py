"""
估值分析模块（Module 5）
总分 20 分 = 定量 17 分 + 定性 3 分（增长确定性，Coze LLM）

定量维度：
- 绝对估值水平（6分）
- 相对估值（4分）
- 长期 PEG（3分）
- DCF 安全边际（4分）

定性维度：
- 增长确定性（3分）
"""

from .valuation_analyzer import ValuationAnalyzer
from .valuation_scorer import (
    calculate_pe_base_score,
    calculate_pb_bonus,
    calculate_ps_bonus,
    calculate_absolute_valuation_score,
    calculate_historical_percentile_score,
    calculate_relative_industry_score,
    calculate_relative_valuation_score,
    calculate_long_term_peg_score,
    calculate_dcf_valuation_total,
    calculate_dcf_safety_margin_score,
    get_valuation_level,
)

__all__ = [
    "ValuationAnalyzer",
    "calculate_pe_base_score",
    "calculate_pb_bonus",
    "calculate_ps_bonus",
    "calculate_absolute_valuation_score",
    "calculate_historical_percentile_score",
    "calculate_relative_industry_score",
    "calculate_relative_valuation_score",
    "calculate_long_term_peg_score",
    "calculate_dcf_valuation_total",
    "calculate_dcf_safety_margin_score",
    "get_valuation_level",
]
