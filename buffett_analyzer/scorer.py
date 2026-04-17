"""
企业质量分析评分器
根据巴菲特-芒格投资理念，对ROE、ROIC、增长、负债等维度进行定量评分。
"""

import math
from typing import List, Union, Dict, Any
import numpy as np


def round_score(score: float, step: float = 0.5, max_score: float = None, min_score: float = 0.0) -> float:
    """将分数按最小步长四舍五入，并限制在上下界内。"""
    score = round(score / step) * step
    if max_score is not None:
        score = min(score, max_score)
    if min_score is not None:
        score = max(score, min_score)
    return float(score)


def score_roe(avg_roe: float) -> float:
    """ROE评分（满分4分）"""
    if avg_roe >= 25:   return 4.0
    elif avg_roe >= 20: return 3.5
    elif avg_roe >= 15: return 3.0
    elif avg_roe >= 12: return 2.0
    elif avg_roe >= 8:  return 1.0
    elif avg_roe >= 5:  return 0.5
    else:               return 0.0


def score_roic(avg_roic: float) -> float:
    """ROIC评分（满分6分）"""
    if avg_roic >= 20:   return 6.0
    elif avg_roic >= 15: return 5.0
    elif avg_roic >= 12: return 4.0
    elif avg_roic >= 8:  return 3.0
    elif avg_roic >= 5:  return 1.5
    else:                return 0.0


def score_growth(cagr: float) -> float:
    """营收/利润增长评分（满分3分）"""
    if cagr >= 20:   return 3.0
    elif cagr >= 15: return 2.5
    elif cagr >= 8:  return 2.0
    elif cagr >= 3:  return 1.0
    else:            return 0.0


def calculate_cagr(values: List[float]) -> Union[float, None]:
    """
    计算复合年均增长率（CAGR）。
    如果期初或期末值为非正数，返回 None（避免误导性计算）。
    """
    if len(values) < 2:
        return None
    start_val = values[0]
    end_val = values[-1]
    if start_val <= 0 or end_val <= 0:
        return None
    years = len(values) - 1
    cagr = (end_val / start_val) ** (1 / years) - 1
    return cagr * 100


def analyze_roe_stability(roes: List[float]) -> Dict[str, Any]:
    """
    ROE稳定性分析（供AI定性判断，满分2分）。
    输出建议基础分、波动级别、趋势方向等定量信息。
    """
    if len(roes) < 4:
        return {
            "error": "ROE数据不足4年，无法进行稳定性分析",
            "roe_values": [round(x, 2) for x in roes]
        }

    std = float(np.std(roes, ddof=1))
    mean = float(np.mean(roes))

    # 稳定性级别与建议基础分
    if std <= 3:
        stability = "高度稳定"
        base_score = 2.0
    elif std <= 5:
        stability = "比较稳定"
        base_score = 1.5
    elif std <= 7:
        stability = "一般稳定"
        base_score = 1.0
    elif std <= 9:
        stability = "较不稳定"
        base_score = 0.5
    else:
        stability = "很不稳定"
        base_score = 0.0

    # 趋势方向判断（后2年均值 - 前2年均值）
    first_two = np.mean(roes[:2])
    last_two = np.mean(roes[-2:])
    trend_diff = float(last_two - first_two)

    if trend_diff >= 3:       trend = "明显上升"
    elif trend_diff >= 1:     trend = "温和上升"
    elif trend_diff >= -1:    trend = "基本稳定"
    elif trend_diff >= -3:    trend = "温和下降"
    else:                     trend = "明显下降"

    lower = round_score(max(0.0, base_score - 1.0), step=0.5)
    upper = round_score(min(2.0, base_score + 1.0), step=0.5)

    return {
        "roe_values": [round(x, 2) for x in roes],
        "roe_std": round(std, 2),
        "roe_mean": round(mean, 2),
        "roe_min": round(min(roes), 2),
        "roe_max": round(max(roes), 2),
        "stability_level": stability,
        "trend_direction": trend,
        "trend_diff": round(trend_diff, 2),
        "suggested_base_score": base_score,
        "ai_adjustment_range": f"{lower:.1f} - {upper:.1f} 分",
        "max_score": 2.0,
        "min_score": 0.0
    }


def analyze_debt_ratio(ratio: float, industry_type: str = "general") -> Dict[str, Any]:
    """
    资产负债率分析（供AI定性判断，满分2分）。
    根据不同行业的合理负债区间给出建议基础分。
    """
    thresholds = {
        "general":     [(30, 2.0, "低"), (50, 1.5, "中等"), (70, 1.0, "较高"), (float('inf'), 0.0, "过高")],
        "banking":     [(85, 2.0, "低"), (90, 1.5, "中等"), (93, 1.0, "较高"), (float('inf'), 0.0, "过高")],
        "insurance":   [(80, 2.0, "低"), (85, 1.5, "中等"), (90, 1.0, "较高"), (float('inf'), 0.0, "过高")],
        "real_estate": [(60, 2.0, "低"), (70, 1.5, "中等"), (80, 1.0, "较高"), (float('inf'), 0.0, "过高")],
        "utilities":   [(50, 2.0, "低"), (60, 1.5, "中等"), (70, 1.0, "较高"), (float('inf'), 0.0, "过高")],
    }

    levels = thresholds.get(industry_type, thresholds["general"])
    base_score = 0.0
    level_desc = "过高"

    for threshold, score, desc in levels:
        if ratio <= threshold:
            base_score = score
            level_desc = desc
            break

    lower = round_score(max(0.0, base_score - 0.5), step=0.5)
    upper = round_score(min(2.0, base_score + 0.5), step=0.5)

    return {
        "debt_ratio": round(ratio, 2),
        "industry_type": industry_type,
        "debt_level": level_desc,
        "suggested_base_score": base_score,
        "ai_adjustment_range": f"{lower:.1f} - {upper:.1f} 分",
        "max_score": 2.0,
        "min_score": 0.0
    }
