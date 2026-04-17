"""
毛利率稳定性评分工具（定量部分，满分 4 分）
"""

from typing import List, Dict, Any
import numpy as np


def calculate_gross_margin_stability_score(gross_margin_std: float) -> float:
    """
    计算毛利率稳定性基础评分。

    Args:
        gross_margin_std: 过去5年毛利率标准差

    Returns:
        毛利率稳定性建议基础分（0-4分）
    """
    if gross_margin_std <= 1:
        return 4.0
    elif gross_margin_std <= 2:
        return 3.0
    elif gross_margin_std <= 3:
        return 2.0
    elif gross_margin_std <= 5:
        return 1.0
    else:
        return 0.0


def calculate_gross_margin_trend(gross_margin_years: List[float]) -> Dict[str, Any]:
    """
    计算毛利率趋势分析。
    趋势差值 = (后2年平均 - 前2年平均)

    Args:
        gross_margin_years: 过去5年毛利率数据列表，[year1, year2, year3, year4, year5]

    Returns:
        包含趋势分析结果的字典
    """
    if len(gross_margin_years) < 5:
        return {
            "trend_diff": None,
            "trend_direction": "数据不足",
            "note": "需要5年数据才能进行趋势分析",
        }

    front_2y_avg = (gross_margin_years[0] + gross_margin_years[1]) / 2
    back_2y_avg = (gross_margin_years[3] + gross_margin_years[4]) / 2
    trend_diff = back_2y_avg - front_2y_avg

    if trend_diff >= 5:
        trend_direction = "明显上升"
    elif trend_diff >= 2:
        trend_direction = "温和上升"
    elif trend_diff >= -2:
        trend_direction = "基本稳定"
    elif trend_diff >= -5:
        trend_direction = "温和下降"
    else:
        trend_direction = "明显下降"

    return {
        "trend_diff": round(trend_diff, 2),
        "trend_direction": trend_direction,
        "front_2y_avg": round(front_2y_avg, 2),
        "back_2y_avg": round(back_2y_avg, 2),
    }


def compute_gross_margin_score(gross_margin_values: List[float]) -> Dict[str, Any]:
    """
    综合计算毛利率稳定性评分（含趋势调整）。

    规则：
      - 基础分由标准差决定（0-4 分）
      - 趋势调整 ±0.5（明显上升+0.5，明显下降-0.5，其他不变）
      - 最终分限制在 [0, 4]

    Returns:
        {
            "base_score": float,      # 标准差基础分
            "trend_adjustment": float,# 趋势调整值
            "final_score": float,     # 最终分（0-4）
            "std": float,             # 标准差
            "values": list,           # 原始值
            "trend": dict,            # 趋势分析结果
        }
    """
    if len(gross_margin_values) < 5:
        return {
            "base_score": None,
            "trend_adjustment": None,
            "final_score": None,
            "std": None,
            "values": [round(v, 2) for v in gross_margin_values],
            "trend": {"trend_direction": "数据不足", "note": f"仅有 {len(gross_margin_values)} 年数据"},
        }

    std = float(np.std(gross_margin_values, ddof=1))
    base_score = calculate_gross_margin_stability_score(std)
    trend = calculate_gross_margin_trend(gross_margin_values)

    # 趋势调整
    trend_adj = 0.0
    if trend["trend_direction"] == "明显上升":
        trend_adj = 0.5
    elif trend["trend_direction"] == "明显下降":
        trend_adj = -0.5

    final_score = round(max(0.0, min(4.0, base_score + trend_adj)) * 2) / 2

    return {
        "base_score": base_score,
        "trend_adjustment": trend_adj,
        "final_score": final_score,
        "std": round(std, 2),
        "values": [round(v, 2) for v in gross_margin_values],
        "trend": trend,
    }
