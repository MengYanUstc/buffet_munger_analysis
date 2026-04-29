"""
毛利率评分工具（定量部分，满分 5 分）
拆分为：
  - 毛利率绝对值评分（2.5 分）
  - 毛利率稳定性评分（2.5 分）
"""

from typing import List, Dict, Any
import numpy as np


# ------------------------------------------------------------------
# 1. 毛利率绝对值评分（2.5 分）
# ------------------------------------------------------------------

def calculate_gross_margin_absolute_score(avg_gross_margin: float) -> float:
    """
    计算毛利率绝对值评分。

    Args:
        avg_gross_margin: 过去5年平均毛利率（百分比，如 45.2 表示 45.2%）

    Returns:
        毛利率绝对值评分（0-2.5分，步长0.5）
    """
    if avg_gross_margin >= 50:
        return 2.5
    elif avg_gross_margin >= 40:
        return 2.0
    elif avg_gross_margin >= 30:
        return 1.5
    elif avg_gross_margin >= 20:
        return 1.0
    else:
        return 0.0


# ------------------------------------------------------------------
# 2. 毛利率稳定性评分（2.5 分）
# ------------------------------------------------------------------

def calculate_gross_margin_stability_base_score(gross_margin_cv: float) -> float:
    """
    计算毛利率稳定性基础评分（由变异系数 CV 决定）。

    Args:
        gross_margin_cv: 过去5年毛利率变异系数（CV = 标准差 / 均值），小数形式

    Returns:
        毛利率稳定性基础分（0-2.5分，步长0.5）
        阈值: 4%/6%/8%/10%/12%，按0.5分递减
    """
    if gross_margin_cv <= 0.04:
        return 2.5
    elif gross_margin_cv <= 0.06:
        return 2.0
    elif gross_margin_cv <= 0.08:
        return 1.5
    elif gross_margin_cv <= 0.10:
        return 1.0
    elif gross_margin_cv <= 0.12:
        return 0.5
    else:
        return 0.0


def calculate_gross_margin_trend(gross_margin_years: List[float]) -> Dict[str, Any]:
    """
    计算毛利率趋势分析。
    趋势差值 = (后2年平均 - 前2年平均)

    Args:
        gross_margin_years: 毛利率数据列表（按时间顺序），函数内部自动取最近5年

    Returns:
        包含趋势分析结果的字典
    """
    # 只取最近5年数据
    gross_margin_years = gross_margin_years[-5:] if len(gross_margin_years) > 5 else gross_margin_years

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
    综合计算毛利率评分（含绝对值和稳定性两部分）。

    规则：
      - 绝对值评分由平均毛利率决定（0-2 分）
      - 稳定性基础分由变异系数（CV = 标准差/均值）决定（0-3 分）
      - 趋势调整：明显上升+1，温和上升+0.5，温和下降-0.5，明显下降-1
      - 稳定性最终分限制在 [0, 3]

    Returns:
        {
            "absolute": {
                "score": float,       # 绝对值评分（0-2）
                "avg_margin": float,  # 平均毛利率
            },
            "stability": {
                "base_score": float,      # CV基础分（0-2.5）
                "trend_adjustment": float,# 趋势调整值
                "final_score": float,     # 稳定性最终分（0-2.5）
                "cv": float,              # 变异系数（百分比）
                "std": float,             # 标准差
                "trend": dict,            # 趋势分析结果
            },
            "total_score": float,     # 两项合计（0-5）
            "values": list,           # 原始值
        }
    """
    # 只取最近5年数据
    gross_margin_values = gross_margin_values[-5:] if len(gross_margin_values) > 5 else gross_margin_values

    if len(gross_margin_values) < 5:
        return {
            "absolute": {
                "score": None,
                "avg_margin": None,
            },
            "stability": {
                "base_score": None,
                "trend_adjustment": None,
                "final_score": None,
                "cv": None,
                "std": None,
                "trend": {"trend_direction": "数据不足", "note": f"仅有 {len(gross_margin_values)} 年数据"},
            },
            "total_score": None,
            "values": [round(v, 2) for v in gross_margin_values],
        }

    # 绝对值评分
    avg_margin = float(np.mean(gross_margin_values))
    absolute_score = calculate_gross_margin_absolute_score(avg_margin)

    # 稳定性评分（基于变异系数 CV = 标准差 / 均值）
    std = float(np.std(gross_margin_values, ddof=1))
    cv = std / abs(avg_margin) if avg_margin != 0 else 0.0
    stability_base = calculate_gross_margin_stability_base_score(cv)
    trend = calculate_gross_margin_trend(gross_margin_values)

    # 趋势调整：温和 +/-0.5，明显 +/-1.0
    trend_adj = 0.0
    if trend["trend_direction"] == "明显上升":
        trend_adj = 1.0
    elif trend["trend_direction"] == "温和上升":
        trend_adj = 0.5
    elif trend["trend_direction"] == "温和下降":
        trend_adj = -0.5
    elif trend["trend_direction"] == "明显下降":
        trend_adj = -1.0

    stability_final = round(max(0.0, min(2.5, stability_base + trend_adj)) * 2) / 2

    total_score = round(absolute_score + stability_final, 1)

    return {
        "absolute": {
            "score": absolute_score,
            "avg_margin": round(avg_margin, 2),
        },
        "stability": {
            "base_score": stability_base,
            "trend_adjustment": trend_adj,
            "final_score": stability_final,
            "cv": round(cv * 100, 2),   # 变异系数（百分比形式，如 4.76%）
            "std": round(std, 2),
            "trend": trend,
        },
        "total_score": total_score,
        "values": [round(v, 2) for v in gross_margin_values],
    }
