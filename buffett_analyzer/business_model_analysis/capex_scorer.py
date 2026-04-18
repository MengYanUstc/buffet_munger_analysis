"""
资本开支（CapEx）效率评分器

评分规则（总分 4 分）：

1. 基础分（0-4分）：根据平均资本开支/净利润比率
   行业类型不同，阈值分布不同：

   轻资产（light）：
   - < 0.20:   4分（资本效率极高）
   - 0.20~0.40: 3分
   - 0.40~0.60: 2分
   - 0.60~0.80: 1分
   - ≥ 0.80:   0分

   中等资产（medium）：
   - < 0.30:   4分
   - 0.30~0.50: 3分
   - 0.50~0.70: 2分
   - 0.70~0.90: 1分
   - ≥ 0.90:   0分

   重资产（heavy）：
   - < 0.40:   4分
   - 0.40~0.60: 3分
   - 0.60~0.80: 2分
   - 0.80~1.00: 1分
   - ≥ 1.00:   0分

2. 稳定性调整（-1~+1分，0.5步长）：资本开支波动率
   - CV < 0.15: +1.0分（极稳定，规划性强）
   - CV < 0.35: +0.5分（相对稳定）
   - CV < 0.55: 0分（正常波动）
   - CV < 0.75: -0.5分（波动较大）
   - CV ≥ 0.75: -1.0分（波动极大，规划性弱）

最终分 = max(0.0, min(4.0, 基础分 + 稳定性调整))
"""

from typing import List, Dict, Any
import statistics

# 行业类型对应的 capex/净利润 阈值分布
_INDUSTRY_THRESHOLDS = {
    "light":  [0.20, 0.40, 0.60, 0.80],   # 轻资产：严格阈值
    "medium": [0.30, 0.50, 0.70, 0.90],   # 中等资产：放宽0.1
    "heavy":  [0.40, 0.60, 0.80, 1.00],   # 重资产：再放宽0.1
}


def compute_capex_score(
    capex_values: List[float],
    net_profit_values: List[float],
    industry_type: str = "medium",
) -> Dict[str, Any]:
    """
    计算资本开支效率评分（总分 4 分）。

    Args:
        capex_values: 各年度资本开支（亿元/万元，与净利润单位一致）
        net_profit_values: 各年度归母净利润
        industry_type: 行业类型（light/medium/heavy）
    """
    if len(capex_values) == 0 or len(net_profit_values) == 0:
        return {"final_score": None, "reason": "数据不足，无法计算"}

    # 对齐长度（取较短者）
    n = min(len(capex_values), len(net_profit_values))
    capex = capex_values[:n]
    profits = net_profit_values[:n]

    # 1. 计算每年资本开支比率
    yearly_details = []
    yearly_ratios = []
    for i, (c, p) in enumerate(zip(capex, profits)):
        ratio = c / p if p != 0 else 999.0
        yearly_ratios.append(ratio)
        yearly_details.append({
            "year_index": i,
            "capex": round(c, 2),
            "net_profit": round(p, 2),
            "ratio": round(ratio, 3),
        })

    avg_ratio = statistics.mean(yearly_ratios) if yearly_ratios else 0.0
    cv_value = _cv(capex)

    # 2. 基础分（0-4分），按行业类型区分阈值
    base_score = _base_score_by_ratio(avg_ratio, industry_type)

    # 3. 稳定性调整（-1~+1分，0.5步长）
    stability_adj = _stability_adjustment(cv_value)

    # 4. 最终分数（严格限制在 [0, 4]）
    raw_score = base_score + stability_adj
    final_score = max(0.0, min(4.0, raw_score))

    reason = (
        f"平均资本开支/净利润比率={avg_ratio:.2f}（行业类型={industry_type}），基础分={base_score}分；"
        f"资本开支波动率(CV)={cv_value:.2f}，稳定性调整={stability_adj:+.1f}分；"
        f"最终得分={final_score:.1f}分（上限4分）"
    )

    return {
        "final_score": round(final_score, 1),
        "base_score": base_score,
        "stability_adjustment": stability_adj,
        "raw_score": round(raw_score, 1),
        "avg_capex_ratio": round(avg_ratio, 3),
        "cv": round(cv_value, 3),
        "industry_type": industry_type,
        "yearly_scores": yearly_details,
        "reason": reason,
    }


def _base_score_by_ratio(ratio: float, industry_type: str = "medium") -> float:
    """根据平均资本开支比率和行业类型计算基础分（0-4分）。"""
    thresholds = _INDUSTRY_THRESHOLDS.get(industry_type, _INDUSTRY_THRESHOLDS["medium"])
    t1, t2, t3, t4 = thresholds

    if ratio < t1:
        return 4.0
    elif ratio < t2:
        return 3.0
    elif ratio < t3:
        return 2.0
    elif ratio < t4:
        return 1.0
    else:
        return 0.0


def _stability_adjustment(cv: float) -> float:
    """
    稳定性调整（-1~+1分，0.5步长）：
    资本开支波动率越低越优秀，波动越大越扣分。
    """
    if cv < 0.15:
        return 1.0
    elif cv < 0.35:
        return 0.5
    elif cv < 0.55:
        return 0.0
    elif cv < 0.75:
        return -0.5
    else:
        return -1.0


def _cv(values: List[float]) -> float:
    """计算变异系数（标准差/均值绝对值）。"""
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    if mean == 0:
        return 0.0
    std = statistics.stdev(values)
    return abs(std / mean)
