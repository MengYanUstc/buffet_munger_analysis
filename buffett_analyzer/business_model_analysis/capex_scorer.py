"""
资本开支（CapEx）效率评分器

评分规则（总分 2 分）：
只看平均资本开支/净利润比率，不考虑波动。

轻资产（light）：
- < 0.20:   2分（资本效率极高）
- 0.20~0.40: 1.5分
- 0.40~0.60: 1.0分
- 0.60~0.80: 0.5分
- ≥ 0.80:   0分

中等资产（medium）：
- < 0.30:   2分
- 0.30~0.50: 1.5分
- 0.50~0.70: 1.0分
- 0.70~0.90: 0.5分
- ≥ 0.90:   0分

重资产（heavy）：
- < 0.40:   2分
- 0.40~0.60: 1.5分
- 0.60~0.80: 1.0分
- 0.80~1.00: 0.5分
- ≥ 1.00:   0分

最终分 = 基础分（0 / 0.5 / 1.0 / 1.5 / 2.0）
"""

from typing import List, Dict, Any
import statistics

# 行业类型对应的 capex/净利润 阈值分布
_INDUSTRY_THRESHOLDS = {
    "light":  [0.20, 0.40, 0.60, 0.80],   # 轻资产：宽松阈值
    "medium": [0.30, 0.50, 0.70, 0.90],   # 中等资产
    "heavy":  [0.40, 0.60, 0.80, 1.00],   # 重资产
}


def compute_capex_score(
    capex_values: List[float],
    net_profit_values: List[float],
    industry_type: str = "medium",
    phase_type: str = "mature",
) -> Dict[str, Any]:
    """
    计算资本开支效率评分（总分 2 分）。

    Args:
        capex_values: 各年度资本开支（亿元/万元，与净利润单位一致）
        net_profit_values: 各年度归母净利润
        industry_type: 行业类型（light/medium/heavy）
        phase_type: 发展阶段（startup/growth/mature/decline）
    """
    if len(capex_values) == 0 or len(net_profit_values) == 0:
        return {"final_score": None, "reason": "数据不足，无法计算"}

    # 对齐长度（取较短者）
    n = min(len(capex_values), len(net_profit_values))
    capex = capex_values[:n]
    profits = net_profit_values[:n]

    # 1. 计算每年资本开支比率
    # 防护：净利润 <= 0、NaN、Inf 时该年记录跳过，比率无意义
    import math
    yearly_details = []
    yearly_ratios = []
    for i, (c, p) in enumerate(zip(capex, profits)):
        if not (math.isfinite(c) and math.isfinite(p)):
            continue
        if p <= 0:
            continue
        ratio = c / p
        yearly_ratios.append(ratio)
        yearly_details.append({
            "year_index": i,
            "capex": round(c, 2),
            "net_profit": round(p, 2),
            "ratio": round(ratio, 3),
        })

    if not yearly_ratios:
        return {"final_score": None, "reason": "净利润均 <= 0 或数据异常，资本开支比率无意义"}

    avg_ratio = statistics.mean(yearly_ratios) if yearly_ratios else 0.0

    # 2. 基础分（0-2分），按行业类型区分阈值
    base_score = _base_score_by_ratio(avg_ratio, industry_type)

    # 3. 阶段找补（保留）
    phase_bonus = 0.5 if phase_type in ("startup", "growth") else 0.0

    # 最终分 = 基础分 + 阶段找补（只看比率和阶段，不考虑波动）
    final_score = round(max(0.0, min(2.0, base_score + phase_bonus)) * 2) / 2

    reason = (
        f"平均资本开支/净利润比率={avg_ratio:.2f}（行业类型={industry_type}），"
        f"基础分={base_score}分；"
        f"阶段找补={phase_bonus:+.1f}分；"
        f"最终得分={final_score:.1f}分（满分2分）。"
        f"只看比率，不考虑波动。"
    )

    return {
        "final_score": round(final_score, 1),
        "base_score": base_score,
        "stability_adjustment": 0.0,
        "phase_bonus": phase_bonus,
        "raw_score": round(final_score, 1),
        "avg_capex_ratio": round(avg_ratio, 3),
        "cv": 0.0,
        "industry_type": industry_type,
        "phase_type": phase_type,
        "yearly_scores": yearly_details,
        "reason": reason,
    }


def _base_score_by_ratio(ratio: float, industry_type: str = "medium") -> float:
    """根据平均资本开支比率和行业类型计算基础分（0-2分）。"""
    thresholds = _INDUSTRY_THRESHOLDS.get(industry_type, _INDUSTRY_THRESHOLDS["medium"])
    t1, t2, t3, t4 = thresholds

    if ratio < t1:
        return 2.0
    elif ratio < t2:
        return 1.5
    elif ratio < t3:
        return 1.0
    elif ratio < t4:
        return 0.5
    else:
        return 0.0





def _cv(values: List[float]) -> float:
    """计算变异系数（标准差/均值绝对值）。"""
    import math
    if len(values) < 2:
        return 0.0
    # 过滤非有限值，防止 statistics 模块崩溃
    clean = [v for v in values if math.isfinite(v)]
    if len(clean) < 2:
        return 0.0
    mean = statistics.mean(clean)
    if mean == 0:
        return 0.0
    std = statistics.stdev(clean)
    return abs(std / mean)
