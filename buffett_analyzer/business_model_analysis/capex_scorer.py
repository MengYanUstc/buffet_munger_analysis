"""
资本开支（CapEx）效率评分器

评分规则（总分 4 分）：

1. 基础分（0-4分）：根据平均资本开支/净利润比率
   - < 0.2:  4分（资本效率极高）
   - 0.2~0.4: 3分
   - 0.4~0.6: 2分
   - 0.6~0.8: 1分
   - ≥ 0.8:   0分

2. 稳定性找补（0~+1分）：资本开支波动率越低越优秀
   - CV < 0.15: +1.0分（极稳定，规划性强）
   - CV < 0.35: +0.5分（相对稳定）
   - 否则: 0分

3. 行业找补（0~+0.5分）：重资产企业 capex 高是行业特性
   - heavy（重资产）: +0.5分
   - medium / light: 0分

4. 阶段找补（0~+0.5分）：成长期/初创期 capex 高是扩张需要
   - growth / startup: +0.5分
   - mature / decline: 0分

最终分 = min(4.0, 基础分 + 稳定性找补 + 行业找补 + 阶段找补)

设计原则：三项调整均为"找补"——针对被基础分"一刀切"低估的企业进行补偿，
不用于给已经高分的企业继续加分。最终分严格限制在 [0, 4]。
"""

from typing import List, Dict, Any
import statistics


def compute_capex_score(
    capex_values: List[float],
    net_profit_values: List[float],
    industry_type: str = "medium",
    growth_stage: str = "mature",
) -> Dict[str, Any]:
    """
    计算资本开支效率评分（总分 4 分）。

    Args:
        capex_values: 各年度资本开支（亿元/万元，与净利润单位一致）
        net_profit_values: 各年度归母净利润
        industry_type: 行业类型（light/medium/heavy）
        growth_stage: 发展阶段（startup/growth/mature/decline）
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

    # 2. 基础分（0-4分）
    base_score = _base_score_by_ratio(avg_ratio)

    # 3. 稳定性找补（0~+1分）
    stability_adj = _stability_adjustment(capex)

    # 4. 行业找补（0~+0.5分）
    industry_adj = _industry_adjustment(industry_type)

    # 5. 阶段找补（0~+0.5分）
    stage_adj = _growth_stage_adjustment(growth_stage)

    # 6. 最终分数（严格限制在 [0, 4]）
    raw_score = base_score + stability_adj + industry_adj + stage_adj
    final_score = max(0.0, min(4.0, raw_score))

    reason = (
        f"平均资本开支/净利润比率={avg_ratio:.2f}，基础分={base_score}分；"
        f"资本开支波动率(CV)={_cv(capex):.2f}，稳定性找补={stability_adj:+.1f}分；"
        f"行业类型={industry_type}，行业找补={industry_adj:+.1f}分；"
        f"发展阶段={growth_stage}，阶段找补={stage_adj:+.1f}分；"
        f"最终得分={final_score:.1f}分（上限4分）"
    )

    return {
        "final_score": round(final_score, 1),
        "base_score": base_score,
        "stability_adjustment": stability_adj,
        "industry_adjustment": industry_adj,
        "growth_stage_adjustment": stage_adj,
        "raw_score": round(raw_score, 1),
        "avg_capex_ratio": round(avg_ratio, 3),
        "industry_type": industry_type,
        "growth_stage": growth_stage,
        "yearly_scores": yearly_details,
        "reason": reason,
    }


def _base_score_by_ratio(ratio: float) -> float:
    """根据平均资本开支比率计算基础分（0-4分）。"""
    if ratio < 0.2:
        return 4.0
    elif ratio < 0.4:
        return 3.0
    elif ratio < 0.6:
        return 2.0
    elif ratio < 0.8:
        return 1.0
    else:
        return 0.0


def _stability_adjustment(capex_values: List[float]) -> float:
    """稳定性找补（0~+1分）：资本开支波动率越低越优秀。"""
    cv = _cv(capex_values)
    if cv < 0.15:
        return 1.0
    elif cv < 0.35:
        return 0.5
    else:
        return 0.0


def _industry_adjustment(industry_type: str) -> float:
    """行业找补（0~+0.5分）：重资产企业 capex 高是行业特性。"""
    return 0.5 if industry_type == "heavy" else 0.0


def _growth_stage_adjustment(growth_stage: str) -> float:
    """阶段找补（0~+0.5分）：成长期/初创期 capex 高是扩张需要。"""
    return 0.5 if growth_stage in ("startup", "growth") else 0.0


def _cv(values: List[float]) -> float:
    """计算变异系数（标准差/均值绝对值）。"""
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    if mean == 0:
        return 0.0
    std = statistics.stdev(values)
    return abs(std / mean)
