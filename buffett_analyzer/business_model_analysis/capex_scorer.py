"""
资本开支（CapEx）效率评分器

评分规则：
1. 基础分（0-4分）：根据资本开支/净利润比率
   - < 0.2: 4分（资本效率极高）
   - 0.2-0.4: 3分
   - 0.4-0.6: 2分
   - 0.6-0.8: 1分
   - > 0.8: 0分

2. 稳定性调整（±2分）：根据资本开支波动率
   - 波动率极低（std/mean < 0.1）: +2分
   - 低波动（std/mean 0.1-0.3）: +1分
   - 中波动（std/mean 0.3-0.5）: 0分
   - 高波动（std/mean 0.5-1.0）: -1分
   - 极高波动（std/mean > 1.0）: -2分

3. 行业与发展阶段调整：
   - 轻资产（light）：+1分（天然 capex 低）
   - 中资产（medium）：0分
   - 重资产（heavy）：-1分（capex 高是行业特性）
   - 成熟期（mature）：+1分（成熟企业 capex 应下降）
   - 成长期（growth）：0分
   - 初创期（startup）：-1分（初创 capex 高是正常的）
   - 衰退期（decline）：-1分（衰退期 capex 高可能无意义）

总分 = base_score + stability_adj + industry_adj + stage_adj
最终分数限制在 [0, 10] 区间
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
    计算资本开支效率评分。

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
    yearly_ratios = []
    yearly_details = []
    for i, (c, p) in enumerate(zip(capex, profits)):
        if p != 0:
            ratio = c / p
        else:
            ratio = 999.0  # 净利润为0时视为极高
        yearly_ratios.append(ratio)
        yearly_details.append({
            "year_index": i,
            "capex": c,
            "net_profit": p,
            "ratio": round(ratio, 2),
        })

    avg_ratio = statistics.mean(yearly_ratios) if yearly_ratios else 0.0

    # 2. 基础分（0-4分）
    base_score = _base_score_by_ratio(avg_ratio)

    # 3. 稳定性调整（±2分）
    stability_adj = _stability_adjustment(capex)

    # 4. 行业类型调整
    industry_adj = _industry_adjustment(industry_type)

    # 5. 发展阶段调整
    stage_adj = _growth_stage_adjustment(growth_stage)

    # 6. 最终分数
    final_score = base_score + stability_adj + industry_adj + stage_adj
    final_score = max(0.0, min(10.0, final_score))

    reason = (
        f"平均资本开支/净利润比率={avg_ratio:.2f}，基础分={base_score}分\n"
        f"资本开支波动率={_cv(capex):.2f}，稳定性调整={stability_adj:+d}分\n"
        f"行业类型={industry_type}，调整={industry_adj:+d}分\n"
        f"发展阶段={growth_stage}，调整={stage_adj:+d}分\n"
        f"最终得分={final_score:.1f}分"
    )

    return {
        "final_score": round(final_score, 1),
        "base_score": base_score,
        "stability_adjustment": stability_adj,
        "industry_adjustment": industry_adj,
        "growth_stage_adjustment": stage_adj,
        "avg_capex_ratio": round(avg_ratio, 2),
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


def _stability_adjustment(capex_values: List[float]) -> int:
    """根据资本开支波动性计算调整分（±2分）。"""
    cv = _cv(capex_values)
    if cv < 0.1:
        return 2
    elif cv < 0.3:
        return 1
    elif cv < 0.5:
        return 0
    elif cv < 1.0:
        return -1
    else:
        return -2


def _industry_adjustment(industry_type: str) -> int:
    """行业类型调整（轻资产+1，重资产-1）。"""
    if industry_type == "light":
        return 1
    elif industry_type == "heavy":
        return -1
    else:
        return 0


def _growth_stage_adjustment(growth_stage: str) -> int:
    """发展阶段调整（成熟期+1，初创/衰退-1）。"""
    if growth_stage == "mature":
        return 1
    elif growth_stage in ("startup", "decline"):
        return -1
    else:
        return 0


def _cv(values: List[float]) -> float:
    """计算变异系数（标准差/均值）。"""
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    if mean == 0:
        return 0.0
    std = statistics.stdev(values)
    return abs(std / mean)
