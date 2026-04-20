"""
估值评分核心库

评分维度（17分总分，全部定量）：
1. 绝对估值水平（6分）：PE为基础，PB/PS为辅助加分
2. 相对估值（4分）：历史分位 + 行业对比 + 一致性调整
3. 长期PEG（3分）：PE / 盈利增长率
4. DCF 安全边际（4分）：基于 FCF 折现模型计算企业价值，与当前市值比较

DCF 模型说明：
- 采用简化多阶段 FCF 折现模型
- 增长率序列根据 profit_cagr 自动选择
- 折现率默认 8%，永续增长率默认 3%
- 当前市值近似 = PE_ttm × 最近年度净利润（万元）

所有输入数据均从 SQLite 读取。
"""

from typing import Optional, List, Dict, Any


# ------------------------------------------------------------------
# 1. 绝对估值水平（6分）
# ------------------------------------------------------------------

def calculate_pe_base_score(pe: Optional[float]) -> float:
    """
    PE基础评分（0-6分）。

    规则：
    - PE <= 0（亏损）或 PE > 100（异常）：0分
    - PE < 12：6分
    - PE < 15：5分
    - PE < 20：4分
    - PE < 25：3分
    - PE < 30：2分
    - PE < 35：1分
    - PE >= 35：0分
    """
    if pe is None or pe <= 0 or pe > 100:
        return 0.0
    if pe < 12:
        return 6.0
    elif pe < 15:
        return 5.0
    elif pe < 20:
        return 4.0
    elif pe < 25:
        return 3.0
    elif pe < 30:
        return 2.0
    elif pe < 35:
        return 1.0
    else:
        return 0.0


def calculate_pb_bonus(pb: Optional[float]) -> float:
    """
    PB加分项（0到+1分），不扣分。
    """
    if pb is None:
        return 0.0
    if pb < 2.0:
        return 1.0
    elif pb < 3.0:
        return 0.5
    else:
        return 0.0


def calculate_ps_bonus(ps: Optional[float]) -> float:
    """
    PS加分项（0到+1分），不扣分。
    """
    if ps is None:
        return 0.0
    if ps < 2.0:
        return 1.0
    elif ps < 3.0:
        return 0.5
    else:
        return 0.0


def calculate_absolute_valuation_score(
    pe: Optional[float], pb: Optional[float], ps: Optional[float]
) -> float:
    """
    绝对估值水平综合评分（0-6分）。
    """
    pe_base = calculate_pe_base_score(pe)
    pb_bonus = calculate_pb_bonus(pb)
    ps_bonus = calculate_ps_bonus(ps)
    total = pe_base + pb_bonus + ps_bonus
    return max(0.0, min(6.0, total))


# ------------------------------------------------------------------
# 2. 相对估值（4分）
# ------------------------------------------------------------------

def calculate_historical_percentile_score(percentile: Optional[float]) -> float:
    """
    历史估值分位评分（0-3分）。
    """
    if percentile is None:
        return 0.0
    if percentile < 20:
        return 3.0
    elif percentile < 40:
        return 2.0
    elif percentile < 60:
        return 1.5
    elif percentile < 80:
        return 0.5
    else:
        return 0.0


def calculate_relative_industry_score(relative_ratio: Optional[float]) -> float:
    """
    相对行业平均评分（0-1分）。
    relative_ratio = 公司PE / 行业PE
    """
    if relative_ratio is None:
        return 0.0
    if relative_ratio <= 0.85:
        return 1.0
    elif relative_ratio < 1.15:
        return 0.5
    else:
        return 0.0


def calculate_relative_valuation_score(
    historical_percentile: Optional[float],
    relative_industry_ratio: Optional[float],
) -> float:
    """
    相对估值综合评分（0-4分）。
    """
    hist_score = calculate_historical_percentile_score(historical_percentile)
    ind_score = calculate_relative_industry_score(relative_industry_ratio)
    base = hist_score + ind_score

    consistency_bonus = 0.0
    hist_low = historical_percentile is not None and historical_percentile < 40
    hist_high = historical_percentile is not None and historical_percentile > 60
    ind_low = relative_industry_ratio is not None and relative_industry_ratio <= 0.85
    ind_high = relative_industry_ratio is not None and relative_industry_ratio >= 1.15

    if hist_low and ind_low:
        consistency_bonus = 0.5
    elif hist_high and ind_high:
        consistency_bonus = -0.5

    return max(0.0, min(4.0, base + consistency_bonus))


# ------------------------------------------------------------------
# 3. 长期 PEG（3分）
# ------------------------------------------------------------------

def calculate_long_term_peg_score(
    peg: Optional[float], absolute_valuation_score: Optional[float] = None
) -> float:
    """
    长期PEG评分（0-3分）。
    """
    if peg is None or peg <= 0:
        base = 0.0
    elif peg <= 0.8:
        base = 3.0
    elif peg <= 1.2:
        base = 2.5
    elif peg <= 1.6:
        base = 2.0
    elif peg <= 2.0:
        base = 1.0
    else:
        base = 0.0

    if absolute_valuation_score is not None and absolute_valuation_score >= 6.0:
        return max(1.0, base)
    return base


# ------------------------------------------------------------------
# 4. DCF 安全边际（4分）
# ------------------------------------------------------------------

def get_growth_rates_by_cagr(cagr: float) -> tuple:
    """
    根据 profit_cagr 自动选择5年增长序列。

    Returns:
        (增长序列[小数列表], 序列描述)
    """
    if cagr < 0:
        return [0.00, 0.00, 0.00, 0.00, 0.00], "CAGR < 0%（零增长）"
    elif cagr > 0.30:
        return [0.30, 0.25, 0.20, 0.15, 0.10], "CAGR > 30%"
    elif cagr > 0.25:
        return [0.25, 0.20, 0.15, 0.12, 0.08], "CAGR > 25%"
    elif cagr > 0.20:
        return [0.20, 0.15, 0.12, 0.10, 0.08], "CAGR > 20%"
    elif cagr > 0.15:
        return [0.15, 0.12, 0.10, 0.08, 0.08], "CAGR > 15%"
    elif cagr > 0.10:
        return [0.10, 0.08, 0.06, 0.06, 0.06], "CAGR > 10%"
    elif cagr > 0.05:
        return [0.08, 0.06, 0.05, 0.05, 0.05], "CAGR > 5%"
    else:
        return [0.05, 0.05, 0.05, 0.05, 0.05], "CAGR <= 5%"


def calculate_dcf_valuation_total(
    base_fcf: float,
    profit_cagr: float,
    discount_rate: float = 0.08,
    perpetual_growth: float = 0.03,
) -> Dict[str, Any]:
    """
    总层面 DCF 估值（万元）。

    Args:
        base_fcf: 最近年度自由现金流（万元）
        profit_cagr: 净利润复合增长率（小数）
        discount_rate: 折现率（默认 8%）
        perpetual_growth: 永续增长率（默认 3%）

    Returns:
        dict: 包含企业价值、现值分解、增长序列等信息
    """
    growth_rates, seq_name = get_growth_rates_by_cagr(profit_cagr)

    # 5年 FCF 现值
    pv_fcf = 0.0
    for i, g in enumerate(growth_rates):
        future_fcf = base_fcf * (1 + g) ** (i + 1)
        pv_fcf += future_fcf / (1 + discount_rate) ** (i + 1)

    # 终值
    year5_fcf = base_fcf * (1 + growth_rates[-1]) ** 5
    terminal_value = year5_fcf * (1 + perpetual_growth) / (discount_rate - perpetual_growth)
    terminal_pv = terminal_value / (1 + discount_rate) ** 5

    enterprise_value = pv_fcf + terminal_pv

    return {
        "enterprise_value": round(enterprise_value, 2),
        "pv_fcf": round(pv_fcf, 2),
        "terminal_value": round(terminal_value, 2),
        "terminal_pv": round(terminal_pv, 2),
        "growth_rates": [round(g * 100, 1) for g in growth_rates],
        "sequence_name": seq_name,
        "discount_rate": discount_rate,
        "perpetual_growth": perpetual_growth,
    }


def calculate_dcf_safety_margin_score(
    enterprise_value: Optional[float],
    market_cap_approx: Optional[float],
) -> float:
    """
    DCF 安全边际评分（0-4分）。

    Args:
        enterprise_value: DCF 计算的企业价值（万元）
        market_cap_approx: 近似当前市值 = PE_ttm × 最近年度净利润（万元）

    Returns:
        0-4分
    """
    if enterprise_value is None or market_cap_approx is None:
        return 0.0
    if market_cap_approx <= 0 or enterprise_value <= 0:
        return 0.0

    safety_margin = enterprise_value / market_cap_approx

    if safety_margin >= 1.5:
        return 4.0
    elif safety_margin >= 1.3:
        return 3.0
    elif safety_margin >= 1.1:
        return 2.0
    elif safety_margin >= 1.0:
        return 1.0
    else:
        return 0.0


# ------------------------------------------------------------------
# 评级
# ------------------------------------------------------------------

def get_valuation_level(total_score: float) -> str:
    """根据总分获取估值评级。"""
    if total_score >= 14:
        return "极具吸引力"
    elif total_score >= 10:
        return "合理偏低"
    elif total_score >= 6:
        return "合理"
    else:
        return "偏高"
