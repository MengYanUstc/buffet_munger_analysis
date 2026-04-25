# -*- coding: utf-8 -*-
"""
综合动量（Momentum）评分模型

融合5个维度的价格动量信息，输出 0~100 的综合得分：
- 80~100: 极强多头动量
- 60~79 : 较强多头
- 40~59 : 中性/震荡
- 20~39 : 较弱空头
- 0~19  : 极强空头

维度及权重：
1. 短期价格动量（10日ROC）     权重 25%
2. 中期价格动量（20日ROC）     权重 15%
3. 连续涨跌天数（10日）         权重 20%
4. 均线排列得分（5/10/20日）    权重 20%
5. 量价配合得分               权重 20%
"""

from typing import Dict, Any
import pandas as pd
import numpy as np


def calculate_momentum(df: pd.DataFrame, close_col: str = "close", volume_col: str = "volume") -> Dict[str, Any]:
    """
    计算综合动量得分。

    Args:
        df: 日K DataFrame，必须包含 close_col 和 volume_col
        close_col: 收盘价列名
        volume_col: 成交量列名

    Returns:
        {
            "score": float,          # 0~100 综合得分
            "level": str,            # 极强多头/较强多头/中性/较弱空头/极强空头
            "details": {
                "roc_10d": {"value": float, "sub_score": float, "weight": float},
                "roc_20d": {"value": float, "sub_score": float, "weight": float},
                "up_days_10d": {"value": int, "sub_score": float, "weight": float},
                "ma_alignment": {"value": str, "sub_score": float, "weight": float},
                "volume_score": {"value": float, "sub_score": float, "weight": float},
            },
            "trend_direction": str,   # up / down / sideways
            "confidence": str,        # high / medium / low
        }
    """
    if len(df) < 25:
        return {
            "score": 50.0,
            "level": "数据不足",
            "details": {},
            "trend_direction": "unknown",
            "confidence": "low",
        }

    df = df.copy()
    df[close_col] = pd.to_numeric(df[close_col], errors="coerce")
    if volume_col in df.columns:
        df[volume_col] = pd.to_numeric(df[volume_col], errors="coerce")

    # ========== 1. 短期价格动量（10日ROC）==========
    close_now = df[close_col].iloc[-1]
    close_10d = df[close_col].iloc[-11]
    roc_10d = (close_now - close_10d) / close_10d * 100 if close_10d != 0 else 0
    # 子得分：-15%~+15% 映射到 0~100
    score_roc_10d = min(100, max(0, 50 + roc_10d * 50 / 15))

    # ========== 2. 中期价格动量（20日ROC）==========
    close_20d = df[close_col].iloc[-21]
    roc_20d = (close_now - close_20d) / close_20d * 100 if close_20d != 0 else 0
    score_roc_20d = min(100, max(0, 50 + roc_20d * 50 / 20))

    # ========== 3. 连续涨跌天数（10日）==========
    up_days = 0
    for i in range(-10, 0):
        if df[close_col].iloc[i] > df[close_col].iloc[i - 1]:
            up_days += 1
    # 子得分：0~10天 映射到 0~100
    score_up_days = up_days * 10

    # ========== 4. 均线排列得分 ==========
    ma5 = df[close_col].tail(5).mean()
    ma10 = df[close_col].tail(10).mean()
    ma20 = df[close_col].tail(20).mean()

    ma_score = 50  # 基准中性
    ma_trend = "sideways"

    if ma5 > ma10 > ma20:
        # 多头排列
        diff_5_10 = (ma5 - ma10) / ma10 * 100
        diff_10_20 = (ma10 - ma20) / ma20 * 100
        ma_score = min(100, 70 + diff_5_10 * 5 + diff_10_20 * 3)
        ma_trend = "up"
    elif ma5 < ma10 < ma20:
        # 空头排列
        diff_5_10 = (ma10 - ma5) / ma10 * 100
        diff_10_20 = (ma20 - ma10) / ma20 * 100
        ma_score = max(0, 30 - diff_5_10 * 5 - diff_10_20 * 3)
        ma_trend = "down"
    else:
        # 混乱排列（金叉/死叉中）
        ma_score = 50
        ma_trend = "sideways"

    # ========== 5. 量价配合得分 ==========
    vol_score = 50  # 默认中性
    if volume_col in df.columns and df[volume_col].notna().sum() >= 10:
        # 最近5天
        recent_5 = df.tail(5)
        prev_5 = df.iloc[-10:-5]

        # 上涨天数成交量 vs 下跌天数成交量
        up_vol = 0
        down_vol = 0
        up_count = 0
        down_count = 0

        for i in range(-5, 0):
            vol = df[volume_col].iloc[i]
            if df[close_col].iloc[i] > df[close_col].iloc[i - 1]:
                up_vol += vol
                up_count += 1
            else:
                down_vol += vol
                down_count += 1

        if down_vol > 0 and up_count > 0 and down_count > 0:
            ratio = up_vol / down_vol
            # 上涨放量 > 1.2倍 → 多头确认；< 0.8倍 → 上涨无量，可疑
            vol_score = min(100, max(0, 50 + (ratio - 1) * 50))
        elif up_count == 5:
            vol_score = 75  # 连涨5天，默认偏多
        elif down_count == 5:
            vol_score = 25  # 连跌5天，默认偏空

    # ========== 综合加权 ==========
    weights = {
        "roc_10d": 0.25,
        "roc_20d": 0.15,
        "up_days_10d": 0.20,
        "ma_alignment": 0.20,
        "volume_score": 0.20,
    }

    final_score = (
        score_roc_10d * weights["roc_10d"]
        + score_roc_20d * weights["roc_20d"]
        + score_up_days * weights["up_days_10d"]
        + ma_score * weights["ma_alignment"]
        + vol_score * weights["volume_score"]
    )

    # 置信度：各子得分差异大 → 低置信；差异小 → 高置信
    sub_scores = [score_roc_10d, score_roc_20d, score_up_days, ma_score, vol_score]
    std_sub = np.std(sub_scores)
    confidence = "high" if std_sub < 15 else "medium" if std_sub < 30 else "low"

    level = (
        "极强多头" if final_score >= 80 else
        "较强多头" if final_score >= 60 else
        "中性震荡" if final_score >= 40 else
        "较弱空头" if final_score >= 20 else
        "极强空头"
    )

    return {
        "score": round(final_score, 1),
        "level": level,
        "details": {
            "roc_10d": {
                "value": round(roc_10d, 2),
                "sub_score": round(score_roc_10d, 1),
                "weight": weights["roc_10d"],
            },
            "roc_20d": {
                "value": round(roc_20d, 2),
                "sub_score": round(score_roc_20d, 1),
                "weight": weights["roc_20d"],
            },
            "up_days_10d": {
                "value": up_days,
                "sub_score": round(score_up_days, 1),
                "weight": weights["up_days_10d"],
            },
            "ma_alignment": {
                "value": ma_trend,
                "sub_score": round(ma_score, 1),
                "weight": weights["ma_alignment"],
            },
            "volume_score": {
                "value": round(vol_score, 1),
                "sub_score": round(vol_score, 1),
                "weight": weights["volume_score"],
            },
        },
        "trend_direction": ma_trend,
        "confidence": confidence,
    }


def momentum_signal(momentum_result: Dict[str, Any], pct_b: float = None) -> str:
    """
    综合动量 + 布林线 %B 生成交易信号。

    规则：
    - momentum 极强 + %B 触轨 → 趋势加速，持有
    - momentum 中性 + %B 触轨 → 极值反转，操作
    - momentum 与 %B 方向矛盾 → 观望
    """
    score = momentum_result.get("score", 50)
    level = momentum_result.get("level", "中性震荡")
    conf = momentum_result.get("confidence", "medium")

    # 根据动量强度调整布林线阈值
    if score >= 70:  # 较强/极强多头
        if pct_b is not None and pct_b > 100:
            return "hold_trend"  # 趋势中，别卖
        if pct_b is not None and pct_b < 0:
            return "buy_dip"  # 回调买入机会
        return "hold"

    if score <= 30:  # 较弱/极强空头
        if pct_b is not None and pct_b < 0:
            return "wait"  # 下跌趋势，别抄底
        if pct_b is not None and pct_b > 100:
            return "sell_bounce"  # 反弹卖出
        return "hold"

    # 中性震荡（40~60）→ 布林线极值策略最有效
    if pct_b is not None:
        if pct_b > 100:
            return "sell"
        if pct_b < 0:
            return "buy"

    return "hold"
