# -*- coding: utf-8 -*-
"""
布林线（Bollinger Bands）计算工具
基于日K收盘价计算，支持自定义周期和倍数。

公式：
- 中轨（MB）= N日收盘价的简单移动平均（SMA）
- 标准差（STD）= N日收盘价的样本标准差
- 上轨（UP）= MB + k × STD
- 下轨（DN）= MB - k × STD
- 带宽（Bandwidth）= (UP - DN) / MB × 100%
- %B = (Close - DN) / (UP - DN) × 100%

常用参数：N=20, k=2
"""

from typing import Dict, Any, Optional
import pandas as pd
import numpy as np


def calculate_bollinger(
    df: pd.DataFrame,
    period: int = 20,
    multiplier: float = 2.0,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    计算布林线指标，返回带布林线列的 DataFrame。

    Args:
        df: 日K DataFrame，必须包含 close_col 列
        period: 移动平均周期，默认20日
        multiplier: 标准差倍数，默认2.0
        close_col: 收盘价列名

    Returns:
        原 DataFrame 附加以下列：
        - mb: 中轨（N日SMA）
        - up: 上轨
        - dn: 下轨
        - bandwidth: 带宽（%）
        - pct_b: %B 指标（0~100，>100在轨上，<0在轨下）
    """
    if df.empty or close_col not in df.columns:
        return df.copy()

    result = df.copy()
    # 确保收盘价是数值类型
    result[close_col] = pd.to_numeric(result[close_col], errors="coerce")

    # 中轨 = N日SMA
    result["mb"] = result[close_col].rolling(window=period, min_periods=period).mean()

    # 标准差 = N日样本标准差
    result["std"] = result[close_col].rolling(window=period, min_periods=period).std(ddof=1)

    # 上轨、下轨
    result["up"] = result["mb"] + multiplier * result["std"]
    result["dn"] = result["mb"] - multiplier * result["std"]

    # 带宽 = (上轨 - 下轨) / 中轨 × 100%
    result["bandwidth"] = ((result["up"] - result["dn"]) / result["mb"] * 100).round(2)

    # %B = (收盘价 - 下轨) / (上轨 - 下轨) × 100
    band_range = result["up"] - result["dn"]
    result["pct_b"] = ((result[close_col] - result["dn"]) / band_range * 100).round(2)

    # 清理中间列
    result = result.drop(columns=["std"])

    return result


def bollinger_signal(
    df: pd.DataFrame,
    close_col: str = "close",
) -> Optional[str]:
    """
    基于最新一条布林线数据生成简单交易信号。

    规则（仅供参考，后续策略可自行扩展）：
    - 收盘价跌破下轨 → "oversold"（超卖/潜在买点）
    - 收盘价突破上轨 → "overbought"（超买/潜在卖点）
    - 中轨附近 ±1% → "neutral"（中性）
    - 其他 → "watch"

    Returns:
        信号字符串，或 None（数据不足）
    """
    if df.empty or "up" not in df.columns or "dn" not in df.columns:
        return None

    latest = df.iloc[-1]
    close = float(latest[close_col]) if pd.notna(latest.get(close_col)) else None
    up = float(latest["up"]) if pd.notna(latest.get("up")) else None
    dn = float(latest["dn"]) if pd.notna(latest.get("dn")) else None
    mb = float(latest["mb"]) if pd.notna(latest.get("mb")) else None

    if close is None or up is None or dn is None or mb is None:
        return None

    if close < dn:
        return "oversold"
    if close > up:
        return "overbought"
    if abs(close - mb) / mb < 0.01:
        return "neutral"
    return "watch"


def get_latest_bollinger(df: pd.DataFrame) -> Dict[str, Any]:
    """
    获取最新一条布林线数据摘要。

    Returns:
        {
            "trade_date": str,
            "close": float,
            "mb": float,
            "up": float,
            "dn": float,
            "bandwidth": float,
            "pct_b": float,
            "signal": str,
        }
    """
    if df.empty or "up" not in df.columns:
        return {}

    latest = df.iloc[-1]
    signal = bollinger_signal(df)

    return {
        "trade_date": str(latest.get("trade_date", "")),
        "close": float(latest["close"]) if pd.notna(latest.get("close")) else None,
        "mb": round(float(latest["mb"]), 2) if pd.notna(latest.get("mb")) else None,
        "up": round(float(latest["up"]), 2) if pd.notna(latest.get("up")) else None,
        "dn": round(float(latest["dn"]), 2) if pd.notna(latest.get("dn")) else None,
        "bandwidth": float(latest["bandwidth"]) if pd.notna(latest.get("bandwidth")) else None,
        "pct_b": float(latest["pct_b"]) if pd.notna(latest.get("pct_b")) else None,
        "signal": signal,
    }
