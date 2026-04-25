# -*- coding: utf-8 -*-
"""
指数波段交易系统 v3.0 — 最终版（收益率最高方案）

交易标的：沪深300（sh.000300）、创业板指（sz.399006）

买入：%B < 0（跌破下轨）+ 动量 >= 40 + 中轨不走弱 → 满仓买入
卖出：%B > 85 + 动量5日衰减 > 15 → 全部卖出

原则：买跌不买涨，买入持有，不止损
"""

from typing import Dict, Any
import pandas as pd

from .bollinger import calculate_bollinger
from .momentum import calculate_momentum


# 交易标的
trading_universe = {
    "sh.000300": "沪深300",
    "sz.399006": "创业板指",
}


def get_trade_signal(df: pd.DataFrame) -> Dict[str, Any]:
    """
    生成交易信号（买入/卖出/持有）。

    Returns:
        {
            "signal": "BUY" | "SELL" | "HOLD",
            "action": str,
            "reason": str,
            "price": float,
            "pct_b": float,
            "momentum_score": float,
            "momentum_level": str,
            "ma_trend": str,
        }
    """
    if len(df) < 25:
        return {
            "signal": "HOLD",
            "action": "数据不足，观望",
            "reason": "数据不足",
            "price": None,
            "pct_b": None,
            "momentum_score": None,
            "momentum_level": None,
            "ma_trend": None,
        }

    df_bb = calculate_bollinger(df, period=20, multiplier=2.0)
    latest = df_bb.iloc[-1]
    price = float(latest["close"])
    pct_b = float(latest["pct_b"])
    mb_now = float(latest["mb"])
    mb_5d_ago = float(df_bb.iloc[-6]["mb"]) if len(df_bb) >= 6 else mb_now
    ma_trend = "up" if mb_now >= mb_5d_ago else "down"

    mom = calculate_momentum(df)
    score = mom.get("score", 50)
    level = mom.get("level", "中性震荡")

    # 动量5日衰减
    mom_prev = calculate_momentum(df.head(-5))
    score_prev = mom_prev.get("score", score)
    decay = score_prev - score

    # ========== 买入信号 ==========
    if pct_b < 0 and score >= 40 and ma_trend != "down":
        return {
            "signal": "BUY",
            "action": "回调到底，满仓买入",
            "reason": (
                f"%B={pct_b:.1f} 跌破下轨（<0），"
                f"动量={score:.0f}（{level}），"
                f"中轨趋势={ma_trend}"
            ),
            "price": round(price, 2),
            "pct_b": round(pct_b, 1),
            "momentum_score": round(score, 1),
            "momentum_level": level,
            "ma_trend": ma_trend,
        }

    # ========== 卖出信号 ==========
    if pct_b > 85 and decay > 15:
        return {
            "signal": "SELL",
            "action": "上涨乏力，全部卖出",
            "reason": (
                f"%B={pct_b:.1f} 进入超买区（>85），"
                f"动量5天衰减 {decay:.0f} 分（>15），"
                f"上涨动能减弱"
            ),
            "price": round(price, 2),
            "pct_b": round(pct_b, 1),
            "momentum_score": round(score, 1),
            "momentum_level": level,
            "ma_trend": ma_trend,
        }

    # ========== 持有 ==========
    return {
        "signal": "HOLD",
        "action": "维持现状",
        "reason": (
            f"%B={pct_b:.1f}，动量={score:.0f}（{level}），"
            f"未触发买卖条件"
        ),
        "price": round(price, 2),
        "pct_b": round(pct_b, 1),
        "momentum_score": round(score, 1),
        "momentum_level": level,
        "ma_trend": ma_trend,
    }


def backtest_strategy(df: pd.DataFrame, initial_capital: float = 1000000.0) -> Dict[str, Any]:
    """
    历史回测。空仓时满仓买入，持仓时全部卖出，买入后持有不止损。
    """
    if len(df) < 25:
        return {"error": "数据不足"}

    cash = initial_capital
    shares = 0.0
    position = False
    trades = []
    values = []
    max_value = initial_capital
    max_drawdown = 0.0
    hold_days = 0
    empty_days = 0

    for i in range(20, len(df)):
        slice_df = df.iloc[: i + 1]
        sig = get_trade_signal(slice_df)
        price = sig["price"]

        if price is None:
            continue

        date = str(slice_df.iloc[-1]["trade_date"])

        if not position:
            empty_days += 1
            if sig["signal"] == "BUY":
                shares = cash / price
                cash = 0
                position = True
                trades.append({
                    "date": date,
                    "action": "BUY",
                    "price": price,
                    "reason": sig["reason"],
                })
        else:
            hold_days += 1
            if sig["signal"] == "SELL":
                cash = shares * price
                shares = 0
                position = False
                trades.append({
                    "date": date,
                    "action": "SELL",
                    "price": price,
                    "reason": sig["reason"],
                    "return_pct": round((price / trades[-1]["price"] - 1) * 100, 2) if trades else 0,
                })

        value = cash + shares * price
        values.append(value)
        if value > max_value:
            max_value = value
        drawdown = (max_value - value) / max_value * 100
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    final_value = cash + shares * (df.iloc[-1]["close"] if position else 0)

    win_trades = [t for t in trades if t.get("return_pct", 0) > 0]
    lose_trades = [t for t in trades if t.get("return_pct", 0) <= 0]

    return {
        "trades": trades,
        "trade_count": len(trades),
        "final_value": round(final_value, 2),
        "total_return_pct": round((final_value / initial_capital - 1) * 100, 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "win_count": len(win_trades),
        "lose_count": len(lose_trades),
        "win_rate": round(len(win_trades) / max(1, len(win_trades) + len(lose_trades)) * 100, 1),
        "hold_days": hold_days,
        "empty_days": empty_days,
    }
