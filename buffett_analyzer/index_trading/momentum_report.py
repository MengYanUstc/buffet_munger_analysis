# -*- coding: utf-8 -*-
"""
每日开盘前动量报告生成器（优化排版版，支持50+只股票）

排版策略：
- 指数：保持展开式（2个）
- 股票：汇总表格（一行一只，一目了然）
- 关键发现：自动提取
"""

import os
import re
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

from ..data_warehouse.cache_manager import CacheManager
from ..data_warehouse.database import Database
from .bollinger import calculate_bollinger
from .momentum import calculate_momentum


def scan_high_score_stocks(reports_dir="reports/latest", min_score=70.0):
    stock_map = {}
    for fname in os.listdir(reports_dir):
        if not fname.endswith(".md"):
            continue
        fpath = os.path.join(reports_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            continue
        score_match = re.search(r"(\d+\.\d)/100", content)
        if not score_match:
            continue
        score = float(score_match.group(1))
        if score < min_score:
            continue
        code_match = re.search(r"(\d{6})", fname)
        name_match = re.search(r"_\d{6}_(.+?)_\d{8}\.md", fname)
        code = code_match.group(1) if code_match else ""
        name = name_match.group(1) if name_match else ""
        if code not in stock_map or score > stock_map[code]["score"]:
            stock_map[code] = {"code": code, "name": name, "score": score}
    return sorted(stock_map.values(), key=lambda x: x["score"], reverse=True)


def calc_momentum_series(df, days=20):
    results = []
    if len(df) < 25:
        return results
    for offset in range(-days, 1):
        end_idx = len(df) + offset
        if end_idx < 25:
            continue
        slice_df = df.iloc[:end_idx]
        mom = calculate_momentum(slice_df)
        results.append({
            "date": str(slice_df.iloc[-1]["trade_date"]),
            "score": round(mom.get("score", 50), 1),
            "level": mom.get("level", ""),
        })
    return results


def momentum_trend(series):
    if len(series) < 2:
        return {"trend": "未知", "change_1d": 0, "change_5d": 0, "change_10d": 0,
                "from_peak": 0, "from_trough": 0, "peak_date": "", "trough_date": "",
                "peak_score": 0, "trough_score": 0}

    latest = series[-1]["score"]
    prev = series[-2]["score"]
    change_1d = latest - prev
    change_5d = latest - series[-6]["score"] if len(series) >= 6 else 0
    change_10d = latest - series[-11]["score"] if len(series) >= 11 else 0

    scores = [s["score"] for s in series]
    peak_score = max(scores)
    trough_score = min(scores)
    peak_idx = scores.index(peak_score)
    trough_idx = scores.index(trough_score)

    from_peak = latest - peak_score
    from_trough = latest - trough_score

    if change_5d > 10:
        trend = "加速上涨"
    elif change_5d > 3:
        trend = "温和上涨"
    elif change_5d < -10:
        trend = "加速下跌"
    elif change_5d < -3:
        trend = "温和下跌"
    else:
        trend = "横盘整理"

    return {
        "trend": trend,
        "change_1d": round(change_1d, 1),
        "change_5d": round(change_5d, 1),
        "change_10d": round(change_10d, 1),
        "from_peak": round(from_peak, 1),
        "from_trough": round(from_trough, 1),
        "peak_date": series[peak_idx]["date"],
        "trough_date": series[trough_idx]["date"],
        "peak_score": round(peak_score, 1),
        "trough_score": round(trough_score, 1),
    }


def generate_trend_desc(series):
    """根据20天动量序列生成中文走势描述（替代ASCII sparkline）。"""
    if len(series) < 5:
        return "数据不足"

    scores = [s["score"] for s in series]
    latest = scores[-1]
    mn = min(scores)
    mx = max(scores)

    # 当前在20天区间中的位置
    if mx == mn:
        position = "中位"
    else:
        ratio = (latest - mn) / (mx - mn)
        if ratio >= 0.75:
            position = "高位"
        elif ratio <= 0.25:
            position = "低位"
        else:
            position = "中位"

    # 近5天变化趋势
    change_5d = latest - scores[-6] if len(scores) >= 6 else latest - scores[0]

    if change_5d > 12:
        trend = "急升"
    elif change_5d > 4:
        trend = "回升"
    elif change_5d < -12:
        trend = "急跌"
    elif change_5d < -4:
        trend = "回落"
    else:
        trend = "震荡"

    desc_map = {
        ("高位", "急升"): "高位急升",
        ("高位", "回升"): "高位续涨",
        ("高位", "震荡"): "高位震荡",
        ("高位", "回落"): "高位回调",
        ("高位", "急跌"): "高位跳水",
        ("中位", "急升"): "快速拉升",
        ("中位", "回升"): "温和上涨",
        ("中位", "震荡"): "窄幅震荡",
        ("中位", "回落"): "震荡走弱",
        ("中位", "急跌"): "加速下跌",
        ("低位", "急升"): "触底反弹",
        ("低位", "回升"): "低位回升",
        ("低位", "震荡"): "低位盘整",
        ("低位", "回落"): "持续探底",
        ("低位", "急跌"): "加速赶底",
    }

    return desc_map.get((position, trend), position + trend)


def _get_stock_data(cache, code, name, report_score=None):
    df = cache.read_prices(code, "stock_daily_prices")
    if df.empty:
        return None
    series = calc_momentum_series(df)
    if not series:
        return None
    trend = momentum_trend(series)
    latest = series[-1]
    trend_desc = generate_trend_desc(series)

    try:
        df_bb = calculate_bollinger(df, period=20, multiplier=2.0)
        pct_b = float(df_bb.iloc[-1]["pct_b"])
    except Exception:
        pct_b = None

    special = ""
    if abs(trend["from_peak"]) < 0.5:
        special = "新高"
    elif abs(trend["from_trough"]) < 0.5:
        special = "新低"

    return {
        "name": name,
        "code": code,
        "score": latest["score"],
        "level": latest["level"],
        "pct_b": pct_b,
        "change_1d": trend["change_1d"],
        "change_5d": trend["change_5d"],
        "change_10d": trend["change_10d"],
        "from_peak": trend["from_peak"],
        "from_trough": trend["from_trough"],
        "peak_score": trend["peak_score"],
        "peak_date": trend["peak_date"],
        "trend": trend["trend"],
        "special": special,
        "trend_desc": trend_desc,
        "report_score": report_score,
    }


def generate_momentum_report(db_path=None, reports_dir="reports/latest"):
    db = Database(db_path)
    cache = CacheManager(db)

    lines = []
    today = datetime.now().strftime("%Y-%m-%d")
    lines.append(f"# 每日动量报告 ({today})")
    lines.append("")

    # ========== 指数部分 ==========
    lines.append("## 一、指数动量")
    lines.append("")

    index_codes = [
        ("sh.000300", "沪深300"),
        ("sz.399006", "创业板指"),
    ]

    for code, name in index_codes:
        df = cache.read_index_prices(code, "index_daily_prices")
        if df.empty:
            continue
        series = calc_momentum_series(df)
        trend = momentum_trend(series)
        latest = series[-1] if series else {"score": 0, "level": ""}
        trend_desc = generate_trend_desc(series)

        lines.append(f"### {name} ({code})")
        lines.append(f"- 最新动量: **{latest['score']:.1f}** ({latest['level']}) | 1日: {trend['change_1d']:+.1f} | 5日: {trend['change_5d']:+.1f} | 10日: {trend['change_10d']:+.1f}")
        lines.append(f"- 峰值: {trend['peak_score']:.1f}@{trend['peak_date']} | 距峰值: {trend['from_peak']:+.1f} | 距谷底: {trend['from_trough']:+.1f}")
        lines.append(f"- 走势: {trend_desc}")
        lines.append("")

    # ========== 自选股票表格 ==========
    watchlist = [
        ("600566", "济川药业"),
        ("003000", "劲仔食品"),
        ("603288", "海天味业"),
        ("000538", "云南白药"),
        ("000858", "五粮液"),
        ("002142", "宁波银行"),
        ("002415", "海康威视"),
        ("000333", "美的集团"),
        ("600298", "安琪酵母"),
    ]

    watch_data = []
    for code, name in watchlist:
        d = _get_stock_data(cache, code, name)
        if d:
            watch_data.append(d)

    if watch_data:
        lines.append("## 二、自选股票动量")
        lines.append("")
        lines.append(_format_stock_table(watch_data))
        lines.append("")

    # ========== 高分股票表格 ==========
    stocks = scan_high_score_stocks(reports_dir)
    stock_data = []
    for s in stocks:
        d = _get_stock_data(cache, s["code"], s["name"], s["score"])
        if d:
            stock_data.append(d)

    if stock_data:
        lines.append("## 三、高分股票动量（报告总分>=70分）")
        lines.append("")
        lines.append(_format_stock_table(stock_data, show_report_score=True))
        lines.append("")

    # ========== 关键发现 ==========
    all_data = []
    for code, name in index_codes:
        df = cache.read_index_prices(code, "index_daily_prices")
        if not df.empty:
            series = calc_momentum_series(df)
            trend = momentum_trend(series)
            latest = series[-1] if series else {"score": 0}
            all_data.append({"name": name, "code": code, "score": latest["score"],
                             "from_peak": trend["from_peak"], "from_trough": trend["from_trough"],
                             "change_1d": trend["change_1d"], "type": "指数"})

    for d in watch_data + stock_data:
        all_data.append({"name": d["name"], "code": d["code"], "score": d["score"],
                         "from_peak": d["from_peak"], "from_trough": d["from_trough"],
                         "change_1d": d["change_1d"], "type": "股票"})

    lines.append("## 四、关键发现")
    lines.append("")

    new_highs = [d for d in all_data if abs(d["from_peak"]) < 0.5]
    if new_highs:
        lines.append("### 今日创20天动量新高")
        for d in new_highs:
            lines.append(f"- **{d['name']}** ({d['code']}): 动量 {d['score']:.1f}")
        lines.append("")

    new_lows = [d for d in all_data if abs(d.get("from_trough", 0)) < 0.5]
    if new_lows:
        lines.append("### 今日创20天动量新低")
        for d in new_lows:
            lines.append(f"- **{d['name']}** ({d['code']}): 动量 {d['score']:.1f}")
        lines.append("")

    sorted_change = sorted(all_data, key=lambda x: abs(x["change_1d"]), reverse=True)
    if sorted_change:
        lines.append("### 单日动量变化最大")
        for d in sorted_change[:5]:
            direction = "+" if d["change_1d"] > 0 else ""
            lines.append(f"- **{d['name']}**: {direction}{d['change_1d']:.1f} ({d['type']})")
        lines.append("")

    lines.append("---")
    lines.append(f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


def _format_stock_table(data_list, show_report_score=False):
    """生成Markdown表格。"""
    lines = []

    # 表头
    header = "| 名称 | 代码 | 动量 | 级别 | 1日 | 5日 | 10日 | 距峰值 | 趋势 | 走势 |"
    if show_report_score:
        header = "| 名称 | 代码 | 报告分 | 动量 | 级别 | 1日 | 5日 | 10日 | 距峰值 | 趋势 | 走势 |"
    lines.append(header)

    # 分隔线
    sep = "|------|------|------|------|-----|-----|------|--------|------|------|"
    if show_report_score:
        sep = "|------|------|--------|------|------|-----|-----|------|--------|------|------|"
    lines.append(sep)

    # 排序：按动量从高到低
    sorted_data = sorted(data_list, key=lambda x: x["score"], reverse=True)

    for d in sorted_data:
        pct_b_str = f"%B={d['pct_b']:.1f}" if d['pct_b'] is not None else "-"
        special = f" **{d['special']}**" if d['special'] else ""

        row = f"| {d['name']}{special} | {d['code']} | **{d['score']:.1f}** | {d['level']} | {d['change_1d']:+.1f} | {d['change_5d']:+.1f} | {d['change_10d']:+.1f} | {d['from_peak']:+.1f} | {d['trend']} | {d['trend_desc']} |"

        if show_report_score:
            row = f"| {d['name']}{special} | {d['code']} | {d['report_score']:.1f} | **{d['score']:.1f}** | {d['level']} | {d['change_1d']:+.1f} | {d['change_5d']:+.1f} | {d['change_10d']:+.1f} | {d['from_peak']:+.1f} | {d['trend']} | {d['trend_desc']} |"

        lines.append(row)

    return "\n".join(lines)


if __name__ == "__main__":
    report = generate_momentum_report()
    print(report)
