# -*- coding: utf-8 -*-
"""
每日开盘前动量报告生成入口

使用方法:
    python generate_momentum_report.py

流程:
    1. 刷新所有需要的股票和指数日K数据（增量更新）
    2. 生成动量报告

输出: reports/momentum_report_YYYYMMDD.md
"""

import sys
from datetime import datetime

sys.path.insert(0, '.')

from buffett_analyzer.index_trading.momentum_report import generate_momentum_report, scan_high_score_stocks
from buffett_analyzer.data_warehouse.collector import DataCollector
from buffett_analyzer.index_trading.index_collector import IndexCollector


def main():
    today = datetime.now().strftime("%Y%m%d")
    output_path = f"reports/momentum_report_{today}.md"
    db_path = "data/stock_cache.db"

    # ========== 1. 数据刷新 ==========
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 更新指数数据...")
    idx_collector = IndexCollector(db_path)
    for code, name in [("sh.000300", "沪深300"), ("sz.399006", "创业板指")]:
        result = idx_collector.collect_single(code, period="daily", years=5)
        print(f"  {name}: {result['source']} ({result['rows']} 条)")
    idx_collector.logout()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 更新股票数据...")
    collector = DataCollector(db_path)

    # 自选股票
    watchlist = [
        ("600566", "济川药业"),
        ("003000", "劲仔食品"),
        ("603288", "海天味业"),
        ("000538", "云南白药"),
        ("000858", "五粮液"),
        ("002142", "宁波银行"),
        ("002415", "海康威视"),
        ("000333", "美的集团"),
    ]
    for code, name in watchlist:
        result = collector.collect_prices(code)
        print(f"  {name}({code}): daily={result['sources'].get('daily', 'skip')}")

    # 高分股票
    high_score_stocks = scan_high_score_stocks(reports_dir="reports", min_score=70.0)
    for s in high_score_stocks:
        # 避免重复更新已在 watchlist 中的股票
        if s["code"] in [c for c, _ in watchlist]:
            continue
        result = collector.collect_prices(s["code"])
        print(f"  {s['name']}({s['code']}): daily={result['sources'].get('daily', 'skip')}")

    collector.close()

    # ========== 2. 生成报告 ==========
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始生成动量报告...")
    report = generate_momentum_report(db_path=db_path)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 报告已保存: {output_path}")


if __name__ == "__main__":
    main()
