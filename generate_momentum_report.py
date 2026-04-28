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

import os
import sys
from datetime import datetime

sys.path.insert(0, '.')

from buffett_analyzer.index_trading.momentum_report import generate_momentum_report, scan_high_score_stocks
from buffett_analyzer.data_warehouse.collector import DataCollector
from buffett_analyzer.index_trading.index_collector import IndexCollector
from buffett_analyzer.utils.report_archiver import archive_momentum_report, LATEST_DIR


def main():
    today = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(LATEST_DIR, f"momentum_report_{today}.md")
    db_path = "data/stock_cache.db"

    # ========== 1. 数据刷新 ==========
    # 注意：Baostock 是全局单会话，IndexCollector 和 DataCollector 共用
    # 必须先完成所有数据拉取，最后再统一 logout，否则中途 logout 会中断后续请求
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 更新数据...")
    idx_collector = IndexCollector(db_path)
    collector = DataCollector(db_path)

    # 指数
    for code, name in [("sh.000300", "沪深300"), ("sz.399006", "创业板指")]:
        result = idx_collector.collect_single(code, period="daily", years=5)
        print(f"  {name}: {result['source']} ({result['rows']} 条)")

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
        ("600298", "安琪酵母"),
    ]
    for code, name in watchlist:
        result = collector.collect_prices(code)
        print(f"  {name}({code}): daily={result['sources'].get('daily', 'skip')}")

    # 高分股票
    high_score_stocks = scan_high_score_stocks(reports_dir=LATEST_DIR, min_score=70.0)
    for s in high_score_stocks:
        if s["code"] in [c for c, _ in watchlist]:
            continue
        result = collector.collect_prices(s["code"])
        print(f"  {s['name']}({s['code']}): daily={result['sources'].get('daily', 'skip')}")

    # 所有数据拉完后再 logout（Baostock 全局会话）
    idx_collector.logout()
    collector.close()

    # ========== 2. 归档旧动量报告并生成新报告 ==========
    archive_momentum_report()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始生成动量报告...")
    report = generate_momentum_report(db_path=db_path, reports_dir=LATEST_DIR)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 报告已保存: {output_path}")


if __name__ == "__main__":
    main()
