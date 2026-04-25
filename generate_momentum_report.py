# -*- coding: utf-8 -*-
"""
每日开盘前动量报告生成入口

使用方法:
    python generate_momentum_report.py

输出: reports/momentum_report_YYYYMMDD.md
"""

import sys
from datetime import datetime

sys.path.insert(0, '.')

from buffett_analyzer.index_trading.momentum_report import generate_momentum_report


def main():
    today = datetime.now().strftime("%Y%m%d")
    output_path = f"reports/momentum_report_{today}.md"

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始生成动量报告...")
    report = generate_momentum_report()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 报告已保存: {output_path}")


if __name__ == "__main__":
    main()
