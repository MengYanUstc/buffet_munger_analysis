#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业质量分析模块 - 命令行入口
示例：python main.py --code 600519 --industry general
新增：python main.py --code 600519 --collect-enhanced
新增：python main.py --code 00700 --fill-valuation pe_percentile_5y=35 pb_percentile_5y=20
"""

import json
import argparse
from buffett_analyzer.quality_analysis import QualityAnalyzer
from buffett_analyzer.data_warehouse import DataCollector


def parse_manual_fields(field_strs):
    """解析 --fill-valuation 传入的 key=value 列表。"""
    result = {}
    for s in field_strs:
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        try:
            result[k.strip()] = float(v.strip())
        except ValueError:
            result[k.strip()] = v.strip()
    return result


def main():
    parser = argparse.ArgumentParser(description="巴菲特-芒格企业质量分析模块")
    parser.add_argument("--code", required=True, help="股票代码，例如 600519")
    parser.add_argument(
        "--industry", default="general",
        choices=["general", "banking", "insurance", "real_estate", "utilities"],
        help="行业类型，用于资产负债率评分"
    )
    parser.add_argument(
        "--source", default="akshare", choices=["akshare"],
        help="数据源（当前仅支持 akshare）"
    )
    parser.add_argument(
        "--output", default=None,
        help="输出 JSON 文件路径，默认输出到控制台"
    )
    parser.add_argument(
        "--collect", action="store_true",
        help="使用数据收集模块（DataCollector）获取财务+估值数据并缓存到 SQLite"
    )
    parser.add_argument(
        "--collect-enhanced", action="store_true",
        help="使用增强版数据收集（包含行业估值 enrich + 联网搜索补缺）"
    )
    parser.add_argument(
        "--fill-valuation", nargs="+", metavar="FIELD=VALUE",
        help="手动填补估值字段，如 pe_percentile_5y=35.0 pb_percentile_5y=20.0"
    )

    args = parser.parse_args()

    collector = DataCollector()

    if args.fill_valuation:
        fields = parse_manual_fields(args.fill_valuation)
        collector.manual_fill_valuation(args.code, fields, note="手动CLI填充")
        collector.close()
        return

    if args.collect_enhanced:
        result = collector.collect_enhanced(args.code)
        collector.close()
        if result.get("financial_reports") is not None:
            result["financial_reports"] = result["financial_reports"].to_dict(orient='records')
        json_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    elif args.collect:
        result = collector.collect(args.code)
        collector.close()
        if result.get("financial_reports") is not None:
            result["financial_reports"] = result["financial_reports"].to_dict(orient='records')
        json_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    else:
        analyzer = QualityAnalyzer(
            stock_code=args.code,
            industry_type=args.industry,
            source=args.source
        )
        result = analyzer.run()
        json_str = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_str)
        print(f"结果已保存至: {args.output}")
    else:
        print(json_str)


if __name__ == "__main__":
    main()
