#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业分析系统 - 统一命令行入口
支持模块：quality（企业质量）、management（管理层定性）
未来可扩展：valuation（估值）、moat（竞争优势）、risk（风险）

示例：
  python main.py --code 600519 --industry general          # 默认运行 quality 模块
  python main.py --code 600519 --module management          # 运行管理层分析
  python main.py --code 600519 --module quality --module management  # 运行多个模块
  python main.py --code 600519 --module all                 # 运行所有已注册模块
  python main.py --code 600519 --collect-enhanced           # 数据收集
"""

import json
import argparse
from typing import List

from buffett_analyzer.core import AnalyzerRegistry
from buffett_analyzer.quality_analysis import QualityAnalyzer
from buffett_analyzer.management_analysis import ManagementAnalyzer
from buffett_analyzer.moat_analysis import MoatAnalyzer
from buffett_analyzer.business_model_analysis import BusinessModelAnalyzer
from buffett_analyzer.valuation import ValuationAnalyzer
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


def register_analyzers():
    """注册所有分析模块。新增模块时只需在此添加一行。"""
    AnalyzerRegistry.register(QualityAnalyzer)
    AnalyzerRegistry.register(ManagementAnalyzer)
    AnalyzerRegistry.register(MoatAnalyzer)
    AnalyzerRegistry.register(BusinessModelAnalyzer)
    AnalyzerRegistry.register(ValuationAnalyzer)


def run_modules(stock_code: str, industry_type: str, module_ids: List[str], source: str = "akshare"):
    """按顺序运行指定模块，返回合并后的结果字典。"""
    results = {}
    for mid in module_ids:
        analyzer = AnalyzerRegistry.build(mid, stock_code=stock_code, industry_type=industry_type, source=source)
        report = analyzer.run()
        results[mid] = report.to_dict()
    return results


def main():
    register_analyzers()

    parser = argparse.ArgumentParser(description="企业分析系统 CLI")
    parser.add_argument("--code", required=True, help="股票代码，如 600519 或 00700")
    parser.add_argument("--industry", default="general",
                        help="行业类型 (general/banking/insurance/real_estate/utilities)")
    parser.add_argument("--source", default="akshare", help="数据源 (目前仅支持 akshare)")
    parser.add_argument("--output", help="结果保存到指定 JSON 文件路径")

    # 模块选择（支持多选，兼容旧版 --management 单标志）
    parser.add_argument("--module", nargs="+", dest="modules",
                        help="要运行的分析模块，如 quality management。使用 'all' 运行全部。")
    parser.add_argument("--management", action="store_true",
                        help="【兼容旧版】等价于 --module management")

    # 数据收集
    parser.add_argument("--collect", action="store_true",
                        help="仅收集并缓存财务数据与估值指标")
    parser.add_argument("--collect-enhanced", action="store_true",
                        help="收集数据 + 行业估值 enrichment + 缺失数据联网补全")
    parser.add_argument("--fill-valuation", nargs="+", metavar="FIELD=VALUE",
                        help="手动填补估值字段，如 pe_percentile_5y=35.0 pb_percentile_5y=20.0")

    args = parser.parse_args()
    collector = DataCollector()

    # 1. 手动填充估值
    if args.fill_valuation:
        fields = parse_manual_fields(args.fill_valuation)
        collector.manual_fill_valuation(args.code, fields, note="手动CLI填充")
        collector.close()
        return

    # 2. 数据收集模式
    if args.collect_enhanced:
        result = collector.collect_enhanced(args.code)
        collector.close()
        if result.get("financial_reports") is not None:
            result["financial_reports"] = result["financial_reports"].to_dict(orient="records")
        json_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    elif args.collect:
        result = collector.collect(args.code)
        collector.close()
        if result.get("financial_reports") is not None:
            result["financial_reports"] = result["financial_reports"].to_dict(orient="records")
        json_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    else:
        # 3. 分析模式：确定要运行的模块
        modules_to_run = []
        if args.modules:
            if "all" in args.modules:
                modules_to_run = AnalyzerRegistry.list_modules()
            else:
                modules_to_run = args.modules
        elif args.management:
            modules_to_run = ["management"]
        else:
            # 默认运行 quality 模块（保持向后兼容）
            modules_to_run = ["quality"]

        result = run_modules(args.code, args.industry, modules_to_run, args.source)
        json_str = json.dumps(result, ensure_ascii=False, indent=2, default=str)

    # 输出
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_str)
        print(f"报告已保存至: {args.output}")
    else:
        print(json_str)


if __name__ == "__main__":
    main()
