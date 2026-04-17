"""
管理层分析模块端到端测试（使用生产代码 fetch_all，不 patch fallback）
验证：
1. fetch_all 在默认配置下不再因 DDGS 超时
2. 规则评分插件输出正常
3. 生成对比报告
"""

import json
import sys
import time

sys.path.insert(0, ".")

from buffett_analyzer.management_analysis.data_fetcher import ManagementDataFetcher
from buffett_analyzer.management_analysis.plugins.capital_allocation_plugin import CapitalAllocationPlugin
from buffett_analyzer.management_analysis.plugins.integrity_plugin import IntegrityPlugin

STOCKS = [
    ("000333", "美的集团", False),
    ("000858", "五粮液", False),
    ("600519", "贵州茅台", False),
    ("03333", "恒大集团", True),
]


def analyze_stock(code, name, is_hk):
    fetcher = ManagementDataFetcher()
    print(f"\n[{code} {name}] fetch_all 开始...")
    t0 = time.time()
    data = fetcher.fetch_all(code)
    print(f"[{code} {name}] fetch_all 完成，耗时 {time.time()-t0:.2f}s")

    context = {
        "stock_code": code,
        "industry_type": "general",
        "roic_trend": data.get("roic_trend"),
        "dividend": data.get("dividend"),
        "pledge": data.get("pledge"),
        "mergers": data.get("mergers"),
        "violations": data.get("violations"),
        "management_holdings": data.get("management_holdings"),
    }

    cap_plugin = CapitalAllocationPlugin()
    int_plugin = IntegrityPlugin()

    cap_result = cap_plugin.compute(context)
    int_result = int_plugin.compute(context)

    def summarize_source(field):
        val = data.get(field) or {}
        note = val.get("note", "")
        snippets = val.get("snippets", [])
        records = val.get("records", [])
        total = len(snippets) + len(records)
        return {"note": note, "count": total}

    return {
        "stock_code": code,
        "stock_name": name,
        "is_hk": is_hk,
        "fetch_time": round(time.time() - t0, 2),
        "data_sources": {
            "roic_trend": data.get("roic_trend"),
            "pledge": data.get("pledge"),
            "dividend": summarize_source("dividend"),
            "mergers": summarize_source("mergers"),
            "violations": summarize_source("violations"),
            "management_holdings": summarize_source("management_holdings"),
        },
        "rule_based_scores": {
            "capital_allocation": {
                "base_score": cap_result.base_score,
                "penalty_score": cap_result.penalty_score,
                "reason": cap_result.reason,
            },
            "management_integrity": {
                "base_score": int_result.base_score,
                "penalty_score": int_result.penalty_score,
                "reason": int_result.reason,
            },
        },
        "total_rule_score": (cap_result.penalty_score or 0) + (int_result.penalty_score or 0),
    }


def main():
    results = []
    for code, name, is_hk in STOCKS:
        try:
            r = analyze_stock(code, name, is_hk)
            results.append(r)
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    with open("report_mgmt_4stocks_e2e.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print("\n完整报告已保存至: report_mgmt_4stocks_e2e.json")

    print("\n" + "=" * 80)
    print("管理层分析 - 代码级规则评分摘要（未含 LLM 微调）")
    print("=" * 80)
    for r in results:
        cap = r["rule_based_scores"]["capital_allocation"]
        inte = r["rule_based_scores"]["management_integrity"]
        print(f"\n【{r['stock_code']} {r['stock_name']}】 耗时: {r['fetch_time']}s")
        print(f"  资本配置能力: {cap['penalty_score']}/6.0  (基准: {cap['base_score']})")
        print(f"  管理层诚信:   {inte['penalty_score']}/4.0  (基准: {inte['base_score']})")
        print(f"  代码级总分:   {r['total_rule_score']}/10.0")
        print(f"  数据来源:")
        for k, v in r["data_sources"].items():
            if isinstance(v, dict) and "note" in v and "count" in v:
                print(f"    {k}: {v['note']} | 条数: {v['count']}")
            elif isinstance(v, dict):
                print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
