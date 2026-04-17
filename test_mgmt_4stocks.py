"""
测试管理层分析模块在 4 只股票上的表现
由于当前环境未配置 LLM_API_KEY，本测试覆盖：
1. ManagementDataFetcher 数据获取（akshare + top sites 直接 API，跳过 DDGS 避免超时）
2. CapitalAllocationPlugin / IntegrityPlugin 的代码级规则评分
3. 生成结构化的对比报告
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


def timed(label, fn, *args, **kwargs):
    t0 = time.time()
    try:
        r = fn(*args, **kwargs)
        print(f"  [{label}] OK in {time.time()-t0:.2f}s")
        return r
    except Exception as e:
        print(f"  [{label}] ERROR in {time.time()-t0:.2f}s: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_stock(code, name, is_hk):
    fetcher = ManagementDataFetcher()
    fetcher._search_all = lambda stock_name, stock_code: ({}, "")
    fetcher._top_sites.fetch_all = lambda stock_code, stock_name, is_hk: {}

    print(f"\n正在分析 {code} {name}...")
    data = {
        "roic_trend": timed("roic_trend", fetcher.fetch_roic_trend, code, is_hk=is_hk),
        "dividend": timed("dividend", fetcher.fetch_dividend, code, is_hk=is_hk),
        "pledge": timed("pledge", fetcher.fetch_pledge, code, is_hk=is_hk),
        "violations": timed("violations", fetcher.fetch_violations, code),
        "management_holdings": timed("management_holdings", fetcher.fetch_management_holdings, code),
        "mergers": {"note": "并购与商誉数据本地未接入，由联网搜索补充"},
    }

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

    cap_result = timed("capital_plugin", cap_plugin.compute, context)
    int_result = timed("integrity_plugin", int_plugin.compute, context)

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
                "base_score": cap_result.base_score if cap_result else None,
                "penalty_score": cap_result.penalty_score if cap_result else None,
                "reason": cap_result.reason if cap_result else None,
            },
            "management_integrity": {
                "base_score": int_result.base_score if int_result else None,
                "penalty_score": int_result.penalty_score if int_result else None,
                "reason": int_result.reason if int_result else None,
            },
        },
        "total_rule_score": (
            (cap_result.penalty_score or 0) if cap_result else 0
        ) + (
            (int_result.penalty_score or 0) if int_result else 0
        ),
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

    with open("report_mgmt_4stocks.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print("\n完整报告已保存至: report_mgmt_4stocks.json")

    print("\n" + "=" * 80)
    print("管理层分析 - 代码级规则评分摘要（未含 LLM 微调）")
    print("=" * 80)
    for r in results:
        cap = r["rule_based_scores"]["capital_allocation"]
        inte = r["rule_based_scores"]["management_integrity"]
        print(f"\n【{r['stock_code']} {r['stock_name']}】")
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
