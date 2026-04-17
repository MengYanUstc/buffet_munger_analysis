"""
最终测试：TopSitesFetcher 的 7 个定向来源
说明：
- 东方财富 / 财联社 / 巨潮资讯：直接调用官方 JSON API，结果真实可靠。
- 雪球 / 证券时报 / 新浪财经 / 同花顺：基于 site: 限定的搜索引擎兜底策略，
  需要外部 DDGS/Bing API 可用时才能返回结果。本脚本会同时测试其逻辑正确性。
"""

import sys, os
sys.path.insert(0, ".")
sys.path.insert(0, os.path.join(".", "buffett_analyzer", "management_analysis"))

from top_sites_fetcher import (
    EastmoneyNewsFetcher,
    ClsTelegraphFetcher,
    CninfoFetcher,
    XueqiuFetcher,
    StcnFetcher,
    SinaFinanceFetcher,
    TonghuashunFetcher,
)
from web_search_fetcher import ManagementWebSearchFetcher


def test_fetcher(fetcher, stock_code="000661", stock_name="长春高新"):
    try:
        result = fetcher.fetch(stock_code, stock_name)
        total = sum(len(v.get("snippets", [])) for v in result.values())
        return result, total, None
    except Exception as e:
        return {}, 0, str(e)


def print_report(name, result, total, error):
    print(f"\n{'='*60}")
    print(f"来源: {name}")
    print(f"{'='*60}")
    if error:
        print(f"状态: [异常] {error}")
        return
    if total == 0:
        print("状态: [空结果] 返回 0 条（可能当前环境搜索后端不可用或该站点暂无相关资讯）")
        return
    print(f"状态: [OK] 共 {total} 条")
    for key, val in result.items():
        snippets = val.get("snippets", [])
        if not snippets:
            continue
        print(f"\n  -> 维度: {key} | 条数: {len(snippets)}")
        for idx, s in enumerate(snippets[:4], 1):
            title = s.get("title", "")[:70]
            url = s.get("url", "")[:70]
            print(f"    {idx}. {title}")
            if url:
                print(f"       → {url}")
        if len(snippets) > 4:
            print(f"       ... 还有 {len(snippets)-4} 条")


def main():
    stock_code = "000661"
    stock_name = "长春高新"

    print(f"测试股票: {stock_code} ({stock_name})")

    # 1. 直接 API 来源
    direct_fetchers = [
        EastmoneyNewsFetcher(),
        ClsTelegraphFetcher(),
        CninfoFetcher(),
    ]
    for f in direct_fetchers:
        result, total, error = test_fetcher(f, stock_code, stock_name)
        print_report(f.name, result, total, error)

    # 2. 搜索引擎兜底来源
    print(f"\n\n{'#'*60}")
    print("# 以下来源依赖外部搜索后端 (DDGS / Bing API)")
    print(f"{'#'*60}")

    try:
        ddgs = ManagementWebSearchFetcher(max_results=4)
        # 预检一下 DDGS 是否可用
        probe = ddgs.search("test")
        ddgs_usable = len(probe) > 0
    except Exception as e:
        ddgs_usable = False
        print(f"\n[预检] DDGS 不可用: {e}")

    search_fetchers = [
        XueqiuFetcher(search_backend=ddgs),
        StcnFetcher(search_backend=ddgs),
        SinaFinanceFetcher(search_backend=ddgs),
        TonghuashunFetcher(search_backend=ddgs),
    ]
    for f in search_fetchers:
        result, total, error = test_fetcher(f, stock_code, stock_name)
        if not ddgs_usable and total == 0 and not error:
            print(f"\n{'='*60}")
            print(f"来源: {f.name}")
            print(f"{'='*60}")
            print("状态: [SKIP] 当前环境 DDGS 不可用，search-based fetcher 未激活")
        else:
            print_report(f.name, result, total, error)

    # 3. 模拟搜索后端验证 search-based fetcher 逻辑
    print(f"\n\n{'#'*60}")
    print("# Search-Based Fetcher 逻辑自测（使用模拟后端）")
    print(f"{'#'*60}")

    class FakeBackend:
        def search(self, query):
            return [
                {"title": f"{query} 结果1", "summary": "模拟摘要A", "url": "http://fake/1"},
                {"title": f"{query} 结果2", "summary": "模拟摘要B", "url": "http://fake/2"},
            ]

    fake = FakeBackend()
    for cls in [XueqiuFetcher, StcnFetcher, SinaFinanceFetcher, TonghuashunFetcher]:
        f = cls(search_backend=fake)
        result, total, error = test_fetcher(f, stock_code, stock_name)
        print(f"\n[{f.name}] 模拟测试: {total} 条")
        for key, val in result.items():
            print(f"  {key}: {len(val.get('snippets', []))} 条")

    print("\n\n测试完成。")


if __name__ == "__main__":
    main()
