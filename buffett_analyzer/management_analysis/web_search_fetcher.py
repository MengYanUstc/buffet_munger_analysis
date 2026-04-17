"""
管理层分析 - 联网搜索补充材料
利用 DuckDuckGo 搜索分红、并购商誉、违规处罚、高管持股变动等定性信息，
将搜索结果的标题和摘要作为补充事实传给 LLM 进行判断。
"""

import time
import warnings
from typing import Dict, Any, List

# 抑制 duckduckgo_search 包重命名警告
warnings.filterwarnings("ignore", message="This package.*has been renamed to.*ddgs", category=RuntimeWarning)

try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS


class ManagementWebSearchFetcher:
    def __init__(self, max_results: int = 3, request_timeout: int = 15):
        self.max_results = max_results
        self.timeout = request_timeout

    def search_all(self, stock_name: str, stock_code: str) -> Dict[str, Any]:
        """
        对管理层分析所需的四个维度执行联网搜索，返回结构化文本摘要。
        """
        is_hk = len(stock_code) == 5 and stock_code.startswith('0')
        # 港股代码（如03333）容易与手机号/编号混淆，优先仅用名称搜索
        if is_hk and stock_name:
            name_part = stock_name
        else:
            name_part = f"{stock_name} {stock_code}" if stock_name else stock_code

        queries = {
            "dividend": f"{name_part} 分红 派息 股息率 分红比例 近年",
            "mergers": f"{name_part} 并购 商誉减值 重大资产重组",
            "violations": f"{name_part} 证监会 立案 调查 处罚 监管函 违规",
            "management_holdings": f"{name_part} 高管减持 增持 持股变动 董监高",
        }

        # 增强违规记录搜索：分步搜索以提高命中率，A股用代码限定，港股用名称限定
        code_or_name = stock_code if not is_hk else stock_name
        violation_queries = [
            f"{code_or_name} 证监会 立案 调查",
            f"{code_or_name} 财务造假 欺诈发行",
            f"{code_or_name} 行政处罚 监管函",
        ]
        # 高管持股增强
        holdings_queries = [
            f"{name_part} 高管减持 增持",
            f"{name_part} 董监高 持股变动",
            f"{name_part} 离婚式减持 清仓",
        ]

        results = {}

        # 单查询维度
        for key, query in queries.items():
            if key in ("violations", "management_holdings"):
                continue  # 下面做多查询合并
            try:
                snippets = self._search(query)
                results[key] = {
                    "note": f"联网搜索到 {len(snippets)} 条相关结果",
                    "snippets": snippets,
                }
            except Exception as e:
                results[key] = {
                    "note": f"联网搜索失败: {e}",
                    "snippets": [],
                }
            time.sleep(0.5)

        # 多查询合并维度：违规记录
        violation_snippets = []
        for q in violation_queries:
            try:
                violation_snippets.extend(self._search(q))
            except Exception:
                pass
            time.sleep(0.3)
        # 去重（按 url）
        seen = set()
        unique_violations = []
        for s in violation_snippets:
            url = s.get("url", "")
            if url and url not in seen:
                seen.add(url)
                unique_violations.append(s)
        results["violations"] = {
            "note": f"联网搜索到 {len(unique_violations)} 条相关结果",
            "snippets": unique_violations[:self.max_results * 2],
        }

        # 多查询合并维度：高管持股变动
        holdings_snippets = []
        for q in holdings_queries:
            try:
                holdings_snippets.extend(self._search(q))
            except Exception:
                pass
            time.sleep(0.3)
        seen2 = set()
        unique_holdings = []
        for s in holdings_snippets:
            url = s.get("url", "")
            if url and url not in seen2:
                seen2.add(url)
                unique_holdings.append(s)
        results["management_holdings"] = {
            "note": f"联网搜索到 {len(unique_holdings)} 条相关结果",
            "snippets": unique_holdings[:self.max_results * 2],
        }

        return results

    def search(self, query: str) -> List[Dict[str, str]]:
        """执行单次 DDGS 搜索，返回标题+摘要列表。与 BingWebSearchFetcher 接口对齐。"""
        with DDGS(timeout=self.timeout) as ddgs:
            search_results = list(ddgs.text(query, max_results=self.max_results))

        snippets = []
        for sr in search_results:
            title = sr.get("title", "").strip()
            body = sr.get("body", "").strip()
            href = sr.get("href", "").strip()
            if title or body:
                snippets.append({
                    "title": title,
                    "summary": body,
                    "url": href,
                })
        return snippets

    # 保持向后兼容
    _search = search
