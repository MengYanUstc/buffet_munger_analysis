"""
管理层分析 - Bing Web Search API 封装
替代 DuckDuckGo，提升中文财经内容召回质量。
"""

import os
import time
from typing import Dict, Any, List, Optional

import requests


class BingWebSearchFetcher:
    def __init__(self, api_key: Optional[str] = None, max_results: int = 5):
        self.api_key = api_key or os.getenv("BING_SEARCH_API_KEY", "")
        self.endpoint = "https://api.bing.microsoft.com/v7.0/search"
        self.max_results = max_results

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def search(self, query: str, market: str = "zh-CN") -> List[Dict[str, str]]:
        """执行单次 Bing 搜索，返回标题+摘要列表。"""
        if not self.is_configured():
            raise RuntimeError("BING_SEARCH_API_KEY 未配置")

        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        params = {
            "q": query,
            "count": self.max_results,
            "mkt": market,
            "setLang": "zh",
        }
        resp = requests.get(self.endpoint, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        snippets = []
        for item in data.get("webPages", {}).get("value", []):
            title = item.get("name", "").strip()
            body = item.get("snippet", "").strip()
            url = item.get("url", "").strip()
            if title or body:
                snippets.append({"title": title, "summary": body, "url": url})
        return snippets

    def search_all(self, stock_name: str, stock_code: str) -> Dict[str, Any]:
        """
        对管理层分析所需的四个维度执行 Bing 搜索，返回结构化文本摘要。
        接口与 ManagementWebSearchFetcher.search_all 对齐。
        """
        is_hk = len(stock_code) == 5 and stock_code.startswith('0')
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

        results = {}
        for key, query in queries.items():
            try:
                snippets = self.search(query)
                results[key] = {
                    "note": f"Bing 搜索到 {len(snippets)} 条相关结果",
                    "snippets": snippets,
                }
            except Exception as e:
                results[key] = {
                    "note": f"Bing 搜索失败: {e}",
                    "snippets": [],
                }
            time.sleep(0.3)
        return results
