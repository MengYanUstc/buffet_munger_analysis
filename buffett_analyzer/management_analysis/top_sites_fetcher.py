"""
精选头部财经网站轻量爬虫（舆情补充层）
架构：BaseSiteFetcher -> 各站点具体实现 -> TopSitesFetcher 聚合
当前覆盖 7 个核心源：
  1. 东方财富（JSONP API）
  2. 财联社（Telegraph 页面 script JSON）
  3. 巨潮资讯（官方公告 API）
  4. 雪球（DDGS site: 搜索）
  5. 证券时报（DDGS site: 搜索）
  6. 新浪财经（DDGS site: 搜索）
  7. 同花顺财经（DDGS site: 搜索）
"""

import json
import re
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup


class BaseSiteFetcher:
    """单个站点爬虫基类"""

    name: str = ""
    supports_hk: bool = True  # 默认支持港股（搜索类都支持）

    def can_fetch(self, stock_code: str, is_hk: bool = False) -> bool:
        if is_hk and not self.supports_hk:
            return False
        return True

    def fetch(self, stock_code: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _classify_by_keywords(title: str, summary: str = "", url: str = "") -> Dict[str, List[Dict[str, str]]]:
        """
        通用关键词分类，把单条新闻归类到 dividend/mergers/violations/management_holdings。
        若未命中任何关键词，默认放入 violations（作为通用舆情补充，供 LLM 判断）。
        """
        text = f"{title} {summary}"
        res: Dict[str, List[Dict[str, str]]] = {
            "dividend": [],
            "mergers": [],
            "violations": [],
            "management_holdings": [],
        }
        snippet = {"title": title.strip(), "summary": summary.strip(), "url": url.strip()}
        if any(kw in text for kw in ["分红", "派息", "股息", "送转", "权益分派", "利润分配"]):
            res["dividend"].append(snippet)
        if any(kw in text for kw in ["并购", "重组", "重大资产重组", "收购", "商誉减值", "资产注入"]):
            res["mergers"].append(snippet)
        if any(kw in text for kw in ["证监会", "立案", "调查", "处罚", "监管函", "违规", "警示", "退市", "风险提示"]):
            res["violations"].append(snippet)
        if any(kw in text for kw in ["减持", "增持", "持股变动", "清仓", "离婚式减持", "董监高", "高管"]):
            res["management_holdings"].append(snippet)
        # 兜底：避免完全丢弃与个股相关的新闻
        if not any(res.values()):
            res["violations"].append(snippet)
        return res


class EastmoneyNewsFetcher(BaseSiteFetcher):
    """东方财富-个股新闻搜索 JSONP API"""

    name = "东方财富"
    supports_hk = True

    def fetch(self, stock_code: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        inner = {
            "uid": "",
            "keyword": stock_code,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": 1,
                    "pageSize": 4,
                    "preTag": "",
                    "postTag": "",
                }
            },
        }
        url = (
            "https://search-api-web.eastmoney.com/search/jsonp"
            f"?cb=jQuery&param={urllib.parse.quote(json.dumps(inner, ensure_ascii=False, separators=(',', ':')))}"
            "&_=1"
        )
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            resp.raise_for_status()
            text = resp.text.strip()
            # 去掉 JSONP 回调
            if text.startswith("jQuery("):
                text = text[7:]
            if text.endswith(")"):
                text = text[:-1]
            data = json.loads(text)
            items = data.get("result", {}).get("cmsArticleWebOld", [])
        except Exception as e:
            print(f"[{self.name}] API 请求失败: {e}")
            return {}

        result: Dict[str, Any] = {}
        for it in items[:4]:
            title = it.get("title", "").strip()
            media = it.get("mediaName", "").strip()
            pub = it.get("date", "").strip()
            code = it.get("code", "")
            link = f"http://finance.eastmoney.com/a/{code}.html" if code else ""
            summary = " | ".join([p for p in [media, pub] if p])
            classified = self._classify_by_keywords(title, summary, link)
            for key, snippets in classified.items():
                if not snippets:
                    continue
                if key not in result:
                    result[key] = {"note": f"{self.name} 爬取", "snippets": []}
                result[key]["snippets"].extend(snippets)

        return result


class ClsTelegraphFetcher(BaseSiteFetcher):
    """财联社-7×24 电报页 script JSON"""

    name = "财联社"
    supports_hk = False  # 电报页不区分个股，用搜索页会更精准；这里先只针对 A 股关键词过滤

    def fetch(self, stock_code: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        url = "https://www.cls.cn/telegraph"
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            resp.raise_for_status()
            html = resp.text
            # 财联社把初始数据放在某个 script 标签里的 JSON 中
            scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.S)
            data = None
            for s in scripts:
                if '"telegraphList"' in s:
                    try:
                        data = json.loads(s)
                        break
                    except Exception:
                        continue
            if not data:
                return {}
            items = data.get("props", {}).get("initialState", {}).get("telegraph", {}).get("telegraphList", [])
        except Exception as e:
            print(f"[{self.name}] 页面解析失败: {e}")
            return {}

        # 用股票代码或简称优先过滤，若未命中则返回最新电报作为市场舆情补充
        keyword = stock_name or stock_code
        result: Dict[str, Any] = {}
        matched = []
        fallback = []
        for it in items[:30]:
            content = it.get("content", "").strip()
            if not content:
                continue
            if keyword in content:
                matched.append(content)
            else:
                fallback.append(content)
            if len(matched) >= 4:
                break

        sources = matched[:4] if matched else fallback[:4]
        for content in sources:
            link = "https://www.cls.cn/telegraph"
            classified = self._classify_by_keywords(content, "", link)
            for key, snippets in classified.items():
                if not snippets:
                    continue
                if key not in result:
                    result[key] = {"note": f"{self.name} 爬取", "snippets": []}
                result[key]["snippets"].extend(snippets)

        return result


class CninfoFetcher(BaseSiteFetcher):
    """巨潮资讯-公司公告 API"""

    name = "巨潮资讯"
    supports_hk = False

    def fetch(self, stock_code: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        # 判断交易所及 orgId 前缀
        if stock_code.startswith("6"):
            column = "sse"
            plate = "sh"
            org_prefix = "gssh"
        elif stock_code.startswith("0") or stock_code.startswith("3"):
            column = "szse"
            plate = "sz"
            org_prefix = "gssz"
        else:
            column = "bjse"
            plate = "bj"
            org_prefix = "gsbj"

        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        payload = {
            "pageNum": 1,
            "pageSize": 10,
            "tabName": "fulltext",
            "column": column,
            "stock": f"{stock_code},{org_prefix}{stock_code.zfill(7)}",
            "searchkey": "",
            "secid": "",
            "plate": plate,
            "category": "",
            "trade": "",
            "columnTitle": "历年公告",
        }
        try:
            resp = requests.post(url, data=payload, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            announcements = data.get("announcements") or []
        except Exception as e:
            print(f"[{self.name}] API 请求失败: {e}")
            return {}

        result: Dict[str, Any] = {}
        for ann in announcements[:4]:
            title = ann.get("announcementTitle", "").strip()
            url_path = ann.get("adjunctUrl", "")
            link = f"http://static.cninfo.com.cn/{url_path}" if url_path else ""
            classified = self._classify_by_keywords(title, "", link)
            for key, snippets in classified.items():
                if not snippets:
                    continue
                if key not in result:
                    result[key] = {"note": f"{self.name} 爬取", "snippets": []}
                result[key]["snippets"].extend(snippets)

        return result


class SearchBasedFetcher(BaseSiteFetcher):
    """基于搜索引擎 site: 限定语法的兜底 Fetcher"""

    def __init__(self, site: str, search_backend=None):
        self.site = site
        self._search_backend = search_backend

    def _search(self, query: str) -> List[Dict[str, str]]:
        """调用外部搜索后端，返回原始 snippets。"""
        if self._search_backend is None:
            return []
        try:
            # 优先使用项目内统一的 DDGS 搜索方法
            if hasattr(self._search_backend, "search"):
                return self._search_backend.search(query)
            # 兼容 ManagementWebSearchFetcher 的接口
            if hasattr(self._search_backend, "_search"):
                return self._search_backend._search(query)
        except Exception as e:
            print(f"[{self.name}] site:{self.site} 搜索失败: {e}")
        return []

    def fetch(self, stock_code: str, stock_name: Optional[str] = None) -> Dict[str, Any]:
        q = f"{stock_name or ''} {stock_code} site:{self.site}"
        snippets = self._search(q)[:4]
        result: Dict[str, Any] = {}
        for s in snippets:
            title = s.get("title", "").strip()
            summary = s.get("summary", "").strip()
            link = s.get("url", "").strip()
            classified = self._classify_by_keywords(title, summary, link)
            for key, items in classified.items():
                if not items:
                    continue
                if key not in result:
                    result[key] = {"note": f"{self.name} 爬取", "snippets": []}
                result[key]["snippets"].extend(items)
        return result


class XueqiuFetcher(SearchBasedFetcher):
    """雪球 - 通过 DDGS site: 搜索获取讨论/公告摘要"""

    name = "雪球"

    def __init__(self, search_backend=None):
        super().__init__("xueqiu.com", search_backend)


class StcnFetcher(SearchBasedFetcher):
    """证券时报 - 通过 DDGS site: 搜索获取新闻"""

    name = "证券时报"

    def __init__(self, search_backend=None):
        super().__init__("stcn.com", search_backend)


class SinaFinanceFetcher(SearchBasedFetcher):
    """新浪财经 - 通过 DDGS site: 搜索获取新闻"""

    name = "新浪财经"

    def __init__(self, search_backend=None):
        super().__init__("sina.com.cn", search_backend)


class TonghuashunFetcher(SearchBasedFetcher):
    """同花顺财经 - 通过 DDGS site: 搜索获取资讯"""

    name = "同花顺财经"

    def __init__(self, search_backend=None):
        super().__init__("10jqka.com.cn", search_backend)


class TopSitesFetcher:
    """聚合多个精选站点，顺序请求并合并结果。"""

    def __init__(self, delay: float = 0.5):
        import os
        self.delay = delay
        self.fetchers: List[BaseSiteFetcher] = [
            EastmoneyNewsFetcher(),
            ClsTelegraphFetcher(),
            CninfoFetcher(),
        ]
        # 仅在显式开启 DDGS fallback 时才启用 site: 搜索类 fetcher，避免默认路径超时
        if os.getenv("ENABLE_DDGS_FALLBACK", "0") == "1":
            try:
                from .web_search_fetcher import ManagementWebSearchFetcher
                ddgs = ManagementWebSearchFetcher(max_results=4)
            except Exception as e:
                print(f"[TopSitesFetcher] DDGS 初始化失败: {e}")
                ddgs = None
            if ddgs is not None:
                self.fetchers.extend([
                    XueqiuFetcher(search_backend=ddgs),
                    StcnFetcher(search_backend=ddgs),
                    SinaFinanceFetcher(search_backend=ddgs),
                    TonghuashunFetcher(search_backend=ddgs),
                ])

    def fetch_all(self, stock_code: str, stock_name: Optional[str] = None, is_hk: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for fetcher in self.fetchers:
            if not fetcher.can_fetch(stock_code, is_hk=is_hk):
                continue
            try:
                partial = fetcher.fetch(stock_code, stock_name)
                if partial:
                    for key, val in partial.items():
                        if key not in result:
                            result[key] = {"note": f"{fetcher.name} 补充", "snippets": []}
                        # 去重合并
                        seen = {s.get("url", "") for s in result[key]["snippets"]}
                        for s in val.get("snippets", []):
                            url = s.get("url", "")
                            if url not in seen:
                                result[key]["snippets"].append(s)
                                seen.add(url)
                        count = len(val.get("snippets", []))
                        if count:
                            result[key]["note"] += f"；{fetcher.name} {count} 条"
                time.sleep(self.delay)
            except Exception as e:
                print(f"[TopSitesFetcher] {fetcher.name} 异常: {e}")
                continue
        return result
