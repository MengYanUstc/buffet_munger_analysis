"""
管理层分析 - Akshare 数据源聚合
优先使用官方/半官方接口，零爬虫维护成本。
"""

import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional


class AkshareBaseFetcher:
    """防御式 akshare 调用基类。"""

    @staticmethod
    def _try_import_akshare():
        try:
            import akshare as ak
            return ak
        except Exception as e:
            raise RuntimeError(f"akshare 导入失败: {e}")

    @staticmethod
    def _filter_by_keywords(snippets: List[Dict[str, str]], keywords: List[str]) -> List[Dict[str, str]]:
        """按关键词过滤 snippet（标题+摘要匹配任一关键词）。"""
        if not keywords:
            return snippets
        filtered = []
        for s in snippets:
            text = f"{s.get('title', '')} {s.get('summary', '')}"
            if any(kw in text for kw in keywords):
                filtered.append(s)
        return filtered


class AkshareNewsFetcher(AkshareBaseFetcher):
    """东方财富个股新闻：stock_news_em"""

    def fetch(self, stock_code: str, top_n: int = 30) -> Dict[str, Any]:
        ak = self._try_import_akshare()
        try:
            df = ak.stock_news_em(symbol=stock_code)
            if df is None or df.empty:
                return {}
            df = df.head(top_n)
        except Exception as e:
            print(f"[AkshareNewsFetcher] stock_news_em 失败: {e}")
            return {}

        # 统一 snippet
        snippets = []
        for _, row in df.iterrows():
            title = str(row.get("新闻标题", "")).strip()
            content = str(row.get("新闻内容", "")).strip()
            pub_time = str(row.get("发布时间", "")).strip()
            source = str(row.get("文章来源", "")).strip()
            url = str(row.get("新闻链接", "")).strip()
            summary_parts = [p for p in [content, pub_time, source] if p]
            summary = " | ".join(summary_parts)
            if title or summary:
                snippets.append({"title": title, "summary": summary, "url": url})

        # 按维度关键词分类
        result = {}
        result["dividend"] = {
            "note": f"akshare 新闻源: 分红相关 {len(self._filter_by_keywords(snippets, ['分红','派息','股息','送转','权益分派']))} 条",
            "snippets": self._filter_by_keywords(snippets, ["分红", "派息", "股息", "送转", "权益分派"]),
        }
        result["mergers"] = {
            "note": f"akshare 新闻源: 并购重组相关 {len(self._filter_by_keywords(snippets, ['并购','重组','商誉减值','收购','资产注入']))} 条",
            "snippets": self._filter_by_keywords(snippets, ["并购", "重组", "商誉减值", "收购", "资产注入"]),
        }
        result["violations"] = {
            "note": f"akshare 新闻源: 违规处罚相关 {len(self._filter_by_keywords(snippets, ['证监会','立案','调查','处罚','监管函','违规','警示','退市']))} 条",
            "snippets": self._filter_by_keywords(snippets, ["证监会", "立案", "调查", "处罚", "监管函", "违规", "警示", "退市"]),
        }
        result["management_holdings"] = {
            "note": f"akshare 新闻源: 持股变动相关 {len(self._filter_by_keywords(snippets, ['减持','增持','持股变动','清仓','离婚式减持','高管']))} 条",
            "snippets": self._filter_by_keywords(snippets, ["减持", "增持", "持股变动", "清仓", "离婚式减持", "高管"]),
        }
        return result


class AkshareNoticeFetcher(AkshareBaseFetcher):
    """巨潮资讯公司公告：stock_zh_a_disclosure_report_cninfo"""

    def fetch(self, stock_code: str, days_back: int = 365) -> Dict[str, Any]:
        ak = self._try_import_akshare()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")

        try:
            # 先获取全量公告（不指定 category，避免遗漏）
            df = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=stock_code,
                market="沪深京",
                category="",
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                return {}
        except Exception as e:
            print(f"[AkshareNoticeFetcher] stock_zh_a_disclosure_report_cninfo 失败: {e}")
            return {}

        snippets = []
        for _, row in df.iterrows():
            title = str(row.get("公告标题", "")).strip()
            pub_time = str(row.get("公告时间", "")).strip()
            url = str(row.get("公告链接", "")).strip()
            if title or pub_time:
                snippets.append({"title": title, "summary": pub_time, "url": url})

        result = {}
        # 权益分派
        dividend_snippets = self._filter_by_keywords(snippets, ["分红", "派息", "股息", "权益分派", "送转", "利润分配"])
        result["dividend"] = {
            "note": f"akshare 公告源: 权益分派公告 {len(dividend_snippets)} 条",
            "snippets": dividend_snippets,
        }
        # 并购重组
        merger_snippets = self._filter_by_keywords(snippets, ["并购", "重组", "重大资产重组", "收购", "资产注入", "商誉"])
        result["mergers"] = {
            "note": f"akshare 公告源: 并购重组公告 {len(merger_snippets)} 条",
            "snippets": merger_snippets,
        }
        # 违规处罚/风险提示
        violation_snippets = self._filter_by_keywords(
            snippets,
            ["证监会", "立案", "调查", "处罚", "监管函", "违规", "警示", "退市", "风险提示", "特别处理", "ST"]
        )
        result["violations"] = {
            "note": f"akshare 公告源: 违规/风险公告 {len(violation_snippets)} 条",
            "snippets": violation_snippets,
        }
        # 持股变动
        holdings_snippets = self._filter_by_keywords(
            snippets,
            ["减持", "增持", "持股变动", "清仓", "权益变动", "董监高"]
        )
        result["management_holdings"] = {
            "note": f"akshare 公告源: 持股变动公告 {len(holdings_snippets)} 条",
            "snippets": holdings_snippets,
        }
        return result


class AkshareDividendFetcher(AkshareBaseFetcher):
    """历史分红数据：stock_dividents_cninfo / stock_dividend_cninfo"""

    def fetch(self, stock_code: str, top_n: int = 10) -> Dict[str, Any]:
        ak = self._try_import_akshare()

        df = None
        for func_name in ["stock_dividents_cninfo", "stock_dividend_cninfo"]:
            try:
                func = getattr(ak, func_name)
                df = func(symbol=stock_code)
                if df is not None and not df.empty:
                    break
            except Exception:
                # 静默失败：akshare版本差异导致API可能不存在
                continue

        if df is None or df.empty:
            return {}

        df = df.head(top_n)
        records = df.to_dict(orient="records")
        return {
            "dividend": {
                "note": f"akshare 数据源: 历史分红记录 {len(records)} 条",
                "records": records,
            }
        }


class AkshareCompositeFetcher(AkshareBaseFetcher):
    """
    聚合以上所有 akshare fetcher，按维度合并结果。
    输出格式与 ManagementDataFetcher 期望的 web_search 结构保持一致。
    """

    def __init__(self):
        self.news = AkshareNewsFetcher()
        self.notice = AkshareNoticeFetcher()
        self.dividend = AkshareDividendFetcher()

    def fetch_all(self, stock_code: str) -> Dict[str, Any]:
        """
        返回结构示例:
        {
            "dividend": {"note": "...", "snippets": [...], "records": [...]},
            "mergers": {"note": "...", "snippets": [...]},
            "violations": {"note": "...", "snippets": [...]},
            "management_holdings": {"note": "...", "snippets": [...], "records": [...]},
        }
        """
        result = {}

        # 1. 分红
        dividend_res = self.dividend.fetch(stock_code)
        if dividend_res:
            result["dividend"] = dividend_res["dividend"]

        # 2. 新闻（覆盖分红/并购/违规/持股变动）
        news_res = self.news.fetch(stock_code)
        for key in ["dividend", "mergers", "violations", "management_holdings"]:
            if key in news_res:
                if key not in result:
                    result[key] = news_res[key]
                else:
                    # 合并 snippets
                    existing = result[key].get("snippets", [])
                    new_snippets = news_res[key].get("snippets", [])
                    # 去重（按 url）
                    seen = {s.get("url", "") for s in existing}
                    merged = existing[:]
                    for s in new_snippets:
                        if s.get("url", "") not in seen:
                            merged.append(s)
                            seen.add(s.get("url", ""))
                    result[key]["snippets"] = merged
                    result[key]["note"] += f"；新闻补充 {len(new_snippets)} 条"

        # 4. 公告（覆盖分红/并购/违规/持股变动）
        notice_res = self.notice.fetch(stock_code)
        for key in ["dividend", "mergers", "violations", "management_holdings"]:
            if key in notice_res:
                if key not in result:
                    result[key] = notice_res[key]
                else:
                    existing = result[key].get("snippets", [])
                    new_snippets = notice_res[key].get("snippets", [])
                    seen = {s.get("url", "") for s in existing}
                    merged = existing[:]
                    for s in new_snippets:
                        if s.get("url", "") not in seen:
                            merged.append(s)
                            seen.add(s.get("url", ""))
                    result[key]["snippets"] = merged
                    result[key]["note"] += f"；公告补充 {len(new_snippets)} 条"

        return result
