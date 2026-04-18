"""
管理层分析数据获取模块
获取资本配置与诚信评估所需的辅助数据。
当前为初版，部分数据源（如违规记录）待后续接入。
"""

import re
import pandas as pd
from typing import Dict, Any, List

from ..data_fetcher import DataFetcher
from ..utils import is_hk_stock
from .web_search_fetcher import ManagementWebSearchFetcher
from .bing_search_fetcher import BingWebSearchFetcher
from .akshare_fetchers import AkshareCompositeFetcher
from .top_sites_fetcher import TopSitesFetcher


class ManagementDataFetcher:
    def __init__(self, source: str = 'akshare'):
        self.source = source
        self._base_fetcher = DataFetcher(source=source)
        self._akshare = AkshareCompositeFetcher()
        self._top_sites = TopSitesFetcher(delay=0.5)
        self._bing_api = BingWebSearchFetcher(max_results=5)
        self._ddgs = ManagementWebSearchFetcher(max_results=3)

    @staticmethod
    def _is_garbage_result(result: Dict[str, Any]) -> bool:
        """校验搜索结果是否为垃圾/不相关内容。"""
        total_snippets = 0
        chinese_chars = 0
        total_text_len = 0
        for v in result.values():
            snippets = v.get("snippets", [])
            total_snippets += len(snippets)
            for s in snippets:
                title = s.get("title", "")
                summary = s.get("summary", "")
                text = title + summary
                total_text_len += len(text)
                chinese_chars += len(re.findall(r'[\u4e00-\u9fff]', text))
        if total_snippets == 0:
            return True
        # 如果中文字符占比过低（<10%）且总长度>0，视为垃圾内容
        if total_text_len > 0 and chinese_chars / total_text_len < 0.10:
            return True
        # 如果平均每个 snippet 中文字符少于 5 个，视为垃圾
        if chinese_chars / total_snippets < 5:
            return True
        return False

    def _search_all(self, stock_name: str, stock_code: str) -> Dict[str, Any]:
        """按优先级尝试多种搜索后端：Bing API > DDGS（仅在 ENABLE_DDGS_FALLBACK=1 时启用）。"""
        import os
        enable_ddgs = os.getenv("ENABLE_DDGS_FALLBACK", "0") == "1"
        backends = [
            ("Bing API", self._bing_api, self._bing_api.is_configured()),
            ("DDGS", self._ddgs, enable_ddgs),
        ]
        for name, backend, usable in backends:
            if not usable:
                continue
            try:
                print(f"[ManagementDataFetcher] 尝试使用 {name} 搜索...")
                result = backend.search_all(stock_name, stock_code)
                if self._is_garbage_result(result):
                    print(f"[ManagementDataFetcher] {name} 返回结果质量过低，尝试下一个后端...")
                    continue
                return result, name
            except Exception as e:
                print(f"[ManagementDataFetcher] {name} 搜索失败: {e}")
                continue
        return {}, ""

    def fetch_all(self, stock_code: str) -> Dict[str, Any]:
        """汇总管理层分析所需的全部数据。优先使用 akshare 官方接口，缺失时 fallback 到搜索引擎。"""
        is_hk = is_hk_stock(stock_code)
        result = {
            "roic_trend": self.fetch_roic_trend(stock_code, is_hk=is_hk),
            "dividend": self.fetch_dividend(stock_code, is_hk=is_hk),
            "pledge": self.fetch_pledge(stock_code, is_hk=is_hk),
            "violations": self.fetch_violations(stock_code),
            "management_holdings": self.fetch_management_holdings(stock_code),
            "mergers": {"note": "并购与商誉数据本地未接入，由联网搜索补充"},
        }

        keys_to_fill = ["dividend", "violations", "management_holdings", "mergers"]

        # 第一步：A 股优先使用 akshare 官方接口（港股公告接口不全，跳过）
        if not is_hk:
            try:
                ak_results = self._akshare.fetch_all(stock_code)
                for key in keys_to_fill:
                    if key in ak_results and (ak_results[key].get("snippets") or ak_results[key].get("records")):
                        result[key].update(ak_results[key])
                        # 覆盖占位符 note
                        result[key]["note"] = ak_results[key].get("note", "")
            except Exception as e:
                print(f"[ManagementDataFetcher] akshare 聚合获取失败: {e}")

        # 第二步：对仍缺失或空的维度，尝试精选网站爬虫补充
        needs_top_sites = False
        for key in keys_to_fill:
            note = result[key].get("note", "")
            has_content = bool(result[key].get("snippets") or result[key].get("records"))
            if not has_content or "待后续接入" in note or "未接入" in note or "拉取超时" in note or "失败" in note:
                needs_top_sites = True

        if needs_top_sites:
            stock_name = self._get_stock_name(stock_code, is_hk=is_hk) or ""
            try:
                top_results = self._top_sites.fetch_all(stock_code, stock_name=stock_name, is_hk=is_hk)
                for key in keys_to_fill:
                    if key in top_results and top_results[key].get("snippets"):
                        if key not in result or not result[key].get("snippets"):
                            result[key]["snippets"] = top_results[key]["snippets"]
                            result[key]["note"] = top_results[key].get("note", "")
                        else:
                            existing = result[key].get("snippets", [])
                            new_snippets = top_results[key].get("snippets", [])
                            seen = {s.get("url", "") for s in existing}
                            for s in new_snippets:
                                if s.get("url", "") not in seen:
                                    existing.append(s)
                                    seen.add(s.get("url", ""))
                            result[key]["snippets"] = existing
                            old_note = result[key].get("note", "")
                            result[key]["note"] = f"{old_note}；{top_results[key].get('note', '')}" if old_note else top_results[key].get("note", "")
            except Exception as e:
                print(f"[ManagementDataFetcher] 精选网站爬虫层失败: {e}")

        # 第三步：对仍缺失或空的维度，fallback 搜索引擎
        needs_search = False
        for key in keys_to_fill:
            note = result[key].get("note", "")
            has_content = bool(result[key].get("snippets") or result[key].get("records"))
            if not has_content or "待后续接入" in note or "未接入" in note or "拉取超时" in note or "失败" in note:
                needs_search = True

        if needs_search:
            stock_name = self._get_stock_name(stock_code, is_hk=is_hk) or ""
            web_results, backend_name = self._search_all(stock_name, stock_code)
            if web_results:
                for key in keys_to_fill:
                    if key not in web_results:
                        continue
                    existing_snippets = result[key].get("snippets", [])
                    new_snippets = web_results[key].get("snippets", [])
                    seen = {s.get("url", "") for s in existing_snippets}
                    for s in new_snippets:
                        if s.get("url", "") not in seen:
                            existing_snippets.append(s)
                            seen.add(s.get("url", ""))
                    result[key]["snippets"] = existing_snippets
                    old_note = result[key].get("note", "")
                    append_msg = f"已补充 {backend_name} 搜索结果"
                    result[key]["note"] = f"{old_note}；{append_msg}" if old_note else append_msg
                    result[key]["web_search"] = web_results[key]

        return result

    # 常见港股名称缓存，避免调用极慢的 akshare 全市场接口
    _HK_NAME_CACHE = {
        "03333": "恒大集团",
        "00700": "腾讯控股",
        "03690": "美团-W",
        "01810": "小米集团-W",
        "09988": "阿里巴巴-SW",
        "09618": "京东集团-SW",
        "09888": "百度集团-SW",
    }

    def _get_stock_name(self, stock_code: str, is_hk: bool = False) -> str:
        """尝试获取股票简称。A股用 akshare 个股信息接口；港股优先查本地缓存，再尝试港股通列表。"""
        try:
            import akshare as ak
            if is_hk:
                if stock_code in self._HK_NAME_CACHE:
                    return self._HK_NAME_CACHE[stock_code]
                # 港股通列表通常较快（<2s），若未命中则不调用极慢的全市场接口
                df = ak.stock_hk_ggt_components_em()
                matched = df[df['代码'] == stock_code]
                if not matched.empty:
                    return str(matched.iloc[0]['名称'])
            else:
                df = ak.stock_individual_info_em(symbol=stock_code)
                if not df.empty:
                    name_row = df[df['item'] == '股票简称']
                    if not name_row.empty:
                        return str(name_row.iloc[0]['value'])
        except Exception:
            pass
        return ""

    def fetch_roic_trend(self, stock_code: str, is_hk: bool = False) -> Dict[str, Any]:
        """获取近5年ROIC数据及趋势。支持港股。"""
        try:
            if is_hk:
                import akshare as ak
                df = ak.stock_financial_hk_analysis_indicator_em(symbol=stock_code)
                if df.empty or 'ROIC_YEARLY' not in df.columns:
                    return {"note": "港股ROIC数据缺失"}
                df = df.sort_values('REPORT_DATE')
                values = pd.to_numeric(df['ROIC_YEARLY'], errors='coerce').dropna().tail(5).tolist()
            else:
                df = self._base_fetcher.fetch_indicator_data(stock_code)
                if df.empty or 'ROIC' not in df.columns:
                    return {"note": "ROIC数据缺失"}
                values = df['ROIC'].dropna().tail(5).tolist()
        except Exception as e:
            return {"note": f"ROIC数据获取失败: {e}"}

        if len(values) < 2:
            return {"values": values, "trend": "数据不足"}
        first_mean = sum(values[:2]) / 2
        last_mean = sum(values[-2:]) / 2
        diff = last_mean - first_mean
        if diff >= 3:
            trend = "明显上升"
        elif diff >= 1:
            trend = "温和上升"
        elif diff >= -1:
            trend = "基本稳定"
        elif diff >= -3:
            trend = "温和下降"
        else:
            trend = "明显下降"
        return {
            "values": [round(v, 2) for v in values],
            "trend": trend,
            "trend_diff": round(diff, 2),
        }

    def fetch_dividend(self, stock_code: str, is_hk: bool = False) -> Dict[str, Any]:
        """尝试获取分红数据。"""
        # 当前 akshare 分红接口对个股支持不稳定，先返回占位，由联网搜索补充
        return {"note": "分红数据待后续接入（当前数据源不稳定）"}

    def fetch_pledge(self, stock_code: str, is_hk: bool = False) -> Dict[str, Any]:
        """尝试获取股权质押数据。"""
        if is_hk:
            return {"note": "港股股权质押数据暂未接入"}
        try:
            import akshare as ak
            df = ak.stock_gpzy_pledge_ratio_em()
            row = df[df['股票代码'] == stock_code]
            if not row.empty:
                r = row.iloc[0]
                return {
                    "note": "已获取股权质押数据",
                    "pledge_ratio": r.get('质押比例'),
                    "pledge_amount": r.get('质押股数'),
                }
            return {"note": "未找到该股票的股权质押数据"}
        except Exception as e:
            return {"note": f"股权质押数据获取失败: {e}"}

    def fetch_violations(self, stock_code: str) -> Dict[str, Any]:
        """尝试获取违规/处罚记录。"""
        # 当前未接入稳定的数据源，返回占位，由 fetch_all 补充联网搜索结果
        return {"note": "违规记录数据待后续接入", "records": [], "has_fraud_flag": None}

    def fetch_management_holdings(self, stock_code: str) -> Dict[str, Any]:
        """尝试获取高管持股变动。"""
        # 当前接口全量拉取极慢，返回占位，由 fetch_all 补充联网搜索结果
        return {"note": "高管持股变动数据待后续接入（当前数据源拉取超时）"}
