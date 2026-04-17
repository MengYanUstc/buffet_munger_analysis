"""
Web 搜索补缺模块
利用 DuckDuckGo 搜索缺失的估值分位数据，并尝试从网页中提取数值。
主要用于填补港股缺失的近7年 PE/PB/PS 历史分位。
"""

import re
import time
import warnings
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

# 抑制 duckduckgo_search 包重命名警告
warnings.filterwarnings("ignore", message="This package.*has been renamed to.*ddgs", category=RuntimeWarning)

try:
    from ddgs import DDGS
except ImportError:
    from duckduckgo_search import DDGS


class WebSearchFetcher:
    def __init__(self, max_results: int = 5, request_timeout: int = 10):
        self.max_results = max_results
        self.timeout = request_timeout

    def fill_missing(self, stock_code: str, stock_name: str, missing_fields: List[str]) -> Dict[str, Any]:
        """
        对缺失字段执行联网搜索并尝试提取数值。
        返回：
        {
            "pe_percentile_5y": float or None,
            "pb_percentile_5y": float or None,
            "ps_percentile_5y": float or None,
            "data_source": "web_search",
            "note": "..."
        }
        """
        result = {
            "pe_percentile_5y": None,
            "pb_percentile_5y": None,
            "ps_percentile_5y": None,
            "data_source": "web_search",
            "note": ""
        }
        notes = []

        field_keywords = {
            "pe_percentile_5y": ["PE 历史分位", "市盈率 历史分位", "PE percentile 5 year", "PE 分位数"],
            "pb_percentile_5y": ["PB 历史分位", "市净率 历史分位", "PB percentile 5 year", "PB 分位数"],
            "ps_percentile_5y": ["PS 历史分位", "市销率 历史分位", "PS percentile 5 year", "PS 分位数"],
        }

        for field in missing_fields:
            if field not in field_keywords:
                continue
            keywords = field_keywords[field]
            best_val = None
            best_source = ""

            for kw in keywords:
                query = f"{stock_name} {stock_code} {kw}"
                try:
                    with DDGS() as ddgs:
                        search_results = list(ddgs.text(query, max_results=self.max_results))
                except Exception as e:
                    notes.append(f"DDGS搜索失败 ({field}, {kw}): {e}")
                    continue

                for sr in search_results:
                    url = sr.get("href", "")
                    title = sr.get("title", "")
                    body = sr.get("body", "")
                    # 先在摘要中尝试匹配
                    val = self._extract_percentile(body)
                    if val is not None:
                        best_val = val
                        best_source = f"{title} ({url})"
                        break

                    # 摘要未命中，尝试抓取网页
                    val = self._fetch_and_extract(url)
                    if val is not None:
                        best_val = val
                        best_source = f"{title} ({url})"
                        break

                    time.sleep(0.5)  # 礼貌延迟

                if best_val is not None:
                    break

            if best_val is not None:
                result[field] = best_val
                notes.append(f"{field}={best_val:.2f}% 来自 {best_source}")
            else:
                notes.append(f"{field} 未从搜索结果中解析到有效数值")

        result["note"] = "; ".join(notes)
        return result

    def _extract_percentile(self, text: str) -> Optional[float]:
        """从文本中提取百分位数值（0-100之间）。"""
        if not text:
            return None
        patterns = [
            # 中文常见格式
            r"(?:PE|PB|PS|市盈率|市净率|市销率).*?(?:历史)?分位[（\(]?(?:7年|近7年|5年|近5年)?[）\)]?[：:]\s*(\d+\.?\d*)\s*%?",
            r"(?:历史分位|估值分位|百分位).*?(?:7年|近7年|5年|近5年)?[:：]?\s*(\d+\.?\d*)\s*%?",
            r"(\d+\.?\d*)\s*%\s*(?:历史分位|估值分位|分位)",
            # 英文格式
            r"(?:PE|PB|PS)\s+(?:5[-\s]?year)?\s*percentile[:\s]+(\d+\.?\d*)\s*%?",
            r"percentile\s*[:\s]+(\d+\.?\d*)\s*%?",
            # 更宽松：在 PE/PB/PS 附近出现的 0-100 之间的数字+%
            r"(?:PE|PB|PS|市盈率|市净率|市销率).*?(\d{1,2}(?:\.\d+)?)\s*%",
        ]
        for pat in patterns:
            matches = re.findall(pat, text, re.IGNORECASE)
            for m in matches:
                try:
                    val = float(m)
                    if 0 <= val <= 100:
                        return val
                except ValueError:
                    continue
        return None

    def _fetch_and_extract(self, url: str) -> Optional[float]:
        """抓取网页并尝试提取分位数值。"""
        if not url.startswith("http"):
            return None
        # 跳过明显不相关的文件
        skip_suffixes = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".mp4", ".jpg", ".png")
        if any(url.lower().endswith(s) for s in skip_suffixes):
            return None

        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                )
            }
            resp = requests.get(url, headers=headers, timeout=self.timeout)
            if resp.status_code != 200:
                return None
            # 只处理 HTML
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" not in content_type:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            # 移除 script/style
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
            # 限制长度，避免噪音
            text = text[:20000]
            return self._extract_percentile(text)
        except Exception:
            return None
