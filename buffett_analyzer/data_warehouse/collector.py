"""
统一数据收集入口
实现缓存优先策略：优先读取 SQLite，缺失时从网络获取并回写。
增强版支持行业估值 enrich 和联网搜索补缺。
"""

from typing import Dict, Any, List, Optional
import datetime
import pandas as pd
import akshare as ak
from .database import Database
from .cache_manager import CacheManager
from .fetchers.akshare_fetcher import AkShareFetcher
from .fetchers.baostock_fetcher import BaoStockFetcher
from .fetchers.industry_fetcher import IndustryFetcher
from .fetchers.web_search_fetcher import WebSearchFetcher


class DataCollector:
    def __init__(self, db_path: str = None):
        self.db = Database(db_path)
        self.cache = CacheManager(self.db)
        self.ak_fetcher = AkShareFetcher()
        self.bs_fetcher = BaoStockFetcher()
        self.industry_fetcher = IndustryFetcher()
        self.web_search_fetcher = WebSearchFetcher()

    @staticmethod
    def _is_hk_stock(code: str) -> bool:
        return AkShareFetcher._is_hk_stock(code)

    def _get_stock_name(self, stock_code: str) -> str:
        """尝试获取股票简称，用于搜索关键词。"""
        try:
            if self._is_hk_stock(stock_code):
                df = ak.stock_hk_valuation_comparison_em(symbol=stock_code)
                if not df.empty:
                    # 第二列通常为名称
                    return str(df.iloc[0, 1])
            else:
                df = ak.stock_info_a_code_name()
                matched = df[df["代码"] == stock_code]
                if not matched.empty:
                    return str(matched.iloc[0]["名称"])
        except Exception:
            pass
        return stock_code

    def _fetch_hk_valuation(self, stock_code: str) -> Dict[str, Any]:
        """使用 akshare 获取港股当前估值（含行业分位）。"""
        try:
            df_val = ak.stock_hk_valuation_comparison_em(symbol=stock_code)
            if df_val.empty:
                return {}
            row = df_val.iloc[0]

            df_price = ak.stock_hk_daily(symbol=stock_code)
            latest_price = df_price.iloc[-1] if not df_price.empty else None

            today = datetime.date.today().strftime('%Y-%m-%d')
            return {
                "trade_date": today,
                "close_price": float(latest_price['close']) if latest_price is not None else None,
                "pe_ttm": float(row.iloc[2]) if pd.notna(row.iloc[2]) else None,
                "pb": float(row.iloc[6]) if pd.notna(row.iloc[6]) else None,
                "ps_ttm": float(row.iloc[10]) if pd.notna(row.iloc[10]) else None,
                "pe_percentile_5y": float(row.iloc[3]) if pd.notna(row.iloc[3]) else None,
                "pb_percentile_5y": float(row.iloc[7]) if pd.notna(row.iloc[7]) else None,
                "ps_percentile_5y": float(row.iloc[11]) if pd.notna(row.iloc[11]) else None,
                "data_source": "akshare_hk",
                "note": "港股基础估值来自akshare; 分位字段为行业/市场分位, 非严格近7年历史分位"
            }
        except Exception as e:
            print(f"[DataCollector] 港股估值获取失败 ({stock_code}): {e}")
            return {}

    # ------------------------------------------------------------------
    # 基础收集（向后兼容）
    # ------------------------------------------------------------------
    def collect(self, stock_code: str) -> Dict[str, Any]:
        result = {
            "stock_code": stock_code,
            "financial_reports": None,
            "valuation": None,
            "sources": {}
        }
        is_hk = self._is_hk_stock(stock_code)

        # 1. 财务数据
        if self.cache.has_seven_year_financials(stock_code):
            result["financial_reports"] = self.cache.read_financial_reports(stock_code)
            result["sources"]["financial"] = "cache"
        else:
            fetch_result = self.ak_fetcher.fetch_financial_data(stock_code)
            df_fin = fetch_result["financial_reports"]
            if not df_fin.empty:
                if pd.api.types.is_datetime64_any_dtype(df_fin['report_date']):
                    df_fin['report_date'] = df_fin['report_date'].dt.strftime('%Y-%m-%d')
                self.cache.write_financial_reports(stock_code, df_fin)
                self.cache.update_meta(stock_code, "financial_reports", len(df_fin))
                result["financial_reports"] = self.cache.read_financial_reports(stock_code)
                result["sources"]["financial"] = "akshare"
            else:
                result["financial_reports"] = pd.DataFrame()
                result["sources"]["financial"] = "failed"

        # 2. 估值数据
        if self.cache.has_latest_valuation(stock_code):
            result["valuation"] = self.cache.read_valuation(stock_code)
            result["sources"]["valuation"] = "cache"
        else:
            if is_hk:
                val = self._fetch_hk_valuation(stock_code)
                if val and val.get("trade_date"):
                    self.cache.write_valuation(stock_code, val)
                    self.cache.update_meta(stock_code, "valuation_metrics", 1)
                    result["valuation"] = val
                    result["sources"]["valuation"] = "akshare_hk"
                else:
                    result["valuation"] = {}
                    result["sources"]["valuation"] = "failed"
            else:
                try:
                    bs_result = self.bs_fetcher.fetch_valuation(stock_code)
                    latest = bs_result.get("latest", {})
                    if latest and latest.get("trade_date"):
                        latest["data_source"] = "baostock"
                        self.cache.write_valuation(stock_code, latest)
                        self.cache.update_meta(stock_code, "valuation_metrics", len(bs_result.get("valuation_df", pd.DataFrame())))
                        result["valuation"] = latest
                        result["sources"]["valuation"] = "baostock"
                    else:
                        result["valuation"] = {}
                        result["sources"]["valuation"] = "failed"
                except Exception as e:
                    print(f"[DataCollector] 估值数据获取失败 ({stock_code}): {e}")
                    result["valuation"] = {}
                    result["sources"]["valuation"] = "failed"

        return result

    # ------------------------------------------------------------------
    # 增强收集：包含行业估值 enrich 和联网搜索补缺
    # ------------------------------------------------------------------
    def collect_enhanced(self, stock_code: str) -> Dict[str, Any]:
        """
        1) 执行基础 collect
        2) enrich 行业估值
        3) 检测缺失字段（港股历史分位）
        4) 联网搜索补缺
        5) 返回完整数据
        """
        # 1. 基础收集
        result = self.collect(stock_code)

        # 2. 行业估值 enrich（无论估值是否来自缓存，都重新 enrich）
        industry_data = self.industry_fetcher.fetch(stock_code)
        self._merge_industry_to_db(stock_code, industry_data)

        # 3. 检测缺失字段
        missing = self._detect_missing(stock_code)

        # 4. 联网搜索补缺
        if missing:
            stock_name = self._get_stock_name(stock_code)
            web_data = self.web_search_fetcher.fill_missing(stock_code, stock_name, missing)
            self._write_enrichment_to_db(stock_code, web_data)

        # 5. 统一获取定性分析数据（减少各模块独立调用 LLM 次数）
        self.collect_qualitative(stock_code)

        # 6. 重新读取完整估值
        full_val = self.cache.read_valuation(stock_code)
        if full_val:
            result["valuation"] = full_val

        return result

    def _merge_industry_to_db(self, stock_code: str, industry_data: Dict[str, Any]):
        """将行业估值数据 merge 到已有的 valuation_metrics 记录中。"""
        if not industry_data:
            return
        row = self.db.fetchone(
            "SELECT * FROM valuation_metrics WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (stock_code,)
        )
        if not row:
            return

        old_val = dict(row)
        new_val = old_val.copy()
        updated = False

        for key in ["industry_pe", "industry_pb", "industry_ps",
                    "pe_vs_industry", "pb_vs_industry", "ps_vs_industry"]:
            if industry_data.get(key) is not None:
                new_val[key] = industry_data[key]
                updated = True

        if updated:
            # 追加 note，保留原有 note；不修改 data_source
            existing_note = old_val.get("note", "") or ""
            ind_note = industry_data.get("note", "")
            if ind_note and ind_note not in existing_note:
                new_val["note"] = f"{existing_note}; {ind_note}".strip("; ")
            self.cache.write_valuation(stock_code, new_val)
            # 记录 enrichment log
            for key in ["industry_pe", "industry_pb", "industry_ps"]:
                if new_val.get(key) != old_val.get(key) and new_val.get(key) is not None:
                    self.cache.log_enrichment(
                        stock_code, key,
                        old_value=old_val.get(key),
                        new_value=new_val.get(key),
                        source="industry_fetcher"
                    )

    def _detect_missing(self, stock_code: str) -> List[str]:
        """
        检测 valuation_metrics 中需要联网搜索增强的字段。
        策略：
        - A股由 baostock 提供严格历史分位，不缺则跳过。
        - 港股当前 akshare_hk 提供的是行业/市场分位，若 data_source 不是 web_search/baostock，
          则视为"待增强"，尝试用 web_search 获取更准确的近7年历史分位。
        """
        missing = []
        row = self.db.fetchone(
            """SELECT pe_percentile_5y, pb_percentile_5y, ps_percentile_5y, data_source
               FROM valuation_metrics WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1""",
            (stock_code,)
        )
        if not row:
            return missing

        ds = row["data_source"] or ""
        # A股已有 baostock 历史分位，不搜索
        if not self._is_hk_stock(stock_code):
            return missing

        # 港股：若数据源已经是 web_search 或 baostock，视为已满足
        if ds in ("web_search", "baostock"):
            return missing

        for field in ["pe_percentile_5y", "pb_percentile_5y", "ps_percentile_5y"]:
            if row[field] is None:
                missing.append(field)
            else:
                # 即使已有值（行业分位），也加入待增强列表，尝试搜索历史分位
                missing.append(field)
        return missing

    def _write_enrichment_to_db(self, stock_code: str, web_data: Dict[str, Any]):
        """将联网搜索获取到的分位数据写入数据库，并记录 log。"""
        if not web_data:
            return
        row = self.db.fetchone(
            "SELECT * FROM valuation_metrics WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (stock_code,)
        )
        if not row:
            return

        old_val = dict(row)
        new_val = old_val.copy()
        updated = False

        for field in ["pe_percentile_5y", "pb_percentile_5y", "ps_percentile_5y"]:
            if web_data.get(field) is not None and old_val.get(field) is None:
                new_val[field] = web_data[field]
                updated = True
                self.cache.log_enrichment(
                    stock_code, field,
                    old_value=old_val.get(field),
                    new_value=web_data[field],
                    source="web_search"
                )

        if updated:
            new_val["data_source"] = "web_search"
            existing_note = old_val.get("note", "") or ""
            web_note = web_data.get("note", "")
            if web_note and web_note not in existing_note:
                new_val["note"] = f"{existing_note}; {web_note}".strip("; ")
            self.cache.write_valuation(stock_code, new_val)

    def manual_fill_valuation(self, stock_code: str, fields: Dict[str, Any], note: str = ""):
        """
        手动填补估值缺失字段，并记录来源为 manual。
        fields 示例: {"pe_percentile_5y": 35.0, "pb_percentile_5y": 20.0}
        """
        row = self.db.fetchone(
            "SELECT * FROM valuation_metrics WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (stock_code,)
        )
        if not row:
            print(f"[DataCollector] 未找到 {stock_code} 的估值记录，无法手动填充")
            return

        old_val = dict(row)
        new_val = old_val.copy()
        updated = False

        for field, value in fields.items():
            if value is not None:
                new_val[field] = float(value) if isinstance(value, (int, float, str)) and str(value).replace('.', '').isdigit() else value
                updated = True
                self.cache.log_enrichment(
                    stock_code, field,
                    old_value=old_val.get(field),
                    new_value=value,
                    source="manual"
                )

        if updated:
            new_val["data_source"] = "manual"
            existing_note = old_val.get("note", "") or ""
            if note and note not in existing_note:
                new_val["note"] = f"{existing_note}; {note}".strip("; ")
            self.cache.write_valuation(stock_code, new_val)
            print(f"[DataCollector] 已手动更新 {stock_code} 的估值数据: {list(fields.keys())}")

    # ------------------------------------------------------------------
    # 定性数据统一收集（减少LLM调用次数）
    # 策略：护城河1次 + 商业模式1次（含增长确定性，供估值模块复用）
    # ------------------------------------------------------------------
    def collect_qualitative(self, stock_code: str, industry_type: str = "general"):
        """统一获取定性分析数据，写入 SQLite 缓存供各模块复用。"""
        import os
        from ..quality_scoring.coze_client import CozeLLMClient

        token = os.getenv("COZE_API_TOKEN")
        if token:
            client = CozeLLMClient(api_token=token)
        else:
            client = CozeLLMClient(
                api_token="eyJhbGciOiJSUzI1NiIsImtpZCI6ImZmOTI5ZWIzLWM5NjctNGI5YS05ZGM0LTllMDYwODYxMTU1MCJ9.eyJpc3MiOiJodHRwczovL2FwaS5jb3plLmNuIiwiYXVkIjpbIlE3TFZ0ZkdwZzNEMVVKQ0pmdjhJcU1SdFJna2V1V20zIl0sImV4cCI6ODIxMDI2Njg3Njc5OSwiaWF0IjoxNzc2NDI4NzY5LCJzdWIiOiJzcGlmZmU6Ly9hcGkuY296ZS5jbi93b3JrbG9hZF9pZGVudGl0eS9pZDo3NjE1NTE0NzI0MDkxODIyMTA3Iiwic3JjIjoiaW5ib3VuZF9hdXRoX2FjY2Vzc190b2tlbl9pZDo3NjI5NzAzNDY5NzEyMDE1Mzg3In0.ZtfPq2Btc6ThWGiIG2kt3qmbw69ccPGQA_Rt7nXxUDPtLICKptgdkjU47fWISalpi1Wr7vbYEJM1Y5dXLmnVHlKLjUpwrH79unmURLgSieMlMAth4txWQYSdDbAeNRmTOW6PxN7gST35sRDpnhIWYn8dDnnEshr6L_H1mnAUTGOv7RgJDBjqxBsl2GyRkkF3hcUPKo4ALWZT09k-zeS1P6jnuAGozKwnC9dARZ6EvbSrQwRSUMLRAQ4a8h-WbkkJ23Pc-xUKq-IB_g1X2q_CyylL9AGCdASkcz7kfi4wQFM2svKnlulk_akWYruqVJTN7b2gqAWaExJaptfB0EjHqA"
            )

        if not client.is_configured():
            print("[DataCollector] Coze API Token 未配置，跳过定性数据收集")
            return

        # 1. 护城河定性
        if not self.cache.has_fresh_qualitative(stock_code, 'moat'):
            try:
                from ..moat_analysis.moat_analyzer import MoatAnalyzer
                # 毛利率数据用于 prompt 中的已知事实
                df = self.cache.read_financial_reports(stock_code)
                gm_values = df["gross_margin"].dropna().tail(5).tolist() if not df.empty and "gross_margin" in df.columns else []
                gm_result = {"values": gm_values, "std": 0.0, "trend": {"trend_direction": "N/A"}} if gm_values else {}
                prompt = MoatAnalyzer.build_qualitative_prompt(stock_code, gm_result)
                result = client.call(prompt, timeout=120)
                self.cache.write_qualitative_result(stock_code, 'moat', result)
                print(f"[DataCollector] 护城河定性数据已缓存 ({stock_code})")
            except Exception as e:
                print(f"[DataCollector] 护城河定性收集失败: {e}")
        else:
            print(f"[DataCollector] 护城河定性缓存命中 ({stock_code})")

        # 2. 商业模式定性（包含增长确定性，供估值模块复用）
        if not self.cache.has_fresh_qualitative(stock_code, 'business_model'):
            try:
                from ..business_model_analysis.business_model_analyzer import BusinessModelAnalyzer
                prompt = BusinessModelAnalyzer.build_qualitative_prompt(stock_code)
                result = client.call(prompt, timeout=120)
                self.cache.write_qualitative_result(stock_code, 'business_model', result)
                print(f"[DataCollector] 商业模式定性数据已缓存 ({stock_code})")
            except Exception as e:
                print(f"[DataCollector] 商业模式定性收集失败: {e}")
        else:
            print(f"[DataCollector] 商业模式定性缓存命中 ({stock_code})")

    def get_qualitative_result(self, stock_code: str, analysis_type: str) -> Optional[Dict[str, Any]]:
        """从缓存读取定性分析结果。"""
        return self.cache.read_qualitative_result(stock_code, analysis_type)

    def close(self):
        self.bs_fetcher.logout()
