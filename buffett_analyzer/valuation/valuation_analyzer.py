"""
估值分析主模块（Module 5）
总分 20 分 = 定量 17 分 + 定性 3 分（增长确定性，Coze LLM）

定量维度：
- 绝对估值水平（6分）：PE为基础，PB/PS为辅助加分
- 相对估值（4分）：历史分位 + 行业对比 + 一致性调整
- 长期 PEG（3分）：PE / 盈利增长率
- DCF 安全边际（4分）：基于 FCF 折现模型计算企业价值，与当前市值比较

定性维度：
- 增长确定性（3分）：由 Coze LLM 基于历史增长、行业前景、竞争地位等判断

DCF 模型：
- 总层面简化多阶段 FCF 折现
- 增长率序列根据 profit_cagr 自动选择
- 折现率 8%，永续增长率 3%
- 当前市值近似 = PE_ttm × 最近年度净利润（万元）

数据流：
  SQLite (valuation_metrics + financial_reports) → 定量计算 + LLM 定性 → 评分
"""

from typing import Dict, Any, List, Optional
import numpy as np
import pandas as pd

from ..core import AnalyzerBase, AnalysisReport
from ..data_warehouse.collector import DataCollector
from .valuation_scorer import (
    calculate_absolute_valuation_score,
    calculate_relative_valuation_score,
    calculate_long_term_peg_score,
    calculate_dcf_valuation_total,
    calculate_dcf_safety_margin_score,
    calculate_pe_base_score,
    calculate_pb_bonus,
    calculate_ps_bonus,
    calculate_historical_percentile_score,
    calculate_pb_ps_percentile_bonus,
)


class ValuationAnalyzer(AnalyzerBase):
    module_id = "valuation"
    module_name = "估值分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.collector = DataCollector()

    def run(self) -> AnalysisReport:
        # 0. 确保财务与估值数据已入库
        self.collector.collect(self.stock_code)

        # 1. 读取估值数据（valuation_metrics 最新一条）
        val_data = self.collector.cache.read_valuation(self.stock_code) or {}

        # 2. 读取财务数据（financial_reports，用于 PEG 和 DCF）
        df_fin = self.collector.cache.read_financial_reports(self.stock_code)

        # 3. 提取核心指标
        pe = val_data.get("pe_ttm")
        pb = val_data.get("pb")
        ps = val_data.get("ps_ttm")
        pe_percentile = val_data.get("pe_percentile_5y")
        pb_percentile = val_data.get("pb_percentile_5y")
        ps_percentile = val_data.get("ps_percentile_5y")
        close_price = val_data.get("close_price")

        # 盈利 CAGR（用于 PEG 和 DCF 增长率序列）
        profit_cagr = self._calculate_profit_cagr(df_fin)

        # DCF 基础数据：5年平均FCF转化率 + 最近年度净利润
        dcf_base = self._get_dcf_base_data(df_fin)
        latest_net_profit = dcf_base.get("latest_net_profit")
        avg_fcf_conversion = dcf_base.get("avg_fcf_conversion")
        latest_fcf = dcf_base.get("latest_fcf")
        fcf_yearly_records = dcf_base.get("yearly_records", [])

        # 4. 计算定量各维度评分

        # 4.1 绝对估值（6分）
        abs_score = calculate_absolute_valuation_score(pe, pb, ps)
        abs_detail = {
            "pe": pe,
            "pb": pb,
            "ps": ps,
            "pe_base_score": calculate_pe_base_score(pe),
            "pb_bonus": calculate_pb_bonus(pb),
            "ps_bonus": calculate_ps_bonus(ps),
            "score": abs_score,
            "max_score": 6.0,
            "reason": self._abs_reason(pe, pb, ps, abs_score),
        }

        # 4.2 相对估值（4分）
        rel_score = calculate_relative_valuation_score(pe_percentile, pb_percentile, ps_percentile)
        rel_detail = {
            "pe_percentile_5y": pe_percentile,
            "pb_percentile_5y": pb_percentile,
            "ps_percentile_5y": ps_percentile,
            "pe_historical_score": calculate_historical_percentile_score(pe_percentile),
            "pb_percentile_bonus": calculate_pb_ps_percentile_bonus(pb_percentile),
            "ps_percentile_bonus": calculate_pb_ps_percentile_bonus(ps_percentile),
            "score": rel_score,
            "max_score": 4.0,
            "reason": self._rel_reason(pe_percentile, pb_percentile, ps_percentile, rel_score),
        }

        # 4.3 长期 PEG（3分）
        peg = self._calculate_peg(pe, profit_cagr)
        peg_score = calculate_long_term_peg_score(peg, abs_score)
        peg_detail = {
            "pe": pe,
            "profit_cagr": profit_cagr,
            "peg": round(peg, 2) if peg is not None else None,
            "score": peg_score,
            "max_score": 3.0,
            "reason": self._peg_reason(peg, profit_cagr, peg_score),
        }

        # 4.4 DCF 安全边际（4分）
        dcf_result = self._calculate_dcf_detail(
            latest_net_profit=latest_net_profit,
            avg_fcf_conversion=avg_fcf_conversion,
            profit_cagr=profit_cagr,
            pe=pe,
            latest_fcf=latest_fcf,
            fcf_yearly_records=fcf_yearly_records,
        )
        dcf_score = dcf_result["score"]
        dcf_detail = {
            "latest_fcf": latest_fcf,
            "latest_net_profit": latest_net_profit,
            "profit_cagr": profit_cagr,
            "enterprise_value": dcf_result.get("enterprise_value"),
            "market_cap_approx": dcf_result.get("market_cap_approx"),
            "safety_margin": dcf_result.get("safety_margin"),
            "dcf_params": dcf_result.get("dcf_params"),
            "score": dcf_score,
            "max_score": 4.0,
            "reason": dcf_result.get("reason", ""),
        }

        # 5. 定性：增长确定性（3分，复用 business_model 缓存）
        growth_result = self._get_growth_certainty_result()
        growth_certainty_score = growth_result.get("score", 0.0)
        growth_detail = {
            "score": growth_certainty_score,
            "max_score": 3.0,
            "reason": growth_result.get("reason", ""),
        }

        # 6. 汇总
        quantitative_total = round(abs_score + rel_score + peg_score + dcf_score, 1)
        total_score = round(quantitative_total + growth_certainty_score, 1)
        rating = self._rating(total_score)

        dimensions = {
            "absolute_valuation": abs_detail,
            "relative_valuation": rel_detail,
            "long_term_peg": peg_detail,
            "dcf_safety_margin": dcf_detail,
            "growth_certainty": growth_detail,
        }

        summary = {
            "quantitative_score": quantitative_total,
            "quantitative_max": 17.0,
            "qualitative_score": growth_certainty_score,
            "qualitative_max": 3.0,
            "total_score": total_score,
            "max_score": 20.0,
            "rating": rating,
        }

        raw_data = {
            "valuation_metrics": val_data,
            "financial_years": len(df_fin) if not df_fin.empty else 0,
            "llm_growth_certainty_raw": growth_result.get("_raw_text", ""),
        }

        return AnalysisReport(
            module_id=self.module_id,
            module_name=self.module_name,
            stock_code=self.stock_code,
            total_score=total_score,
            max_score=20.0,
            rating=rating,
            dimensions=dimensions,
            summary=summary,
            raw_data=raw_data,
        )

    # ------------------------------------------------------------------
    # 辅助计算
    # ------------------------------------------------------------------

    @staticmethod
    def _calculate_profit_cagr(df) -> Optional[float]:
        """从 financial_reports 计算近5年净利润 CAGR。"""
        if df.empty:
            return None
        profit_col = None
        for col in ["parent_net_profit", "net_profit", "deduct_net_profit"]:
            if col in df.columns and df[col].notna().any():
                profit_col = col
                break
        if profit_col is None:
            return None

        series = df[profit_col].dropna().tail(5)
        if len(series) < 2:
            return None

        first = float(series.iloc[0])
        last = float(series.iloc[-1])
        if first <= 0 or last <= 0:
            return None

        n = len(series) - 1
        cagr = (last / first) ** (1 / n) - 1
        return round(cagr, 3)

    @staticmethod
    def _get_dcf_base_data(df) -> Dict[str, Any]:
        """
        获取 DCF 计算所需的基础数据。
        采用 5 年平均 FCF 转化率（FCF/净利润）作为 base_fcf 的推算依据。

        Returns:
            {
                "latest_net_profit": 最近年度净利润,
                "avg_fcf_conversion": 5年平均FCF转化率,
                "latest_fcf": 最近年度FCF,
                "yearly_records": [{"year": ..., "fcf": ..., "profit": ..., "ratio": ...}],
            }
        """
        if df.empty:
            return {}

        # 获取近5年有效数据（FCF和净利润同时非空）
        records = []
        profit_col = None
        for col in ["parent_net_profit", "net_profit"]:
            if col in df.columns:
                profit_col = col
                break

        if profit_col is None or "fcf" not in df.columns:
            return {}

        # 取最近5行，逐行检查
        tail_df = df.tail(5)
        for _, row in tail_df.iterrows():
            fcf = row.get("fcf")
            profit = row.get(profit_col)
            report_date = row.get("report_date", "")
            year = str(report_date)[:4] if report_date else ""
            if pd.notna(fcf) and pd.notna(profit) and float(profit) > 0:
                ratio = float(fcf) / float(profit)
                records.append({
                    "year": year,
                    "fcf": round(float(fcf), 2),
                    "profit": round(float(profit), 2),
                    "ratio": round(ratio, 3),
                })

        if not records:
            return {}

        # 5年平均 FCF 转化率（各年比率的算术平均）
        avg_conversion = sum(r["ratio"] for r in records) / len(records)

        # 最近年度数据
        latest = records[-1]

        return {
            "latest_net_profit": latest["profit"],
            "avg_fcf_conversion": round(avg_conversion, 3),
            "latest_fcf": latest["fcf"],
            "yearly_records": records,
        }

    @staticmethod
    def _calculate_peg(pe: Optional[float], profit_cagr: Optional[float]) -> Optional[float]:
        """计算 PEG = PE / (CAGR × 100)。"""
        if pe is None or profit_cagr is None or profit_cagr <= 0:
            return None
        peg = pe / (profit_cagr * 100)
        return peg

    def _calculate_dcf_detail(
        self,
        latest_net_profit: Optional[float],
        avg_fcf_conversion: Optional[float],
        profit_cagr: Optional[float],
        pe: Optional[float],
        latest_fcf: Optional[float] = None,
        fcf_yearly_records: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        计算 DCF 安全边际详细结果。
        base_fcf = 最近年度净利润 × 5年平均FCF转化率（参考商业模式分析的FCF转化率计算方式）。
        当前市值近似 = PE_ttm × 最近年度净利润（万元）。
        """
        if latest_net_profit is None or avg_fcf_conversion is None or profit_cagr is None or pe is None:
            return {
                "score": 0.0,
                "enterprise_value": None,
                "market_cap_approx": None,
                "safety_margin": None,
                "dcf_params": None,
                "reason": "净利润、FCF转化率、CAGR 或 PE 数据缺失，无法计算 DCF",
            }

        if latest_net_profit <= 0 or pe <= 0 or avg_fcf_conversion < 0:
            return {
                "score": 0.0,
                "enterprise_value": None,
                "market_cap_approx": None,
                "safety_margin": None,
                "dcf_params": None,
                "reason": "净利润、PE 为负/零，或 FCF 转化率为负，无法计算 DCF",
            }

        # base_fcf 采用 5年平均FCF转化率 × 最近年度净利润
        base_fcf = latest_net_profit * avg_fcf_conversion

        # DCF 估值（总层面）
        dcf = calculate_dcf_valuation_total(
            base_fcf=base_fcf,
            profit_cagr=profit_cagr,
        )

        # 近似市值
        market_cap_approx = pe * latest_net_profit

        # 安全边际评分
        score = calculate_dcf_safety_margin_score(
            dcf["enterprise_value"], market_cap_approx
        )

        safety_margin = dcf["enterprise_value"] / market_cap_approx if market_cap_approx > 0 else 0

        reason = (
            f"5年平均FCF转化率={avg_fcf_conversion:.2f}，"
            f"base_fcf={base_fcf:.0f}万（={latest_net_profit:.0f}万×{avg_fcf_conversion:.2f}），"
            f"DCF企业价值={dcf['enterprise_value']:.0f}万，"
            f"近似市值={market_cap_approx:.0f}万，"
            f"安全边际={safety_margin:.2f}，"
            f"得分={score:.0f}分（满分4分）"
        )

        return {
            "score": score,
            "enterprise_value": dcf["enterprise_value"],
            "market_cap_approx": round(market_cap_approx, 2),
            "safety_margin": round(safety_margin, 4),
            "dcf_params": {
                "base_fcf": base_fcf,
                "avg_fcf_conversion": avg_fcf_conversion,
                "latest_fcf": latest_fcf,
                "latest_net_profit": latest_net_profit,
                "fcf_yearly_records": fcf_yearly_records or [],
                "growth_rates": dcf["growth_rates"],
                "sequence_name": dcf["sequence_name"],
                "discount_rate": dcf["discount_rate"],
                "perpetual_growth": dcf["perpetual_growth"],
                "pv_fcf": dcf["pv_fcf"],
                "terminal_value": dcf["terminal_value"],
                "terminal_pv": dcf["terminal_pv"],
            },
            "reason": reason,
        }

    # ------------------------------------------------------------------
    # 内联评分辅助（用于 detail 展示）
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # 定性：增长确定性（3分）
    # ------------------------------------------------------------------

    def _get_growth_certainty_result(self) -> Dict[str, Any]:
        """从统一缓存读取增长确定性定性结果。
        复用 business_model 定性结果中的 growth_certainty 字段，避免重复 LLM 调用。
        """
        # 优先从 business_model 缓存读取增长确定性
        bm_result = self.collector.get_qualitative_result(self.stock_code, "business_model")
        if bm_result is not None and "growth_certainty" in bm_result:
            gc = bm_result["growth_certainty"]
            return {
                "score": gc.get("score", 0.0),
                "reason": gc.get("reason", ""),
                "key_facts": bm_result.get("key_facts", []),
                "risk_warnings": bm_result.get("risk_warnings", []),
            }

        print("[ValuationAnalyzer] business_model 缓存未命中或缺少 growth_certainty，fallback...")
        return self._empty_growth_result("缺少 business_model 定性缓存")

    @staticmethod
    def _empty_growth_result(reason: str) -> Dict[str, Any]:
        return {
            "score": 0.0,
            "reason": f"LLM 调用失败: {reason}",
            "key_facts": [],
            "risk_warnings": [],
        }

    @staticmethod
    def _rating(total: float) -> str:
        """估值模块评级（满分20分）。"""
        if total >= 17:
            return "极具吸引力"
        elif total >= 13:
            return "合理偏低"
        elif total >= 9:
            return "合理"
        else:
            return "偏高"

    # ------------------------------------------------------------------
    # 理由生成
    # ------------------------------------------------------------------

    @staticmethod
    def _abs_reason(pe, pb, ps, score) -> str:
        parts = []
        if pe is not None:
            parts.append(f"PE={pe:.1f}")
        if pb is not None:
            parts.append(f"PB={pb:.1f}")
        if ps is not None:
            parts.append(f"PS={ps:.1f}")
        return f"{'，'.join(parts)}，绝对估值得分={score:.1f}分（满分6分）"

    @staticmethod
    def _rel_reason(pe_pct, pb_pct, ps_pct, score) -> str:
        parts = []
        if pe_pct is not None:
            parts.append(f"PE历史分位={pe_pct:.1f}%")
        if pb_pct is not None:
            parts.append(f"PB历史分位={pb_pct:.1f}%")
        if ps_pct is not None:
            parts.append(f"PS历史分位={ps_pct:.1f}%")
        return f"{'，'.join(parts)}，相对估值得分={score:.1f}分（满分4分）"

    @staticmethod
    def _peg_reason(peg, cagr, score) -> str:
        if peg is None:
            return f"PEG无法计算（CAGR={cagr}），得0分"
        return f"PEG={peg:.2f}（PE / 盈利增长率 {cagr*100:.1f}%），得分={score:.1f}分（满分3分）"
