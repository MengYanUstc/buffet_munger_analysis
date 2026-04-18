"""
深度分析报告生成器
按照统一 Markdown 模板，将 5 模块分析结果渲染为可读报告并保存。
"""

import os
import re
import uuid
import datetime
from typing import Dict, Any, Optional, List

from .data_warehouse import DataCollector
from .core import AnalyzerRegistry
from .quality_analysis import QualityAnalyzer
from .management_analysis import ManagementAnalyzer
from .moat_analysis import MoatAnalyzer
from .business_model_analysis import BusinessModelAnalyzer
from .valuation import ValuationAnalyzer


class ReportGenerator:
    """生成单只股票的深度分析报告。"""

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.source = source
        self.collector = DataCollector()
        self.cache = self.collector.cache

    def run(self) -> str:
        """执行完整报告生成流程，返回保存的文件路径。"""
        # 0. 预收集：确保定性缓存已填充（供 moat / business_model / valuation 复用）
        self.collector.collect_enhanced(self.stock_code)
        self.collector.collect_qualitative(self.stock_code)

        # 1. 运行所有分析模块
        reports = self._run_analyzers()

        # 2. 加载补充数据
        df_fin, val_data, company_name = self._load_supplemental_data()

        # 3. 组装数据上下文
        context = self._build_context(reports, df_fin, val_data, company_name)

        # 4. 渲染 Markdown
        markdown = self._render_markdown(context)

        # 5. 保存
        file_path = self._save_report(markdown, company_name)
        return file_path

    # ------------------------------------------------------------------
    # 数据收集
    # ------------------------------------------------------------------

    def _run_analyzers(self) -> Dict[str, Any]:
        """运行全部 5 个分析模块并收集结果。"""
        result = {}
        for module_id in ["quality", "management", "moat", "business_model", "valuation"]:
            analyzer = AnalyzerRegistry.build(
                module_id,
                stock_code=self.stock_code,
                industry_type=self.industry_type,
                source=self.source,
            )
            report = analyzer.run()
            result[module_id] = report.to_dict()
        return result

    def _load_supplemental_data(self):
        """加载财务 DataFrame、估值数据、公司名称。"""
        df_fin = self.cache.read_financial_reports(self.stock_code)
        val_data = self.cache.read_valuation(self.stock_code) or {}
        company_name = self._get_company_name()
        return df_fin, val_data, company_name

    def _get_company_name(self) -> str:
        """获取公司名称，优先从 akshare 查询。"""
        import akshare as ak
        try:
            if len(self.stock_code) == 5 and self.stock_code.startswith('0'):
                # 港股
                df = ak.stock_hk_valuation_comparison_em(symbol=self.stock_code)
                if not df.empty:
                    return str(df.iloc[0, 1])
            else:
                # A股（列名可能为 code/name 或 代码/名称，兼容处理）
                df = ak.stock_info_a_code_name()
                code_col = 'code' if 'code' in df.columns else '代码'
                name_col = 'name' if 'name' in df.columns else '名称'
                matched = df[df[code_col] == self.stock_code]
                if not matched.empty:
                    return str(matched.iloc[0][name_col])
        except Exception:
            pass
        return self.stock_code

    def _build_context(
        self,
        reports: Dict[str, Any],
        df_fin,
        val_data: Dict[str, Any],
        company_name: str,
    ) -> Dict[str, Any]:
        """将所有原始数据组装成模板渲染所需的上下文字典。"""
        ctx = {
            "stock_code": self.stock_code,
            "company_name": company_name,
            "analysis_date": datetime.date.today().strftime("%Y-%m-%d"),
            "reports": reports,
            "valuation": val_data,
            "financial": self._parse_financial(df_fin),
        }
        return ctx

    def _parse_financial(self, df_fin) -> Dict[str, Any]:
        """从财务 DataFrame 提取最近 5 年数据，统一转为亿元。"""
        if df_fin is None or df_fin.empty:
            return {}

        # 取最近5年
        df = df_fin.tail(5).reset_index(drop=True)
        records = []
        for _, row in df.iterrows():
            records.append({
                "year": str(row.get("report_date", ""))[:4],
                "roe": self._fmt_pct(row.get("roe")),
                "roic": self._fmt_pct(row.get("roic")),
                "revenue": self._fmt_yi(row.get("revenue")),
                "net_profit": self._fmt_yi(row.get("net_profit")),
                "deduct_net_profit": self._fmt_yi(row.get("deduct_net_profit")),
                "parent_net_profit": self._fmt_yi(row.get("parent_net_profit")),
                "gross_margin": self._fmt_pct(row.get("gross_margin")),
                "net_margin": self._fmt_pct(row.get("net_margin")),
                "debt_ratio": self._fmt_pct(row.get("debt_ratio")),
                "operating_cash_flow": self._fmt_yi(row.get("operating_cash_flow")),
                "fcf": self._fmt_yi(row.get("fcf")),
                "capex": self._fmt_yi(row.get("capex")),
            })

        return {
            "records": records,
            "years": [r["year"] for r in records],
        }

    @staticmethod
    def _fmt_pct(val):
        """格式化百分比，保留1位小数。"""
        if val is None or (isinstance(val, float) and val != val):  # NaN check
            return None
        try:
            return round(float(val), 1)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _fmt_yi(val):
        """将元转换为亿元，保留2位小数。"""
        if val is None or (isinstance(val, float) and val != val):
            return None
        try:
            return round(float(val) / 1e8, 2)
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Markdown 渲染
    # ------------------------------------------------------------------

    def _render_markdown(self, ctx: Dict[str, Any]) -> str:
        """按模板渲染完整 Markdown。"""
        parts = []
        parts.append(self._render_header(ctx))
        parts.append(self._render_basic_info(ctx))
        parts.append(self._render_core_conclusion(ctx))
        parts.append(self._render_quality(ctx))
        parts.append(self._render_moat(ctx))
        parts.append(self._render_business_model(ctx))
        parts.append(self._render_management(ctx))
        parts.append(self._render_valuation(ctx))
        parts.append(self._render_final_score(ctx))
        parts.append(self._render_disclaimer(ctx))
        return "\n\n".join(parts)

    # ===== 辅助提取方法 =====

    def _deep_get(self, d: Dict, keys: List[str], default=None):
        """安全深度字典取值。"""
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k, default)
            if d is None:
                return default
        return d

    def _r(self, ctx, module: str, key: str, default=None):
        """快捷获取某模块维度数据。"""
        return self._deep_get(ctx["reports"], [module, "dimensions", key], default)

    def _rs(self, ctx, module: str, key: str, default=None):
        """快捷获取某模块 summary 数据。"""
        return self._deep_get(ctx["reports"], [module, "summary", key], default)

    def _fmt_chain(self, records: List[Dict], key: str, suffix: str = "") -> str:
        """将多年数据格式化为 Year1→Year2→... 链条。"""
        vals = [r.get(key) for r in records]
        parts = []
        for i, v in enumerate(vals):
            if v is not None:
                parts.append(f"{records[i]['year']}年:{v}{suffix}")
        return " → ".join(parts) if parts else "数据暂缺"

    def _safe(self, val, fmt="{}", default="数据暂缺"):
        if val is None or (isinstance(val, float) and val != val):
            return default
        return fmt.format(val)

    @staticmethod
    def _debt_ratio_range_desc(industry_type: str) -> str:
        """根据行业类型返回资产负债率合理区间的描述。"""
        thresholds = {
            "general":     [(30, "低"), (50, "中等"), (70, "较高"), (float('inf'), "过高")],
            "banking":     [(85, "低"), (90, "中等"), (93, "较高"), (float('inf'), "过高")],
            "insurance":   [(80, "低"), (85, "中等"), (90, "较高"), (float('inf'), "过高")],
            "real_estate": [(60, "低"), (70, "中等"), (80, "较高"), (float('inf'), "过高")],
            "utilities":   [(50, "低"), (60, "中等"), (70, "较高"), (float('inf'), "过高")],
        }
        levels = thresholds.get(industry_type, thresholds["general"])
        parts = []
        prev = 0
        for threshold, desc in levels:
            if prev == 0:
                parts.append(f"≤{threshold}%为{desc}负债")
            elif threshold == float('inf'):
                parts.append(f">{prev}%为{desc}负债")
            else:
                parts.append(f"{prev}%-{threshold}%为{desc}负债")
            prev = threshold
        return "；".join(parts)

    # ===== 各章节渲染 =====

    def _render_header(self, ctx: Dict[str, Any]) -> str:
        total = sum(
            self._deep_get(ctx["reports"], [m, "total_score"], 0.0)
            for m in ["quality", "management", "moat", "business_model", "valuation"]
        )
        return f"# {ctx['company_name']}价值分析（{total:.1f}/100分）\n\n**分析日期**：{ctx['analysis_date']}"

    def _render_basic_info(self, ctx: Dict[str, Any]) -> str:
        val = ctx.get("valuation", {})
        close_price = val.get("close_price")
        # 市值使用 DCF 近似值（元 -> 亿元）
        dcf = self._deep_get(ctx["reports"], ["valuation", "dimensions", "dcf_safety_margin"], {})
        market_cap = dcf.get("market_cap_approx")
        if market_cap:
            market_cap = round(market_cap / 1e8, 2)

        fin = ctx.get("financial", {})
        records = fin.get("records", [])

        lines = [
            "## 一、基本信息",
            "",
            f"- **分析日期**：{ctx['analysis_date']}",
            "- **框架版本**：巴菲特芒格投资分析框架 v1.0",
        ]
        if close_price is not None:
            lines.append(f"- **最新股价**：{close_price}元（{val.get('trade_date', '今日')}）")
        else:
            lines.append("- **最新股价**：数据暂缺")

        if market_cap is not None:
            lines.append(f"- **最新市值**：{market_cap}亿元（基于PE×净利润估算）")
        else:
            lines.append("- **最新市值**：数据暂缺")

        lines.append("- **营收结构**：")
        lines.append("  - 未获取到详细业务结构数据")
        lines.append("- **公司简介**：")
        lines.append(f"  {ctx['company_name']}（{ctx['stock_code']}），基于公开财务数据与AI定性分析进行综合评估。")
        return "\n".join(lines)

    def _render_core_conclusion(self, ctx: Dict[str, Any]) -> str:
        q = self._deep_get(ctx["reports"], ["quality", "total_score"], 0)
        m = self._deep_get(ctx["reports"], ["moat", "total_score"], 0)
        b = self._deep_get(ctx["reports"], ["business_model", "total_score"], 0)
        mg = self._deep_get(ctx["reports"], ["management", "total_score"], 0)
        v = self._deep_get(ctx["reports"], ["valuation", "total_score"], 0)
        total = q + m + b + mg + v

        # 提取亮点（各模块高分维度）
        highlights = []
        if q >= 14:
            highlights.append("企业质量优秀：ROE、ROIC 处于较高水平，盈利能力稳健。")
        if m >= 21:
            highlights.append("护城河深厚：具备较强的竞争优势与定价权。")
        if b >= 14:
            highlights.append("商业模式健康：现金流质量良好，资本开支合理。")
        if mg >= 7.5:
            highlights.append("管理层可信：资本配置合理，无重大诚信风险。")
        if v >= 14:
            highlights.append("估值合理：当前估值处于历史相对低位，具备安全边际。")

        # 提取风险
        risks = []
        growth = self._deep_get(ctx["reports"], ["quality", "dimensions", "revenue_growth", "score"], 0)
        if growth <= 1.0:
            risks.append("增长乏力：营收或利润增长缓慢，需关注成长性。")
        moat_score = self._deep_get(ctx["reports"], ["moat", "total_score"], 0)
        if moat_score <= 15:
            risks.append("护城河偏弱：行业竞争激烈，竞争优势可能难以持续。")
        roe_stab = self._deep_get(ctx["reports"], ["quality", "dimensions", "roe_stability", "score"], 2)
        if roe_stab <= 0.5:
            risks.append("盈利波动：ROE 稳定性较差，盈利存在较大不确定性。")

        if not highlights:
            highlights = ["整体表现中规中矩，需结合具体维度深入分析。"]
        if not risks:
            risks = ["未发现显著风险点，建议持续跟踪基本面变化。"]

        lines = [
            "## 二、核心结论",
            "",
            f"{ctx['company_name']}综合得分 {total:.1f}/100 分。企业质量 {q:.1f}/20 分，护城河 {m:.1f}/30 分，商业模式 {b:.1f}/20 分，管理层 {mg:.1f}/10 分，估值 {v:.1f}/20 分。",
            "",
            "### 关键亮点",
        ]
        for i, h in enumerate(highlights, 1):
            lines.append(f"{i}. {h}")

        lines.append("")
        lines.append("### 风险提示")
        for i, r in enumerate(risks, 1):
            lines.append(f"{i}. {r}")

        lines.append("")
        lines.append("### 评语")
        lines.append(f"综合评估，{ctx['company_name']}在盈利能力与估值水平方面表现{'较为突出' if q+v >= 30 else '一般'}，")
        lines.append(f"{'护城河稳固' if m >= 21 else '竞争优势有待加强'}，")
        lines.append(f"{'管理层值得信赖' if mg >= 7.5 else '管理层表现中规中矩'}。")
        lines.append(f"建议{'积极关注' if total >= 75 else '谨慎观察' if total >= 50 else '暂时回避'}。")

        return "\n".join(lines)

    def _render_quality(self, ctx: Dict[str, Any]) -> str:
        q = ctx["reports"]["quality"]
        dims = q.get("dimensions", {})
        fin = ctx.get("financial", {})
        records = fin.get("records", [])

        roe = dims.get("roe", {})
        roic = dims.get("roic", {})
        rev = dims.get("revenue_growth", {})
        prof = dims.get("profit_growth", {})
        roe_stab = dims.get("roe_stability", {})
        debt = dims.get("debt_ratio", {})

        lines = [
            f"## 三、企业质量分析（{q.get('total_score', 0)}/20分）",
            "",
            f"**质量评级**：{q.get('rating', '未评级')}",
            "",
            "### ROE评分",
            f"- 5年平均ROE：{self._safe(roe.get('avg_roe'), '{}%', '数据暂缺')}",
            f"- 各年份ROE：{self._fmt_chain(records, 'roe', '%')}",
            "- ROE趋势分析：" + ("ROE 整体稳定" if roe.get('avg_roe', 0) >= 15 else "ROE 水平一般，需关注盈利能力持续性。"),
            "- ROE与行业对比：数据暂缺",
            f"- **最终得分：{roe.get('score', 0)}/4**",
            "",
            "### ROE稳定性评分（⚠️ 定性判断）",
            f"- 5年平均ROE：{self._safe(roe_stab.get('roe_mean'), '{}%', '数据暂缺')}",
            f"- ROE标准差：{self._safe(roe_stab.get('roe_std'), '{}', '数据暂缺')}",
            f"- 脚本建议分：{roe_stab.get('penalty_score', 0)}/2",
            f"- 最终得分：{roe_stab.get('score', 0)}/2（评分阶段已确定，报告阶段不可修改）",
            f"- 得分说明：{roe_stab.get('ai_reason') or roe_stab.get('reason') or '数据暂缺'}",
            "",
            "### ROIC评分（✅ 完全定量）",
            f"- 5年平均ROIC：{self._safe(roic.get('avg_roic'), '{}%', '数据暂缺')}",
            f"- 各年份ROIC：{self._fmt_chain(records, 'roic', '%')}",
            "- 评分规则：≥20%得6分 | ≥15%得5分 | ≥12%得4分 | ≥8%得3分 | ≥5%得1.5分 | <5%得0分",
            f"- **最终得分：{roic.get('score', 0)}/6**",
            f"- 得分说明：平均 ROIC {roic.get('avg_roic', 'N/A')}%，{'盈利能力优异' if roic.get('score', 0) >= 5 else '盈利能力良好' if roic.get('score', 0) >= 3 else '盈利能力一般'}。",
            "",
            "### 营收增长评分（✅ 完全定量）",
            f"- 营收CAGR：{self._safe(rev.get('cagr'), '{}%', '数据暂缺')}",
            f"- 各年份营收：{self._fmt_chain(records, 'revenue', '亿')}",
            "- 营收增长分析：" + ("营收保持较快增长，业务扩张势头良好。" if rev.get('score', 0) >= 2 else "营收增长放缓，需关注增长动力。" if rev.get('score', 0) >= 1 else "营收增长乏力，业务面临增长瓶颈。"),
            f"- **最终得分：{rev.get('score', 0)}/3**",
            "",
            "### 扣非净利润增长评分",
            f"- 扣非净利润CAGR：{self._safe(prof.get('cagr'), '{}%', '数据暂缺')}",
            f"- 各年份扣非净利润：{self._fmt_chain(records, 'deduct_net_profit', '亿')}",
            "- 利润增长分析：" + ("利润增长稳健，盈利质量较好。" if prof.get('score', 0) >= 2 else "利润增长承压，需关注盈利质量。"),
            f"- **最终得分：{prof.get('score', 0)}/3**",
            "",
            "### 资产负债率评分",
            f"- 行业类型：{debt.get('industry_type', 'general')}",
            f"- 资产负债率：{self._safe(debt.get('debt_ratio'), '{}%', '数据暂缺')}",
            f"- 行业合理区间：{self._debt_ratio_range_desc(debt.get('industry_type', 'general'))}",
            f"- 脚本建议分：{debt.get('suggested_base_score', 0)}/2",
        ]
        # AI调整：仅在存在非零调整时显示
        ai_adj = (debt.get('ai_score') or 0) - (debt.get('suggested_base_score') or 0)
        if ai_adj != 0:
            lines.append(f"- AI调整：{ai_adj:+.1f}")
        lines += [
            f"- 负债率分析：{debt.get('ai_reason') or debt.get('reason') or '数据暂缺'}",
            f"- **最终得分：{debt.get('score', 0)}/2**",
            "",
            f"**企业质量总分：{q.get('total_score', 0)}/20分**",
            "",
            "**质量评语**：",
            f"{ctx['company_name']}企业质量得分 {q.get('total_score', 0)}/20 分。"
            f"ROE {'表现优异' if roe.get('score', 0) >= 3 else '表现一般'}，"
            f"ROIC {'处于较高水平' if roic.get('score', 0) >= 4 else '有待提升'}，"
            f"{'增长动力充足' if rev.get('score', 0) >= 2 and prof.get('score', 0) >= 2 else '增长面临一定压力'}，"
            f"财务杠杆{'合理可控' if debt.get('score', 0) >= 1.5 else '需关注风险'}。",
        ]
        return "\n".join(lines)

    def _render_moat(self, ctx: Dict[str, Any]) -> str:
        moat = ctx["reports"]["moat"]
        dims = moat.get("dimensions", {})
        fin = ctx.get("financial", {})
        records = fin.get("records", [])

        gm = dims.get("gross_margin_stability", {})
        iq = dims.get("industry_quality", {})
        mt = dims.get("moat_type", {})
        ms = dims.get("moat_sustainability", {})
        pp = dims.get("pricing_power", {})

        lines = [
            f"## 四、护城河分析（{moat.get('total_score', 0)}/30分）",
            "",
            f"**护城河评级**：{moat.get('rating', '未评级')}",
            "",
            "### 护城河类型识别",
            "- 识别出的护城河类型：品牌、渠道、成本优势、技术优势",
            "",
            "#### 护城河1：品牌护城河",
            f"- 强度：{'强' if mt.get('score', 0) >= 5 else '中' if mt.get('score', 0) >= 3.5 else '弱'}",
            f"- 详细说明：{mt.get('reason', '数据暂缺')[:200]}...",
            "- 典型特征体现：品牌知名度高、客户忠诚度高、定价能力强",
            "",
            f"- 护城河强度综合判断：{'强' if mt.get('score', 0) >= 5 else '中' if mt.get('score', 0) >= 3.5 else '弱'}",
            f"- **最终得分：{mt.get('score', 0)}/7**",
            "",
            "### 护城河可持续性",
            f"- 可持续性等级：{'极高' if ms.get('score', 0) >= 6 else '高' if ms.get('score', 0) >= 4.5 else '中等' if ms.get('score', 0) >= 3 else '低'}",
            "",
            "#### 维度1：历史时长",
            "- 评估：公司已积累较长时间的市场经验",
            f"- 分析：{ms.get('reason', '数据暂缺')[:150]}...",
            "",
            "#### 维度2：趋势",
            f"- 评估：{'护城河加强' if ms.get('score', 0) >= mt.get('score', 0) else '护城河稳定' if abs(ms.get('score', 0) - mt.get('score', 0)) <= 1 else '护城河削弱'}",
            f"- 分析：{ms.get('reason', '数据暂缺')[150:300] if len(ms.get('reason', '')) > 150 else '数据暂缺'}...",
            "",
            f"- **最终得分：{ms.get('score', 0)}/7**",
            "",
            "### 行业评分",
            f"- 行业集中度：{'极高' if iq.get('score', 0) >= 5 else '中高' if iq.get('score', 0) >= 3.5 else '中等'}",
            "- 行业成长性：数据暂缺",
            "- 行业进入壁垒：数据暂缺",
            "- 需求稳定性：数据暂缺",
            f"- 行业竞争格局分析：{iq.get('reason', '数据暂缺')[:200]}...",
            f"- **最终得分：{iq.get('score', 0)}/6**",
            "",
            "### 定价权评估",
            f"- 定价权等级：{'强' if pp.get('score', 0) >= 4.5 else '中' if pp.get('score', 0) >= 3 else '弱'}",
            "",
            "#### 维度1：提价历史",
            "- 评估：数据暂缺",
            "- 分析：数据暂缺",
            "",
            f"- **最终得分：{pp.get('score', 0)}/6**",
            "",
            "### 毛利率稳定性",
            f"- 毛利率标准差：{self._safe(gm.get('std'), '{}%', '数据暂缺')}",
            f"- 稳定性级别：{'极稳定' if gm.get('score', 0) >= 3.5 else '高度稳定' if gm.get('score', 0) >= 3 else '较稳定' if gm.get('score', 0) >= 2 else '一般'}",
            f"- 趋势方向：{gm.get('trend_direction', '数据暂缺')}",
            f"- 各年份毛利率：{self._fmt_chain(records, 'gross_margin', '%')}",
            f"- 脚本建议分：{gm.get('base_score', 0)}/4",
            f"- AI调整：{gm.get('trend_adjustment', 0):+.1f}",
            f"- 毛利率稳定性分析：{gm.get('reason', '数据暂缺')}",
            f"- **最终得分：{gm.get('score', 0)}/4**",
            "",
            f"**护城河总分：{moat.get('total_score', 0)}/30分**",
            "",
            "**护城河评语**：",
            f"{ctx['company_name']}护城河得分 {moat.get('total_score', 0)}/30 分。"
            f"{'护城河深厚，竞争优势明显' if moat.get('total_score', 0) >= 21 else '护城河中等，具备一定竞争优势' if moat.get('total_score', 0) >= 15 else '护城河偏弱，竞争优势有限'}，"
            f"{'行业地位稳固' if iq.get('score', 0) >= 4 else '行业竞争激烈'}，"
            f"{'定价能力较强' if pp.get('score', 0) >= 4 else '定价能力一般'}。",
        ]
        return "\n".join(lines)

    def _render_business_model(self, ctx: Dict[str, Any]) -> str:
        bm = ctx["reports"]["business_model"]
        dims = bm.get("dimensions", {})
        fin = ctx.get("financial", {})
        records = fin.get("records", [])

        fcf_q = dims.get("fcf_quality", {})
        capex = dims.get("capex_efficiency", {})
        inc_stab = dims.get("income_stability", {})
        bm_qual = dims.get("business_model_quality", {})

        lines = [
            f"## 五、商业模式分析（{bm.get('total_score', 0)}/20分）",
            "",
            "### 自由现金流质量",
            f"- 行业类型：{capex.get('industry_type', 'medium')}",
            f"- 5年平均FCF转化率：{self._safe(fcf_q.get('fcf_ratio'), '{}', '数据暂缺')}",
            f"- 各年份FCF：{self._fmt_chain(records, 'fcf', '亿')}",
            f"- 脚本基础分：{self._safe(fcf_q.get('base_score'), '{:.1f}', '数据暂缺')}/6",
            f"- 自由现金流分析：{fcf_q.get('reason', '数据暂缺')}",
            f"- **最终得分：{fcf_q.get('score', 0)}/6**",
            "",
            "### 资本开支",
            f"- 行业类型：{capex.get('industry_type', 'medium')}",
            f"- 5年平均CapEx/净利润：{self._safe(capex.get('avg_capex_ratio'), '{}%', '数据暂缺')}",
            f"- 各年份CapEx：{self._fmt_chain(records, 'capex', '亿')}",
            f"- 脚本基础分：{capex.get('base_score', 0)}/4",
            f"- 资本开支分析：{capex.get('reason', '数据暂缺')}",
            f"- **最终得分：{capex.get('score', 0)}/4**",
            "",
            "### 收入稳定性",
            "- 收入稳定性分析：" + inc_stab.get("reason", "数据暂缺")[:200] + "...",
            "- 前五大客户占比：数据暂缺",
            "- 客户集中度分析：数据暂缺",
            f"- **最终得分：{inc_stab.get('score', 0)}/5**",
            "",
            "### 商业模式质量",
            "- 赚钱逻辑清晰度：数据暂缺",
            "- 盈利能力：数据暂缺",
            "- 可复制性：数据暂缺",
            "- 规模化能力：数据暂缺",
            "- 抗风险能力：数据暂缺",
            f"- 商业模式深度分析：{bm_qual.get('reason', '数据暂缺')[:200]}...",
            f"- **最终得分：{bm_qual.get('score', 0)}/5**",
            "",
            f"**商业模式总分：{bm.get('total_score', 0)}/20分**",
            "",
            "**商业模式评语**：",
            f"{ctx['company_name']}商业模式得分 {bm.get('total_score', 0)}/20 分。"
            f"{'现金流质量优秀' if fcf_q.get('score', 0) >= 5 else '现金流质量一般'}，"
            f"{'资本开支控制良好' if capex.get('score', 0) >= 3 else '资本开支偏高'}，"
            f"{'商业模式成熟稳健' if bm.get('total_score', 0) >= 14 else '商业模式有待优化'}。",
        ]
        return "\n".join(lines)

    def _render_management(self, ctx: Dict[str, Any]) -> str:
        mg = ctx["reports"]["management"]
        dims = mg.get("dimensions", {})
        raw = mg.get("raw_data", {})

        ca = dims.get("capital_allocation", {})
        integrity = dims.get("management_integrity", {})
        risks = mg.get("risk_warnings", [])

        lines = [
            f"## 六、管理层分析（{mg.get('total_score', 0)}/10分）",
            "",
            "### 资本配置能力评分",
            "- ROIC表现：数据暂缺",
            "- 分红政策：数据暂缺",
            "- 并购与扩张：数据暂缺",
            "- 股权质押：数据暂缺",
            f"- 资本配置分析：{ca.get('reason', '数据暂缺')[:150]}...",
            f"- **最终得分：{ca.get('score', 0)}/6**",
            "",
            "### 管理层诚信评分",
            "- 违规与诚信问题：数据暂缺",
            "- 减持行为：数据暂缺",
            "- 治理结构：数据暂缺",
            f"- 诚信分析：{integrity.get('reason', '数据暂缺')[:150]}...",
            f"- **最终得分：{integrity.get('score', 0)}/4**",
            "",
            "### 一票否决检查",
            "- 财务造假被立案：否",
            "",
            f"**管理层总分：{mg.get('total_score', 0)}/10分**",
            "",
            f"**评级**：{mg.get('rating', '未评级')}",
            "",
            "**评级标准**：9-10分卓越 | 7.5-8.5分优秀 | 6-7分良好 | 5-5.5分中等 | 4-4.5分较差 | <4分差",
        ]
        if risks:
            lines.append("")
            lines.append("### 风险提示（如有）")
            for r in risks:
                lines.append(f"⚠️ {r}")

        lines.append("")
        lines.append("**管理层评语**：")
        lines.append(
            f"{ctx['company_name']}管理层得分 {mg.get('total_score', 0)}/10 分。"
            f"{'资本配置能力优秀' if ca.get('score', 0) >= 5 else '资本配置能力一般'}，"
            f"{'管理层诚信记录良好' if integrity.get('score', 0) >= 3.5 else '需关注管理层诚信风险'}。"
        )
        return "\n".join(lines)

    def _render_valuation(self, ctx: Dict[str, Any]) -> str:
        val = ctx["reports"]["valuation"]
        dims = val.get("dimensions", {})

        abs_v = dims.get("absolute_valuation", {})
        rel_v = dims.get("relative_valuation", {})
        peg = dims.get("long_term_peg", {})
        dcf = dims.get("dcf_safety_margin", {})
        growth = dims.get("growth_certainty", {})

        # DCF 参数
        dcf_params = dcf.get("dcf_params", {})

        lines = [
            f"## 七、估值分析（{val.get('total_score', 0)}/20分）",
            "",
            "### 绝对估值水平",
            f"- PE（市盈率）：{self._safe(abs_v.get('pe'), '{:.1f}倍', '数据暂缺')}",
            f"- PB（市净率）：{self._safe(abs_v.get('pb'), '{:.1f}倍', '数据暂缺')}",
            f"- PS（市销率）：{self._safe(abs_v.get('ps'), '{:.1f}倍', '数据暂缺')}",
            "- 绝对估值深度分析：当前估值水平" + ("处于合理区间" if abs_v.get('score', 0) >= 4 else "偏高" if abs_v.get('score', 0) <= 2 else "适中") + "。",
            f"- **最终得分：{abs_v.get('score', 0)}/6**",
            "",
            "### 相对估值",
            f"- 历史估值分位：{self._safe(rel_v.get('pe_percentile_5y'), '{:.1f}%', '数据暂缺')}",
            f"- 相对行业比率：{self._safe(rel_v.get('pe_vs_industry'), '{:.2f}', '数据暂缺')}",
            "- 相对估值深度分析：" + ("当前估值处于历史较低分位，具备相对安全边际。" if rel_v.get('score', 0) >= 2 else "当前估值处于历史中高分位，需关注相对估值风险。"),
            f"- **最终得分：{rel_v.get('score', 0)}/4**",
            "",
            "### DCF安全边际",
            "- DCF测算关键假设：",
        ]
        if dcf_params:
            gr = dcf_params.get("growth_rates", [])
            lines.append(f"  - 增长序列：{' → '.join(f'{g}%' for g in gr)}")
            lines.append(f"  - 折现率：{dcf_params.get('discount_rate', 0.08) * 100:.0f}%")
            lines.append(f"  - 永续增长率：{dcf_params.get('perpetual_growth', 0.03) * 100:.0f}%")
        else:
            lines.append("  - 增长序列：数据暂缺")
            lines.append("  - 折现率：数据暂缺")
            lines.append("  - 永续增长率：数据暂缺")

        ev = dcf.get("enterprise_value")
        mc = dcf.get("market_cap_approx")
        if ev and mc:
            ev_yi = round(ev / 1e4, 2)
            mc_yi = round(mc / 1e4, 2)
            lines.append(f"  - FCF转化率：数据暂缺")
            lines.append(f"  - DCF内在价值：{ev_yi}亿元")
            lines.append(f"  - 当前市值：{mc_yi}亿元")
            lines.append(f"  - 安全边际：{dcf.get('safety_margin', '数据暂缺')}")
        else:
            lines.append("  - DCF内在价值：数据暂缺")
            lines.append("  - 当前市值：数据暂缺")
            lines.append("  - 安全边际：数据暂缺")

        lines.append(f"- DCF安全边际分析：{dcf.get('reason', '数据暂缺')}")
        lines.append(f"- **最终得分：{dcf.get('score', 0)}/4**")
        lines.append("")
        lines.append("### 长期PEG")
        if peg.get("peg") is not None:
            lines.append(f"- PEG：{peg['peg']:.2f}")
        else:
            lines.append("- PEG：无法计算（利润增长为负或数据不足）")
        lines.append(f"- 长期PEG分析：{peg.get('reason', '数据暂缺')}")
        lines.append(f"- **最终得分：{peg.get('score', 0)}/3**")
        lines.append("")
        lines.append("### 增长确定性")
        lines.append(f"- 增长确定性：{'高' if growth.get('score', 0) >= 2 else '中等' if growth.get('score', 0) >= 1 else '低'}")
        lines.append(f"- 增长确定性分析：{growth.get('reason', '数据暂缺')[:200]}...")
        lines.append(f"- **最终得分：{growth.get('score', 0)}/3**")
        lines.append("")
        lines.append(f"**估值总分：{val.get('total_score', 0)}/20分**")
        lines.append("")
        lines.append("**估值评语**：")
        lines.append(
            f"{ctx['company_name']}估值得分 {val.get('total_score', 0)}/20 分。"
            f"{'当前估值具备较高安全边际' if val.get('total_score', 0) >= 14 else '当前估值处于合理区间' if val.get('total_score', 0) >= 10 else '当前估值偏高或增长确定性不足'}，"
            f"{'DCF测算显示内在价值高于市值' if dcf.get('safety_margin', 0) > 1.2 else 'DCF安全边际一般'}。"
        )
        return "\n".join(lines)

    def _render_final_score(self, ctx: Dict[str, Any]) -> str:
        scores = {
            "quality": self._deep_get(ctx["reports"], ["quality", "total_score"], 0),
            "moat": self._deep_get(ctx["reports"], ["moat", "total_score"], 0),
            "business_model": self._deep_get(ctx["reports"], ["business_model", "total_score"], 0),
            "management": self._deep_get(ctx["reports"], ["management", "total_score"], 0),
            "valuation": self._deep_get(ctx["reports"], ["valuation", "total_score"], 0),
        }
        total = sum(scores.values())

        # 投资评级
        if total >= 85:
            rating = "超级公司"
        elif total >= 75:
            rating = "优秀公司"
        elif total >= 65:
            rating = "可投资"
        elif total >= 50:
            rating = "一般"
        else:
            rating = "回避"

        lines = [
            "## 八、最终评分",
            "",
            f"**最终总分**：{total:.1f}/100分",
            "",
            "**评分构成**：",
            f"- 企业质量：{scores['quality']:.1f}/20分",
            f"- 护城河：{scores['moat']:.1f}/30分",
            f"- 商业模式：{scores['business_model']:.1f}/20分",
            f"- 管理层：{scores['management']:.1f}/10分",
            f"- 估值：{scores['valuation']:.1f}/20分",
            "",
            "### 投资评级",
            "",
            "| 总分区间 | 评级 |",
            "|---------|------|",
            "| 85-100分 | 超级公司 |",
            "| 75-84分 | 优秀公司 |",
            "| 65-74分 | 可投资 |",
            "| 50-64分 | 一般 |",
            "| <50分 | 回避 |",
            "",
            f"**最终评级**：{rating}",
            "",
            "### SWOT分析",
            "",
            "**优势（Strengths）**：",
            f"1. {'盈利能力稳健' if scores['quality'] >= 14 else '盈利能力有待提升'}",
            f"2. {'护城河深厚' if scores['moat'] >= 21 else '竞争优势一般'}",
            f"3. {'商业模式健康' if scores['business_model'] >= 14 else '商业模式有待优化'}",
            "",
            "**劣势（Weaknesses）**：",
            f"1. {'增长动力不足' if scores['quality'] < 11 else '增长持续性需观察'}",
            f"2. {'估值安全边际有限' if scores['valuation'] < 12 else '估值相对合理'}",
            "",
            "**机会（Opportunities）**：",
            "1. 行业整合与集中度提升带来的份额增长机会",
            "2. 新业务拓展与产品线延伸",
            "",
            "**威胁（Threats）**：",
            "1. 宏观经济波动与行业周期风险",
            "2. 竞争加剧与价格战风险",
            "",
            "### 综合投资建议",
            "",
            "#### 投资逻辑",
            f"{ctx['company_name']}综合得分 {total:.1f}/100 分，评级为'{rating}'。"
            f"{'公司具备较强的竞争优势与盈利能力，估值合理，适合长期配置。' if total >= 75 else '公司基本面尚可，但需关注护城河可持续性与增长确定性。' if total >= 50 else '公司基本面存在较多不确定性，建议回避或观望。'}",
            "",
            "#### 关键跟踪指标",
            "1. ROE 与 ROIC 变化趋势：衡量盈利能力稳定性",
            "2. 营收与利润增速：衡量成长性",
            "3. 毛利率与行业分位：衡量竞争优势变化",
        ]
        return "\n".join(lines)

    def _render_disclaimer(self, ctx: Dict[str, Any]) -> str:
        return (
            "## 九、免责声明\n\n"
            "本报告基于公开信息进行分析，仅供参考，不构成投资建议。投资有风险，入市需谨慎。\n\n"
            "报告中的数据和结论可能因信息更新、市场变化等因素而发生变化。投资者应独立判断，审慎决策。\n\n"
            "分析框架：巴菲特芒格投资分析框架 v1.0"
        )

    # ------------------------------------------------------------------
    # 文件保存
    # ------------------------------------------------------------------

    def _save_report(self, markdown: str, company_name: str) -> str:
        """保存报告到 reports/ 目录，返回文件路径。"""
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)

        today = datetime.date.today().strftime("%Y%m%d")
        uid = uuid.uuid4().hex[:8]
        # 清理公司简称中的特殊字符
        safe_name = re.sub(r'[\\/:*?"<>|]', '', company_name)
        filename = f"{self.stock_code}_{safe_name}_{today}_{uid}.md"
        filepath = os.path.join(reports_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(markdown)

        print(f"[ReportGenerator] 报告已保存: {filepath}")
        return filepath
