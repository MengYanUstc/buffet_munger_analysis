"""
商业模式分析主模块（Module 4）
总分 20 分 = 定性 12 分（Coze LLM：收入稳定性 4 + 商业模式质量 4 + 简单易懂 4）
              + 定量 8 分（资本开支 2 分 + 自由现金流 6 分）

增长确定性（3分）已移至估值模块，由估值模块复用本模块的定性缓存。
所有定量分析数据严格从 SQLite 读取，不直接走网络 fallback。
"""

import json
import os
from typing import Dict, Any

import pandas as pd

from ..core import AnalyzerBase, AnalysisReport
from ..data_warehouse.collector import DataCollector
from .capex_scorer import compute_capex_score


class BusinessModelAnalyzer(AnalyzerBase):
    module_id = "business_model"
    module_name = "商业模式分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.source = source
        self.collector = DataCollector()

    def run(self) -> AnalysisReport:
        # 0. 确保财务数据已入库（缓存优先策略：先入库，后读取）
        self.collector.collect(self.stock_code)

        # 1. 从统一缓存读取定性评分 + 行业分类/发展阶段
        llm_result = self._get_qualitative_result()

        # 2. 获取 LLM 判断的行业分类和发展阶段
        industry_classification = llm_result.get("industry_classification", "medium")
        # 兼容 LLM 可能返回 development_stage 或 growth_stage 两种字段名
        development_stage = llm_result.get("development_stage")
        if not development_stage:
            development_stage = llm_result.get("growth_stage", "mature")

        # 3. 从数据库读取并计算资本开支定量评分（2分）
        capex_result = self._compute_capex_quantitative(industry_classification, development_stage)

        # 4. 从数据库读取并计算自由现金流定量评分（6分）
        fcf_result = self._compute_fcf_quantitative()

        # 5. 构建维度
        dimensions = {}

        # 定性维度（注意：growth_certainty 已移至估值模块，此处保留在缓存中供估值复用）
        stability = llm_result.get("income_stability", {})
        quality = llm_result.get("business_model_quality", {})
        # 兼容 LLM 可能返回 business_model_simplicity 或 business_simplicity 两种字段名
        simplicity = llm_result.get("business_model_simplicity") or llm_result.get("business_simplicity", {})

        dimensions["income_stability"] = {
            "score": stability.get("score", 0.0),
            "max_score": 4.0,
            "reason": stability.get("reason", ""),
        }
        dimensions["business_model_quality"] = {
            "score": quality.get("score", 0.0),
            "max_score": 4.0,
            "reason": quality.get("reason", ""),
        }
        dimensions["business_model_simplicity"] = {
            "score": simplicity.get("score", 0.0),
            "max_score": 4.0,
            "reason": simplicity.get("reason", ""),
        }

        # 定量维度：资本开支（满分 2 分）
        if capex_result.get("final_score") is not None:
            dimensions["capex_efficiency"] = {
                "score": capex_result["final_score"],
                "max_score": 2.0,
                "base_score": capex_result["base_score"],
                "stability_adjustment": capex_result["stability_adjustment"],
                "phase_bonus": capex_result.get("phase_bonus", 0.0),
                "raw_score": capex_result["raw_score"],
                "avg_capex_ratio": capex_result["avg_capex_ratio"],
                "cv": capex_result.get("cv"),
                "industry_type": capex_result["industry_type"],
                "phase_type": capex_result.get("phase_type", "mature"),
                "reason": capex_result["reason"],
            }
        else:
            dimensions["capex_efficiency"] = {
                "score": 0.0,
                "max_score": 2.0,
                "reason": capex_result.get("reason", "资本开支数据不足"),
            }

        # 定量维度：自由现金流质量（满分 6 分）
        if fcf_result.get("final_score") is not None:
            dimensions["fcf_quality"] = {
                "score": fcf_result["final_score"],
                "max_score": 6.0,
                "base_score": fcf_result["base_score"],
                "fcf_ratio": fcf_result["fcf_ratio"],
                "reason": fcf_result["reason"],
            }
        else:
            dimensions["fcf_quality"] = {
                "score": 0.0,
                "max_score": 6.0,
                "reason": fcf_result.get("reason", "自由现金流数据不足"),
            }

        # 6. 汇总
        qualitative_total = (
            dimensions["income_stability"]["score"]
            + dimensions["business_model_quality"]["score"]
            + dimensions["business_model_simplicity"]["score"]
        )
        quantitative_total = (
            dimensions["capex_efficiency"]["score"]
            + dimensions["fcf_quality"]["score"]
        )
        total_score = round(qualitative_total + quantitative_total, 1)

        rating = self._rating(total_score)

        summary = {
            "qualitative_score": round(qualitative_total, 1),
            "qualitative_max": 12.0,
            "quantitative_score": quantitative_total,
            "quantitative_max": 8.0,
            "total_score": total_score,
            "max_score": 20.0,
            "rating": rating,
        }

        # 额外信息
        extra_info = {
            "industry_classification": industry_classification,
            "business_model_description": llm_result.get("business_model_description", ""),
        }

        raw_data = {
            "llm_raw": llm_result.get("_raw_text", ""),
            "extra_info": extra_info,
            "capex_detail": capex_result.get("yearly_scores", []),
            "fcf_detail": fcf_result.get("yearly_scores", []),
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
            risk_warnings=llm_result.get("risk_warnings", []),
            key_facts=llm_result.get("key_facts", []),
            raw_data=raw_data,
        )

    def _get_qualitative_result(self) -> Dict[str, Any]:
        """从统一缓存读取商业模式定性结果，缓存未命中则返回空结果。"""
        cached = self.collector.get_qualitative_result(self.stock_code, "business_model")
        if cached is not None:
            return cached
        print("[BusinessModelAnalyzer] 警告: 商业模式定性缓存未命中，跳过")
        return self._empty_result("缓存未命中")

    def _compute_capex_quantitative(
        self, industry_classification: str, development_stage: str
    ) -> Dict[str, Any]:
        """
        从数据库读取资本开支数据，计算定量评分（2分）。
        数据流：SQLite → read_financial_reports → compute_capex_score
        """
        try:
            df = self.collector.cache.read_financial_reports(self.stock_code)
            if df.empty or "capex" not in df.columns:
                return {"final_score": None, "reason": "数据库中缺少资本开支数据"}

            # 优先用扣非净利润，缺失时 fallback 到归母净利润
            profit_col = None
            for col in ["deduct_net_profit", "parent_net_profit", "net_profit"]:
                if col in df.columns and df[col].notna().any():
                    profit_col = col
                    break
            if profit_col is None:
                return {"final_score": None, "reason": "数据库中缺少净利润数据"}

            capex_values = df["capex"].dropna().tail(5).tolist()
            profit_values = df[profit_col].dropna().tail(5).tolist()

            if len(capex_values) == 0 or len(profit_values) == 0:
                return {"final_score": None, "reason": "资本开支或净利润数据为空"}

            result = compute_capex_score(
                capex_values=capex_values,
                net_profit_values=profit_values,
                industry_type=industry_classification,
                phase_type=development_stage,
            )
            result["source_note"] = "数据来源：SQLite 缓存"
            return result
        except Exception as e:
            print(f"[BusinessModelAnalyzer] 资本开支计算失败: {e}")
            return {"final_score": None, "reason": str(e)}

    def _compute_fcf_quantitative(self) -> Dict[str, Any]:
        """
        从数据库读取自由现金流和净利润数据，计算 FCF 质量评分（6分）。
        数据流：SQLite → read_financial_reports → 近5年汇总 → 按总比率直接评分
        """
        try:
            df = self.collector.cache.read_financial_reports(self.stock_code)
            if df.empty or "fcf" not in df.columns:
                return {"final_score": None, "reason": "数据库中缺少自由现金流数据"}

            # 优先用扣非净利润，缺失时 fallback 到归母净利润/净利润
            profit_col = None
            for col in ["deduct_net_profit", "parent_net_profit", "net_profit"]:
                if col in df.columns and df[col].notna().any():
                    profit_col = col
                    break

            if profit_col is None:
                return {"final_score": None, "reason": "数据库中缺少净利润数据"}

            # 只取近5年有效数据
            yearly_records = []
            for _, row in df.tail(5).iterrows():
                fcf = row.get("fcf")
                profit = row.get(profit_col)
                if pd.notna(fcf) and pd.notna(profit) and profit != 0:
                    ratio = float(fcf) / float(profit)
                    yearly_records.append({
                        "report_date": str(row.get("report_date", "")),
                        "fcf": round(float(fcf), 2),
                        "net_profit": round(float(profit), 2),
                        "fcf_ratio": round(ratio, 3),
                    })

            if not yearly_records:
                return {"final_score": None, "reason": "自由现金流或净利润有效数据为空"}

            # 按5年总FCF / 总净利润 计算总比率，直接评分（不惩罚年度波动）
            total_fcf = sum(r["fcf"] for r in yearly_records)
            total_profit = sum(r["net_profit"] for r in yearly_records)
            base_score, overall_ratio = _calculate_fcf_score(total_fcf, total_profit)
            final_score = round(base_score, 1)

            yearly_ratios = [r["fcf_ratio"] for r in yearly_records]
            avg_ratio = sum(yearly_ratios) / len(yearly_ratios)

            reason = (
                f"共 {len(yearly_records)} 年数据，FCF/扣非净利润 总比率={overall_ratio:.2f}，"
                f"各年比率均值={avg_ratio:.2f}，直接得分={final_score}分（满分6分）"
            )

            return {
                "final_score": final_score,
                "base_score": base_score,
                "fcf_ratio": round(overall_ratio, 3),
                "yearly_scores": yearly_records,
                "reason": reason,
            }
        except Exception as e:
            print(f"[BusinessModelAnalyzer] 自由现金流计算失败: {e}")
            return {"final_score": None, "reason": str(e)}

    @staticmethod
    def build_qualitative_prompt(stock_code: str) -> str:
        """构建商业模式定性分析 Prompt（供外部统一调用）。"""
        return f"""你是一位资深中国A股投资分析师，擅长巴菲特-芒格式的价值投资框架中的商业模式分析。

请对 **{stock_code}** 进行深度商业模式评估。
要求完全基于你所掌握的公开信息（财报、年报、行业报告、新闻报道等）独立判断。

---

## 第一步：行业分类与发展阶段判断

请先判断以下两项：

**行业分类**（三选一）：
- light: 轻资产（软件、互联网、金融、咨询、白酒）
- medium: 中等资产（消费品、零售、医药）
- heavy: 重资产（制造业、公用事业、能源、交通）

**发展阶段**（四选一）：
- startup: 初创期
- growth: 成长期
- mature: 成熟期
- decline: 衰退期

---

## 第二步：商业模式描述（必做，200-500字）

在评分前，请先描述该公司如何赚钱：
- 公司提供什么产品/服务？核心产品占比多少？
- 谁是主要客户？客户画像和地域分布？
- 销售渠道是什么？渠道控制力如何？
- 收入来源和成本结构是什么？
- 行业有无季节性/周期性？

---

## 第三步：评分（共12分）

### 1. 收入稳定性评估（满分 4 分）

评估因素：
- **客户集中度**：前五大客户占比多少？是否依赖单一客户？
- **产品结构**：产品线多元化还是单一化？单一产品占比是否过高（>60%）？
- **周期性影响**：行业有无明显周期性？收入波动率大概多少？
- **客户粘性**：B2B合同续约率？B2C复购率/留存率？是否有高转换成本？

锚点：
- **4分**：客户高度分散、产品多元化、无周期性、高复购率
- **2.5分**：客户较集中或产品单一或有一定周期性
- **1分**：大客户依赖严重、产品单一、强周期性、客户粘性低

### 2. 商业模式质量评估（满分 4 分）

评估因素：
- **赚钱逻辑是否清晰**：如何创造价值？如何获取价值？盈利能力如何（ROE、净利率、现金流）？
- **可复制性**：业务能否标准化扩张？扩张壁垒是什么？
- **规模化能力**：规模扩大能否带来成本优势？能否通过提价实现盈利增长？边际成本是递增还是递减？
- **抗风险能力**：现金流质量如何？负债水平高吗？面临哪些行业/政策/竞争风险？

锚点：
- **4分**：赚钱逻辑清晰+盈利强、难以复制、强规模效应、抗风险能力强
- **2.5分**：赚钱逻辑清晰但盈利一般，或规模效应不明显，或有一定风险
- **1分**：赚钱逻辑不清晰、盈利困难、抗风险能力弱

### 3. 商业模式简单易懂评估（满分 4 分）

评估因素：
- **业务理解难度**：一个外行能否在10分钟内理解公司如何赚钱？
- **产品/服务可见性**：公司提供的是否为看得见摸得着的产品或服务？
- **盈利路径清晰度**：收入来源是否单一明确？是否存在复杂的关联交易或多元跨界？
- **行业类比性**：是否可以用日常经验类比理解？（如白酒、酱油 vs 半导体设备、金融衍生品）

锚点：
- **4分**：业务极度简单，一眼就能看懂（如卖水、卖酱油、开银行收息）
- **2.5分**：业务基本可以理解，但需要一定行业知识（如消费品、医药零售）
- **1分**：业务复杂难懂，涉及多环节协同或专业技术壁垒（如芯片设计、复杂金融工具）

注意：巴菲特只投资"能力圈"范围内的企业。如果无法理解其商业模式，就不应该投资。

### 4. 增长确定性评估（满分 3 分，供估值模块复用）

核心问题：该公司未来5年盈利增长的确定性如何？

评估因素：
- **历史增长质量**：过去5年营收和净利润CAGR是多少？增长趋势是加速、稳定还是放缓？增长驱动因素是否可持续？
- **行业增长前景**：行业处于什么生命周期阶段？市场规模增长空间有多大？政策环境是否有利？
- **竞争地位**：市场份额是多少？趋势是提升还是下滑？护城河强度如何？竞争优势是否可持续？
- **增长驱动因素**：是否有新产品或新业务储备？是否有产能扩张计划？是否有市场拓展空间？是否有提价能力？

锚点：
- **3分（高确定性）**：过去5年持续增长CAGR>10%、行业处于成长期或成熟期、行业龙头地位稳固、有明确增长驱动因素
- **2分（中等确定性）**：增长有波动但趋势向上、行业增长稳定但竞争加剧、有一定竞争优势、增长驱动因素存在但不确定性较大
- **1分（低确定性）**：增长趋势放缓或停滞、行业面临转型或挑战、竞争地位不稳固、缺乏明确增长驱动因素
- **0分（不确定）**：负增长或大幅波动、行业前景黯淡、份额持续下滑、公司经营面临重大风险

注意：避免过度乐观；关注拐点信号；区分周期性与结构性；存在重大不确定性时，倾向于给较低评分。

---

## 输出要求（严格 JSON 格式）

只输出 JSON，不要任何其他文字：

```json
{{
  "stock_code": "{stock_code}",
  "industry_classification": "light/medium/heavy",
  "industry_classification_desc": "具体说明",
  "development_stage": "startup/growth/mature/decline",
  "development_stage_desc": "具体说明",
  "business_model_description": "200-500字商业模式描述",
  "income_stability": {{
    "score": X.X,
    "max_score": 4.0,
    "reason": "详细说明，引用具体事实"
  }},
  "business_model_quality": {{
    "score": X.X,
    "max_score": 4.0,
    "reason": "详细说明，引用具体事实"
  }},
  "business_model_simplicity": {{
    "score": X.X,
    "max_score": 4.0,
    "reason": "详细说明，引用具体事实"
  }},
  "growth_certainty": {{
    "score": X.X,
    "max_score": 3.0,
    "reason": "详细说明，引用具体事实"
  }},
  "total_score": X.X,
  "max_total": 15.0,
  "rating": "优秀/良好/中等/较差",
  "key_facts": ["事实1", "事实2"],
  "risk_warnings": ["风险1"]
}}
```

注意：
- 分数以 0.5 分为最小单位
- 理由必须引用具体事实（数据、年份、事件名称）
- 如果你不确定某个事实，请诚实说明
"""

    @staticmethod
    def _empty_result(reason: str) -> Dict[str, Any]:
        return {
            "industry_classification": "medium",
            "development_stage": "mature",
            "business_model_description": f"LLM 调用失败: {reason}",
            "income_stability": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "business_model_quality": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "business_model_simplicity": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "business_simplicity": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "growth_certainty": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "total_score": 0.0,
        }

    @staticmethod
    def _rating(total: float) -> str:
        # 总分 20 分 = 定性 12 + 定量 8
        if total >= 17.0:
            return "优秀"
        elif total >= 13.5:
            return "良好"
        elif total >= 9.0:
            return "中等"
        else:
            return "较差"


def _calculate_fcf_score(fcf: float, net_profit: float) -> tuple:
    """
    计算自由现金流质量基础分（单年度数据）

    Args:
        fcf: 自由现金流（万元）
        net_profit: 扣非净利润（万元）

    Returns:
        (基础分, FCF/扣非净利润 ratio)
    """
    if net_profit == 0:
        return 0, 0.0

    fcf_ratio = fcf / net_profit

    # FCF 为负
    if fcf <= 0:
        return 0, fcf_ratio

    # FCF 为正，按比例评分（6分制）
    if fcf_ratio >= 1.0:
        return 6, fcf_ratio
    elif fcf_ratio >= 0.8:
        return 5, fcf_ratio
    elif fcf_ratio >= 0.6:
        return 4, fcf_ratio
    elif fcf_ratio >= 0.4:
        return 3, fcf_ratio
    elif fcf_ratio >= 0.2:
        return 2, fcf_ratio
    else:
        return 1, fcf_ratio
