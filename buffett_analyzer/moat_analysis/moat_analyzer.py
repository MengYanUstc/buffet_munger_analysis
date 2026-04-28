"""
护城河分析主模块（Module 3）
总分 30 分 = 毛利率绝对值(2.5分, 代码定量) + 毛利率稳定性(2.5分, 代码定量) + 定性分析(25分, Coze LLM)
"""

import json
import os
import re
from typing import Dict, Any, List

from ..core import AnalyzerBase, AnalysisReport
from .gross_margin_scorer import compute_gross_margin_score
from ..data_warehouse.collector import DataCollector


class MoatAnalyzer(AnalyzerBase):
    module_id = "moat"
    module_name = "护城河分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.collector = DataCollector()

    def run(self) -> AnalysisReport:
        # 0. 确保财务数据已入库
        self.collector.collect(self.stock_code)

        # 1. 从数据库读取毛利率数据（定量基础）
        gm_data = self._fetch_gross_margin_data()

        # 2. 代码计算毛利率评分（5分 = 绝对值2.5分 + 稳定性2.5分）
        gm_result = compute_gross_margin_score(gm_data.get("values", []))

        # 3. 从统一缓存读取护城河定性分析（25分）
        llm_result = self._get_qualitative_result(gm_result)

        # 4. 汇总
        dimensions = {}

        # 毛利率绝对值维度（2.5分）
        gm_abs = gm_result.get("absolute", {})
        if gm_abs.get("score") is not None:
            dimensions["gross_margin_absolute"] = {
                "score": gm_abs["score"],
                "max_score": 2.5,
                "avg_margin": gm_abs["avg_margin"],
                "reason": (
                    f"近5年平均毛利率 {gm_abs['avg_margin']}%，"
                    f"对应绝对值得分 {gm_abs['score']}/2.5"
                ),
            }
        else:
            dimensions["gross_margin_absolute"] = {
                "score": 0.0,
                "max_score": 2.5,
                "reason": "毛利率数据不足，无法评分",
            }

        # 毛利率稳定性维度（2.5分）
        gm_stab = gm_result.get("stability", {})
        if gm_stab.get("final_score") is not None:
            dimensions["gross_margin_stability"] = {
                "score": gm_stab["final_score"],
                "max_score": 2.5,
                "base_score": gm_stab["base_score"],
                "trend_adjustment": gm_stab["trend_adjustment"],
                "cv": gm_stab.get("cv"),
                "std": gm_stab["std"],
                "values": gm_result.get("values", []),
                "trend_direction": gm_stab["trend"].get("trend_direction"),
                "trend_diff": gm_stab["trend"].get("trend_diff"),
                "reason": (
                    f"近5年毛利率变异系数（CV）{gm_stab.get('cv')}%"
                    f"（标准差{gm_stab['std']}% / 均值{gm_result.get('absolute', {}).get('avg_margin')}%），"
                    f"趋势为'{gm_stab['trend'].get('trend_direction')}'，"
                    f"基础分 {gm_stab['base_score']} + 趋势调整 {gm_stab['trend_adjustment']} = "
                    f"最终分 {gm_stab['final_score']}/2.5"
                ),
            }
        else:
            dimensions["gross_margin_stability"] = {
                "score": 0.0,
                "max_score": 2.5,
                "reason": "毛利率数据不足，无法评分",
            }

        # LLM 定性维度
        qualitative_dims = [
            ("industry_quality", "行业质量", 5.0),
            ("moat_type", "护城河类型", 7.0),
            ("moat_sustainability", "护城河可持续性", 7.0),
            ("pricing_power", "定价权", 6.0),
        ]
        qualitative_total = 0.0
        for key, name, max_s in qualitative_dims:
            dim = llm_result.get(key, {})
            
            # 护城河可持续性：优先使用结构化字段公式计算
            if key == "moat_sustainability" and "history_duration_years" in dim:
                score = self._compute_sustainability_score(dim)
                reason = dim.get("reason", "")
                # 在 reason 中补充公式计算细节
                detail = (
                    f"【公式计算】历史时长{dim.get('history_duration_years',0)}年"
                    f" + 周期考验{dim.get('cycle_tests_count',0)}轮"
                    f" + 突破难度{dim.get('breakthrough_difficulty','')}"
                    f" + 趋势{dim.get('trend_judgment','')}"
                    f" = 得分{score}分。"
                )
                reason = detail + " " + reason if reason else detail
            else:
                score = dim.get("score", 0.0)
                reason = dim.get("reason", "")
            
            dim_data = {
                "score": score,
                "max_score": max_s,
                "reason": reason,
            }
            # 如果是护城河可持续性，把结构化字段也带过去供报告渲染
            if key == "moat_sustainability":
                for field in ["history_duration_years", "cycle_tests_count", "breakthrough_difficulty", "trend_judgment"]:
                    if field in dim:
                        dim_data[field] = dim[field]
            dimensions[key] = dim_data
            qualitative_total += score

        # 总分
        gm_total = dimensions["gross_margin_absolute"]["score"] + dimensions["gross_margin_stability"]["score"]
        total_score = round(gm_total + qualitative_total, 1)
        max_score = 30.0

        rating = self._rating(total_score)

        summary = {
            "gross_margin_score": gm_total,
            "qualitative_score": round(qualitative_total, 1),
            "total_score": total_score,
            "max_score": max_score,
            "rating": rating,
        }

        raw_data = {
            "gross_margin": gm_result,
            "qualitative_llm_raw": llm_result.get("_raw_text", ""),
        }

        return AnalysisReport(
            module_id=self.module_id,
            module_name=self.module_name,
            stock_code=self.stock_code,
            total_score=total_score,
            max_score=max_score,
            rating=rating,
            dimensions=dimensions,
            summary=summary,
            risk_warnings=llm_result.get("risk_warnings", []),
            key_facts=llm_result.get("key_facts", []),
            raw_data=raw_data,
        )

    def _fetch_gross_margin_data(self) -> Dict[str, Any]:
        """从数据库读取近5年毛利率数据。"""
        try:
            df = self.collector.cache.read_financial_reports(self.stock_code)
            if df.empty or "gross_margin" not in df.columns:
                return {"values": []}
            values = df["gross_margin"].dropna().tail(5).tolist()
            return {"values": values}
        except Exception as e:
            print(f"[MoatAnalyzer] 毛利率数据读取失败: {e}")
            return {"values": []}

    def _get_qualitative_result(self, gm_result: Dict[str, Any]) -> Dict[str, Any]:
        """从统一缓存读取护城河定性结果，缓存未命中则返回空结果。"""
        cached = self.collector.get_qualitative_result(self.stock_code, "moat")
        if cached is not None:
            return cached
        print("[MoatAnalyzer] 警告: 护城河定性缓存未命中，跳过")
        return self._empty_qualitative_result("缓存未命中")

    @staticmethod
    def build_qualitative_prompt(stock_code: str, gm_result: Dict[str, Any]) -> str:
        """构建护城河定性分析 Prompt（供外部统一调用）。"""
        gm_text = ""
        if gm_result and gm_result.get("values"):
            abs_part = gm_result.get("absolute", {})
            stab_part = gm_result.get("stability", {})
            gm_text = (
                f"近5年毛利率数据：{gm_result['values']}%，"
                f"平均毛利率 {abs_part.get('avg_margin', 'N/A')}%（绝对值得分 {abs_part.get('score', 'N/A')}/2.5），"
                f"变异系数（CV）{stab_part.get('cv', 'N/A')}%（稳定性得分 {stab_part.get('final_score', 'N/A')}/2.5），"
                f"趋势：{stab_part.get('trend', {}).get('trend_direction', 'N/A')}"
            )

        return f"""你是一位资深中国A股投资分析师，擅长巴菲特-芒格式的价值投资框架中的护城河分析。

请对 **{stock_code}** 的护城河进行深度定性评估。
要求完全基于你所掌握的公开信息（财报、行业报告、新闻、公告等）独立判断。

## 已知财务事实
{gm_text}

---

## 评分维度（共25分）

### 1. 行业质量（满分 5 分）
评估该公司所在行业的整体质量：
- 行业集中度（CR3/CR5）
- 行业成长性（近5年CAGR）
- 进入壁垒（技术/资金/牌照/品牌）
- 需求稳定性（周期性 vs 刚需）

锚点：
- 5分：极高质量行业（如高端白酒、创新药）
- 3-4分：高质量行业
- 1-2分：中等质量行业
- 0分：低质量行业（过度竞争、强周期）

### 2. 护城河类型与强度（满分 7 分）
识别并评估公司护城河的类型和强度：
- 品牌护城河（品牌溢价、消费者认知）
- 转换成本护城河（客户粘性）
- 网络效应护城河（用户规模效应）
- 成本优势护城河（规模效应、成本领先）
- 技术优势护城河（专利、研发壁垒）
- 渠道优势护城河（终端覆盖）
- 资源垄断护城河（稀缺资源、牌照）

锚点：
- 7分：极强护城河，无法复制（如茅台品牌）
- 5-6分：强护城河，难以突破
- 3-4分：中等护城河，可能被挑战
- 1-2分：弱护城河
- 0分：无护城河

### 3. 护城河可持续性（满分 7 分）
评估护城河能持续多久：
- 历史持续时长（是否经受过经济周期考验）
- 近3-5年趋势（加强/稳定/削弱）
- 竞争对手突破难度
- 技术/行业变革风险
- 公司是否持续投资维护护城河

锚点：
- 7分：极高可持续性（>50年历史，多轮周期，趋势加强）
- 5-6分：高可持续性（30-50年，2-3轮周期）
- 4分：较高可持续性（15-30年）
- 3分：中等（5-15年）
- 1-2分：低可持续性
- 0分：不可持续

### 4. 定价权评估（满分 6 分）
评估公司自主定价能力：
- 提价历史（近5年是否多次提价且销量不受影响）
- 产品差异化程度
- 客户忠诚度/复购率
- 供应链议价能力
- 客户价格敏感度

锚点：
- 6分：强定价权（多次提价销量增长）
- 4-5分：较强定价权
- 3分：中等
- 1-2分：弱定价权
- 0分：无定价权

---

## 输出要求（严格 JSON 格式）

只输出 JSON，不要任何其他文字：

```json
{{
  "stock_code": "{stock_code}",
  "industry_quality": {{
    "score": X.X,
    "max_score": 5.0,
    "reason": "详细说明，引用具体事实"
  }},
  "moat_type": {{
    "score": X.X,
    "max_score": 7.0,
    "reason": "详细说明，引用具体事实"
  }},
  "moat_sustainability": {{
    "history_duration_years": XX,
    "cycle_tests_count": X,
    "breakthrough_difficulty": "等级(分值)",
    "trend_judgment": "等级(分值)",
    "reason": "详细说明，引用具体事实"
  }},
  "pricing_power": {{
    "score": X.X,
    "max_score": 6.0,
    "reason": "详细说明，引用具体事实"
  }},
  "qualitative_total": X.X,
  "qualitative_max": 25.0,
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
    def _empty_qualitative_result(reason: str) -> Dict[str, Any]:
        """LLM 失败时的空结果。"""
        return {
            "industry_quality": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "moat_type": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "moat_sustainability": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "pricing_power": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "qualitative_total": 0.0,
        }

    @staticmethod
    def _compute_sustainability_score(data: dict) -> float:
        """
        基于 LLM 返回的结构化字段计算护城河可持续性得分（满分7分，步长0.5）。

        字段：
          - history_duration_years: 历史时长（年）
          - cycle_tests_count: 周期考验轮数
          - breakthrough_difficulty: 突破难度等级（如"很难(3)"）
          - trend_judgment: 趋势判断等级（如"加强(3)"）
        """
        years = data.get("history_duration_years", 0)
        cycles = data.get("cycle_tests_count", 0)
        difficulty_str = str(data.get("breakthrough_difficulty", ""))
        trend_str = str(data.get("trend_judgment", ""))

        def _extract_level(text: str) -> int:
            m = re.search(r'\((\d+)\)', text)
            if m:
                return int(m.group(1))
            m = re.search(r'\d+', text)
            return int(m.group(0)) if m else 0

        difficulty = _extract_level(difficulty_str)
        trend = _extract_level(trend_str)

        # 1. 历史时长基础分
        if years > 50:
            base = 5.0
        elif years >= 30:
            base = 4.0
        elif years >= 20:
            base = 3.0
        elif years >= 10:
            base = 2.0
        elif years >= 5:
            base = 1.0
        else:
            base = 0.5

        # 2. 周期考验调整
        if cycles >= 3:
            cycle_adj = 1.0
        elif cycles >= 2:
            cycle_adj = 0.5
        else:
            cycle_adj = 0.0

        # 3. 突破难度调整
        if difficulty >= 4:
            diff_adj = 1.0
        elif difficulty >= 3:
            diff_adj = 0.5
        elif difficulty >= 2:
            diff_adj = 0.0
        else:
            diff_adj = -0.5

        # 4. 趋势判断调整
        if trend >= 3:
            trend_adj = 0.5
        elif trend >= 2:
            trend_adj = 0.0
        else:
            trend_adj = -0.5

        total = base + cycle_adj + diff_adj + trend_adj
        return round(max(0, min(7, total)) * 2) / 2

    @staticmethod
    def _rating(total: float) -> str:
        if total >= 25:
            return "极深护城河"
        elif total >= 20:
            return "深护城河"
        elif total >= 14:
            return "中等护城河"
        elif total >= 8:
            return "弱护城河"
        else:
            return "无护城河"
