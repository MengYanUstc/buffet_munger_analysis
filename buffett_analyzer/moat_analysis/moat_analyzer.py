"""
护城河分析主模块（Module 3）
总分 30 分 = 毛利率稳定性(4分, 代码定量) + 定性分析(26分, Coze LLM)
"""

import json
import os
from typing import Dict, Any, List

from ..core import AnalyzerBase, AnalysisReport
from .gross_margin_scorer import compute_gross_margin_score
from ..quality_scoring.coze_client import CozeLLMClient
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

        # 2. 代码计算毛利率稳定性评分（4分）
        gm_result = compute_gross_margin_score(gm_data.get("values", []))

        # 3. 从统一缓存读取护城河定性分析（26分）
        llm_result = self._get_qualitative_result(gm_result)

        # 4. 汇总
        dimensions = {}

        # 毛利率稳定性维度
        if gm_result.get("final_score") is not None:
            dimensions["gross_margin_stability"] = {
                "score": gm_result["final_score"],
                "max_score": 4.0,
                "base_score": gm_result["base_score"],
                "trend_adjustment": gm_result["trend_adjustment"],
                "std": gm_result["std"],
                "values": gm_result["values"],
                "trend_direction": gm_result["trend"].get("trend_direction"),
                "trend_diff": gm_result["trend"].get("trend_diff"),
                "reason": (
                    f"近5年毛利率标准差 {gm_result['std']}%，"
                    f"趋势为'{gm_result['trend'].get('trend_direction')}'，"
                    f"基础分 {gm_result['base_score']} + 趋势调整 {gm_result['trend_adjustment']} = "
                    f"最终分 {gm_result['final_score']}/4.0"
                ),
            }
        else:
            dimensions["gross_margin_stability"] = {
                "score": 0.0,
                "max_score": 4.0,
                "reason": "毛利率数据不足，无法评分",
            }

        # LLM 定性维度
        qualitative_dims = [
            ("industry_quality", "行业质量", 6.0),
            ("moat_type", "护城河类型", 7.0),
            ("moat_sustainability", "护城河可持续性", 7.0),
            ("pricing_power", "定价权", 6.0),
        ]
        qualitative_total = 0.0
        for key, name, max_s in qualitative_dims:
            dim = llm_result.get(key, {})
            score = dim.get("score", 0.0)
            reason = dim.get("reason", "")
            dimensions[key] = {
                "score": score,
                "max_score": max_s,
                "reason": reason,
            }
            qualitative_total += score

        # 总分
        gm_score = dimensions["gross_margin_stability"]["score"]
        total_score = round(gm_score + qualitative_total, 1)
        max_score = 30.0

        rating = self._rating(total_score)

        summary = {
            "gross_margin_stability_score": gm_score,
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
        """从统一缓存读取护城河定性结果，若缺失则 fallback 到本地调用。"""
        # 优先从统一缓存读取
        cached = self.collector.get_qualitative_result(self.stock_code, "moat")
        if cached is not None:
            return cached

        # fallback：本地调用 Coze（兼容模式）
        print("[MoatAnalyzer] 缓存未命中，本地调用 Coze LLM...")
        from ..utils.constants import DEFAULT_COZE_API_TOKEN
        token = os.getenv("COZE_API_TOKEN")
        if token:
            client = CozeLLMClient(api_token=token)
        else:
            client = CozeLLMClient(api_token=DEFAULT_COZE_API_TOKEN)
        if not client.is_configured():
            return self._empty_qualitative_result("Coze API Token 未配置")

        prompt = self.build_qualitative_prompt(self.stock_code, gm_result)
        try:
            result = client.call(prompt, timeout=600)
            result["_raw_text"] = ""
            return result
        except Exception as e:
            print(f"[MoatAnalyzer] Coze LLM 调用失败: {e}")
            return self._empty_qualitative_result(str(e))

    @staticmethod
    def build_qualitative_prompt(stock_code: str, gm_result: Dict[str, Any]) -> str:
        """构建护城河定性分析 Prompt（供外部统一调用）。"""
        gm_text = ""
        if gm_result and gm_result.get("values"):
            gm_text = (
                f"近5年毛利率数据：{gm_result['values']}%，"
                f"标准差 {gm_result.get('std', 'N/A')}%，"
                f"趋势：{gm_result['trend'].get('trend_direction', 'N/A')}"
            )

        return f"""你是一位资深中国A股投资分析师，擅长巴菲特-芒格式的价值投资框架中的护城河分析。

请对 **{stock_code}** 的护城河进行深度定性评估。
要求完全基于你所掌握的公开信息（财报、行业报告、新闻、公告等）独立判断。

## 已知财务事实
{gm_text}

---

## 评分维度（共26分）

### 1. 行业质量（满分 6 分）
评估该公司所在行业的整体质量：
- 行业集中度（CR3/CR5）
- 行业成长性（近5年CAGR）
- 进入壁垒（技术/资金/牌照/品牌）
- 需求稳定性（周期性 vs 刚需）

锚点：
- 6分：极高质量行业（如高端白酒、创新药）
- 4-5分：高质量行业
- 2-3分：中等质量行业
- 0-1分：低质量行业（过度竞争、强周期）

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
    "max_score": 6.0,
    "reason": "详细说明，引用具体事实"
  }},
  "moat_type": {{
    "score": X.X,
    "max_score": 7.0,
    "reason": "详细说明，引用具体事实"
  }},
  "moat_sustainability": {{
    "score": X.X,
    "max_score": 7.0,
    "reason": "详细说明，引用具体事实"
  }},
  "pricing_power": {{
    "score": X.X,
    "max_score": 6.0,
    "reason": "详细说明，引用具体事实"
  }},
  "qualitative_total": X.X,
  "qualitative_max": 26.0,
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
