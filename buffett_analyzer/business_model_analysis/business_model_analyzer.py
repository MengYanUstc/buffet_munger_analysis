"""
商业模式分析主模块（Module 4）
定性部分 10 分 = 收入稳定性(5分) + 商业模式质量(5分)
全部由 Coze LLM 基于公开信息独立判断。
"""

import json
import os
from typing import Dict, Any

from ..core import AnalyzerBase, AnalysisReport
from ..quality_scoring.coze_client import CozeLLMClient


# 行业分类与发展阶段定义（用于 Prompt 中指导 LLM）
INDUSTRY_TYPES = {
    "light": "轻资产（软件、互联网、金融、咨询、白酒）",
    "medium": "中等资产（消费品、零售、医药）",
    "heavy": "重资产（制造业、公用事业、能源、交通）",
}

GROWTH_STAGES = {
    "startup": "初创期",
    "growth": "成长期",
    "mature": "成熟期",
    "decline": "衰退期",
}


class BusinessModelAnalyzer(AnalyzerBase):
    module_id = "business_model"
    module_name = "商业模式分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type

    def run(self) -> AnalysisReport:
        # 调用 Coze LLM 进行纯定性分析
        llm_result = self._call_llm_qualitative()

        # 构建维度
        dimensions = {}
        stability = llm_result.get("income_stability", {})
        quality = llm_result.get("business_model_quality", {})

        dimensions["income_stability"] = {
            "score": stability.get("score", 0.0),
            "max_score": 5.0,
            "reason": stability.get("reason", ""),
        }
        dimensions["business_model_quality"] = {
            "score": quality.get("score", 0.0),
            "max_score": 5.0,
            "reason": quality.get("reason", ""),
        }

        total_score = round(
            dimensions["income_stability"]["score"] + dimensions["business_model_quality"]["score"],
            1,
        )

        rating = self._rating(total_score)

        summary = {
            "income_stability_score": dimensions["income_stability"]["score"],
            "business_model_quality_score": dimensions["business_model_quality"]["score"],
            "total_score": total_score,
            "max_score": 10.0,
            "rating": rating,
        }

        # 额外信息：行业分类与发展阶段
        extra_info = {
            "industry_classification": llm_result.get("industry_classification", ""),
            "growth_stage": llm_result.get("growth_stage", ""),
            "business_model_description": llm_result.get("business_model_description", ""),
        }

        raw_data = {
            "llm_raw": llm_result.get("_raw_text", ""),
            "extra_info": extra_info,
        }

        return AnalysisReport(
            module_id=self.module_id,
            module_name=self.module_name,
            stock_code=self.stock_code,
            total_score=total_score,
            max_score=10.0,
            rating=rating,
            dimensions=dimensions,
            summary=summary,
            raw_data=raw_data,
        )

    def _call_llm_qualitative(self) -> Dict[str, Any]:
        """调用 Coze LLM 进行商业模式定性分析。"""
        token = os.getenv("COZE_API_TOKEN")
        if token:
            client = CozeLLMClient(api_token=token)
        else:
            client = CozeLLMClient(
                api_token="eyJhbGciOiJSUzI1NiIsImtpZCI6ImZmOTI5ZWIzLWM5NjctNGI5YS05ZGM0LTllMDYwODYxMTU1MCJ9.eyJpc3MiOiJodHRwczovL2FwaS5jb3plLmNuIiwiYXVkIjpbIlE3TFZ0ZkdwZzNEMVVKQ0pmdjhJcU1SdFJna2V1V20zIl0sImV4cCI6ODIxMDI2Njg3Njc5OSwiaWF0IjoxNzc2NDI4NzY5LCJzdWIiOiJzcGlmZmU6Ly9hcGkuY296ZS5jbi93b3JrbG9hZF9pZGVudGl0eS9pZDo3NjE1NTE0NzI0MDkxODIyMTA3Iiwic3JjIjoiaW5ib3VuZF9hdXRoX2FjY2Vzc190b2tlbl9pZDo3NjI5NzAzNDY5NzEyMDE1Mzg3In0.ZtfPq2Btc6ThWGiIG2kt3qmbw69ccPGQA_Rt7nXxUDPtLICKptgdkjU47fWISalpi1Wr7vbYEJM1Y5dXLmnVHlKLjUpwrH79unmURLgSieMlMAth4txWQYSdDbAeNRmTOW6PxN7gST35sRDpnhIWYn8dDnnEshr6L_H1mnAUTGOv7RgJDBjqxBsl2GyRkkF3hcUPKo4ALWZT09k-zeS1P6jnuAGozKwnC9dARZ6EvbSrQwRSUMLRAQ4a8h-WbkkJ23Pc-xUKq-IB_g1X2q_CyylL9AGCdASkcz7kfi4wQFM2svKnlulk_akWYruqVJTN7b2gqAWaExJaptfB0EjHqA"
            )

        if not client.is_configured():
            return self._empty_result("Coze API Token 未配置")

        prompt = self._build_prompt()
        try:
            result = client.call(prompt, timeout=120)
            return result
        except Exception as e:
            print(f"[BusinessModelAnalyzer] Coze LLM 调用失败: {e}")
            return self._empty_result(str(e))

    def _build_prompt(self) -> str:
        """构建商业模式定性分析 Prompt。"""
        return f"""你是一位资深中国A股投资分析师，擅长巴菲特-芒格式的价值投资框架中的商业模式分析。

请对 **{self.stock_code}** 进行深度商业模式评估。
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

## 第三步：评分（共10分）

### 1. 收入稳定性评估（满分 5 分）

评估因素：
- **客户集中度**：前五大客户占比多少？是否依赖单一客户？
- **产品结构**：产品线多元化还是单一化？单一产品占比是否过高（>60%）？
- **周期性影响**：行业有无明显周期性？收入波动率大概多少？
- **客户粘性**：B2B合同续约率？B2C复购率/留存率？是否有高转换成本？

锚点：
- **5分**：客户高度分散、产品多元化、无周期性、高复购率
- **3分**：客户较集中或产品单一或有一定周期性
- **1分**：大客户依赖严重、产品单一、强周期性、客户粘性低
- 中间分数根据实际情况插值

### 2. 商业模式质量评估（满分 5 分）

评估因素：
- **赚钱逻辑是否清晰**：如何创造价值？如何获取价值？盈利能力如何（ROE、净利率、现金流）？
- **可复制性**：业务能否标准化扩张？扩张壁垒是什么？
- **规模化能力**：规模扩大能否带来成本优势？能否通过提价实现盈利增长？边际成本是递增还是递减？
- **抗风险能力**：现金流质量如何？负债水平高吗？面临哪些行业/政策/竞争风险？

锚点：
- **5分**：赚钱逻辑清晰+盈利强、难以复制、强规模效应、抗风险能力强
- **3分**：赚钱逻辑清晰但盈利一般，或规模效应不明显，或有一定风险
- **1分**：赚钱逻辑不清晰、盈利困难、抗风险能力弱
- 中间分数根据实际情况插值

---

## 输出要求（严格 JSON 格式）

只输出 JSON，不要任何其他文字：

```json
{{
  "stock_code": "{self.stock_code}",
  "industry_classification": "light/medium/heavy",
  "industry_classification_desc": "具体说明",
  "growth_stage": "startup/growth/mature/decline",
  "growth_stage_desc": "具体说明",
  "business_model_description": "200-500字商业模式描述",
  "income_stability": {{
    "score": X.X,
    "max_score": 5.0,
    "reason": "详细说明，引用具体事实"
  }},
  "business_model_quality": {{
    "score": X.X,
    "max_score": 5.0,
    "reason": "详细说明，引用具体事实"
  }},
  "total_score": X.X,
  "max_total": 10.0,
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
            "industry_classification": "",
            "growth_stage": "",
            "business_model_description": f"LLM 调用失败: {reason}",
            "income_stability": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "business_model_quality": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "total_score": 0.0,
        }

    @staticmethod
    def _rating(total: float) -> str:
        if total >= 8.5:
            return "优秀"
        elif total >= 6.5:
            return "良好"
        elif total >= 4.5:
            return "中等"
        else:
            return "较差"
