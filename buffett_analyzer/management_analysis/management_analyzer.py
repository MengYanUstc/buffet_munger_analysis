"""
管理层定性分析主模块（已统一为 Coze 定性流程）
与护城河、商业模式分析模块保持一致：
  - 定性数据通过 DataCollector.collect_qualitative() 缓存获取
  - 直接使用 CozeLLMClient 平铺 JSON 返回
  - 解析入口统一
"""

from typing import Dict, Any

from ..core import AnalyzerBase, AnalysisReport
from ..data_warehouse.collector import DataCollector


class ManagementAnalyzer(AnalyzerBase):
    module_id = "management"
    module_name = "管理层定性分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.collector = DataCollector()

    def run(self) -> AnalysisReport:
        # 0. 确保财务数据已入库
        self.collector.collect(self.stock_code)

        # 1. 从统一缓存读取管理层定性分析结果
        llm_result = self._get_qualitative_result()

        # 2. 从数据库读取ROIC趋势（用于报告展示）
        roic_trend = self._fetch_roic_trend()

        # 3. 解析维度得分
        cap = llm_result.get("capital_allocation", {})
        integrity = llm_result.get("management_integrity", {})

        cap_score = float(cap.get("score", 0.0))
        int_score = float(integrity.get("score", 0.0))
        total_score = round(cap_score + int_score, 2)
        rating = self._rating(total_score)

        # 4. 风险提示（合并 LLM 返回和代码生成）
        risks = llm_result.get("risk_warnings", [])
        if int_score <= 1.0:
            risks.append("管理层诚信评分极低，存在重大治理或违规风险")
        if cap_score <= 2.0:
            risks.append("资本配置能力评分极低，可能存在资本浪费或并购失败")

        dimensions = {
            "capital_allocation": {
                "score": cap_score,
                "max_score": 6.0,
                "reason": cap.get("reason", "数据暂缺"),
            },
            "management_integrity": {
                "score": int_score,
                "max_score": 4.0,
                "reason": integrity.get("reason", "数据暂缺"),
            },
        }

        summary = {
            "capital_allocation_score": cap_score,
            "management_integrity_score": int_score,
            "total_score": total_score,
            "max_score": 10.0,
            "rating": rating,
        }

        raw_data = {
            "llm_raw": llm_result.get("_raw_text", ""),
            "roic_trend": roic_trend,
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
            risk_warnings=risks,
            key_facts=llm_result.get("key_facts", []),
            raw_data=raw_data,
        )

    def _get_qualitative_result(self) -> Dict[str, Any]:
        """从统一缓存读取管理层定性结果，若缺失则 fallback 到本地调用。"""
        cached = self.collector.get_qualitative_result(self.stock_code, "management")
        if cached is not None:
            return cached

        # fallback：本地调用 Coze（兼容模式）
        print("[ManagementAnalyzer] 缓存未命中，本地调用 Coze LLM...")
        import os
        from ..quality_scoring.coze_client import CozeLLMClient
        from ..utils.constants import DEFAULT_COZE_API_TOKEN
        token = os.getenv("COZE_API_TOKEN")
        if token:
            client = CozeLLMClient(api_token=token)
        else:
            client = CozeLLMClient(api_token=DEFAULT_COZE_API_TOKEN)
        if not client.is_configured():
            return self._empty_result("Coze API Token 未配置")

        prompt = self.build_qualitative_prompt(self.stock_code)
        try:
            result = client.call(prompt, timeout=600)
            result["_raw_text"] = ""
            return result
        except Exception as e:
            print(f"[ManagementAnalyzer] Coze LLM 调用失败: {e}")
            return self._empty_result(str(e))

    def _fetch_roic_trend(self) -> Dict[str, Any]:
        """从数据库读取近5年ROIC数据并计算趋势。"""
        try:
            df = self.collector.cache.read_financial_reports(self.stock_code)
            if df.empty or "roic" not in df.columns:
                return {}
            values = df["roic"].dropna().tail(5).tolist()
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
        except Exception:
            return {}

    @staticmethod
    def build_qualitative_prompt(stock_code: str, context: Dict[str, Any] = None) -> str:
        """构建管理层定性分析 Prompt（供外部统一调用）。"""
        context = context or {}
        roic_text = ""
        roic_trend = context.get("roic_trend", {})
        if roic_trend and roic_trend.get("values"):
            roic_text = (
                f"近5年ROIC数据：{roic_trend['values']}%，"
                f"趋势：{roic_trend.get('trend', 'N/A')}，"
                f"差值：{roic_trend.get('trend_diff', 'N/A')}"
            )

        pledge_text = ""
        pledge = context.get("pledge", {})
        if pledge and pledge.get("pledge_ratio") is not None:
            pledge_text = f"大股东股权质押比例：{pledge['pledge_ratio']}%"

        known_facts = "\n".join(filter(None, [roic_text, pledge_text])) or "暂无额外定量数据。"

        return f"""你是一位资深中国A股投资分析师，擅长巴菲特-芒格式的价值投资框架中的管理层评估。

请对 **{stock_code}** 的管理层进行深度评估。
要求完全基于你所掌握的公开信息（财报、年报、行业报告、新闻报道、监管公告等）独立判断。

## 已知财务事实
{known_facts}

---

## 评分维度（共10分）

### 1. 资本配置能力（满分 6 分）

评估管理层如何运用公司资本：
- **ROIC表现**：近5年ROIC趋势是上升、平稳还是下降？资本使用效率是否稳定？
- **分红政策**：分红是否稳定持续？分红比例是否合理（30%-70%）？
- **并购与扩张**：是否有重大并购？并购后业绩表现如何？商誉是否有大额减值风险？
- **股权质押**：大股东质押比例多少？是否存在平仓风险或控制权不稳风险？
- **再投资决策**：留存收益是否创造了更高的ROIC？

锚点：
- 6分：ROIC稳定或上升、分红稳定合理、无重大并购失败、无股权质押风险
- 4-5分：ROIC温和下降、分红不稳定或比例偏低、有小额质押
- 2-3分：ROIC明显下滑、长期不分红、并购失败或高质押
- 0-1分：ROIC严重下滑、资本配置能力极差

### 2. 管理层诚信（满分 4 分）

评估管理层的诚信度：
- **违规记录**：是否有财务造假、欺诈发行等重大违规？是否有行政处罚或监管函？
- **减持行为**：管理层是否频繁减持或清仓式减持？减持时机是否敏感？
- **治理结构**：董事长是否频繁更替？是否存在关联交易或利益输送风险？
- **信息披露**：财报是否透明？是否存在会计政策频繁变更或异常调节？

特殊规则（一票否决）：若确认财务造假被立案或欺诈发行，诚信直接0分。

锚点：
- 4分：无违规记录、无异常减持、治理完善、信息披露透明
- 3分：轻微违规或治理有小瑕疵
- 1-2分：有违规记录或减持异常或治理缺陷
- 0分：财务造假被立案、系统性腐败、严重治理缺陷

---

## 输出要求（严格 JSON 格式）

只输出 JSON，不要任何其他文字：

```json
{{
  "stock_code": "{stock_code}",
  "capital_allocation": {{
    "score": X.X,
    "max_score": 6.0,
    "reason": "详细说明，引用具体事实"
  }},
  "management_integrity": {{
    "score": X.X,
    "max_score": 4.0,
    "reason": "详细说明，引用具体事实"
  }},
  "total_score": X.X,
  "max_total": 10.0,
  "rating": "卓越/优秀/良好/中等/较差/差",
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
        """LLM 失败时的空结果。"""
        return {
            "capital_allocation": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "management_integrity": {"score": 0.0, "reason": f"LLM 调用失败: {reason}"},
            "total_score": 0.0,
        }

    @staticmethod
    def _rating(total: float) -> str:
        if total >= 9:
            return "卓越"
        elif total >= 7.5:
            return "优秀"
        elif total >= 6:
            return "良好"
        elif total >= 5:
            return "中等"
        elif total >= 4:
            return "较差"
        else:
            return "差"
