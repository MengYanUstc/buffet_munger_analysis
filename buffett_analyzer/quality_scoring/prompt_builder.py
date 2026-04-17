"""
统一 Prompt 构建器
将多个 AI 评分插件的事实数据组装成一条 Batch Prompt
"""

import json
from typing import Dict, Any, List

from .plugin_base import ScoringType


class PromptBuilder:
    @staticmethod
    def build(
        stock_code: str,
        industry_type: str,
        plugin_entries: List[Dict[str, Any]],
        enable_web_search: bool = False,
    ) -> str:
        """
        plugin_entries: 每个元素为 {
            "plugin": ScoringPlugin 实例,
            "facts": dict 或 None,
            "base_result": ScoringResult 或 None (AI_BASED 时需要 base_score)
        }
        """
        lines = [
            "你是一位遵循巴菲特-芒格价值投资理念的资深财务分析师。",
            "请根据以下企业的事实数据，对各个维度进行定性评分。",
            "",
            "【全局约束】",
            "1. 每个维度的最终得分必须是 0.5 的整数倍（如 0.0, 0.5, 1.0, 1.5 ...）。",
            "2. 评分必须严格落在允许区间内，不得超过满分，不得低于 0。",
            "3. 对于 AI_BASED 类型，最终得分 = 基础分 + AI调整值；调整值必须在指定调整区间内。",
            "4. 对于 QUALITATIVE_ONLY 类型，由你直接根据事实给出 0 ~ 满分 的得分。",
            "5. 每个维度必须给出 30 字以内的评分理由。",
            *( [
                "",
                "【联网搜索辅助】",
                "你已获得内置的 $web_search 联网搜索工具权限。",
                "如果提供的事实数据不足以支撑某个维度的评分（如分红记录、违规处罚、高管减持、并购商誉等信息缺失），请主动调用 $web_search 搜索补充最新公开信息后再给出评分。",
               ] if enable_web_search else []
            ),
            "",
            f"股票代码: {stock_code}",
            f"行业类型: {industry_type}",
            "",
            "【评分维度】",
        ]

        for idx, entry in enumerate(plugin_entries, 1):
            plugin = entry["plugin"]
            facts = entry.get("facts") or {}
            base_result = entry.get("base_result")

            lines.append(f"\n--- 维度 {idx}: {plugin.name} ({plugin.dimension_id}) ---")
            lines.append(f"评分类型: {plugin.score_type.value}")
            lines.append(f"满分: {plugin.max_score}")
            lines.append(f"最小步长: {plugin.step}")

            if plugin.score_type == ScoringType.AI_BASED and base_result is not None:
                lines.append(f"基础分(由脚本计算): {base_result.base_score}")
                if base_result.penalty_score is not None and base_result.penalty_score != base_result.base_score:
                    lines.append(f"强制规则调整后的基准分(penalty_score): {base_result.penalty_score}")
                    ref = base_result.penalty_score
                else:
                    ref = base_result.base_score
                lower = max(0.0, ref - 0.5)
                upper = min(plugin.max_score, ref + 0.5)
                lines.append(f"AI微调区间: [{lower:.1f}, {upper:.1f}] (相对于基准分)")

            if plugin.get_rubric():
                lines.append(f"评分标准:\n{plugin.get_rubric()}")

            if facts:
                lines.append(f"事实数据:\n{json.dumps(facts, ensure_ascii=False, indent=2, default=str)}")
            else:
                lines.append("事实数据: 无额外定量数据，请基于一般商业常识判断。")

        # 构建输出格式说明（支持各插件自定义 schema）
        output_schema = {"scores": {}}
        for entry in plugin_entries:
            plugin = entry["plugin"]
            schema = plugin.get_output_schema()
            output_schema["scores"][plugin.dimension_id] = schema

        lines.extend([
            "",
            "【输出格式】",
            "仅返回以下 JSON，不要任何额外解释:",
            json.dumps(output_schema, ensure_ascii=False, indent=2)
        ])

        return "\n".join(lines)
