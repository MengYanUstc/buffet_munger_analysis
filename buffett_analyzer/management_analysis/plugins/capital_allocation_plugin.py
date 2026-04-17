"""
资本配置能力评估插件（满分 6 分）
采用 AI_BASED 模式：代码级规则生成 base_score / penalty_score，LLM 做 [-0.5, +0.5] 微调。
"""

from typing import Dict, Any

from ...quality_scoring.plugin_base import ScoringPlugin, ScoringResult, ScoringType


class CapitalAllocationPlugin(ScoringPlugin):
    dimension_id = "capital_allocation"
    name = "资本配置能力"
    max_score = 6.0
    score_type = ScoringType.AI_BASED
    step = 0.5

    def _compute_base_score(self, context: Dict[str, Any]) -> float:
        """基于定量数据和简单规则计算基准分。"""
        score = 4.5  # 从中性偏上开始

        roic = context.get("roic_trend", {})
        trend = roic.get("trend", "")
        trend_diff = roic.get("trend_diff", 0)
        if trend == "明显下降":
            score -= 1.5
        elif trend == "温和下降":
            score -= 0.5
        elif trend == "明显上升":
            score += 0.5
        elif trend == "温和上升":
            score += 0.5

        pledge = context.get("pledge", {})
        pledge_ratio = pledge.get("pledge_ratio")
        if pledge_ratio is not None:
            try:
                pr = float(pledge_ratio)
                if pr > 50:
                    score -= 1.5
                elif pr > 30:
                    score -= 0.5
                elif pr > 0:
                    score -= 0.0  # 有小额质押但无风险
            except (ValueError, TypeError):
                pass

        # 分红与并购数据不足时暂不额外惩罚，由 LLM 根据搜索结果微调
        return max(0.0, min(self.max_score, round(score / self.step) * self.step))

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        base_score = self._compute_base_score(context)
        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=0.0,
            max_score=self.max_score,
            score_type=self.score_type,
            base_score=base_score,
            penalty_score=base_score,
            reason=f"代码级规则基准分 {base_score}/{self.max_score}（ROIC趋势与股权质押），由 LLM 结合搜索摘要微调。",
        )

    def get_facts(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "roic_trend": context.get("roic_trend", {}),
            "dividend": context.get("dividend", {}),
            "pledge": context.get("pledge", {}),
            "mergers": context.get("mergers", {}),
        }

    def get_rubric(self) -> str:
        return (
            "评估管理层的资本配置能力，满分6分。\n"
            "\n"
            "你当前的任务是：在系统已根据 ROIC 趋势和股权质押比例计算出的 base_score 基础上，做 [-0.5, +0.5] 的微调。\n"
            "\n"
            "思考要点：\n"
            "1. ROIC表现：近5年ROIC趋势是上升、平稳还是下降？下降幅度多大？资本使用效率是否稳定？\n"
            "2. 分红政策：分红是否稳定持续？分红比例是否合理（30%-70%）？不分红是否有合理理由？\n"
            "   （如 dividend 字段中包含 web_search，请结合联网搜索摘要判断）\n"
            "3. 并购与扩张：是否有重大并购？并购后业绩表现如何？商誉是否有大额减值风险？\n"
            "   （如 mergers 字段中包含 web_search，请结合联网搜索摘要判断）\n"
            "4. 股权质押：大股东质押比例多少？是否存在平仓风险或控制权不稳风险？\n"
            "\n"
            "评分锚点：\n"
            "- 6分：ROIC稳定或上升、分红稳定合理、无重大并购失败、无股权质押风险\n"
            "- 4-5分：ROIC温和下降、分红不稳定或比例偏低、有小额质押\n"
            "- 2-3分：ROIC明显下滑、长期不分红、并购失败或高质押\n"
            "- 0-1分：ROIC严重下滑、资本配置能力极差\n"
            "\n"
            "输出说明：最终 score 会在 base_score 基础上被引擎自动限制在 [-0.5, +0.5] 区间内，所以你只需根据事实给出合理微调即可。\n"
        )

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "score": "0.0-6.0 之间的数字，必须是 0.5 的倍数",
            "reason": "50-100字的综合分析",
            "roic_comment": "ROIC表现评价（20字左右）",
            "dividend_comment": "分红政策评价（20字左右）",
            "merger_comment": "并购与扩张评价（20字左右）",
            "pledge_comment": "股权质押评价（20字左右）",
        }
