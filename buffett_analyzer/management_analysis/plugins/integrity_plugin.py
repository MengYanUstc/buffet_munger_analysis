"""
管理层诚信评估插件（满分 4 分）
采用 AI_BASED 模式：代码级关键词扫描生成 penalty_score，LLM 只做 [-0.5, +0.5] 微调。
包含"一票否决"规则：若确认财务造假被立案，诚信直接 0 分。
"""

import re
from typing import Dict, Any, List

from ...quality_scoring.plugin_base import ScoringPlugin, ScoringResult, ScoringType


class IntegrityPlugin(ScoringPlugin):
    dimension_id = "management_integrity"
    name = "管理层诚信"
    max_score = 4.0
    score_type = ScoringType.AI_BASED
    step = 0.5

    # 关键词惩罚规则（在 snippets/records 文本中匹配）
    # 每条规则：("关键词1|关键词2", 扣分值, 描述)
    KEYWORD_PENALTIES = [
        # 一票否决级
        (r"证监会立案|立案调查|欺诈发行|财务造假|系统性腐败|终身市场禁入|被证监会处罚", 4.0, "财务造假/立案"),
        # 严重违规
        (r"行政处罚|监管函|通报批评|责令改正|内幕交易|操纵市场|违规披露|重大违法", 1.5, "行政处罚或严重违规"),
        # 治理缺陷
        (r"关联交易.*利益输送|利益输送|资金占用|违规担保|实控人占用|侵占上市公司利益", 1.0, "利益输送或资金占用"),
        # 异常减持
        (r"离婚式减持|清仓式减持|减持引发股价大跌|敏感期减持|高管集体减持|实控人减持套现", 1.5, "异常减持行为"),
        # 一般减持
        (r"大额减持|持续减持|减持套现|高管密集减持|减持计划", 0.5, "管理层减持"),
    ]

    def _extract_text(self, data: Dict[str, Any]) -> str:
        """从 violations / management_holdings 等字段中提取所有文本。"""
        texts = []
        if isinstance(data, dict):
            for val in data.values():
                if isinstance(val, str):
                    texts.append(val)
                elif isinstance(val, list):
                    for item in val:
                        if isinstance(item, str):
                            texts.append(item)
                        elif isinstance(item, dict):
                            texts.append(item.get("title", ""))
                            texts.append(item.get("summary", ""))
                            texts.append(item.get("description", ""))
                elif isinstance(val, dict):
                    texts.append(val.get("title", ""))
                    texts.append(val.get("summary", ""))
                    texts.append(val.get("description", ""))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    texts.append(item)
                elif isinstance(item, dict):
                    texts.append(item.get("title", ""))
                    texts.append(item.get("summary", ""))
        return " ".join(texts)

    def _compute_penalty_score(self, context: Dict[str, Any]) -> float:
        """基于代码级关键词扫描计算 penalty_score。"""
        violations = context.get("violations", {})
        holdings = context.get("management_holdings", {})
        text = self._extract_text(violations) + " " + self._extract_text(holdings)
        text = text.lower()

        total_penalty = 0.0
        matched_reasons = []
        for pattern, penalty, desc in self.KEYWORD_PENALTIES:
            if re.search(pattern, text, re.IGNORECASE):
                total_penalty += penalty
                matched_reasons.append(desc)

        base = self.max_score
        penalty_score = max(0.0, base - total_penalty)
        return penalty_score, matched_reasons

    def compute(self, context: Dict[str, Any]) -> ScoringResult:
        violations = context.get("violations", {})
        # 一票否决：如果数据中明确标记存在财务造假立案，直接 0 分
        if violations.get("has_fraud_flag") is True:
            return ScoringResult(
                dimension_id=self.dimension_id,
                name=self.name,
                score=0.0,
                max_score=self.max_score,
                score_type=self.score_type,
                base_score=0.0,
                penalty_score=0.0,
                reason="检测到财务造假被立案或欺诈发行记录，诚信一票否决",
                details={
                    "fraud_flag": True,
                    "violation_records": violations.get("records", []),
                },
            )

        penalty_score, reasons = self._compute_penalty_score(context)
        reason = ""
        if reasons:
            reason = f"关键词扫描命中：{', '.join(reasons)}；初始基准分 {penalty_score}/{self.max_score}，由 LLM 微调。"
        else:
            reason = f"未检测到明显负面关键词；初始基准分 {penalty_score}/{self.max_score}，由 LLM 结合事实微调。"

        return ScoringResult(
            dimension_id=self.dimension_id,
            name=self.name,
            score=0.0,
            max_score=self.max_score,
            score_type=self.score_type,
            base_score=penalty_score,
            penalty_score=penalty_score,
            reason=reason,
        )

    def get_facts(self, context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "violations": context.get("violations", {}),
            "management_holdings": context.get("management_holdings", {}),
            "governance_note": context.get("governance_note", "治理结构详细数据待后续接入"),
        }

    def get_rubric(self) -> str:
        return (
            "评估管理层的诚信度，满分4分。\n"
            "\n"
            "特殊规则（一票否决）：若事实数据中 has_fraud_flag=true（财务造假被立案或欺诈发行），诚信直接0分。\n"
            "\n"
            "你当前的任务是：在系统已根据关键词扫描给出的 penalty_score 基准分基础上，做 [-0.5, +0.5] 的微调。\n"
            "\n"
            "思考要点：\n"
            "1. 违规记录：是否有财务造假、欺诈发行等重大违规？是否有系统性腐败？是否有行政处罚或监管函？\n"
            "   （如 violations 字段中包含 web_search，请结合联网搜索摘要判断）\n"
            "2. 减持行为：管理层是否频繁减持或清仓式减持？减持时机是否敏感？是否有合理理由？\n"
            "   （如 management_holdings 字段中包含 web_search，请结合联网搜索摘要判断）\n"
            "3. 治理结构：董事长是否频繁更替？是否存在关联交易或利益输送风险？是否有有效制衡机制？\n"
            "   （如 management_holdings 或 violations 的 web_search 中提到治理问题，请一并考虑）\n"
            "\n"
            "评分锚点：\n"
            "- 4分：无违规记录、无异常减持、治理完善\n"
            "- 3分：轻微违规或治理有小瑕疵\n"
            "- 1-2分：有违规记录或减持异常（如清仓式减持、敏感期减持、离婚式减持引发重大质疑）或治理缺陷\n"
            "- 0分：财务造假被立案、系统性腐败、严重治理缺陷\n"
            "\n"
            "特别注意：若搜索材料中出现'离婚式减持'、'敏感期减持'、'减持引发股价大跌/市场质疑'等表述，即使未被行政处罚，也应视为减持行为异常。\n"
            "输出说明：最终 score 会在 penalty_score 基础上被引擎自动限制在 [-0.5, +0.5] 区间内，所以你只需根据事实给出合理微调即可。\n"
        )

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "score": "0.0-4.0 之间的数字，必须是 0.5 的倍数",
            "reason": "50-100字的综合分析",
            "violation_comment": "违规记录评价（20字左右）",
            "reduction_comment": "减持行为评价（20字左右）",
            "governance_comment": "治理结构评价（20字左右）",
            "fraud_check": "是/否（财务造假被立案）",
        }
