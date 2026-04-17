"""
AI 定性评分引擎
统一调度三种类型的评分插件：
- QUANTITATIVE_ONLY: 完全定量，脚本直接计算
- AI_BASED: 定量基础分 + AI 调整，批量调用 LLM
- QUALITATIVE_ONLY: 完全定性，批量调用 LLM
"""

import hashlib
import json
from typing import Dict, Any, List, Optional

from .plugin_base import ScoringPlugin, ScoringResult, ScoringType
from .llm_client import LLMClient
from .prompt_builder import PromptBuilder


class AiScoringEngine:
    def __init__(
        self,
        plugins: List[ScoringPlugin],
        llm_client: Optional[LLMClient] = None,
        db=None,
    ):
        self.plugins = {p.dimension_id: p for p in plugins}
        self.plugin_order = [p.dimension_id for p in plugins]
        self.llm = llm_client or LLMClient()
        self.db = db

    def run(self, context: Dict[str, Any]) -> Dict[str, ScoringResult]:
        """
        执行完整评分流程：
        1. 先计算所有插件的基础结果
        2. 分离出需要 AI 的插件
        3. 检查缓存 / 调用 LLM
        4. 回填 AI 结果并做硬约束校验
        5. 写审计日志
        6. 返回所有维度结果
        """
        stock_code = context.get("stock_code", "")
        industry_type = context.get("industry_type", "general")

        # 1. 所有插件先跑一遍 compute（QUANTITATIVE 直接完成，AI 类可能返回占位）
        results: Dict[str, ScoringResult] = {}
        ai_entries: List[Dict[str, Any]] = []

        for dim_id in self.plugin_order:
            plugin = self.plugins[dim_id]
            try:
                result = plugin.compute(context)
            except Exception as e:
                result = ScoringResult(
                    dimension_id=dim_id,
                    name=plugin.name,
                    score=0.0,
                    max_score=plugin.max_score,
                    score_type=plugin.score_type,
                    error=f"compute error: {e}",
                )
            results[dim_id] = result

            if plugin.score_type in (ScoringType.AI_BASED, ScoringType.QUALITATIVE_ONLY):
                facts = plugin.get_facts(context)
                entry = {
                    "plugin": plugin,
                    "facts": facts,
                }
                if plugin.score_type == ScoringType.AI_BASED:
                    entry["base_result"] = result
                ai_entries.append(entry)

        # 2. 批量调用 LLM（如果有需要 AI 的插件）
        if ai_entries:
            cached = self._check_cache(stock_code, industry_type, ai_entries)
            if cached is not None:
                llm_scores = cached
            else:
                llm_scores = self._call_llm_batch(stock_code, industry_type, ai_entries)
                self._save_cache(stock_code, industry_type, ai_entries, llm_scores)

            # 3. 回填并约束
            for dim_id, raw in llm_scores.items():
                if dim_id not in results:
                    continue
                plugin = self.plugins[dim_id]
                result = results[dim_id]
                raw_score = self._to_float(raw.get("score"))
                reason = raw.get("reason", "")

                if plugin.score_type == ScoringType.AI_BASED and result.base_score is not None:
                    # 优先使用 penalty_score 作为调整基准，没有则回退到 base_score
                    reference_score = result.penalty_score if result.penalty_score is not None else result.base_score
                    # 计算原始调整值
                    raw_adj = raw_score - reference_score
                    # 调整区间为 [-0.5, +0.5]，步长 0.5
                    lower_adj = -0.5
                    upper_adj = 0.5
                    clamped_adj = max(lower_adj, min(upper_adj, raw_adj))
                    # 步长对齐
                    clamped_adj = round(clamped_adj / plugin.step) * plugin.step
                    final_score = reference_score + clamped_adj
                else:
                    # QUALITATIVE_ONLY
                    final_score = raw_score
                    clamped_adj = None

                # 最终得分边界 + 步长约束
                final_score = round(final_score / plugin.step) * plugin.step
                final_score = max(0.0, min(plugin.max_score, final_score))

                # 收集 LLM 返回的额外字段到 details
                details = {k: v for k, v in raw.items() if k not in ("score", "reason")}
                if not details:
                    details = None

                # 更新 result（用 plugin.get_facts 补充 facts）
                facts = plugin.get_facts(context) if hasattr(plugin, "get_facts") else result.facts
                results[dim_id] = ScoringResult(
                    dimension_id=dim_id,
                    name=plugin.name,
                    score=final_score,
                    max_score=plugin.max_score,
                    score_type=plugin.score_type,
                    base_score=result.base_score,
                    penalty_score=result.penalty_score,
                    ai_adjustment=clamped_adj,
                    reason=reason,
                    details=details,
                    facts=facts,
                    error=result.error,
                )

        # 4. 写审计日志
        self._write_audit_log(stock_code, industry_type, results)
        return results

    def _call_llm_batch(
        self,
        stock_code: str,
        industry_type: str,
        ai_entries: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """调用 LLM 并解析返回的分数。"""
        # 当前搜索由数据层 Bing/TopSites 爬虫完成，不再依赖 LLM 内置 web_search
        prompt = PromptBuilder.build(stock_code, industry_type, ai_entries, enable_web_search=False)
        try:
            resp = self.llm.call(prompt, enable_web_search=False)
        except Exception as e:
            print(f"[AiScoringEngine] LLM 调用失败: {e}")
            return self._fallback_scores(ai_entries)

        scores_map = resp.get("scores", {})
        if not isinstance(scores_map, dict):
            print("[AiScoringEngine] LLM 返回格式异常，使用基础分回退")
            return self._fallback_scores(ai_entries)
        return scores_map

    def _fallback_scores(self, ai_entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """LLM 失败时的回退策略。"""
        fallback = {}
        for entry in ai_entries:
            plugin = entry["plugin"]
            base_result = entry.get("base_result")
            if plugin.score_type == ScoringType.AI_BASED and base_result is not None:
                ref = base_result.penalty_score if base_result.penalty_score is not None else base_result.base_score
                fallback[plugin.dimension_id] = {
                    "score": ref,
                    "reason": "LLM调用失败，回退到规则惩罚后的基准分",
                }
            else:
                fallback[plugin.dimension_id] = {
                    "score": 0.0,
                    "reason": "LLM调用失败，完全定性维度无法评分",
                }
        return fallback

    @staticmethod
    def _to_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _facts_hash(self, ai_entries: List[Dict[str, Any]]) -> str:
        """生成事实数据的哈希，用于缓存命中判断。"""
        data = []
        for entry in ai_entries:
            plugin = entry["plugin"]
            facts = entry.get("facts") or {}
            base = entry.get("base_result")
            data.append({
                "dim": plugin.dimension_id,
                "facts": facts,
                "base_score": base.base_score if base else None,
            })
        return hashlib.md5(json.dumps(data, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()

    def _check_cache(
        self,
        stock_code: str,
        industry_type: str,
        ai_entries: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        if self.db is None:
            return None
        facts_hash = self._facts_hash(ai_entries)
        try:
            row = self.db.fetchone(
                """
                SELECT response_json FROM ai_qualitative_cache
                WHERE stock_code = ? AND industry_type = ? AND facts_hash = ?
                AND created_at > datetime('now', '-1 day')
                ORDER BY created_at DESC LIMIT 1
                """,
                (stock_code, industry_type, facts_hash),
            )
            if row and row["response_json"]:
                return json.loads(row["response_json"])
        except Exception as e:
            print(f"[AiScoringEngine] 缓存读取失败: {e}")
        return None

    def _save_cache(
        self,
        stock_code: str,
        industry_type: str,
        ai_entries: List[Dict[str, Any]],
        llm_scores: Dict[str, Dict[str, Any]],
    ):
        if self.db is None:
            return
        facts_hash = self._facts_hash(ai_entries)
        try:
            self.db.execute(
                """
                INSERT INTO ai_qualitative_cache (stock_code, industry_type, facts_hash, response_json, created_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                """,
                (stock_code, industry_type, facts_hash, json.dumps(llm_scores, ensure_ascii=False)),
            )
        except Exception as e:
            print(f"[AiScoringEngine] 缓存写入失败: {e}")

    def _write_audit_log(self, stock_code: str, industry_type: str, results: Dict[str, ScoringResult]):
        if self.db is None:
            return
        for dim_id, res in results.items():
            try:
                self.db.execute(
                    """
                    INSERT INTO ai_qualitative_scores (
                        stock_code, industry_type, dimension_id, dimension_name,
                        score_type, base_score, ai_adjustment, final_score, max_score,
                        reason, details_json, model_name, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    """,
                    (
                        stock_code,
                        industry_type,
                        dim_id,
                        res.name,
                        res.score_type.value,
                        res.base_score,
                        res.ai_adjustment,
                        res.score,
                        res.max_score,
                        res.reason,
                        json.dumps(res.details, ensure_ascii=False) if res.details else None,
                        self.llm.model if self.llm else None,
                    ),
                )
            except Exception as e:
                print(f"[AiScoringEngine] 审计日志写入失败 ({dim_id}): {e}")
