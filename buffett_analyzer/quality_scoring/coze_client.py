"""
Coze LLM 客户端封装
适配 Coze SSE 流式接口，用于纯定性评分。
接口与 LLMClient 对齐，可无缝替换。
"""

import json
import os
import re
from typing import Dict, Any, Optional

import requests


class CozeLLMClient:
    """
    Coze API 客户端。

    与 LLMClient 保持接口兼容：
      - call(prompt, ...) -> Dict[str, Any]

    差异点：
      - 使用 requests POST + SSE 流解析（非 OpenAI SDK）
      - 返回的文本在 SSE event 的 content.answer 字段中
      - 自动拼接流式分片并提取 JSON
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        base_url: Optional[str] = None,
        project_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ):
        self.api_token = api_token or os.getenv("COZE_API_TOKEN", "")
        self.base_url = base_url or os.getenv("COZE_BASE_URL", "https://6n7dqg7m2x.coze.site/stream_run")
        self.project_id = project_id or os.getenv("COZE_PROJECT_ID", "7630103598810136595")
        self.session_id = session_id or os.getenv("COZE_SESSION_ID", "YqOYJpGsWVZYJTY0dN1-4")

    def is_configured(self) -> bool:
        return bool(self.api_token)

    def call(
        self,
        prompt: str,
        temperature: float = 0.1,
        max_tokens: int = 4096,
        enable_web_search: bool = False,
        timeout: int = 600,
    ) -> Dict[str, Any]:
        """
        调用 Coze LLM，返回解析后的 JSON dict。

        Args:
            prompt: 完整的用户提示文本
            temperature: 已废弃（Coze 暂不支持流式参数透传，保留以兼容接口）
            max_tokens: 已废弃（同上）
            enable_web_search: 已废弃（Coze 的搜索行为由 Bot 配置决定）
            timeout: HTTP 请求超时秒数

        Returns:
            从 LLM 回复中提取的 JSON 字典
        """
        if not self.is_configured():
            raise RuntimeError(
                "Coze API Token 未配置，无法调用 AI 评分。"
                "请设置环境变量 COZE_API_TOKEN 或在初始化时传入 api_token"
            )

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

        payload = {
            "content": {
                "query": {
                    "prompt": [
                        {
                            "type": "text",
                            "content": {"text": prompt},
                        }
                    ]
                }
            },
            "type": "query",
            "session_id": self.session_id,
            "project_id": self.project_id,
        }

        resp = requests.post(self.base_url, headers=headers, json=payload, stream=True, timeout=timeout)

        if resp.status_code != 200:
            raise RuntimeError(f"Coze API 请求失败: HTTP {resp.status_code}, {resp.text[:500]}")

        # 解析 SSE 流，收集所有 answer 分片
        # 新 Agent 事件流：message_start -> tool_request -> tool_response -> ... -> message_end
        # 最终答案在 message_end 事件的 content.answer 中
        answer_parts = []
        for line in resp.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data:"):
                continue
            data_text = line[5:].strip()
            if not data_text:
                continue
            try:
                parsed = json.loads(data_text)
            except json.JSONDecodeError:
                continue

            ev_type = parsed.get("type", "")
            if ev_type not in ("answer", "message_end"):
                continue

            content = parsed.get("content", {})
            if not isinstance(content, dict):
                continue

            # Coze SSE 中，文本内容在 content.answer 字段
            text_chunk = content.get("answer") or content.get("text") or ""
            if text_chunk:
                answer_parts.append(text_chunk)

        full_text = "".join(answer_parts)
        if not full_text.strip():
            raise RuntimeError("Coze LLM 返回空内容，无法解析评分结果")

        return self._parse_json(full_text)

    @staticmethod
    def _parse_json(text: str) -> Dict[str, Any]:
        """
        从 LLM 返回的文本中提取 JSON 对象。
        支持以下格式：
          1. 纯 JSON 文本
          2. Markdown 代码块 ```json {...} ```
          3. 混合文本（从第一个 { 到最后一个 } 提取）
        """
        text = text.strip()

        # 尝试 1：直接解析
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 尝试 2：Markdown 代码块
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # 尝试 3：从文本中提取最外层 JSON 对象
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                pass

        raise ValueError(f"无法从 LLM 回复中提取有效 JSON: {text[:300]}")
