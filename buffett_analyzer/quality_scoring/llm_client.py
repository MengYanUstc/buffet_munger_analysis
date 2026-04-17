"""
LLM 客户端封装
支持 OpenAI 兼容接口（Kimi / OpenAI / DeepSeek 等）
新增：支持 Kimi 内置 $web_search 联网搜索工具
"""

import json
import os
import re
from typing import Dict, Any, List, Optional


class LLMClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("LLM_API_KEY", "")
        self.base_url = base_url or os.getenv("LLM_BASE_URL", "https://api.moonshot.cn/v1")
        self.model = model or os.getenv("LLM_MODEL", "moonshot-v1-32k")
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("使用 LLM 评分需要安装 openai 库: pip install openai")
        self._client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        return self._client

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def call(self, prompt: str, temperature: float = 0.1, max_tokens: int = 4096, enable_web_search: bool = False) -> Dict[str, Any]:
        """
        调用 LLM，可选启用 Kimi 内置 $web_search 工具。
        若启用了 web_search 且模型返回 tool_calls，会自动完成工具调用流程。
        """
        if not self.is_configured():
            raise RuntimeError("LLM API Key 未配置，无法调用 AI 评分。请设置环境变量 LLM_API_KEY")

        client = self._get_client()
        messages = [{"role": "user", "content": prompt}]
        tools = None

        if enable_web_search and "moonshot" in self.base_url.lower():
            tools = [
                {
                    "type": "builtin_function",
                    "function": {"name": "$web_search"},
                }
            ]

        # 第一轮调用：若启用了 web_search，先不加 response_format，避免模型跳过工具调用
        first_resp = client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            tools=tools,
            response_format=None if tools else {"type": "json_object"},
        )
        choice = first_resp.choices[0]

        # 如果模型要求调用工具（且是 $web_search），原封不动把参数传回去
        if choice.finish_reason == "tool_calls" and tools:
            tool_calls = choice.message.tool_calls
            messages.append({
                "role": "assistant",
                "content": choice.message.content or "",
                "tool_calls": [tc.model_dump() for tc in tool_calls],
            })
            for tc in tool_calls:
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tc.function.arguments,  # 对 builtin_function 原封不动返回
                })

            # 第二轮调用：强制 JSON 输出
            second_resp = client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
                tools=tools,
            )
            choice = second_resp.choices[0]

        content = choice.message.content or "{}"
        return self._parse_json(content)

    @staticmethod
    def _parse_json(content: str) -> Dict[str, Any]:
        """尝试解析 JSON，若失败则尝试从 Markdown 代码块中提取。"""
        content = content.strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        match = re.search(r"```(?:json)?\s*(.*?)\s*```", content, re.DOTALL | re.IGNORECASE)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        raise ValueError(f"LLM 返回内容无法解析为 JSON: {content[:200]}")
