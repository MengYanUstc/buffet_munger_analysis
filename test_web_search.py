from openai import OpenAI
import os

api_key = os.getenv("LLM_API_KEY")
base_url = os.getenv("LLM_BASE_URL", "https://api.moonshot.cn/v1")
model = os.getenv("LLM_MODEL", "moonshot-v1-32k")

client = OpenAI(api_key=api_key, base_url=base_url)
prompt = "请查询美的集团（000333）近年的分红政策和股息率，并返回简洁JSON：{\"dividend_policy\": \"...\", \"payout_ratio\": \"...\"}。如果数据不足请调用 $web_search 搜索补充。"

messages = [{"role": "user", "content": prompt}]
tools = [{"type": "builtin_function", "function": {"name": "$web_search"}}]

print("=== first call ===")
resp1 = client.chat.completions.create(
    model=model,
    messages=messages,
    temperature=0.1,
    max_tokens=4096,
    tools=tools,
)
c1 = resp1.choices[0]
print("finish_reason:", c1.finish_reason)
print("content:", c1.message.content)

if c1.finish_reason == "tool_calls":
    print("tool_calls triggered!")
    messages.append({
        "role": "assistant",
        "content": c1.message.content or "",
        "tool_calls": [tc.model_dump() for tc in c1.message.tool_calls],
    })
    for tc in c1.message.tool_calls:
        print("  tool:", tc.function.name)
        print("  args len:", len(tc.function.arguments))
        messages.append({
            "role": "tool",
            "tool_call_id": tc.id,
            "content": tc.function.arguments,
        })

    print("=== second call ===")
    resp2 = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
        max_tokens=4096,
        response_format={"type": "json_object"},
        tools=tools,
    )
    c2 = resp2.choices[0]
    print("finish_reason:", c2.finish_reason)
    print("content:", c2.message.content)
