"""A customer-support agent: Claude answering from the platform's real-time context over MCP."""

from __future__ import annotations

import anthropic
import httpx2
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from .platforms.base import Platform

MODEL = "claude-opus-5-5"

SYSTEM = """You are the support assistant of an online shop. Answer questions about customers, \
orders and stock using the real-time context tools: list the available tables, read a table's \
metadata before querying it, then query it. The data is live: it reflects changes made seconds ago.

The query tool runs simple filtered lookups on one table at a time. It does not aggregate or join, \
so fetch the rows you need and do any counting or arithmetic yourself. Keep row limits small.

Answer concisely. Give the numbers you used and say which table they came from. If the tools \
cannot answer the question, say so instead of guessing."""


async def ask(platform: Platform, question: str, log=print) -> str:
    endpoint = platform.mcp()
    client = anthropic.AsyncAnthropic()
    async with httpx2.AsyncClient(headers=endpoint.headers, timeout=60) as http:
        async with streamable_http_client(endpoint.url, http_client=http) as (read, write, *_):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                log(f"[agent] {len(tools)} context tools: {', '.join(t.name for t in tools)}")
                runner = client.beta.messages.tool_runner(
                    model=MODEL,
                    max_tokens=16000,
                    system=SYSTEM,
                    output_config={"effort": "medium"},
                    # Server-side fallback on a refusal, routed by refusal category.
                    betas=["server-side-fallback-2026-07-01"],
                    fallbacks="default",
                    tools=[async_mcp_tool(t, session) for t in tools],
                    messages=[{"role": "user", "content": question}],
                )
                final = None
                async for message in runner:
                    final = message
                    for block in message.content:
                        if block.type == "tool_use":
                            log(f"[agent] {block.name} {_compact(block.input)}")
    if final is None:
        return "(no answer)"
    if final.stop_reason == "refusal":
        return "The request was declined."
    return "".join(block.text for block in final.content if block.type == "text").strip()


def _compact(value: object) -> str:
    text = str(value)
    return text if len(text) <= 160 else text[:157] + "..."
