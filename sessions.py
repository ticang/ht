# -*- coding: utf-8 -*-
"""
sessions.py —— 多轮会话的追问改写器（Context Builder 的 LLM 侧）

v4 时本文件还有个进程内 SessionStore（dict 存 thread 历史，重启即丢）；
v5 起 ES-only 架构把会话持久化挪进 es_persist.ChatStore（raglab_chat_messages
索引，重启/多 worker 不丢，由 graph.load_context 读取）——本文件只保留
查询改写函数。

查询改写（生产在 agent 的历史组装里完成，demo 独立成函数）：
有历史时先把追问改写成不依赖上下文的独立问题
（"那它的取值范围呢" → "PDCP丢弃定时器的取值范围是多少？"），
改写后的问题用于【分类 + 检索 + MCP】，原始问题原样展示给用户并落历史。
"""
from typing import List, Tuple

REWRITE_PROMPT = """你是问答系统的查询改写器。用户正在进行多轮问答，当前问题可能存在指代或省略。
请把【当前问题】改写成一个不依赖上下文、语义完整的独立问题；如果当前问题本身已经完整，原样返回。
只输出改写后的问题本身，不要任何解释、引号或前缀。

最近对话：
{history}

当前问题：{question}

改写后的独立问题："""


async def rewrite_query(llm, question: str,
                        history: List[Tuple[str, str]]) -> str:
    """结合最近历史把追问改写成独立问题；无历史/失败/输出异常时原样返回。

    改写失败不阻断主链路（降级即用原问题——追问检索质量会差但服务可用，
    与 demo 各层的降级语义一致）。"""
    if not history:
        return question
    hist = "\n".join(f"用户：{q}\n助手：{a[:200]}"
                     for q, a in history[-3:])          # 最近 3 轮足够消解指代
    try:
        rewritten = await llm.ainvoke(REWRITE_PROMPT.format(
            history=hist, question=question))
        rewritten = str(rewritten).strip().strip('"“”').strip()
        # 防模型输出带了前缀/复读问题本身：太短、太长或与原问题相同都回退
        if question.strip() != rewritten and 3 < len(rewritten) <= 200:
            print(f"  [改写] {question} → {rewritten}", flush=True)
            return rewritten
    except Exception as e:
        print(f"  [改写] 失败，回退原问题: {e}", flush=True)
    return question
