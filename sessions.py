# -*- coding: utf-8 -*-
"""
sessions.py —— 多轮会话内存存储 + 追问改写（api_server 会话层的最小实现）

生产对照：agent_core 的 thread 历史 / langgraph checkpoint（持久化会话）；
demo 用进程内 dict + 最近 N 轮 + TTL 懒清理——单 worker 前提（uvicorn 默认
单进程，多 worker/重启会丢会话，生产接 redis/DB 同接口替换本类即可）。

查询改写（生产在 agent 的历史组装里完成，demo 独立成函数）：
有历史时先把追问改写成不依赖上下文的独立问题
（"那它的取值范围呢" → "PDCP丢弃定时器的取值范围是多少？"），
改写后的问题用于【分类 + 检索】，原始问题原样展示给用户——检索层
（rag_demo）保持无状态，会话是 HTTP 层概念，与分层一致。
"""
import time
from typing import Dict, List, Optional, Tuple

REWRITE_PROMPT = """你是问答系统的查询改写器。用户正在进行多轮问答，当前问题可能存在指代或省略。
请把【当前问题】改写成一个不依赖上下文、语义完整的独立问题；如果当前问题本身已经完整，原样返回。
只输出改写后的问题本身，不要任何解释、引号或前缀。

最近对话：
{history}

当前问题：{question}

改写后的独立问题："""


class SessionStore:
    """thread_id → 最近 N 轮 (question, answer)。访问时懒清理过期/超量会话。"""

    def __init__(self, max_turns: int = 6, ttl_s: int = 3600,
                 max_threads: int = 1000):
        self.max_turns = max_turns
        self.ttl_s = ttl_s
        self.max_threads = max_threads
        self._store: Dict[str, Tuple[float, List[Tuple[str, str]]]] = {}

    def history(self, thread_id: str) -> List[Tuple[str, str]]:
        """取该会话的最近 N 轮（不含超出部分）；顺带做一次过期/超量清理。"""
        self._cleanup()
        entry = self._store.get(thread_id)
        return list(entry[1]) if entry else []

    def append(self, thread_id: str, question: str, answer: str):
        """记一轮问答（只留最近 max_turns 轮——上下文膨胀控制）。"""
        turns = self._store.get(thread_id, (0, []))[1]
        turns.append((question, answer))
        self._store[thread_id] = (time.time(), turns[-self.max_turns:])

    def _cleanup(self):
        """懒清理：删过期会话；超量时按最近活跃淘汰最旧的。"""
        now = time.time()
        expired = [k for k, (ts, _) in self._store.items()
                   if now - ts > self.ttl_s]
        for k in expired:
            del self._store[k]
        if len(self._store) > self.max_threads:
            oldest = sorted(self._store, key=lambda k: self._store[k][0])
            for k in oldest[:len(self._store) - self.max_threads]:
                del self._store[k]


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
