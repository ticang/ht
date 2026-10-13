# -*- coding: utf-8 -*-
"""
clients.py —— 三个外部服务的轻量 HTTP 客户端（全部走 config.check_url 白名单校验）

对应真实项目 aichain/embeddings/std_client.py 的 EmbeddingsClient：
生产环境用一台 EMBEDDING_SERVER（bge-m3）同时提供
  /api/v1/embeddings（嵌入）和 /api/v1/rerank（重排）；
本 demo 用 docker TEI 双服务拆开承担：
  tei-embedding : BAAI/bge-m3            → POST /embed   （1024维向量）
  tei-reranker  : BAAI/bge-reranker-v2-m3 → POST /rerank  （真交叉编码打分）
LLM 则对应生产的 get_llm_with_db()，此处为微信 OpenAI 兼容接口。

手写 requests 客户端而非引入 langchain-openai/sentence-transformers：
与真实项目 std_client.py 手写 HTTP 客户端的风格一致，依赖最少。
"""
import os
import re
import time
from typing import List

import requests

import config
from config import check_url

# langfuse 观测单例（obs 模块提供，未启用时全部 no-op）——嵌入/重排/LLM
# 三客户端的模型调用经 generation_call 自动挂到当前请求的 trace（生产由
# langchain callback handler 记录 langchain 模型调用，demo 的手写客户端手动上报）
from obs import obs                              # noqa: F401（埋点用）

# 模块级会话：连接复用 + 统一走 NO_PROXY（config.py 已设置）
_SESSION = requests.Session()


def _post_with_retry(url: str, *, retryable: tuple = (requests.ConnectionError,
                                                      requests.Timeout),
                     retries: int = 2, wait: float = 2.0, **kwargs) -> requests.Response:
    """POST + 连接类错误重试。微信 LLM / TEI 偶发 RemoteDisconnected /
    超时（长评估跑几十次调用必现），重试 2 次、间隔 2s；非连接类错误
    （4xx/5xx 响应）不重试直接返回由调用方处理。
    发请求前过 check_url 白名单校验（仅 http/https + host 白名单，与环境
    约束"发请求前校验 host"一致；URL 均来自 .env 配置，非用户输入）。"""
    url = check_url(url)                              # 每次出站前校验协议与 host
    last_err = None
    for attempt in range(retries + 1):
        try:
            return _SESSION.post(url, **kwargs)
        except retryable as e:
            last_err = e
            if attempt < retries:
                print(f"[http] {url.rsplit('/', 1)[-1]} 连接失败"
                      f"（{type(e).__name__}），{wait}s 后重试 "
                      f"{attempt + 1}/{retries}", flush=True)
                time.sleep(wait)
    raise last_err


# ===========================================================================
# TEI 嵌入客户端 —— 对应 std_client.py 的 embed/aembed（模型 bge-m3, 1024维）
# ===========================================================================
class TEIEmbedClient:
    """POST /embed  {"inputs": [文本...]} → [[1024个float], ...]

    用途两处：
      ① 入库时 embed_documents 批量算向量（对应 ES add_texts 的嵌入步骤）
      ② 检索时 embed_query 算查询向量（对应 hybrid_search 向量路）
      ③ 嵌入去重器 SimpleRedundantFilter 也复用它做两两余弦

    TEI 批量上限（超限返回 413 Payload Too Large）：
      · max_batch_tokens——整批 token 总数上限。本部署 docker compose
        显式设为 2048（远小于 TEI 官方默认 16384），400 字的子块五六个
        就能击穿，整文件一次性 POST 必 413
      · max_client_batch_size——整批条数上限（32）
    客户端按「条数 × 估算 token」双约束自动切批后逐批 POST，并对 413
    做减半自适应重试——与 TEI 实际启动参数解耦，换部署不用改代码。
    """

    # 内置保守默认（对应本部署 TEI 参数；/info 探测失败时兜底）。
    # 取小值只会多批几次、不会 413；TEI 官方默认是 16384/32。
    DEFAULT_MAX_BATCH_TOKENS = 2048
    DEFAULT_MAX_CLIENT_BATCH_SIZE = 32

    def __init__(self, base_url: str, timeout: int = 60):
        self.base_url = check_url(base_url.rstrip("/"))   # 初始化即校验白名单
        self.timeout = timeout
        # 上限取值优先级：env 显式配置 > TEI /info 实测 > 内置保守默认
        self.max_batch_tokens = int(os.getenv("TEI_MAX_BATCH_TOKENS", 0)) or None
        self.max_client_batch_size = int(os.getenv("TEI_MAX_CLIENT_BATCH_SIZE", 0)) or None
        self._probe_limits()
        self.max_batch_tokens = self.max_batch_tokens or self.DEFAULT_MAX_BATCH_TOKENS
        self.max_client_batch_size = \
            self.max_client_batch_size or self.DEFAULT_MAX_CLIENT_BATCH_SIZE
        print(f"[TEI:embed] 批量上限：max_batch_tokens={self.max_batch_tokens}，"
              f"max_client_batch_size={self.max_client_batch_size}", flush=True)

    def _probe_limits(self):
        """GET /info 读服务端真实批量上限（TEI ≥1.2 暴露这两个字段）。
        各部署 TEI 启动参数不同（--max-batch-tokens/--max-client-batch-size），
        按服务端实际上限切批最稳；探测失败（服务未起/老版本无字段）静默
        退回 env 或内置默认，不影响主流程。"""
        try:
            resp = _SESSION.get(check_url(f"{self.base_url}/info"), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if self.max_batch_tokens is None:
                    self.max_batch_tokens = int(data.get("max_batch_tokens") or 0) or None
                if self.max_client_batch_size is None:
                    self.max_client_batch_size = \
                        int(data.get("max_client_batch_size") or 0) or None
        except Exception:
            pass

    @staticmethod
    def _est_tokens(text: str) -> int:
        """无本地 tokenizer 时的保守 token 估算：bge-m3 的 sentencepiece
        对中文≈1字1token、英文≈0.75token/字符，直接取字符数即不低估
        （估大只会让批次更小，是安全方向）。"""
        return max(1, len(text))

    def _batch(self, texts: List[str]):
        """按「条数上限 × 估算 token 上限」双约束把文本列表切批（生成器）。
        单条就超 token 上限时自成一批，交给 _embed_batch 报清晰错误。"""
        cap_tokens = max(256, int(self.max_batch_tokens * 0.9))   # 留10%估算余量
        cap_count = max(1, self.max_client_batch_size)
        batch: List[str] = []
        used = 0
        for t in texts:
            est = self._est_tokens(t)
            if batch and (used + est > cap_tokens or len(batch) >= cap_count):
                yield batch
                batch, used = [], 0
            batch.append(t)
            used += est
        if batch:
            yield batch

    def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        """单批 POST；413（批量超限）时减半重试——/info 探测缺失或估算
        偏差（生僻字/多语言混合）导致单批超限的自愈兜底；单条仍超限则
        抛出带处理建议的明确错误（而非裸 413）。"""
        resp = _post_with_retry(f"{self.base_url}/embed",
                                json={"inputs": texts}, timeout=self.timeout)
        if resp.status_code == 413:
            if len(texts) > 1:
                mid = len(texts) // 2
                print(f"[TEI:embed] 413 批量超限，减半重试（{len(texts)}→"
                      f"{mid}+{len(texts) - mid}）", flush=True)
                return (self._embed_batch(texts[:mid])
                        + self._embed_batch(texts[mid:]))
            raise RuntimeError(
                f"单条文本（{len(texts[0])} 字，估算 {self._est_tokens(texts[0])} "
                f"tokens）超过 TEI 批量上限 {self.max_batch_tokens}：请调大 TEI "
                f"--max-batch-tokens 或减小切分尺寸（OPSGPT_PARENT_CHUNK_SIZE）")
        resp.raise_for_status()
        return resp.json()

    def embed(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入（内部自动切批，返回与输入等长同序的向量列表）。
        TEI 返回与输入等长的向量列表（bge-m3 → 1024维）。
        空列表直接返回（TEI 对空批会 413）。"""
        if not texts:
            return []
        t0 = time.perf_counter()
        longest = max(len(t) for t in texts)
        total_chars = sum(len(t) for t in texts)
        batches = list(self._batch(texts))
        vectors: List[List[float]] = []
        for i, batch in enumerate(batches, 1):
            est = sum(self._est_tokens(t) for t in batch)
            print(f"[TEI:embed] → 批次 {i}/{len(batches)}：{len(batch)} 条"
                  f"（估算 {est} tokens，上限 {self.max_batch_tokens}）", flush=True)
            vectors.extend(self._embed_batch(batch))
        dt = time.perf_counter() - t0
        print(f"[TEI:embed] ← 共 {len(vectors)}×{len(vectors[0])} 维"
              f"（{total_chars} 字，最长 {longest} 字，{len(batches)} 批），"
              f"耗时 {dt:.2f}s", flush=True)
        # langfuse：嵌入模型调用（生产 embedding 调用同被记录）
        obs.generation_call(name="embedding", model="bge-m3",
                            input={"texts": len(texts), "chars": total_chars},
                            output={"dims": len(vectors[0]) if vectors else 0,
                                    "count": len(vectors), "batches": len(batches)},
                            latency_s=dt)
        return vectors

    def embed_query(self, text: str) -> List[float]:
        """单条嵌入（查询侧）。"""
        return self.embed([text])[0]


# ===========================================================================
# TEI 重排客户端 —— 对应 CrossReranker → std_client.rerank/arerank
#（生产是 bge-m3 的 rerank 端点；demo 用专门的交叉编码器 bge-reranker-v2-m3）
# ===========================================================================
class TEIRerankClient:
    """POST /rerank {"query": q, "texts": [文档...]} → [{"index": i, "score": s}, ...]

    返回按相关性降序的 [{index(原列表下标), score(浮点)}]，
    分数是真正的交叉编码相关性（query 与文档逐token交互后回归出的标量），
    不是余弦、不是概率——与生产的 bge-m3 重排分同性质。
    """

    def __init__(self, base_url: str, timeout: int = 120):
        # 重排在CPU上逐对推理较慢（~几十毫秒/条），timeout 放宽
        self.base_url = check_url(base_url.rstrip("/"))
        self.timeout = timeout

    def rerank(self, query: str, texts: List[str]) -> List[dict]:
        if not texts:
            return []
        t0 = time.perf_counter()
        print(f"[TEI:rerank] → query=\"{query[:24]}\" × {len(texts)} 条候选", flush=True)
        resp = _post_with_retry(f"{self.base_url}/rerank",
                                json={"query": query, "texts": texts,
                                      "return_text": False},
                                timeout=self.timeout)
        resp.raise_for_status()
        ranked = resp.json()        # [{"index": int, "score": float}, ...] 已降序
        top1 = f"{ranked[0]['score']:.4f}" if ranked else "-"
        dt = time.perf_counter() - t0
        print(f"[TEI:rerank] ← {len(ranked)} 个分数，top1={top1}，耗时 {dt:.2f}s", flush=True)
        # langfuse：重排模型调用（分数列表上报，生产 rerank 同被记录）
        obs.generation_call(name="reranker", model="bge-reranker-v2-m3",
                            input={"query": query[:60], "candidates": len(texts)},
                            output=[round(r["score"], 4) for r in ranked[:6]],
                            latency_s=dt)
        return ranked


# ===========================================================================
# 微信 LLM 客户端 —— 对应生产的 get_llm_with_db()（OpenAI 兼容 chat/completions）
# ===========================================================================
class WeixinLLM:
    """POST {base}/chat/completions，Bearer 鉴权，模型 Deepseek-v4-flash。

    接口形态与 OpenAI 完全兼容：
      {"model": ..., "messages": [{"role": "user", "content": prompt}], ...}
      → {"choices": [{"message": {"content": 答案}}], ...}
    只实现 demo 需要的 ainvoke(prompt)→str 一个方法；
    温度取0保证回答稳定（生产 QA 生成同样是低温）。
    """

    def __init__(self, timeout: int = 120):
        self.base_url = check_url(config.LLM_BASE_URL.rstrip("/"))
        self.api_key = config.LLM_API_KEY
        self.model = config.LLM_MODEL
        self.timeout = timeout

    def chat(self, prompt: str, temperature: float = 0.0) -> str:
        """同步生成。失败时抛 RuntimeError 并带上响应片段，便于排查
        （key 失效/过期会在这里暴露为 401）。"""
        t0 = time.perf_counter()
        print(f"[LLM] → {self.model}，prompt {len(prompt)} 字，temp={temperature}", flush=True)
        resp = _post_with_retry(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}",
                     "Content-Type": "application/json"},
            json={"model": self.model,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": temperature},
            timeout=self.timeout,
        )
        dt = time.perf_counter() - t0
        if resp.status_code != 200:
            print(f"[LLM] ← HTTP {resp.status_code}，耗时 {dt:.1f}s", flush=True)
            raise RuntimeError(
                f"LLM 调用失败 HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as e:
            raise RuntimeError(f"LLM 返回结构异常: {str(data)[:300]}") from e
        # 剥掉推理型模型可能输出的 <think>...</think> 段（生产 _parse_llm_response 同款处理）
        cleaned = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
        # 推理型模型 <think> 段可能很长——原始/净化长度差即可看出思考占了多少
        print(f"[LLM] ← 原始 {len(content)} 字（含思考）/ 净化后 {len(cleaned)} 字，"
              f"耗时 {dt:.1f}s", flush=True)
        # langfuse：LLM 生成调用（生产 langchain LLM 由 callback 自动记录，
        # demo 手写客户端手动上报；只记规模不记全文，避免 trace 膨胀）
        obs.generation_call(name="llm_chat", model=self.model,
                            input={"prompt_chars": len(prompt),
                                   "temperature": temperature},
                            output={"chars": len(cleaned),
                                    "think_chars": len(content) - len(cleaned)},
                            latency_s=dt)
        return cleaned

    async def ainvoke(self, prompt: str, **kwargs) -> str:
        """异步入口（demo 的链路是 async 的）。CPU/网络阻塞在 requests 上，
        demo 规模可接受；生产是 aiohttp 真异步（std_client.arerank 同理）。"""
        import asyncio
        import functools
        return await asyncio.get_event_loop().run_in_executor(
            None, functools.partial(self.chat, prompt))


# ===========================================================================
# 工厂：主程序统一从这里拿三个客户端（单例语义，避免重复建连）
# ===========================================================================
def build_clients():
    """返回 (embed_client, rerank_client, llm)。"""
    return (TEIEmbedClient(config.TEI_EMBED_URL),
            TEIRerankClient(config.TEI_RERANK_URL),
            WeixinLLM())
