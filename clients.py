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
import re
import time
from typing import List

import requests

import config
from config import check_url

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
    """

    def __init__(self, base_url: str, timeout: int = 60):
        self.base_url = check_url(base_url.rstrip("/"))   # 初始化即校验白名单
        self.timeout = timeout

    def embed(self, texts: List[str]) -> List[List[float]]:
        """批量嵌入。TEI 返回与输入等长的向量列表（bge-m3 → 1024维）。
        空列表直接返回（TEI 对空批会 413）。"""
        if not texts:
            return []
        t0 = time.perf_counter()
        longest = max((len(t) for t in texts), default=0)
        print(f"[TEI:embed] → {len(texts)} 条文本（最长 {longest} 字）", flush=True)
        resp = _post_with_retry(f"{self.base_url}/embed",
                                json={"inputs": texts}, timeout=self.timeout)
        resp.raise_for_status()
        vectors = resp.json()
        dt = time.perf_counter() - t0
        print(f"[TEI:embed] ← {len(vectors)}×{len(vectors[0])} 维，耗时 {dt:.2f}s", flush=True)
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
