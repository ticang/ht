# -*- coding: utf-8 -*-
"""
obs.py —— langfuse 观测埋点封装（demo 自托管 langfuse v2，对齐生产的埋点结构）

生产用法对照：
  · knowledge_v3.py:64-92：接口层收 langfuse_parent_trace_id /
    langfuse_parent_observation_id 两个 header，走 knowledges_retrieve_with_langfuse
    （knowledge.py:78-92，@observe() 装饰器 + langfuse_context.
    get_current_langchain_handler() 把 langchain 回调挂进链）
  · assistant.py create_stream 链路：config 里的 langfuse handler 跟随 agent
    astream_state 透传（RunConfig 的 callback 通道）
  · 生产的层级：一次问答 = 1 个 trace，分类/检索/重排/生成各为 generation/span

demo 复刻（langfuse SDK v2.60，手动埋点而非 langchain callback——demo 的
LLM/TEI 是手写客户端，没有 langchain callback 挂点，语义等价）：
  · Obs.enabled：.env 未配 LANGFUSE_PUBLIC_KEY 时全部为 no-op（不影响主链路）
  · trace：一次 assistant 问答 / 一次 ragas 评估
  · generation：意图识别（model=Deepseek 分类）、知识检索（含三路召回+RRF+重排
    的命中数/分数）、LLM 生成（prompt/答案/字数）
  · 所有事件带 metadata（type/route/scores 等），UI 上可按 trace 查看
  · flush 在请求结束时统一触发（生产由 SDK 后台批量刷新）
"""
import os
import uuid
from typing import Any, Dict, Optional

import config                                     # noqa: F401（触发 .env 加载在 import 时完成）
from config import check_url


class Obs:
    """langfuse 客户端薄封装。所有方法在未启用/异常时静默降级（生产埋点失败
    也不影响业务主链路，assistant.py 里 langfuse 均为旁路）。"""

    def __init__(self):
        self.client = None
        public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
        host = os.getenv("LANGFUSE_HOST", "http://127.0.0.1:3000")
        if public_key:
            try:
                # host 过出站白名单校验（127.0.0.1 已在默认白名单；与
                # clients.py 的防 SSRF 策略统一）
                check_url(host)
                from langfuse import Langfuse
                # SDK 读 LANGFUSE_PUBLIC_KEY/SECRET_KEY/HOST 三个环境变量
                self.client = Langfuse(host=host)
                print(f"[obs] langfuse 已启用：{host}", flush=True)
            except Exception as e:
                print(f"[obs] langfuse 初始化失败（观测停用，不影响主链路）: {e}",
                      flush=True)
        else:
            print("[obs] 未配置 LANGFUSE_PUBLIC_KEY，观测停用", flush=True)

    # ------------------------------------------------------------------
    # trace：一次完整交互（生产 = 一次 /assistant_run/stream 请求）
    # ------------------------------------------------------------------
    def trace(self, name: str, input: Any, metadata: Dict[str, Any] = None,
              id: Optional[str] = None):
        """开一条 trace。id 用请求的 transaction_id（生产同样以
        transaction_id 兼作 langfuse_parent_id，assistant_run.py:19-20）。
        埋点失败静默降级（生产语义：观测是旁路，不拖垮业务主链路）。"""
        if not self.client:
            return None
        try:
            return self.client.trace(
                id=id or str(uuid.uuid4()), name=name,
                input=input, metadata=metadata or {})
        except Exception as e:
            print(f"[obs] trace 埋点失败（忽略）: {e}", flush=True)
            return None

    # ------------------------------------------------------------------
    # generation：一个模型/检索环节
    # ------------------------------------------------------------------
    @staticmethod
    def generation(trace_obj, name: str, input: Any = None, output: Any = None,
                   model: str = None, metadata: Dict[str, Any] = None):
        """在 trace 下挂一条 generation。
        对照生产的层级：分类（question_classifier 的 LLM 调用）、检索
        （knowledges_retrieve，生产由 langchain handler 自动记录 retriever
        run，demo 手动记录命中/分数）、生成（QA prompt→answer）。"""
        if trace_obj is None:
            return None
        try:
            return trace_obj.generation(
                name=name, input=input, output=output,
                model=model, metadata=metadata or {})
        except Exception as e:
            print(f"[obs] generation 埋点失败（忽略）: {e}", flush=True)
            return None

    def langchain_handler(self, trace_obj):
        """为指定 trace 创建 langchain CallbackHandler —— 生产
        langfuse_context.get_current_langchain_handler()（knowledge.py:84）的
        demo 等价：把 handler 经 callbacks 通道挂进 qa_chain（生产
        qa_chain.acall(callbacks=...)，knowledge.py:113-117），链内各 run
        （SimpleRetrievalQA chain → ContextualCompressionRetriever →
        SimpleHybridRetriever / 嵌入去重 / TEIRerank）自动逐级上报为
        trace 的子节点——这就是 UI 里"检索各阶段"的来源。
        未启用/失败返回 None（主链路照跑，callbacks=None）。"""
        if self.client is None or trace_obj is None:
            return None
        try:
            from langfuse.callback import CallbackHandler
            # stateful_client：把 langchain run 树挂到已有 trace；
            # update_stateful_client：run 树结束时回写 trace 的 output
            return CallbackHandler(stateful_client=trace_obj,
                                   update_stateful_client=True)
        except Exception as e:
            print(f"[obs] CallbackHandler 创建失败（忽略）: {e}", flush=True)
            return None

    def flush(self):
        """批量上报。SDK 默认后台异步刷新，请求结束/进程退出前手动 flush
        保证 demo 的 trace 即刻可见（生产 langfuse.flush 同款语义）。"""
        if self.client:
            try:
                self.client.flush()
            except Exception as e:
                print(f"[obs] flush 失败（忽略）: {e}", flush=True)
