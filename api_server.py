# -*- coding: utf-8 -*-
"""
api_server.py —— FastAPI 服务版（v5：网关 + LangGraph 编排入口）

架构位置（"ES-only + LangGraph + LangChain + MCP"五模块）：
  【会话层/网关】鉴权（X-API-Key）+ 限流（RPM 滑窗）+ 幂等键
  （Idempotency-Key 重放）+ 会话管理（thread_id → ES 聊天历史）
  【编排层】assistant_stream → graph.ainvoke（graph.py 的 StateGraph 五路分流）
  【数据层】聊天/任务/反馈/checkpoint 全落 ES（es_persist/es_checkpointer）

生产对照：
  ① aiops-agent-core/backend/agent_core/api/v2/assistant_run.py:32-48
     POST /stream（create_stream）→ AssistantService.stream（assistant.py:1490-1618）
       · 请求体 CreateAssitantRunPayload：question / assistant_uuid / transaction_id
       · header：langfuse_parent_trace_id / langfuse_parent_observation_id
         （transaction_id 存在且无 question 时兼作 parent id，assistant_run.py:19-20）
       · 响应：EventSourceResponse(to_sse(...)) —— SSE 流
  ② aiops-chatops/server/api/v3/knowledge_v3.py:64-145
     POST /v3/knowledges/retrieve（knowledges_retrieve）
       · 请求体：query / knowledges / search_strategy / max_recalls / min_score /
         only_retrieval
       · 响应：{code:200, msg:"success", data:{result, source_documents:[...]}}

SSE 事件格式（复刻 agent_core/utils/stream.py:152-215 to_sse +
assistant.py:2329-2403 _stream_preset_response 的消息序列）：
  ① event: metadata  data: {"run_id": ...}            ← 先产 run_id（:2377）
  ② event: data      data: [HumanMessage]             ← 用户问题（:2380-2395，
     response_metadata 带 start_time/model_name/transaction_id/thread_id 等）
  ③ event: data      data: [ToolMessage]              ← 知识引用（kb_empty 标记，
     to_sse:186-193 靠它统计知识命中）
  ④ event: data      data: [AIMessage, AIMessage...]  ← 回答（生产为 token 级流，
     demo 的 LLM 客户端非流式，按 ~40 字切片产生同 id 的消息块模拟流式）
  ⑤ 异常 → event: error（to_sse:204-215）

v5 与 v4 的差异：分流逻辑从本文件内联 if/else 移入 graph.py（行为等价 +
hybrid 并行分支）；会话历史/幂等/任务/反馈/checkpoint 全部落 ES（重启不丢）。

启动：
    .venv/Scripts/python api_server.py            # 索引不存在时自动入库样例
    # 或先 python rag_demo.py --reset 入库再起服务
验证：
    curl -N -X POST http://127.0.0.1:8000/api/v2/assistant_run/stream \
         -H "Content-Type: application/json" \
         -d '{"question":"PDCP丢弃定时器的默认值是多少？"}'
    curl -X POST http://127.0.0.1:8000/api/v3/knowledges/retrieve \
         -H "Content-Type: application/json" \
         -d '{"query":"A3事件偏置","knowledges":["kb-demo"],"only_retrieval":true}'
"""
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
from fastapi import (BackgroundTasks, Depends, FastAPI, File, Form, Header,
                     HTTPException, Request, UploadFile)
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

# 复用 rag_demo 的检索组件（模块级共享同一套实现）；编排与持久化见各专责模块
from rag_demo import RagComponents, knowledges_retrieve, make_sample_files
from classifier import QuestionClassifier
from clients import build_clients, probe_health as clients_probe
from es_checkpointer import EsCheckpointer
from es_persist import ChatStore, EsTaskRegistry, FeedbackStore
from es_store import ESStore
from graph import ask_graph, build_graph
from ingest_service import IngestService
from mcp_client import McpTicketClient
from obs import Obs

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:                              # pragma: no cover（非标准流）
    pass

KB_ID = "kb-demo"

# ===========================================================================
# 应用与全局单例（生产：每次请求 ChainFactory 组装；客户端/存储为进程级单例）
# ===========================================================================
app = FastAPI(title="rag-lab api", version="5")

embedder, reranker, llm = build_clients()
store = ESStore(embedder)
rag = RagComponents(store, embedder, reranker, llm, top_k=4)
classifier = QuestionClassifier(llm)
mcp = McpTicketClient()
obs = Obs()
# ES-only 持久化（v5）：聊天历史 / 任务 / 反馈 / checkpoint——重启/多 worker 不丢。
# 四个存储共用 make_es_client 出的连接配置（check_url 白名单钉在唯一出口）
chat_store = ChatStore()
feedback_store = FeedbackStore()
checkpointer = EsCheckpointer()
ingest_tasks = EsTaskRegistry()
# LangGraph 编排（一张图全路由；checkpointer 挂自研 ES 版，thread_id 即会话键）。
# 图是进程级单例：节点都是无状态薄壳（检索/分类组件闭包引用），并发请求
# 靠各自的 state + configurable.thread_id 隔离，不共享可变状态
graph = build_graph(classifier, rag, mcp, llm, chat_store,
                    checkpointer=checkpointer, knowledges=[KB_ID])


# ===========================================================================
# 服务防护：鉴权 + 限流（demo 默认全关，.env 配 RAGLAB_API_KEYS /
# RAGLAB_RATE_LIMIT_RPM 开启；生产的租户/ACL 体系这里是单 key 最小实现）
# ===========================================================================
def require_api_key(x_api_key: Optional[str] = Header(default=None,
                                                      alias="X-API-Key")):
    """X-API-Key 校验；未配置 key 集合时直通（demo 调试模式）。"""
    if config.RAGLAB_API_KEYS and x_api_key not in config.RAGLAB_API_KEYS:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


_rate_hits: Dict[str, List[float]] = {}          # key → 最近 60s 命中时刻


def rate_limit(request: Request,
               x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    """滑动窗口限流：每 API key（未鉴权按客户端 IP）每分钟 N 次（0=不限）。"""
    limit = config.RAGLAB_RATE_LIMIT_RPM
    if limit <= 0:
        return
    who = x_api_key or (request.client.host if request.client else "unknown")
    now = time.time()
    hits = [t for t in _rate_hits.get(who, []) if now - t < 60]
    if len(hits) >= limit:
        raise HTTPException(status_code=429,
                            detail=f"rate limit exceeded ({limit} req/min)")
    hits.append(now)
    _rate_hits[who] = hits


GUARDS = [Depends(require_api_key), Depends(rate_limit)]


@app.on_event("startup")
async def startup_ingest():
    """索引不存在时自动入库五类样例（幂等；生产入库走 celery 任务，
    demo 在服务启动时完成，等价保证"检索接口可用"的前置条件）。
    v5：同时确保 ES-only 持久化四索引（聊天/任务/反馈/checkpoint）存在。"""
    chat_store.ensure_index()
    ingest_tasks.ensure_index()
    feedback_store.ensure_index()
    checkpointer.ensure_index()
    if not store.client.indices.exists(index=store.index_name):
        print("[startup] 索引不存在，自动入库样例…", flush=True)
        store.ensure_index(reset=False)
        ingest = IngestService(store)
        for chain, path in make_sample_files():
            if chain == "knowledge_faq":
                ingest.faq_batch_ingest(str(path), column_question=1,
                                        column_answer=2, knowledge_base_id=KB_ID)
            else:
                ingest.file_ingest(chain, str(path), knowledge_base_id=KB_ID)


# ===========================================================================
# 健康检查 —— GET /healthz（无鉴权/限流，供探针与负载均衡）
# ===========================================================================
@app.get("/healthz")
async def healthz():
    """组件级健康探测：ES 是核心（挂 = 503 摘流量）；TEI 挂 = 200 degraded
    （检索可部分降级而非整个服务不可用，生产同款语义）。
    探测 URL 来自 .env 配置（非用户输入），统一走 clients.probe_health
    （内部过 check_url 白名单 + 复用模块会话，与全项目出站约定一致）。"""
    es_ok = await asyncio.to_thread(store.client.ping)
    embed_ok = await asyncio.to_thread(clients_probe, f"{config.TEI_EMBED_URL}/health")
    rerank_ok = await asyncio.to_thread(clients_probe, f"{config.TEI_RERANK_URL}/health")
    status = "down" if not es_ok else ("ok" if embed_ok and rerank_ok else "degraded")
    return JSONResponse(
        {"status": status,
         "components": {"elasticsearch": es_ok, "tei_embed": embed_ok,
                        "tei_rerank": rerank_ok}},
        status_code=503 if status == "down" else 200)


# ===========================================================================
# 请求/响应 schema —— 复刻 CreateAssitantRunPayload 与
# KnowledgesRetrieveRequest / KnowledgeRetrieveResponseV3 的字段
# ===========================================================================
class AssistantRunPayload(BaseModel):
    """对照 agent_core/schema.py CreateAssitantRunPayload（demo 取核心字段：
    question 必填；thread_id 是多轮会话标识——同一 thread_id 的追问经
    graph.load_context 读 ES 历史做查询改写（重启不丢，v5 起 ES 持久化）；
    transaction_id 兼作 langfuse trace id）。"""
    question: str
    assistant_uuid: str = "rag-lab-assistant"
    thread_id: Optional[str] = None
    transaction_id: Optional[str] = None


class KnowledgesRetrieveRequest(BaseModel):
    """对照 knowledge_v3.py KnowledgesRetrieveRequest 全字段。"""
    query: str
    knowledges: List[str] = Field(default_factory=lambda: [KB_ID])
    search_strategy: str = "hybrid"
    max_recalls: int = 4
    min_score: float = 0.5
    only_retrieval: bool = False


# ===========================================================================
# SSE 消息构造 —— 复刻 to_sse 的 chunk 序列化 + _stream_preset_response 的
# 消息结构（response_metadata 的键逐一对齐）
# ===========================================================================
def _meta(transaction_id: str, thread_id: str, assistant_uuid: str,
          model_name: str, duration: float = 0) -> Dict[str, Any]:
    """HumanMessage/AIMessage 的 response_metadata —— 生产 _stream_preset_
    response:2380-2394 同款键：start_time/end_time/duration/model_name/
    model_id/transaction_id/thread_id/assistant_uuid。"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return {"start_time": now, "end_time": now, "duration": duration,
            "model_name": model_name, "model_id": "demo",
            "transaction_id": transaction_id, "thread_id": thread_id,
            "assistant_uuid": assistant_uuid}


def _dump(messages: list) -> str:
    """chunk（消息列表）→ SSE data 字符串。生产 to_sse:196-203 用
    message_chunk_to_message + orjson；demo 用 langchain 消息的 model_dump
    （序列化形态同为 {type, content, response_metadata, ...} 的 JSON 数组）。"""
    return json.dumps([m.model_dump() for m in messages], ensure_ascii=False,
                      default=str)


async def assistant_stream(payload: AssistantRunPayload,
                           parent_trace_id: Optional[str],
                           idem_key: Optional[str] = None):
    """消息流 generator —— v5：分流在 graph.py（StateGraph 五路），本函数只做
    ①幂等重放判断 ②图调用 ③最终 state → SSE 消息序列的翻译（v4 契约不变）。

    幂等键（网关职责，v5 新增）：请求带 Idempotency-Key 且 ES 聊天记录里已有
    同键轮次 → 直接重放已存答案（不重执行图、不重复落历史）——客户端重试/
    网络重发不会产生重复回答与重复计费。

    会话（v5）：多轮历史不再放进程内存——graph 的 load_context/persist 节点
    读写 ES（raglab_chat_messages），服务重启后同 thread_id 续聊仍在。"""
    question = payload.question
    # 多轮会话：thread_id 由客户端携带（连续对话传同一个）；不传则新会话
    thread_id = payload.thread_id or str(uuid.uuid4())
    transaction_id = payload.transaction_id or str(uuid.uuid4())
    t_start = time.time()

    # langfuse trace（生产以 transaction_id 兼作 langfuse_parent_id，
    # assistant_run.py:19-20；demo 直接作 trace id）。trace_scope 把它设为
    # 当前 async 上下文的活跃 trace——图内各节点（分类/检索/重排/生成/MCP）
    # 的 obs 埋点经 contextvars 自动挂到这条 trace 上
    trace = obs.trace(name="assistant_stream", input=question,
                      metadata={"assistant_uuid": payload.assistant_uuid},
                      id=parent_trace_id or transaction_id)
    with obs.trace_scope(trace):
        # ① 先产 run_id（生产 :2377；to_sse:164-170 把 str chunk 转 metadata 事件）
        run_id = str(uuid.uuid4())
        yield run_id

        # ② 用户消息（生产 :2380-2395）
        human_msg = HumanMessage(content=question, response_metadata=_meta(
            transaction_id, thread_id, payload.assistant_uuid, "user"))
        yield [human_msg]

        try:
            # ③ 幂等（原子占位防并发竞态，v5.1）：create 抢占 idem:{key} 文档
            #    · 抢到 → 本次执行，persist 节点回填该文档（同键至多一条）
            #    · 已存在且有 answer → 直接重放（含引用 ToolMessage，事件
            #      序列与首次一致，不重执行、不重复落历史）
            #    · 已存在但无 answer → 首个同键请求仍在途，按普通请求执行
            #    （"先查后写"存在 TOCTOU 窗口——两个并发同键请求会双执行；
            #     ES 的 op_type=create 是版本级原子操作，抢占即去重）
            replay, idem_doc_id = None, ""
            if idem_key:
                existing = await asyncio.to_thread(
                    chat_store.claim_idem, idem_key, thread_id)
                if existing is not None and existing.get("answer"):
                    replay = existing
                elif existing is not None:
                    # 首个同键请求仍在途（占位无答案）：有界等待其完成——
                    # 0.5s 轮询最多 60s（覆盖并发高压下的 LLM+重排时延），
                    # 等到答案即重放；超时才按普通请求执行（极端兜底）
                    print("[幂等] 同键请求在途，等待其完成…", flush=True)
                    for _ in range(120):
                        await asyncio.sleep(0.5)
                        existing = await asyncio.to_thread(
                            chat_store.find_by_idem, idem_key)
                        if existing and existing.get("answer"):
                            replay = existing
                            break
                    if replay is None:
                        print("[幂等] 在途等待超时，按普通流程执行", flush=True)
                else:
                    idem_doc_id = f"idem:{idem_key}"

            if replay:
                answer = replay.get("answer", "")
                route = replay.get("route", "")
                citations = replay.get("citations") or []
                print(f"[幂等] Idempotency-Key 命中，重放已存答案"
                      f"（route={route}，不重执行）", flush=True)
                if route in ("knowledge_retrieve", "hybrid"):
                    tool_msg = ToolMessage(
                        content=json.dumps(citations, ensure_ascii=False),
                        name="knowledges_retrieve",
                        tool_call_id=run_id,
                        response_metadata={
                            "kb_empty": not citations,
                            **_meta(transaction_id, thread_id,
                                    payload.assistant_uuid, "retriever")})
                    yield [tool_msg]
            else:
                # ④ 图调用（生产 :1490-1618 分流的 v5 形态）：一次 ainvoke 完成
                # 意图路由 + 对应分支执行 + 历史落库；状态逐步 checkpoint 到 ES。
                # obs 埋点不在此层：请求入口 trace_scope 设的 contextvars 会
                # 随 asyncio 任务复制传播进图的每个节点（langfuse 上已验证
                # graph_load_context/rewrite/classify + 检索各阶段 span 挂树）
                state = await ask_graph(graph, question, thread_id=thread_id,
                                        transaction_id=transaction_id,
                                        idem_key=idem_key or "",
                                        idem_doc_id=idem_doc_id)
                answer = state.get("answer") or "没有匹配到相关知识，无法回答该问题。"
                route = state.get("route", "")
                docs = state.get("source_documents") or []
                qtype = state.get("classification", {}).get("type")
                trace and trace.update(metadata={
                    "route": route, "type": getattr(qtype, "value", "?")})
                # 引用消息：生产知识库 tool 的 ToolMessage（to_sse:186-193 靠
                # response_metadata.kb_empty 统计知识命中）。v5 起 hybrid 分支
                # 同样带知识引用，一并输出
                if route in ("knowledge_retrieve", "hybrid"):
                    obs.generation(trace, name="knowledges_retrieve",
                                   input=question,
                                   output=[{"title": d.title, "score": d.score}
                                           for d in docs],
                                   model="hybrid+rerank",
                                   metadata={"kb_empty": not docs})
                    tool_msg = ToolMessage(
                        content=json.dumps(
                            [{"title": d.title, "score": d.score,
                              "content": d.content[:100], "url": d.url} for d in docs],
                            ensure_ascii=False),
                        name="knowledges_retrieve",
                        # langchain 0.3 的 ToolMessage 必填 tool_call_id（生产由 agent
                        # 的 tool 调用自动生成；demo 用 run_id 关联本次回复）
                        tool_call_id=run_id,
                        response_metadata={"kb_empty": not docs,
                                           **_meta(transaction_id, thread_id,
                                                   payload.assistant_uuid, "retriever")})
                    yield [tool_msg]

            # ⑤ 回答消息（生产为 token 级流；demo 按 ~40 字切片同 id 消息块模拟）
            duration = round(time.time() - t_start, 2)
            chunk_size, i = 40, 0
            while i * chunk_size < len(answer):
                piece = answer[i * chunk_size:(i + 1) * chunk_size]
                ai_msg = AIMessage(content=piece, id=run_id, response_metadata=_meta(
                    transaction_id, thread_id, payload.assistant_uuid,
                    "Deepseek-v4-flash", duration))
                yield [ai_msg]
                await asyncio.sleep(0.05)          # 肉眼可见的"流式"节奏
                i += 1
            # 多轮历史由图的 persist 节点落 ES（本函数不再写会话存储）
            trace and trace.update(output=answer, metadata={"route": route})
        except Exception as ex:
            # 生产 to_sse:204-215：错误不外泄细节
            yield {"event": "error",
                   "data": json.dumps({"status_code": 500, "message": str(ex)[:200]},
                                      ensure_ascii=False)}
        finally:
            obs.flush()


def to_sse(messages_stream):
    """复刻 agent_core/utils/stream.py:152-215 to_sse：把上层消息流包成
    EventSourceResponse 需要的事件 dict（str chunk→metadata 事件，
    消息列表→data 事件，异常→error 事件；统计/DB 副作用省略）。"""
    async def event_source():
        async for chunk in messages_stream:
            if isinstance(chunk, str):
                yield {"event": "metadata",
                       "data": json.dumps({"run_id": chunk})}
            elif isinstance(chunk, dict):
                yield chunk                          # assistant_stream 已包好的 error 事件
            else:
                yield {"event": "data", "data": _dump(chunk)}
    return event_source()


# ===========================================================================
# 接口 ① POST /api/v2/assistant_run/stream —— 复刻 create_stream
# ===========================================================================
@app.post("/api/v2/assistant_run/stream", dependencies=GUARDS)
async def create_stream(
        payload: AssistantRunPayload,
        langfuse_parent_trace_id: Optional[str] = Header(
            default=None, alias="langfuse_parent_trace_id"),
        langfuse_parent_observation_id: Optional[str] = Header(
            default=None, alias="langfuse_parent_observation_id"),
        idempotency_key: Optional[str] = Header(
            default=None, alias="Idempotency-Key"),
):
    """对照 assistant_run.py:32-48：header 两个 langfuse 父节点 id 透传给
    观测层（demo 用作 trace id；生产挂进 langfuse 的 parent 链）。
    v5 新增 Idempotency-Key（可选）：同键重复请求重放已存答案（见
    assistant_stream ③）。"""
    print("=" * 60, flush=True)
    print(f"[stream] question={payload.question[:40]}"
          + (f" idem={idempotency_key[:16]}" if idempotency_key else ""),
          flush=True)
    return EventSourceResponse(to_sse(assistant_stream(
        payload, langfuse_parent_trace_id or langfuse_parent_observation_id,
        idem_key=idempotency_key)))


# ===========================================================================
# 接口 ① POST /api/v1/knowledge/upload —— 解析入库（异步任务化）
#    对照生产上传链路：server/api/v1/knowledge.py 上传 → 文件落盘（本地
#    存储模式）→ celery 任务 file_ingest(ingest.py:86) / knowledge_faq_
#    batch_ingest(ingest.py:653)。demo 用 BackgroundTasks + ES 任务表
#    （raglab_tasks，v5 起——重启/多 worker 任务状态不丢，对应生产的
#    队列+DB 形态）。
# ===========================================================================
def _run_ingest_task(task_id: str, dest: str, chain: str, kb_id: str,
                     column_question: int, column_answer: int):
    """后台入库任务体（BackgroundTasks 的线程池里跑，无请求上下文——obs
    埋点自动 no-op；失败信息记进任务表，由任务状态接口透出）。"""
    try:
        ingest = IngestService(store)
        if chain == "knowledge_faq":
            chunks = ingest.faq_batch_ingest(
                dest, column_question=column_question,
                column_answer=column_answer, knowledge_base_id=kb_id)
        else:
            chunks = ingest.file_ingest(chain, dest, knowledge_base_id=kb_id)
        ingest_tasks.finish(task_id, chunks=chunks, replaced=ingest.last_replaced)
    except Exception as ex:
        # 生产 file_ingest 失败把 message 写进任务状态（ingest.py:139-140）
        ingest_tasks.finish(task_id, chunks=0, replaced=0, error=str(ex)[:200])


@app.post("/api/v1/knowledge/upload", status_code=202, dependencies=GUARDS)
async def upload_knowledge(
        background: BackgroundTasks,
        file: UploadFile = File(..., description="知识文件：docx/pdf/md/xlsx；FAQ 用 docx/xlsx/csv"),
        chain: str = Form("knowledge_base",
                          description="knowledge_base=普通知识；knowledge_faq=FAQ（双份入库）"),
        knowledge_base_id: str = Form(KB_ID, description="目标知识库 id（检索时按它过滤）"),
        column_question: int = Form(1, description="FAQ 表格的问题列号（1-based）"),
        column_answer: int = Form(2, description="FAQ 表格的答案列号（1-based）"),
):
    """multipart 上传 → 落盘 uploads/（本地存储模式，file_service）→ 返回
    task_id 立即结束；解析入库在后台任务执行。幂等：同名同库重传 = 替换
    （先删旧块再入新块，见 ingest_service）。

    解析链路 = 生产 file_ingest：FILE_LOADER_MAP 分派（pdf/docx/md/xlsx）→
    load_and_split 父子切分（1600/400）→ ES 写入；chain=knowledge_faq 时走
    faq_batch_ingest 双份入库（is_faq=Y + answer 入 metadata）。
    文件名 basename 清洗 + resolve 后锁定 uploads/（防路径穿越）。"""
    from file_service import UPLOAD_DIR, ensure_upload_dir

    safe_name = os.path.basename(file.filename or "")
    if safe_name in ("", ".", ".."):
        return JSONResponse({"code": 400, "msg": "非法文件名", "data": {}},
                            status_code=400)
    ensure_upload_dir()
    upload_base = Path(UPLOAD_DIR).resolve()
    dest_path = (upload_base / safe_name).resolve()
    if dest_path.parent != upload_base:
        return JSONResponse({"code": 400, "msg": "路径越界被拒绝", "data": {}},
                            status_code=400)
    dest = str(dest_path)
    dest_path.write_bytes(await file.read())
    print(f"[upload] {safe_name}（{dest_path.stat().st_size} 字节，chain={chain}）"
          f"→ 已受理，转后台入库", flush=True)

    task_id = ingest_tasks.create(safe_name, chain, knowledge_base_id)
    background.add_task(_run_ingest_task, task_id, dest, chain,
                        knowledge_base_id, column_question, column_answer)
    return {"code": 202, "msg": "accepted",
            "data": {"task_id": task_id, "file": safe_name, "chain": chain,
                     "poll": f"/api/v1/knowledge/tasks/{task_id}"}}


@app.get("/api/v1/knowledge/tasks/{task_id}", dependencies=GUARDS)
async def get_ingest_task(task_id: str):
    """入库任务状态轮询（生产任务状态接口的 demo 等价）：
    status=running/done/failed，done 时带 chunks（新入块数）与 replaced
    （替换删除的旧块数）。"""
    task = ingest_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
    return {"code": 200, "msg": "success", "data": task}


# ===========================================================================
# 接口 ② POST /api/v3/knowledges/retrieve —— 复刻 knowledges_retrieve
# ===========================================================================
@app.post("/api/v3/knowledges/retrieve", dependencies=GUARDS)
async def knowledges_retrieve_api(call_body: KnowledgesRetrieveRequest):
    """对照 knowledge_v3.py:64-145：检索 → 响应组装（title 优先 name 退
    source、FAQ answer 的 <img> 拼回）→ {code, msg, data} 包装。埋点对照
    生产 knowledges_retrieve_with_langfuse（knowledge.py:78-92）。"""
    t0 = time.time()
    trace = obs.trace(name="knowledges_retrieve", input=call_body.query,
                      metadata={"only_retrieval": call_body.only_retrieval})
    with obs.trace_scope(trace):
        # 检索内各阶段的埋点（三路召回/RRF/去重/重排/生成）经 trace_scope
        # 的 contextvars 自动挂本 trace（生产由 langchain handler 通道上报，
        # knowledge.py:78-92，demo 直调等价）
        ret = await knowledges_retrieve(
            rag, call_body.query, call_body.knowledges,
            search_strategy=call_body.search_strategy,
            max_recalls=call_body.max_recalls, min_score=call_body.min_score,
            only_retrieval=call_body.only_retrieval)
    service_cost = time.time() - t0
    obs.generation(trace, name="retrieval",
                   output=[{"title": d.title, "score": d.score}
                           for d in ret["source_documents"]],
                   metadata={"耗时": round(service_cost, 2)})
    obs.flush()
    print(f"[knowledges_retrieve] 完成，耗时={service_cost:.3f}s, "
          f"返回文档数={len(ret['source_documents'])}", flush=True)
    return {"code": 200, "msg": "success",
            "data": {"result": ret["result"],
                     "source_documents": [d.__dict__ for d in ret["source_documents"]]}}


# ===========================================================================
# 接口 ③ POST /api/v1/feedback —— 点赞/点踩（质量运营原始数据）
# ===========================================================================
class FeedbackPayload(BaseModel):
    """单条反馈：rating=up/down；question/answer 由客户端回传（服务无会话
    持久化，与生产的反馈表同位）。"""
    question: str
    answer: str
    rating: str
    route: Optional[str] = None
    thread_id: Optional[str] = None


@app.post("/api/v1/feedback", dependencies=GUARDS)
async def feedback(payload: FeedbackPayload):
    """反馈落 ES（raglab_feedback，v5 起——原 jsonl 文件改 ES-only）——
    "线上反馈 → 离线评估"回流的原始数据：定期从 down 里挑 badcase 补进
    eval_set.jsonl 再跑 ragas 基线对比，即成质量运营闭环。"""
    if payload.rating not in ("up", "down"):
        return JSONResponse({"code": 400, "msg": "rating 仅支持 up/down",
                             "data": {}}, status_code=400)
    await asyncio.to_thread(feedback_store.add, {
        "rating": payload.rating,
        "question": payload.question[:500],
        "answer": payload.answer[:1000],
        "route": payload.route, "thread_id": payload.thread_id})
    return {"code": 200, "msg": "success", "data": {}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
