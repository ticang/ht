# -*- coding: utf-8 -*-
"""
api_server.py —— FastAPI 服务版（复刻生产的两个 HTTP 接口，SSE 流式）

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

分流逻辑与 rag_demo.assistant_ask 完全一致（安全校验跳过/llm_response/
preset/mcp 降级/知识检索，对照 assistant.py:1500-1568），差异仅在输出形态
（SSE 消息流 vs dict）与 langfuse 埋点开启。

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
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, Header, UploadFile
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

# 复用 rag_demo 的检索组件/意图识别（模块级共享同一套实现）
from rag_demo import RagComponents, knowledges_retrieve, make_sample_files
from classifier import QuestionClassifier, QuestionType
from clients import build_clients
from es_store import ESStore
from ingest_service import IngestService
from obs import Obs

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:                              # pragma: no cover
    pass

KB_ID = "kb-demo"

# ===========================================================================
# 应用与全局单例（生产：每次请求 ChainFactory 组装；客户端/存储为进程级单例）
# ===========================================================================
app = FastAPI(title="rag-lab api", version="2")

embedder, reranker, llm = build_clients()
store = ESStore(embedder)
rag = RagComponents(store, embedder, reranker, llm, top_k=4)
classifier = QuestionClassifier(llm)
obs = Obs()


@app.on_event("startup")
async def startup_ingest():
    """索引不存在时自动入库五类样例（幂等；生产入库走 celery 任务，
    demo 在服务启动时完成，等价保证"检索接口可用"的前置条件）。"""
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
# 请求/响应 schema —— 复刻 CreateAssitantRunPayload 与
# KnowledgesRetrieveRequest / KnowledgeRetrieveResponseV3 的字段
# ===========================================================================
class AssistantRunPayload(BaseModel):
    """对照 agent_core/schema.py CreateAssitantRunPayload（demo 取核心三字段：
    question 必填；assistant_uuid/thread 概念 demo 无会话存储，仅透传进
    response_metadata；transaction_id 兼作 langfuse trace id）。"""
    question: str
    assistant_uuid: str = "rag-lab-assistant"
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
                           parent_trace_id: Optional[str]):
    """消息流 generator —— 复刻 AssistantService.stream(:1490-1618) 的分流 +
    _stream_preset_response 的消息序列（生产：preset 分支也是 agent 消息流）。

    生产差异（注释对照）：
      · input_defence_by_prompt 安全校验（:1500）demo 跳过
      · 50s 超时 wrapper（:1578-1597）—— demo 依赖各客户端自身 timeout
      · thread/checkpoint 会话 —— demo 无状态
    """
    question = payload.question
    thread_id = str(uuid.uuid4())                 # demo 无会话存储，仅标识本次
    transaction_id = payload.transaction_id or str(uuid.uuid4())
    t_start = time.time()

    # langfuse trace（生产以 transaction_id 兼作 langfuse_parent_id，
    # assistant_run.py:19-20；demo 直接作 trace id）。trace_scope 把它设为
    # 当前 async 上下文的活跃 trace——后续分类/检索/重排/生成/嵌入等所有
    # 链内埋点（obs.span / generation_call）自动挂到这条 trace 上
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
            # ③ 意图识别（生产 :1503-1506；埋点对照生产的分类 LLM 调用）
            t0 = time.time()
            classification = await classifier.classify_question(question)
            qtype = classification.get("type")
            obs.generation(trace, name="question_classification",
                           input=question, output=qtype.value if qtype else "?",
                           model="Deepseek-v4-flash",
                           metadata={"耗时": round(time.time() - t0, 2),
                                     "confidence": classification.get("classification_confidence")})

            # ④ OTHER 类：分类器已让 LLM 直答（生产 :1511-1527）
            if classification.get("llm_response"):
                answer = classification["llm_response"]
                route = "llm_response"
            # ⑤ 预设回复（生产 :1530-1545）
            elif classification.get("preset_response"):
                answer = classifier.get_preset_content(classification["preset_response"]) \
                         or "抱歉，我暂时无法回答这个问题。"  # 生产 :2360-2361 兜底话术
                route = "preset_response"
            else:
                # ⑥ mcp 分支：demo 无 MCP 服务按降级语义走检索（生产 :1547-1568）
                if qtype == QuestionType.MCP_QUERY:
                    print("  [分流] mcp 类：按生产降级语义转知识检索", flush=True)
                # 知识检索（生产 :1609-1616 走 agent；demo 直连检索服务）。
                # 检索内各阶段（三路召回/RRF/去重/重排/生成）的埋点经
                # trace_scope 的 contextvars 自动挂本 trace（生产走 langchain
                # callback 通道，knowledge.py:84/:113-117，demo 直调等价）
                t0 = time.time()
                ret = await knowledges_retrieve(rag, question, [KB_ID],
                                                max_recalls=4, min_score=0.5)
                docs = ret["source_documents"]
                answer = ret["result"] or "没有匹配到相关知识，无法回答该问题。"
                route = "knowledge_retrieve"
                obs.generation(trace, name="knowledges_retrieve",
                               input=question,
                               output=[{"title": d.title, "score": d.score}
                                       for d in docs],
                               model="hybrid+rerank",
                               metadata={"耗时": round(time.time() - t0, 2),
                                         "kb_empty": not docs})
                # 引用消息：生产知识库 tool 的 ToolMessage（to_sse:186-193 靠
                # response_metadata.kb_empty 统计知识命中）
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

            # ⑦ 回答消息（生产为 token 级流；demo 按 ~40 字切片同 id 消息块模拟）
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
            trace and trace.update(output=answer, metadata={
                "route": route, "type": qtype.value if qtype else "?"})
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
@app.post("/api/v2/assistant_run/stream")
async def create_stream(
        payload: AssistantRunPayload,
        langfuse_parent_trace_id: Optional[str] = Header(
            default=None, alias="langfuse_parent_trace_id"),
        langfuse_parent_observation_id: Optional[str] = Header(
            default=None, alias="langfuse_parent_observation_id"),
):
    """对照 assistant_run.py:32-48：header 两个 langfuse 父节点 id 透传给
    观测层（demo 用作 trace id；生产挂进 langfuse 的 parent 链）。"""
    print("=" * 60, flush=True)
    print(f"[stream] question={payload.question[:40]}", flush=True)
    return EventSourceResponse(to_sse(assistant_stream(
        payload, langfuse_parent_trace_id or langfuse_parent_observation_id)))


# ===========================================================================
# 接口 ① POST /api/v1/knowledge/upload —— 解析入库
#    对照生产上传链路：server/api/v1/knowledge.py 上传 → 文件落盘（本地
#    存储模式）→ celery 任务 file_ingest(ingest.py:86) /
#    knowledge_faq_batch_ingest(ingest.py:653)。demo 同步执行入库。
# ===========================================================================
@app.post("/api/v1/knowledge/upload")
async def upload_knowledge(
        file: UploadFile = File(..., description="知识文件：docx/pdf/md/xlsx；FAQ 用 docx/xlsx/csv"),
        chain: str = Form("knowledge_base",
                          description="knowledge_base=普通知识；knowledge_faq=FAQ（双份入库）"),
        knowledge_base_id: str = Form(KB_ID, description="目标知识库 id（检索时按它过滤）"),
        column_question: int = Form(1, description="FAQ 表格的问题列号（1-based）"),
        column_answer: int = Form(2, description="FAQ 表格的答案列号（1-based）"),
):
    """multipart 上传 → 落盘 uploads/（本地存储模式，file_service）→ 解析入库。

    解析链路 = 生产 file_ingest：FILE_LOADER_MAP 分派（pdf/docx/md/xlsx）→
    load_and_split 父子切分（1600/400）→ ES 写入；chain=knowledge_faq 时走
    faq_batch_ingest 双份入库（is_faq=Y + answer 入 metadata）。
    文件名 basename 清洗 + resolve 后锁定 uploads/（防路径穿越）。"""
    from pathlib import Path
    from file_service import UPLOAD_DIR, ensure_upload_dir

    safe_name = os.path.basename(file.filename or "")
    if safe_name in ("", ".", ".."):
        return {"code": 400, "msg": "非法文件名", "data": {}}
    ensure_upload_dir()
    upload_base = Path(UPLOAD_DIR).resolve()
    dest_path = (upload_base / safe_name).resolve()
    if dest_path.parent != upload_base:
        return {"code": 400, "msg": "路径越界被拒绝", "data": {}}
    dest = str(dest_path)
    dest_path.write_bytes(await file.read())
    print(f"[upload] {safe_name}（{dest_path.stat().st_size} 字节，chain={chain}）", flush=True)

    t0 = time.time()
    trace = obs.trace(name="knowledge_upload", input={"file": safe_name, "chain": chain},
                      metadata={"knowledge_base_id": knowledge_base_id})
    with obs.trace_scope(trace):
        try:
            ingest = IngestService(store)
            if chain == "knowledge_faq":
                n = ingest.faq_batch_ingest(dest, column_question=column_question,
                                            column_answer=column_answer,
                                            knowledge_base_id=knowledge_base_id)
            else:
                n = ingest.file_ingest(chain, dest, knowledge_base_id=knowledge_base_id)
            obs.generation(trace, name="file_ingest", output={"chunks": n},
                           model="loaders+splitter+ES",
                           metadata={"耗时": round(time.time() - t0, 2)})
            obs.flush()
            return {"code": 200, "msg": "success",
                    "data": {"file": safe_name, "chain": chain, "chunks": n,
                             "duration": round(time.time() - t0, 2)}}
        except Exception as ex:
            # 生产 file_ingest 失败同样如实回传 message（ingest.py:139-140）
            obs.flush()
            return {"code": 500, "msg": f"知识生成失败，error: {str(ex)[:200]}",
                    "data": {"file": safe_name}}


# ===========================================================================
# 接口 ② POST /api/v3/knowledges/retrieve —— 复刻 knowledges_retrieve
# ===========================================================================
@app.post("/api/v3/knowledges/retrieve")
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
