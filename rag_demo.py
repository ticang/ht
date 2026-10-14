# -*- coding: utf-8 -*-
"""
rag_demo.py —— aiops-chatops 知识库链路的【全真实组件】复刻 v3
（多类型解析入库 + 意图识别 + 单接口检索问答）

与 v2 相比的结构改造（本次目标）：检索链去嵌套——不再复刻生产的
langchain 链式组装（Chain/BaseRetriever/BaseDocumentCompressor 包装类、
ContextualCompressionRetriever + DocumentCompressorPipeline 管线、每请求
链工厂闭包、acall→ainvoke 兼容层共 6 层脚手架），改为 RagComponents
组件包 + 两层扁平函数直调，检索行为/日志/埋点逐环节等价：

    assistant_ask(classifier, rag, ...)          # 会话分流（意图识别→分流）
    └─ knowledges_retrieve(rag, ...)             # 检索服务 + 响应组装
       └─ knowledge_qa(rag, ...)                 # 双通道并发→合并→父块还原→生成
          └─ _search_channel(...) ×2 并发        # ES三路召回(to_thread)→嵌入去重→重排

功能对齐不变（v2 确立）：
  ① 解析：word/pdf/md/excel/faq 全类型 —— 对齐
     aiops-chatops/server/tasks/ingest.py 的 file_ingest 链
     （FILE_LOADER_MAP 分派 → 各 loader → load_and_split 父子切分 → 入库）
  ② 检索：先意图识别再检索 —— 对齐 aiops-agent-core/backend/agent_core/
     api/v2/assistant_run.py 的 create_stream（AssistantService.stream 的
     分类分流），检索本体对齐 aiops-chatops/server/api/v3/knowledge_v3.py
     的 knowledges_retrieve（KnowledgeService → qa_chain.acall）
  ③ 单接口：生产是"assistant_run /stream → 内部调 knowledges_retrieve 服务"
     两个 HTTP 接口，demo 合成一个 assistant_ask()，分流语义不变
  ④ 切分侧与生产同款 langchain 用法 loader.load_and_split(text_splitter)；
     检索侧的 langchain 链结构是生产为框架集成所需，demo 直调等价实现

文件分工：
    loaders/          各类型解析（word/pdf/md/excel/faq 各一文件，
                      对齐 aichain/document_loader/ 的目录形态）
    text_splitters.py 父子切分（对齐 aichain/ingest/text_splitter.py）
    ingest_service.py 入库服务（对齐 server/tasks/ingest.py 两入口）
    classifier.py     意图识别（对齐 agent_core question_classifier.py）
    es_store.py       ES 三路召回 + RRF（对齐 ElasticVectorSearch7）
    clients.py        TEI 嵌入/重排 + 微信 LLM 客户端
    本文件             检索服务层 + 会话层（单接口）+ 端到端演示

运行：
    cd rag-lab && .venv/Scripts/python rag_demo.py --reset
"""
import asyncio
import dataclasses
import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import docx as docx_lib
import openpyxl
from langchain_core.documents import Document

# 日志重定向到文件时 Python 默认块缓冲（print 攒一大块才落盘，tail 看不到实时
# 进度）——改成按行刷新，运行时再加 PYTHONUNBUFFERED=1 双保险
try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:                              # pragma: no cover（非标准流）
    pass

import config                                       # noqa: F401（白名单校验在 clients 内触发）
from classifier import QuestionClassifier, QuestionType
from clients import build_clients, obs
from es_store import ESStore
from ingest_service import IngestService


# ===========================================================================
# ① 检索组件包 —— 替代生产的每请求链组装（ChainFactory.get_qa_chain，
#    knowledge.py:104-111）。demo 不再包链对象，直接把组件递给下面的
#    扁平函数（纯数据包，不是包装层）
# ===========================================================================
@dataclasses.dataclass
class RagComponents:
    """检索链的全部真实组件。对应生产 qa_chain 所持有的依赖：
    ES 向量库（ElasticVectorSearch7）、嵌入（redundant_filter 用）、
    重排器（CrossEncoderRerank）、LLM（生成）、top_k（max_recalls）。"""
    store: Any
    embedder: Any
    reranker: Any
    llm: Any
    top_k: int = 4


# ===========================================================================
# ② 通道内压缩：嵌入去重 + 重排（对应生产 DocumentCompressorPipeline 的
#    两个 transformer：redundant_filter.py + rerank.py）
# ===========================================================================
def _cosine(a, b) -> float:
    num = sum(x * y for x, y in zip(a, b))
    da = math.sqrt(sum(x * x for x in a)) or 1e-9
    db = math.sqrt(sum(x * x for x in b)) or 1e-9
    return num / (da * db)


def _dedup_by_embedding(embedder, docs: List[Document],
                        similarity_threshold: float = 0.95) -> List[Document]:
    """嵌入去重：余弦>0.95 的对丢第二个（redundant_filter.py:42-57 朴素版，
    生产 FixedEmbeddingsRedundantFilter 的两两余弦循环实现，阈值/语义相同）。"""
    if len(docs) < 2:
        return list(docs)
    with obs.span("redundant_filter", input={"docs": len(docs)}) as s:
        vectors = embedder.embed([d.page_content for d in docs])
        included = list(range(len(docs)))
        for i in range(len(docs)):
            if i not in included:
                continue
            for j in range(i + 1, len(docs)):
                if j in included and _cosine(vectors[i], vectors[j]) > similarity_threshold:
                    included.remove(j)
        if s:
            s.update(output={"kept": len(included)})
    print(f"    [嵌入去重] {len(docs)} → {len(included)} 条", flush=True)
    return [docs[i] for i in sorted(included)]


async def _rerank(reranker, query: str, docs: List[Document],
                  threshold: float, top_n: int) -> List[Document]:
    """真交叉编码重排 —— rerank.py CrossEncoderRerank.acompress_documents(:72-96)
    五步同构：①TEI /rerank 打分 ②降序 ③覆写 metadata['score'] ④threshold 过滤
    ⑤截 top_n。生产双通道阈值可不同；demo 统一用 API 传入值。"""
    if not docs:
        return []
    print(f"    [重排] {len(docs)} 条候选交给 bge-reranker（threshold={threshold}, "
          f"top_n={top_n}）", flush=True)
    # TEI 客户端是同步 requests（CPU推理本身就慢），丢线程池避免阻塞事件循环
    # ——对应生产 acompress_documents 的异步打分。to_thread 复制 contextvars，
    # 线程内 clients 层的 reranker 埋点才能挂上当前 trace
    t0 = time.perf_counter()
    ranked = await asyncio.to_thread(reranker.rerank, query,
                                     [d.page_content for d in docs])
    final: List[Document] = []
    for item in ranked:                              # TEI 已按 score 降序
        doc = docs[item["index"]]
        doc.metadata["score"] = float(item["score"])  # ③ 覆写：交叉编码分
        if item["score"] >= threshold:                # ④ 及格线
            final.append(doc)
    final = final[:top_n]                             # ⑤ 截断
    scores_preview = ", ".join(f"{it['score']:.3f}" for it in ranked[:6])
    print(f"    [重排] 分数降序[{scores_preview}]，≥{threshold} 保留 "
          f"{len(final)} 条，耗时 {time.perf_counter() - t0:.2f}s", flush=True)
    # langfuse：重排阶段汇总（bge-reranker 模型调用本身在 clients 层
    # 已记 generation，这里补"过滤/截断后"的保留数）
    obs.generation_call(name="rerank_stage",
                        model="bge-reranker-v2-m3",
                        input={"candidates": len(docs),
                               "threshold": threshold, "top_n": top_n},
                        output={"kept": len(final),
                                "scores": [round(it["score"], 4) for it in ranked[:6]]})
    return final


async def _search_channel(rag: RagComponents, query: str, multiply: float,
                          threshold: float, flt: Optional[dict]) -> List[Document]:
    """单通道检索：ES 三路召回+RRF → 嵌入去重 → 重排。

    对应生产 base.py:464/480 的双通道裸检索器（文档路 4×top_k、FAQ 路
    2.5×top_k）各自再包一层压缩管线（ContextualCompressionRetriever +
    DocumentCompressorPipeline）；demo 直接顺序直调，行为等价。
    ES 客户端是同步 IO，丢线程池避免阻塞事件循环——用 asyncio.to_thread
    （而非 run_in_executor）：to_thread 会复制当前 contextvars，
    es_store.hybrid_search 里的三路召回/RRF 埋点才能挂到当前请求的
    langfuse trace（线程池不会传播上下文）。"""
    size = math.floor(rag.top_k * multiply)
    docs = await asyncio.to_thread(rag.store.hybrid_search, query, size, filter=flt)
    docs = _dedup_by_embedding(rag.embedder, docs)
    return await _rerank(rag.reranker, query, docs, threshold, rag.top_k)


# ===========================================================================
# ③ 问答主体 —— FixRetrievalQA._acall(qa/chains/base.py:281-327)
# ===========================================================================
# 生产 PROMPT_TEMPLATES['knowledge_base_chat'] 原文
QA_PROMPT = ("已知信息：\n{context}\n\n"
             "根据上述已知信息，简洁和专业地回答用户的问题。"
             "如果无法从中得到答案，请说\"根据已知信息无法回答该问题\"，"
             "不允许在答案中添加编造成分。问题是：{question}")


def _combine_faq_and_general_docs(faq_docs, general_docs) -> List[Document]:
    """合并（base.py:237-279）：FAQ 第一名>0.85 前置 / 否则分数混排 /
    内容去重 / 截 8。"""
    if not faq_docs:
        return general_docs
    if not general_docs:
        return faq_docs
    if faq_docs and faq_docs[0].metadata.get("score", 0) > 0.85:
        all_docs = faq_docs + general_docs
    else:
        all_docs = sorted(faq_docs + general_docs,
                          key=lambda x: x.metadata.get("score", 0), reverse=True)
    seen, unique = set(), []
    for d in all_docs:
        content = d.page_content.strip()
        if content and content not in seen:
            seen.add(content)
            unique.append(d)
    return unique[:8]


async def knowledge_qa(rag: RagComponents, query: str, threshold: float = 0.5,
                       filter: Optional[dict] = None,
                       only_retrieval: bool = False) -> Dict[str, Any]:
    """双通道检索 + 生成 —— 生产 qa_chain.acall(query, threshold, filter)
    一击的等价直调（ChainFactory.get_qa_chain → FixRetrievalQA._acall:281-327）：
      ① 双通道并发（:307-314）：文档路 4×top_k、FAQ 路 2.5×top_k+is_faq 过滤
      ② 合并（:237-279）
      ③ only_retrieval 短路（v3 接口参数 / ragas 评估模式）
      ④ 父子块还原+二次去重（_filter_docs:52-64）
      ⑤ LLM 生成（_acall_llm_by_doc:183）——微信 Deepseek-v4-flash
    返回 {"result": str, "source_documents": List[Document]}。"""
    t_chain = time.perf_counter()

    # ① 双通道并发（:307-314）
    faq_docs, general_docs = await asyncio.gather(
        _search_channel(rag, query, 2.5, threshold,
                        {**(filter or {}), "is_faq": "Y"}),
        _search_channel(rag, query, 4.0, threshold, filter))

    def _top(docs, n=3):
        return ", ".join(f"{d.metadata.get('score', 0):.3f}" for d in docs[:n])

    print(f"  [双通道] FAQ路 {len(faq_docs)} 条[{_top(faq_docs)}] / "
          f"文档路 {len(general_docs)} 条[{_top(general_docs)}]，"
          f"检索+重排耗时 {time.perf_counter() - t_chain:.2f}s", flush=True)

    # ② 合并：FAQ 第一名>0.85 前置/分数混排/内容去重/截8
    combined = _combine_faq_and_general_docs(faq_docs, general_docs)
    pinned = bool(faq_docs) and faq_docs[0].metadata.get("score", 0) > 0.85
    print(f"  [合并] {len(combined)} 条（{'FAQ高分前置' if pinned else '分数混排'}）",
          flush=True)

    # ③ 只检索不生成（ragas 评估用法 / v3 接口的 only_retrieval 参数）
    if only_retrieval:
        return {"result": "\n".join(d.page_content for d in combined),
                "source_documents": combined}

    # ④ 父子块还原+二次去重（_filter_docs:52-64）
    docs, seen = [], set()
    for d in combined:
        d.page_content = d.metadata.get("parent_text") or d.page_content
        if d.page_content not in seen:
            seen.add(d.page_content)
            docs.append(d)

    # ⑤ LLM 生成（_acall_llm_by_doc:183）——微信 Deepseek-v4-flash
    if docs:
        context = "\n".join(d.page_content for d in docs)[:1200]
        print(f"  [生成] {len(docs)} 条引用 / context {len(context)} 字 → 调 LLM",
              flush=True)
        t_llm = time.perf_counter()
        resp = await rag.llm.ainvoke(QA_PROMPT.format(context=context, question=query))
        answer = resp.content if hasattr(resp, "content") else str(resp)
        print(f"  [生成] LLM 返回，答案 {len(answer)} 字，"
              f"链路总耗时 {time.perf_counter() - t_chain:.2f}s", flush=True)
        # langfuse：生成阶段汇总（LLM 模型调用在 clients 层已记 generation）
        obs.generation_call(name="generation_stage", model="qa_generate",
                            input={"question": query[:60],
                                   "context_chars": len(context),
                                   "citations": len(docs)},
                            output={"answer": answer[:200]},
                            latency_s=time.perf_counter() - t_llm)
    else:
        answer = "没有匹配到相关知识，无法回答该问题。"
    return {"result": answer, "source_documents": docs}


# ===========================================================================
# ④ 检索服务层 —— server/service/knowledge.py KnowledgeService.knowledges_retrieve(:95)
#    + server/api/v3/knowledge_v3.py knowledges_retrieve(:64) 的响应组装
# ===========================================================================
@dataclasses.dataclass
class KnowledgeRetrieveItem:
    """检索结果条目 —— 生产 KnowledgeRetrieveResponseItemV3（knowledge_v3.py 的
    schema 定义）：id(ES _id)/content/score/title/url/metadata 六字段。"""
    id: str
    content: str
    score: float
    title: str
    url: str
    metadata: Dict[str, Any]


async def knowledges_retrieve(rag: RagComponents, query: str, knowledges: List[str],
                              search_strategy: str = "hybrid",
                              max_recalls: int = 4, min_score: float = 0.5,
                              only_retrieval: bool = False) -> Dict[str, Any]:
    """知识检索服务 —— 生产 KnowledgeService.knowledges_retrieve(knowledge.py:95-117)
    原样形态：
        return await qa_chain.acall(query, threshold=min_score,
                                    filter={'knowledge_base_id': knowledges},
                                    **kwargs)                     # :113-117
    （demo 的差异：生产每请求 ChainFactory.get_qa_chain 现场组链，demo 由
    调用方持有 RagComponents 组件包直接传入——检索本体都在 knowledge_qa）
    参数对照（v3 请求体字段，knowledge_v3.py KnowledgesRetrieveRequest）：
        knowledges     知识库 id 列表 → ES filter（terms 查询）
        max_recalls    最大召回数 → 检索的 top_k
        min_score      重排阈值 → threshold
        search_strategy 请求体里有、service 层当前未消费（生产 :95-117 同样未用），
                       demo 保留形参注明
        only_retrieval 只检索不生成（短路在 knowledge_qa 内，:82/:92 的
                       **kwargs 通道对应）
    （生产的 callbacks 通道——把 langfuse 的 LangchainCallbackHandler 挂进
    langchain 链逐级上报——demo 扁平化后无链可挂：各阶段直接调 obs 埋点，
    经 contextvars 挂到当前 trace，等价）
    返回 {"result": str, "source_documents": [KnowledgeRetrieveItem]}——
    即 knowledge_v3.py:140-145 响应体 data 的两个字段。"""
    retrieve_ret = await knowledge_qa(
        rag, query, threshold=min_score,
        filter={"knowledge_base_id": knowledges},
        only_retrieval=only_retrieval)

    # ---- 响应组装：knowledge_v3.py:93-138 ----
    new_source_documents = []
    images = []
    result = retrieve_ret.get('result', '')
    for knowledge in retrieve_ret.get('source_documents', []):
        # :102-112 Document → 响应条目（title 优先 name，退 source 文件名）
        title = knowledge.metadata.get("name") if "name" in knowledge.metadata else (
            os.path.split(knowledge.metadata.get("source", ""))[-1]
            if "source" in knowledge.metadata else "-")
        item = KnowledgeRetrieveItem(
            id=knowledge.metadata.get("_id", ""),
            content=knowledge.page_content,
            score=float(knowledge.metadata['score']) if 'score' in knowledge.metadata else -1,
            title=title,
            url=knowledge.metadata.get('source', ''),
            metadata=knowledge.metadata)
        new_source_documents.append(item)
        # :126-133 FAQ 命中：从 metadata['answer'] 提取 <img> 标签（图片随答案
        # 返回——入库时 img_clean 已把图片从正文剥掉，这里拼回展示位）
        if knowledge.metadata.get("is_faq") == "Y":
            pattern = r'<img\s+[^>]*src="[^"]*"[^>]*alt="[^"]*"[^>]*/?>'
            images.extend(re.findall(pattern, knowledge.metadata.get('answer', ''),
                                     re.IGNORECASE))
    # :135-138 图片统一拼在答案后
    if images:
        result = result + "".join(images)
    return {"result": result, "source_documents": new_source_documents}


# ===========================================================================
# ⑤ 会话层（单接口）—— assistant_run.py create_stream → AssistantService.stream
#    (assistant.py:1490-1618) 的非流式合并版
# ===========================================================================
async def assistant_ask(classifier: QuestionClassifier, rag: RagComponents,
                        query: str, knowledges: List[str],
                        max_recalls: int = 4, min_score: float = 0.5) -> Dict[str, Any]:
    """单个问答接口 = 生产两个接口的合并：
        生产：POST /assistant_run/stream（create_stream）内部分流
              └─ 知识类 → 工具调用 → POST /v3/knowledges/retrieve（knowledges_retrieve）
        demo：assistant_ask() 一次调用内完成两层，不拆 HTTP

    分流逻辑（对照 AssistantService.stream 的行号）：
      ① input_defence_by_prompt 输入安全校验（:1500）——demo 跳过（无安全词库）
      ② _classify_question_with_llm 意图识别（:1503-1506）
      ③ llm_response 有值（OTHER 类已直答）→ 直接返回（:1511-1527）
      ④ preset_response 有值 → 预设话术返回（:1530-1545）
      ⑤ MCP_QUERY → 生产 _stream_mcp_with_knowledge_fallback（:1547-1568，
         MCP 服务失败自动降级知识库）；demo 无 MCP 服务，按"降级"语义直接
         走知识检索（注释标明）
      ⑥ KNOWLEDGE_QUERY → 生产 agent astream_state 走知识库 tool →
         KnowledgeService.knowledges_retrieve → qa_chain.acall；demo 直连
         knowledges_retrieve（链路等价，少一层 agent/langgraph 编排）
    返回 {"type", "result", "source_documents", "route"}：route 标分流路径。"""
    # ② 意图识别
    classification_result = await classifier.classify_question(query)
    qtype = classification_result.get("type")

    # ③ OTHER 类：分类器里已让 LLM 自由作答（question_classifier.py:353-416）
    if classification_result.get("llm_response"):
        return {"type": qtype, "route": "llm_response",
                "result": classification_result["llm_response"],
                "source_documents": []}

    # ④ 预设回复（greeting/identity/...）
    if classification_result.get("preset_response"):
        preset = classifier.get_preset_content(classification_result["preset_response"])
        if preset:
            return {"type": qtype, "route": "preset_response",
                    "result": preset, "source_documents": []}

    # ⑤ MCP 分支：demo 无 MCP 服务 → 按生产降级语义走知识检索
    if qtype == QuestionType.MCP_QUERY:
        print("  [分流] mcp 类：demo 无 MCP 服务，按生产降级语义转知识检索", flush=True)

    # ⑥ 知识检索（KNOWLEDGE_QUERY 与 MCP 降级共用）
    ret = await knowledges_retrieve(rag, query, knowledges,
                                    max_recalls=max_recalls, min_score=min_score)
    return {"type": qtype, "route": "knowledge_retrieve",
            "result": ret["result"], "source_documents": ret["source_documents"]}


# ===========================================================================
# ⑥ 样例文件生成（word/pdf/md/excel/faq 五类各一）
# ===========================================================================
# 样例文件的唯一允许目录：脚本目录下的 uploads/（对应生产 ingest_doc_path
# 上传目录概念；写入前统一校验，防路径穿越）
_SCRIPT_DIR = Path(__file__).resolve().parent
_UPLOAD_DIR = _SCRIPT_DIR / "uploads"


def _sample_path(name: str) -> Path:
    """样例文件路径安全阀：文件名（固定字面量，不含目录段/..）拼进 uploads
    目录，resolve 后断言仍以脚本目录为前缀。"""
    if os.path.basename(name) != name or name in ("", ".", ".."):
        raise ValueError(f"非法样例文件名: {name!r}")
    _UPLOAD_DIR.mkdir(exist_ok=True)
    path = (_UPLOAD_DIR / name).resolve()
    if _SCRIPT_DIR not in path.parents:
        raise ValueError(f"路径越界被拒绝: {path}")
    return path


def _write_sample(name: str, data, binary: bool) -> Path:
    """样例落盘唯一出口：_sample_path 校验 + pathlib 写入（md 文本 / pdf 二进制）；
    被占用（正被编辑器/阅读器打开）时换名 name(n) 重试。"""
    for attempt in range(9):
        target = _sample_path(name if attempt == 0 else
                              f"{os.path.splitext(name)[0]}({attempt}){os.path.splitext(name)[1]}")
        try:
            if binary:
                target.write_bytes(data)
            else:
                target.write_text(data, encoding="utf-8")
            return target
        except PermissionError:
            continue
    raise PermissionError(name)


def _save_docx(doc, name: str) -> Path:
    """保存 docx 样例（python-docx 的 save）；被占用时换名，避免 PermissionError。"""
    path = _sample_path(name)
    try:
        doc.save(str(path))
        return path
    except PermissionError:
        root, ext = os.path.splitext(name)
        for i in range(1, 9):
            alt = _sample_path(f"{root}({i}){ext}")
            try:
                doc.save(str(alt))
                print(f"[样例] {name} 被占用，改存 {alt.name}", flush=True)
                return alt
            except PermissionError:
                continue
        raise


def make_sample_files() -> List[tuple]:
    """生成五类样例文件，返回 [(chain类型, 路径), ...]。

    内容刻意互相独立（PDCP=word、切换=md、传输=pdf、参数表=excel、
    密码重置=faq），让演示问题能明确验证"类型分派正确"。
    所有落盘路径经 _sample_path 校验（只允许脚本目录内的固定文件名）。"""

    # ---- 1) word 知识文档（两级 Heading + 参数正文）----
    d = docx_lib.Document()
    d.add_heading("PDCP参数说明", level=1)
    d.add_paragraph("本章描述PDCP层相关参数的取值与影响。")
    d.add_heading("1.1 丢弃定时器", level=2)
    d.add_paragraph("PDCP丢弃定时器（discardTimer）用于控制PDCP层丢弃数据包的时间门限。"
                    "默认值为500毫秒，取值范围50到3000毫秒。定时器超时后未确认的SDU将被丢弃，"
                    "该参数设置过小会导致误丢，设置过大时会增加时延。")
    d.add_heading("1.2 状态报告周期", level=2)
    d.add_paragraph("状态报告周期（statusReportPeriodic）控制PDCP向对端发送状态报告的间隔，"
                    "默认值为200毫秒。缩短周期可以加快丢包恢复，但会增加信令开销。")
    kb_docx = _save_docx(d, "sample_kb.docx")

    # ---- 2) md 知识文档（# 标题 → 转 docx Heading → 标题分块）----
    md_text = """# 切换参数手册

本章描述切换相关参数的取值与影响。

## A3事件偏置

A3事件偏置（a3Offset）用于触发基于邻区质量的切换判决，默认值为3分贝。
增大偏置会延迟切换，减小偏置会提前切换，过小可能导致乒乓切换。

## A5门限

A5事件门限（a5Threshold）用于基于服务小区质量恶化的切换判决，
门限1默认为-100dBm，门限2默认为-105dBm。服务小区质量低于门限1且
邻区质量高于门限2时触发切换测量上报。
"""
    md_path = _write_sample("sample_switch.md", md_text, binary=False)

    # ---- 3) pdf 知识文档 ----
    # 优先 Word COM 导出真 PDF（标准 PDF 文本层，MinerU 解析质量最好）；
    # 机器无 Word 时 fallback 用 fitz 造（内置中文字体 china-s，文本层质量
    # 略差——MinerU 会走 OCR 路径，英文单词可能被拆散，仅演示用）。
    def _pdf_by_word_com(docx_file: str, pdf_file: str) -> bool:
        try:
            import subprocess
            # 17 = Word 的 wdFormatPDF
            subprocess.run(["powershell", "-Command",
                            "$w = New-Object -ComObject Word.Application; "
                            "$w.Visible = $false; "
                            f"$d = $w.Documents.Open('{docx_file}'); "
                            f"$d.SaveAs([ref]'{pdf_file}', [ref]17); "
                            "$d.Close(); $w.Quit()"],
                           check=True, capture_output=True, timeout=120)
            return os.path.exists(pdf_file)
        except Exception:
            return False

    pd = docx_lib.Document()
    pd.add_heading("传输参数说明", level=1)
    pd.add_paragraph("传输带宽参数（transBandwidth）配置空口用户面传输带宽上限，"
                     "默认值为100Mbps，取值范围10到1000Mbps，"
                     "超过上限的流量将在传输层整形排队。")
    pd.add_heading("传输时延阈值", level=2)
    pd.add_paragraph("传输时延阈值（transDelayThreshold）定义传输质量劣化告警门限，"
                     "默认值为50毫秒。实际时延连续5个周期超过该阈值时触发"
                     "传输劣化告警，需检查承载网质量。")
    pdf_docx = _sample_path("_pdf_src.docx")
    pd.save(str(pdf_docx))
    pdf_path = _sample_path("sample_transport.pdf")
    if not _pdf_by_word_com(str(pdf_docx), str(pdf_path)):
        print("[样例] Word COM 不可用，fallback fitz 造 PDF", flush=True)
        import pymupdf as fitz
        pdf = fitz.open()
        page = pdf.new_page()
        y = 72
        page.insert_text((72, y), "传输参数说明", fontsize=18,
                         fontname="china-s")
        y += 28
        for line in ["传输带宽参数（transBandwidth）配置空口用户面传输带宽上限，默认值为100Mbps。",
                     "传输时延阈值（transDelayThreshold）定义传输质量劣化告警门限，默认值为50毫秒。"]:
            page.insert_text((72, y), line, fontsize=11, fontname="china-s")
            y += 20
        pdf_path.write_bytes(pdf.tobytes())
        pdf.close()

    # ---- 4) excel 参数表（'序号'前缀列 → 行主键 → "列名+行主键是值"三元组）----
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "参数表"
    ws.append(["序号", "参数名", "默认值", "取值范围", "说明"])
    ws.append([1, "discardTimer", "500毫秒", "50~3000毫秒", "PDCP丢弃定时器"])
    ws.append([2, "statusReportPeriodic", "200毫秒", "100~1000毫秒", "状态报告周期"])
    ws.append([3, "a3Offset", "3分贝", "0~15分贝", "A3事件偏置"])
    xlsx_path = _sample_path("sample_params.xlsx")
    wb.save(str(xlsx_path))

    # ---- 5) faq 文档 ×2：生产 FAQ 支持三种来源（ingest.py:689-700：
    #      excel 主用——按列号取问题/答案；csv；docx——Heading=问题/正文=答案），
    #      demo 两种主要形态各出一份 ----
    # 5a) excel FAQ（生产主用形态）：第1列=问题、第2列=答案（column_question=1/
    #     column_answer=2，1-based，ingest.py:711-712）
    fw = openpyxl.Workbook()
    fws = fw.active
    fws.title = "FAQ"
    fws.append(["问题", "答案"])
    fws.append(["如何备份设备配置？",
                "登录设备Web页面，进入系统工具→配置文件，点击导出配置并设置加密口令，"
                "导出文件包含全部运行参数。"])
    fws.append(["小区重建完成后参数还不生效怎么办？",
                "检查配置激活状态是否为已激活，并确认小区状态为正常；仍不生效则收集"
                "告警日志联系厂家支持。"])
    faq_xlsx = _sample_path("sample_faq.xlsx")
    fw.save(str(faq_xlsx))

    # 5b) docx FAQ：Heading=问题，正文=答案 → handle_faq_docx → DataFrame
    f = docx_lib.Document()
    f.add_heading("如何重置设备的管理员密码？", level=2)
    f.add_paragraph("登录设备Web页面，进入系统工具→配置文件，先备份配置再选择恢复出厂设置；"
                    "或使用CLI命令 reset admin-password 进行重置。")
    f.add_heading("discardTimer参数改了不生效怎么办？", level=2)
    f.add_paragraph("确认修改的是PDCP配置模板而非单站参数，修改后需要激活配置并等待小区重建完成。")
    faq_docx = _save_docx(f, "sample_faq.docx")

    return [
        ("knowledge_base", kb_docx),
        ("knowledge_base", md_path),
        ("knowledge_base", pdf_path),
        ("knowledge_base", xlsx_path),
        ("knowledge_faq", faq_xlsx),
        ("knowledge_faq", faq_docx),
    ]


# ===========================================================================
# ⑦ 端到端演示
# ===========================================================================
async def main(reset: bool = False):
    t_total = time.perf_counter()
    print("=" * 70, flush=True)
    print("rag-lab v3：多类型解析(word/pdf/md/excel/faq) → 意图识别 → 单接口检索问答", flush=True)
    print("=" * 70, flush=True)

    # 0) 真客户端（TEI嵌入/TEI重排/微信LLM，均过 host 白名单校验）
    print("[步骤0] 构建客户端…", flush=True)
    embedder, reranker, llm = build_clients()

    # 1) ES 存储与索引
    print("[步骤1] 连接 ES 并确保索引…", flush=True)
    store = ESStore(embedder)
    store.ensure_index(reset=reset)
    ingest = IngestService(store)

    # 2) 生成五类样例并按生产入口入库：
    #    普通知识 → file_ingest（FILE_LOADER_MAP 分派）；
    #    FAQ      → faq_batch_ingest（双份入库）
    print("[步骤2] 生成五类样例文件并入库…", flush=True)
    kb_id = "kb-demo"
    for chain, path in make_sample_files():
        t0 = time.perf_counter()
        if chain == "knowledge_faq":
            n = ingest.faq_batch_ingest(str(path), column_question=1, column_answer=2,
                                        knowledge_base_id=kb_id)
        else:
            n = ingest.file_ingest(chain, str(path), knowledge_base_id=kb_id)
        print(f"[入库] {os.path.basename(str(path))}（{chain}）→ {n} 块，"
              f"耗时 {time.perf_counter() - t0:.2f}s\n", flush=True)

    # 3) 检索组件 + 意图识别 + 单接口
    rag = RagComponents(store, embedder, reranker, llm, top_k=4)
    classifier = QuestionClassifier(llm)

    # 4) 各类问题走同一个接口 assistant_ask（生产 /assistant_run/stream 的合并版）
    #    覆盖：word/md/pdf/excel 知识、FAQ、预设类（identity/greeting）、
    #    other 直答、only_retrieval 评估模式
    questions = [
        "PDCP丢弃定时器的默认值是多少？",        # word 知识
        "A5事件的门限默认值是多少？",            # md 知识
        "传输时延阈值超过多少会告警？",          # pdf 知识
        "状态报告周期的取值范围是多少？",        # excel 知识（三元组句子）
        "如何重置设备的管理员密码？",            # FAQ 双份（docx 源）
        "如何备份设备配置？",                   # FAQ 双份（excel 源，生产主用形态）
        "你是谁",                               # identity → 预设
        "你好",                                 # greeting → 预设
        "今天天气怎么样",                       # other → LLM 直答
    ]
    for query in questions:
        print("-" * 70, flush=True)
        print(f"[提问] {query}", flush=True)
        t_q = time.perf_counter()
        ret = await assistant_ask(classifier, rag, query,
                                  knowledges=[kb_id], max_recalls=4, min_score=0.5)
        print(f"[回答]（type={ret['type'].value}, route={ret['route']}）", flush=True)
        print(f"  {ret['result'][:200]}", flush=True)
        if ret["source_documents"]:
            print(f"[引用] {len(ret['source_documents'])} 条：", flush=True)
            for item in ret["source_documents"][:5]:
                tag = "FAQ" if item.metadata.get("is_faq") == "Y" else "文档"
                print(f"    [{tag}] score={item.score:.3f} {item.title} | "
                      f"{item.content[:40]}...", flush=True)
        print(f"[本问耗时] {time.perf_counter() - t_q:.1f}s", flush=True)

    # 5) only_retrieval（v3 接口的评估模式：只检索不生成）
    print("-" * 70, flush=True)
    print("[提问·只检索] A3事件偏置", flush=True)
    r = await knowledges_retrieve(rag, "A3事件偏置", [kb_id], only_retrieval=True)
    print(f"[检索结果] {r['result'][:200]}...", flush=True)

    print("=" * 70, flush=True)
    print(f"[完成] 端到端总耗时 {time.perf_counter() - t_total:.1f}s", flush=True)


if __name__ == "__main__":
    asyncio.run(main(reset="--reset" in sys.argv))
