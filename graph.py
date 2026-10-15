# -*- coding: utf-8 -*-
"""
graph.py —— LangGraph 编排层（v5）：意图路由 → RAG / MCP / Hybrid / Direct

架构位置（"ES-only + LangGraph + LangChain + MCP"五模块中的【编排层】）：
  v4 的扁平 if/else 分流（rag_demo.assistant_ask / api_server.assistant_stream
  里的内联分支）收敛为本文件的一张 StateGraph——分支语义与 v4 完全一致
  （llm_response 直答 / preset 预设 / mcp 成功即答·失败降级检索 / 知识检索），
  v5 新增 hybrid 并行分支（LangGraph 相对扁平路由的增量价值所在）。

图拓扑：

    START → load_context → rewrite → classify
                                        │(条件路由)
        ┌──────────┬──────────┬─────────┼──────────────┐
        direct    preset      mcp ──失败降级──┐       hybrid=fan-out
        (直答)   (预设话术)    │成功          │      ┌── mcp ──┐
        │          │          └──→ persist   rag ←──┘          │（并行）
        │          │                        │       └── rag ──┤
        │          │                        │            summarize（汇总）
        │          │                        │                  │
        └──────────┴────────────────────────┴──────────────────┤
                                                             persist_turn → END

  · load_context/rewrite：Context Builder（会话历史读 ES + 追问改写）
  · mcp 失败降级：mcp_node 返回 mcp_result=None → 条件边转 rag_node
    （生产 _stream_mcp_with_knowledge_fallback 同款语义）
  · hybrid：分类为 hybrid_query 时 fan-out 并行跑 mcp_node ∥ rag_node，
    fan-in 到 summarize_node 合成答案（MCP 失败则汇总时如实说明）
  · persist_turn：所有终点统一经过——一轮问答落 ES（多轮历史的写入侧）

节点实现原则：编排层只做"路由与状态流转"，检索/分类/改写全部直调既有
函数（classifier.QuestionClassifier / sessions.rewrite_query /
rag_demo.knowledges_retrieve / mcp_client.McpTicketClient）——检索管线
本身（es_store 三路召回 + RRF + 重排）不在图里，行为与 v4 逐环节等价，
eval_ragas 直调 knowledges_retrieve 的路径不受影响。

checkpoint：compile(checkpointer=...) 挂自研 EsCheckpointer（调用方传
config={"configurable": {"thread_id": ...}}，图状态随每步落 ES，可回放）。
"""
import asyncio
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from langgraph.graph import END, START, StateGraph

from classifier import QuestionType
from clients import obs
from rag_demo import RagComponents, knowledges_retrieve
from sessions import rewrite_query

# 汇总节点提示词（hybrid 分支专属）：把并行的 MCP 工单结果与 RAG 知识结果
# 整合成一段连贯回答——不编造、失败如实说明
SUMMARIZE_PROMPT = """你是问答系统的汇总节点。用户提出了一个复合问题，系统已并行执行了工单查询和知识检索两部分。请把两部分结果整合成一段连贯、完整的回答。
要求：1.两部分都作答；2.不编造、不添加结果之外的信息；3.某部分无结果时如实说明；4.简洁专业。

用户问题：{question}

工单查询结果：
{mcp_result}

知识检索结果：
{kb_result}

整合后的回答："""


class GraphState(TypedDict, total=False):
    """图状态（全部 LastValue 通道；total=False——每个节点只写自己的键）。

    checkpoint 会把整个 state 序列化落 ES（JsonPlusSerializer 原生支持
    dataclass/Document/基本类型），回放/续跑即由此而来。"""
    question: str                    # 原始问题（展示与落历史用）
    thread_id: str                   # 会话标识（checkpointer/configurable 同款）
    transaction_id: str              # 链路追踪 id（落历史）
    idem_key: str                    # 网关幂等键（落历史，重放判断用）
    idem_doc_id: str                 # 幂等占位文档 _id（claim_idem 抢到的，
                                     # persist 回填它而非新建文档）
    history: List[Tuple[str, str]]   # load_context 读出的最近轮次
    rewritten_query: str             # 改写后的独立问题（分类/检索/MCP 用）
    classification: Dict[str, Any]   # 分类器返回
    mcp_result: Optional[str]        # MCP 工单结果（None=服务失败）
    kb_answer: str                   # 知识检索答案文本（summarize 用；不存
                                     # 完整 kb_result——引用已在
                                     # source_documents，避免 checkpoint 重复序列化）
    answer: str                      # 最终回答
    source_documents: List[Any]      # 引用条目（KnowledgeRetrieveItem）
    route: str                       # 展示路由：llm_response/preset_response/
                                     # mcp/knowledge_retrieve/hybrid


def build_graph(classifier, rag: RagComponents, mcp, llm, chat_store,
                checkpointer=None, knowledges: List[str] = None,
                max_recalls: int = 4, min_score: float = 0.5):
    """组装并编译图。组件由调用方持有（demo 单例；生产是每 assistant 一图）。

    knowledges/max_recalls/min_score 对应 v3 检索接口的默认请求参数
    （v4 里硬编码在调用点，v5 收口到建图参数）。"""
    knowledges = knowledges or ["kb-demo"]

    # ------------------------------------------------------------------
    # Context Builder：读历史 + 追问改写
    # （生产对照：AssistantService.stream 组装 thread 历史传给 agent，
    #  demo 拆成图的前两个节点——历史来源是 ES 而非进程内存）
    # ------------------------------------------------------------------
    async def load_context(state: GraphState) -> Dict:
        """Context Builder 的读取侧：从 ES 捞该 thread 最近 N 轮。

        为什么是节点而不是调用方先查好再传入：让"会话上下文怎么来"
        成为图拓扑的一部分——CLI（rag_demo）与 HTTP（api_server）两个
        入口共享同一段编排，而不是各自实现一遍（v4 的教训：改写逻辑
        写在 api_server 里，脚本入口就没有多轮能力）。"""
        thread_id = state["thread_id"]
        with obs.span("graph_load_context", input={"thread_id": thread_id}) as s:
            # ES 是同步 IO 客户端：丢线程池，避免阻塞事件循环；to_thread
            # 会复制当前 contextvars，本 span 才能挂到请求入口设的 trace 上
            history = await asyncio.to_thread(chat_store.history, thread_id)
            if s:
                s.update(output={"turns": len(history)})
        # 只写 history 键——节点返回的是 state 增量（delta），未提及的键不动
        return {"history": history}

    async def rewrite(state: GraphState) -> Dict:
        """Context Builder 的改写侧：把"那它的取值范围呢"这类依赖上下文的
        追问改写成独立问题（sessions.rewrite_query，最近 3 轮消解指代）。

        关键语义：改写结果进【分类/检索/MCP】，原始问题用于展示与落历史
        ——用户看到的始终是自己问的原话。改写失败/无历史时原样返回
        （增强不是依赖，降级不阻断主链路）。"""
        question = state["question"]
        history = state.get("history") or []
        with obs.span("graph_rewrite", input={"question": question[:60]}) as s:
            rewritten = await rewrite_query(llm, question, history)
            if s:
                s.update(output={"rewritten": rewritten[:60]})
        return {"rewritten_query": rewritten}

    # ------------------------------------------------------------------
    # 意图识别 + 条件路由
    # （生产对照：_classify_question_with_llm assistant.py:1503-1506，
    #  随后的 if/else 分流 :1511-1568 —— demo 把这段 if/else 图化）
    # ------------------------------------------------------------------
    async def classify(state: GraphState) -> Dict:
        """意图分类节点：一次 LLM 分类调用换确定性路由（classify-then-route
        模式，不是 ReAct——工具可预枚举时静态路由省 token、可预测、可测试）。

        注意喂给分类器的是【改写后】的问题：追问场景下原始问题
        （"那它呢"）无法分类，必须用消解过指代的独立问题。"""
        with obs.span("graph_classify",
                      input={"question": state["rewritten_query"][:60]}) as s:
            classification = await classifier.classify_question(
                state["rewritten_query"])
            if s:
                s.update(output={"type": str(classification.get("type"))})
        return {"classification": classification}

    def _route_after_classify(state: GraphState):
        """classify 之后的条件路由——v4 内联 if/else 的图化，分支优先级
        与生产 AssistantService.stream 一致：

          ① llm_response 有值（OTHER 类，分类器内已让 LLM 直答）
             → direct            （生产 :1511-1527）
          ② preset_response 有值（identity/greeting 等预设话术）
             → preset            （生产 :1530-1545）
          ③ HYBRID_QUERY（工单+知识复合）
             → ["mcp", "rag"]    fan-out 并行双节点（demo 超出生产的能力）
          ④ MCP_QUERY
             → mcp               （生产 :1547-1568，失败降级在 _after_mcp）
          ⑤ 兜底（KNOWLEDGE_QUERY / 分类异常回落）
             → rag               （生产 :1609-1616 走 agent，demo 直连检索）

        返回【节点名列表】是 LangGraph 条件边的原生 fan-out 语义：单个
        字符串走串行分支，列表则同 superstep 并行执行再 fan-in。"""
        c = state["classification"]
        if c.get("llm_response"):
            return "direct"
        if c.get("preset_response"):
            return "preset"
        qtype = c.get("type")
        if qtype == QuestionType.HYBRID_QUERY:
            return ["mcp", "rag"]              # fan-out：并行双分支
        if qtype == QuestionType.MCP_QUERY:
            return "mcp"
        return "rag"                            # knowledge/兜底/分类异常

    def _is_hybrid(state: GraphState) -> bool:
        return state["classification"].get("type") == QuestionType.HYBRID_QUERY

    # ------------------------------------------------------------------
    # 执行节点：direct / preset / mcp / rag / summarize
    # ------------------------------------------------------------------
    async def direct(state: GraphState) -> Dict:
        """OTHER 类直答：分类器内已让 LLM 自由作答（question_classifier
        :353-416，temp=0.7），这里只取结果兜个底话术——不再检索不重排，
        省一整条 RAG 管线的算力（生产 :1511-1527 同款"直接返回"）。"""
        answer = state["classification"].get("llm_response") \
                 or "抱歉，我暂时无法回答这个问题。"
        return {"answer": answer, "source_documents": [], "route": "llm_response"}

    async def preset(state: GraphState) -> Dict:
        """预设话术节点：identity/greeting/scope 等 6 类固定回复查表直出
        （生产 :1530-1545 → preset_responses 话术表）。"""
        content = classifier.get_preset_content(
            state["classification"].get("preset_response")) \
            or "抱歉，我暂时无法回答这个问题。"
        return {"answer": content, "source_documents": [], "route": "preset_response"}

    async def mcp_node(state: GraphState) -> Dict:
        """MCP 工单查询节点（复用 mcp_client：mock/HTTP 双模式，成功返回
        字符串、服务失败返回 None——None 由条件边转 rag 降级）。

        两种失败的区分（生产 _stream_mcp_with_knowledge_fallback 的语义）：
          · 业务性无结果（"查无此单"/"请提供工单编号"）= 正常字符串答案
          · 服务不可用（超时/连接失败/5xx）= None → 降级知识检索
        只有后者降级——前者本身就是该给用户的回答。

        非 hybrid 且成功时直接作答（route=mcp，无引用）；hybrid 路只产出
        mcp_result，答案由 summarize 汇总（同一节点两种角色，靠 _is_hybrid
        区分——fan-out 里 mcp 与 rag 同 superstep 并行，各写各的键不冲突）。"""
        result = await mcp.aquery(state["rewritten_query"])
        update = {"mcp_result": result}
        if not _is_hybrid(state) and result is not None:
            update.update({"answer": result, "source_documents": [],
                           "route": "mcp"})
        return update

    def _after_mcp(state: GraphState):
        """mcp 之后的条件边（一个函数管三种去向）：
          hybrid → summarize   fan-in：等并行 rag 路也完成
          成功   → persist      直接作答收口
          失败   → rag          生产降级语义：服务挂了转知识检索"""
        if _is_hybrid(state):
            return "summarize"
        if state.get("mcp_result") is not None:
            return "persist"
        print("  [分流] mcp 查询失败，按生产降级语义转知识检索", flush=True)
        return "rag"

    async def rag_node(state: GraphState) -> Dict:
        """知识检索节点——直调 v4 同款检索服务 knowledges_retrieve（双通道
        → 三路召回 → RRF → 去重 → 重排 → 合并 → 生成），检索管线本体
        【不在图内】：节点只是薄壳，埋点经 contextvars 自动挂当前 trace。

        为什么检索内核不进图：① 行为与 v4 逐环节等价（eval 无翻转证明），
        eval_ragas 直调 knowledges_retrieve 的路径不受图演进影响；
        ② 检索是"纯计算"没有分支拓扑，包成图节点徒增序列化开销。"""
        ret = await knowledges_retrieve(rag, state["rewritten_query"], knowledges,
                                        max_recalls=max_recalls, min_score=min_score)
        # state 只留两样：引用条目（响应组装用）+ 答案文本（hybrid 汇总用）。
        # 不存完整 kb_result——它与 source_documents 内容重复，checkpoint
        # 每步序列化全量 state，翻倍存实测 28KB/问，瘦身后 9KB/问
        update = {"source_documents": ret["source_documents"],
                  "kb_answer": ret["result"] or "没有匹配到相关知识。"}
        if not _is_hybrid(state):               # hybrid 的 answer 由 summarize 定
            update["answer"] = ret["result"] or "没有匹配到相关知识，无法回答该问题。"
            update["route"] = "knowledge_retrieve"
        return update

    def _after_rag(state: GraphState):
        """rag 之后的条件边：hybrid → summarize（fan-in）；其余 → persist。
        mcp 降级进来的也走这里（route 已不是 hybrid，直接收口）。"""
        return "summarize" if _is_hybrid(state) else "persist"

    async def summarize(state: GraphState) -> Dict:
        """hybrid 汇总节点（fan-in 收口）：把并行两路的结果整合成连贯回答。

        三层容错：
          ① mcp_result 为 None（服务失败）→ 汇总文本如实注明"暂不可用"，
             知识部分照答——降级不静默，用户知情；
          ② kb_answer 无命中 → "没有匹配到相关知识"，同样如实呈现；
          ③ LLM 汇总调用失败 → 降级为两段原文拼接（不阻断，答案仍完整）。
        引用保留 rag 路的 source_documents（知识部分的出处）。"""
        mcp_result = state.get("mcp_result")
        kb_answer = state.get("kb_answer") or "没有匹配到相关知识。"
        mcp_text = mcp_result if mcp_result is not None else "（工单查询服务暂不可用）"
        try:
            answer = await llm.ainvoke(SUMMARIZE_PROMPT.format(
                question=state["question"], mcp_result=mcp_text,
                kb_result=kb_answer))
            answer = answer if isinstance(answer, str) else str(
                getattr(answer, "content", answer))
        except Exception as ex:
            print(f"  [汇总] LLM 汇总失败，降级拼接: {ex}", flush=True)
            answer = f"工单查询：{mcp_text}\n知识检索：{kb_answer}"
        docs = state.get("source_documents") or []
        return {"answer": answer, "source_documents": docs, "route": "hybrid"}

    # ------------------------------------------------------------------
    # 终点统一节点：落历史（多轮会话的写入侧）
    # ------------------------------------------------------------------
    async def persist(state: GraphState) -> Dict:
        """所有分支的统一收口：把本轮 (question, answer, route) 落 ES 聊天
        历史——下一轮 load_context 读回来做多轮改写，闭环由此形成。

        统一收口而非各分支各写各的两个原因：① 落库字段/时机只定义一次，
        新增分支（比如以后的工单创建类）自动获得会话记忆；② citations
        的构造规则（哪些 route 带、什么格式）集中在一处，与 api_server
        重放侧的重建逻辑一一对应。

        idem_doc_id：网关幂等占位文档的 _id（claim_idem 抢到后经
        ask_graph 传入）——回填该文档而不是新建，保证同键至多一条。"""
        answer = state.get("answer") or ""
        if answer:                               # 空答案不落库（防御）
            # 知识/混合路带引用：紧凑形态随轮次落库，幂等重放时据此重建
            # ToolMessage（重放的 SSE 事件序列需与首次一致——v5.1 修的
            # 契约缺口：重放只有 AI 消息没有引用消息）
            citations = None
            if state.get("route") in ("knowledge_retrieve", "hybrid"):
                citations = [{"title": d.title, "score": d.score,
                              "content": d.content[:100], "url": d.url}
                             for d in state.get("source_documents") or []]
            await asyncio.to_thread(
                chat_store.append_turn, state["thread_id"], state["question"],
                answer, state.get("route", ""), state.get("transaction_id", ""),
                state.get("idem_key") or None, citations,
                state.get("idem_doc_id") or None)
        return {}                                # 无状态更新（终点节点）

    # ------------------------------------------------------------------
    # 组装：节点注册 + 边接线（拓扑图见模块 docstring）
    # ------------------------------------------------------------------
    g = StateGraph(GraphState)
    g.add_node("load_context", load_context)     # Context Builder 读取侧
    g.add_node("rewrite", rewrite)               # Context Builder 改写侧
    g.add_node("classify", classify)             # 意图分类（一次 LLM 调用）
    g.add_node("direct", direct)                 # OTHER 直答
    g.add_node("preset", preset)                 # 预设话术
    g.add_node("mcp", mcp_node)                  # 工单查询（失败降级）
    g.add_node("rag", rag_node)                  # 知识检索（内核在图外）
    g.add_node("summarize", summarize)           # hybrid 汇总（fan-in）
    g.add_node("persist", persist)               # 统一落库收口

    # 主干：上下文 → 改写 → 分类 → 条件路由（五路分流）
    g.add_edge(START, "load_context")
    g.add_edge("load_context", "rewrite")
    g.add_edge("rewrite", "classify")
    g.add_conditional_edges("classify", _route_after_classify)
    # 各分支收口：direct/preset/summarize 直接到 persist；
    # mcp/rag 各有一条条件边（降级 / fan-in 判断）后再到 persist
    g.add_edge("direct", "persist")
    g.add_edge("preset", "persist")
    g.add_conditional_edges("mcp", _after_mcp)
    g.add_conditional_edges("rag", _after_rag)
    g.add_edge("summarize", "persist")
    g.add_edge("persist", END)

    # compile 挂 checkpointer：此后每次 ainvoke 的每一步状态都落 ES
    # （EsCheckpointer），同 thread_id 可回放/断点续跑（aget_state 系列）
    return g.compile(checkpointer=checkpointer)


async def ask_graph(compiled_graph, question: str, thread_id: str,
                    transaction_id: str = "", idem_key: str = "",
                    idem_doc_id: str = "") -> Dict[str, Any]:
    """统一调用入口：构造初始 state + checkpointer 的 configurable thread_id，
    返回最终 state（answer/route/source_documents/...）。

    thread_id 同时是 ES 聊天历史键与 checkpoint 键——同一 thread 连续调用，
    多轮改写与图状态回放都指向同一条会话。idem_doc_id 是网关幂等占位文档
    的 _id（ChatStore.claim_idem 抢到后传入，persist 回填该文档）。"""
    return await compiled_graph.ainvoke(
        {"question": question, "thread_id": thread_id,
         "transaction_id": transaction_id, "idem_key": idem_key,
         "idem_doc_id": idem_doc_id},
        config={"configurable": {"thread_id": thread_id}})
