# -*- coding: utf-8 -*-
"""
es_persist.py —— ES-only 业务持久化（v5）：聊天记录 / 入库任务 / 用户反馈

架构定位（对照"ES-only + LangGraph"五模块里的【数据层】）：
  生产 aiops-agent-core 把会话历史放 langgraph checkpoint（MySQL/PG）、任务
  状态放业务库；本 demo 按 ES-only 约束，把所有需要持久化的业务数据统一落在
  同一个 ES 集群（与 RAG 索引 rag_lab_demo 共存），重启/多 worker 均不丢：

  · ChatStore      → raglab_chat_messages：多轮会话历史（Context Builder 的
                     数据源，graph.load_context 读它做追问改写）+ 幂等键索引
  · EsTaskRegistry → raglab_tasks：异步入库任务状态（替代 v4 的进程内 dict，
                     服务重启后 /tasks/{id} 仍可查——这是 ES 化的直接收益）
  · FeedbackStore  → raglab_feedback：用户点赞/点踩（替代 v4 的 jsonl 文件）

  索引 mapping 全部走动态映射（结构简单的业务文档无需显式 mapping；查询
  字段 thread_id/status 等用 .keyword 精确匹配，与 RAG 索引的 metadata
  过滤同一套路）。客户端从 es_store.make_es_client 出（check_url 白名单
  钉在唯一出口）。
"""
import time
import uuid
from typing import Dict, List, Optional, Tuple

import config
from es_store import make_es_client


def _now_ms() -> int:
    """毫秒时间戳（ES date 字段用 epoch_millis，数值排序天然正确）。"""
    return int(time.time() * 1000)


class ChatStore:
    """多轮会话历史 + 幂等键 —— raglab_chat_messages。

    v4 的 SessionStore 是进程内 dict（重启即丢）；本类同接口语义但落 ES：
      history()  term thread_id + ts 倒序取最近 N 轮，并用 ts>now-ttl 过滤
                 （TTL 从"内存懒清理"变成"查询时忽略"，语义等价）
      append_turn()  一轮问答一条 doc（含 route/transaction_id/幂等键）
    """

    def __init__(self, index_name: str = None, request_timeout: int = 15):
        self.index_name = index_name or config.CHAT_INDEX_NAME
        self.client = make_es_client(request_timeout=request_timeout)

    def ensure_index(self):
        """索引不存在则创建。显式 mapping（而非动态映射）：history/find_by_idem
        要在可能为空的索引上按 ts 排序——动态映射下空索引没有 ts 字段，
        ES 会报 "No mapping found for [ts]"，显式声明后空索引也能查。"""
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, mappings={
                "properties": {
                    "thread_id": {"type": "keyword"},
                    "question": {"type": "text"},
                    "answer": {"type": "text"},
                    "route": {"type": "keyword"},
                    "transaction_id": {"type": "keyword"},
                    "idem_key": {"type": "keyword"},
                    "ts": {"type": "long"},
                }})
            print(f"[ES] 已创建会话索引 {self.index_name}", flush=True)

    def append_turn(self, thread_id: str, question: str, answer: str,
                    route: str = "", transaction_id: str = "",
                    idem_key: str = None, citations: List[Dict] = None,
                    doc_id: str = None):
        """记一轮问答。answer 原样全文落库（幂等重放时直接取回）；
        citations 为知识引用的紧凑形态（title/score/content/url），随轮次
        落库——幂等重放时据此重建 ToolMessage（SSE 契约要求重放与首次
        事件序列一致）。doc_id 用于幂等占位文档回填（见 claim_idem）。"""
        doc = {
            "thread_id": thread_id,
            "question": question,
            "answer": answer,
            "route": route,
            "transaction_id": transaction_id,
            "idem_key": idem_key or "",
            "citations": citations or [],
            "ts": _now_ms(),
        }
        self.client.index(index=self.index_name,
                          id=doc_id or f"{thread_id}:{uuid.uuid4().hex[:12]}",
                          document=doc, refresh=True)

    def claim_idem(self, idem_key: str, thread_id: str) -> Optional[Dict]:
        """幂等键原子占位（修 TOCTOU 竞态：两个并发同键请求只有一个能建文档）。

        _id 固定为 "idem:{key}"，op_type=create 抢占：
          · 抢占成功 → 返回 None（调用方继续执行图，结束后回填该文档）
          · 已存在  → 返回已存文档（有 answer 则直接重放；answer 为空说明
            首个同键请求仍在途——调用方按普通请求执行即可，孤占位无害）
        ES 的 create 是版本级原子操作，不等价于"先查后写"。"""
        if not idem_key:
            return None
        try:
            self.client.index(index=self.index_name, id=f"idem:{idem_key}",
                              document={"thread_id": thread_id, "question": "",
                                        "answer": "", "route": "",
                                        "transaction_id": "",
                                        "idem_key": idem_key, "citations": [],
                                        "ts": _now_ms()},
                              op_type="create", refresh=True)
            return None
        except Exception:
            return self.find_by_idem(idem_key)

    def history(self, thread_id: str, max_turns: int = 6,
                ttl_s: int = 3600) -> List[Tuple[str, str]]:
        """最近 N 轮 (question, answer)，按时间正序返回（旧→新，喂改写
        prompt 的"最近对话"格式）。

        查询形态：term thread_id 精确匹配 + range ts 过滤 + ts 倒序取
        max_turns 条。TTL 的语义是【按轮次年龄过滤】（超过 1 小时的轮次
        不进上下文）——与 v4 进程内存版"按会话最后活跃过期"略有差异：
        长会话里老轮次会更早掉出改写窗口，对"最近 3 轮消解指代"的场景
        无实际影响。倒序取完 reversed 回正序，省去 ES 的升序深分页。"""
        since = _now_ms() - ttl_s * 1000
        resp = self.client.search(index=self.index_name, query={
            "bool": {"filter": [
                {"term": {"thread_id": thread_id}},
                {"range": {"ts": {"gte": since}}},
            ]}},
            sort=[{"ts": {"order": "desc"}}], size=max_turns)
        turns = [(h["_source"]["question"], h["_source"]["answer"])
                 for h in resp["hits"]["hits"]]
        return list(reversed(turns))                      # 正序：旧→新

    def find_by_idem(self, idem_key: str) -> Optional[Dict]:
        """按幂等键找最近一轮（网关重放判断；命中则不再执行图）。"""
        if not idem_key:
            return None
        resp = self.client.search(index=self.index_name, query={
            "bool": {"filter": [
                {"term": {"idem_key": idem_key}},
            ]}},
            sort=[{"ts": {"order": "desc"}}], size=1)
        hits = resp["hits"]["hits"]
        return hits[0]["_source"] if hits else None


class EsTaskRegistry:
    """入库任务状态 —— raglab_tasks（替代 v4 进程内 dict，重启可查）。

    方法签名与 v4 的 IngestTaskRegistry 完全一致（create/finish/get），
    api_server 的调用点零改动。"""

    def __init__(self, index_name: str = None, request_timeout: int = 15):
        self.index_name = index_name or config.TASK_INDEX_NAME
        self.client = make_es_client(request_timeout=request_timeout)

    def ensure_index(self):
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name)
            print(f"[ES] 已创建任务索引 {self.index_name}", flush=True)

    def create(self, file_name: str, chain: str, kb_id: str) -> str:
        """登记新任务（status=running），返回 task_id——直接用 ES 文档 _id
        （12 位 hex，不可猜测），上传接口 202 响应把它带给客户端轮询。"""
        task_id = uuid.uuid4().hex[:12]
        self.client.index(index=self.index_name, id=task_id, document={
            "task_id": task_id, "file": file_name, "chain": chain,
            "knowledge_base_id": kb_id, "status": "running",
            "chunks": None, "replaced": None, "error": None,
            "started_at": _now_ms(), "finished_at": None,
        }, refresh=True)          # 立即可轮询（客户端拿到 202 马上会查）
        return task_id

    def finish(self, task_id: str, chunks: int, replaced: int, error: str = None):
        """任务落定（done/failed）——doc 局部更新（只改状态相关字段，
        started_at 等登记信息不动）。后台任务在 BackgroundTasks 线程里
        调本方法，与请求无共享内存——状态流转全靠 ES 文档。"""
        self.client.update(index=self.index_name, id=task_id, body={
            "doc": {"status": "failed" if error else "done", "chunks": chunks,
                    "replaced": replaced, "error": error,
                    "finished_at": _now_ms()}}, refresh=True)

    def get(self, task_id: str) -> Optional[Dict]:
        """查任务；不存在（或索引还没建）返回 None——与 v4 dict.get 一致，
        调用方（任务接口）据此回 404。宽恕式捕获：ES 瞬断也当"查不到"
        而非 500（demo 级取舍，注释注明）。"""
        try:
            resp = self.client.get(index=self.index_name, id=task_id)
            return resp["_source"]
        except Exception:
            return None


class FeedbackStore:
    """用户反馈 —— raglab_feedback（替代 v4 的 feedback/feedback.jsonl）。"""

    def __init__(self, index_name: str = None, request_timeout: int = 15):
        self.index_name = index_name or config.FEEDBACK_INDEX_NAME
        self.client = make_es_client(request_timeout=request_timeout)

    def ensure_index(self):
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name)
            print(f"[ES] 已创建反馈索引 {self.index_name}", flush=True)

    def add(self, payload: Dict):
        """一条反馈一个 doc（字段同 v4 jsonl：question/answer/rating/route/
        thread_id + ts）。"""
        doc = dict(payload)
        doc["ts"] = _now_ms()
        self.client.index(index=self.index_name,
                          id=uuid.uuid4().hex[:12], document=doc, refresh=True)
