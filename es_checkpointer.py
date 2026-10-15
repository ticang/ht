# -*- coding: utf-8 -*-
"""
es_checkpointer.py —— 自研 LangGraph ES Checkpointer（v5，ES-only 持久化）

为什么自研：LangGraph 官方只提供 sqlite/postgres/mongo/mysql 等 Checkpointer
包，没有 ES 版（2026-10 已核实 PyPI/GitHub 均无）；生产 aiops-agent-core 同样
是自研（agent_core/assitant/checkpoint/mysql_checkpoint.py 继承
BaseCheckpointSaver 实现 aput/aget_tuple/alist）——本文件即 demo 版的同类做法，
把生产"MySQLCheckpoint"的存储介质换成 ES（对齐 ES-only 约束）。

接口（对齐本机 langgraph-checkpoint 4.2.0 的 BaseCheckpointSaver）：
  put/put_writes/get_tuple/list/delete_thread + async 双面（async = to_thread
  包同步实现；基类 async 桩各自独立，无自动桥接）。

存储模型（单索引 raglab_checkpoints，双文档类型）：
  type=checkpoint  _id = "{thread_id}|{ns}|{checkpoint_id}"
                   blob = serde.dumps_typed(checkpoint)，meta_blob = 同款序列化
                   metadata；parent_checkpoint_id 用于回放父链
  type=write       _id = "{thread_id}|{ns}|{cp_id}|{task_id}|{idx}"
                   put_writes 对 channel 非 None 的写入先删同键再插（upsert，
                   对齐官方 sqlite saver 语义——节点重试时覆盖而非追加）

排序依据：checkpoint_id 是 uuid6（时间有序），字典序 = 时间序，ES keyword
倒序即"最新优先"——与 MemorySaver 的 max(checkpoints.keys()) 同理。

序列化：走 serde.dumps_typed/loads_typed（JsonPlusSerializer 默认），存
(type_tag, base64 blob) 二元组，loads 时按 tag 还原——协议要求的标准通路。

能力边界（demo 注记）：
  · list(filter=...) 仅支持元数据列 meta_source/meta_step 的浅过滤（官方
    saver 支持任意 metadata 过滤；demo 图不使用该参数，够用即可）
  · delete_for_runs / copy_thread / prune（DeltaChannel 高级运维面）未实现，
    基类桩保持 NotImplementedError
"""
import asyncio
import base64
from datetime import datetime
from typing import Any, Iterator, List, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
    get_checkpoint_id,
)

import config
from elasticsearch.helpers import bulk

from es_store import make_es_client


def _iso_to_ms(ts: str) -> int:
    """checkpoint['ts']（ISO8601）→ epoch 毫秒（排序辅助列；解析失败给 0）。"""
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00"))
                   .timestamp() * 1000)
    except Exception:
        return 0


class EsCheckpointer(BaseCheckpointSaver):
    """ES 后端的 LangGraph Checkpointer（同步实现 + async 桥接）。"""

    def __init__(self, index_name: str = None, request_timeout: int = 30):
        super().__init__()
        self.index_name = index_name or config.CHECKPOINT_INDEX_NAME
        self.client = make_es_client(request_timeout=request_timeout)

    # ------------------------------------------------------------------
    # 索引管理
    # ------------------------------------------------------------------
    def ensure_index(self):
        """不存在则创建。blob 用 binary 类型（base64、默认不索引、无长度限制）。"""
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, mappings={
                "properties": {
                    "type": {"type": "keyword"},            # checkpoint | write
                    "thread_id": {"type": "keyword"},
                    "checkpoint_ns": {"type": "keyword"},
                    "checkpoint_id": {"type": "keyword"},
                    "parent_checkpoint_id": {"type": "keyword"},
                    "task_id": {"type": "keyword"},
                    "task_path": {"type": "keyword"},
                    "idx": {"type": "integer"},
                    "channel": {"type": "keyword"},
                    "type_tag": {"type": "keyword"},        # serde 的类型标签
                    "blob": {"type": "binary"},
                    "ts": {"type": "long"},                 # epoch ms（辅助排序）
                    "meta_source": {"type": "keyword"},     # metadata 浅过滤列
                    "meta_step": {"type": "long"},
                }})
            print(f"[ES] 已创建 checkpoint 索引 {self.index_name}", flush=True)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _dump(self, obj: Any) -> Tuple[str, str]:
        """对象 → (type_tag, base64 blob)——serde 协议的标准落库形态。

        为什么用 dumps_typed 而不是 dumps：typed 版本多带一个类型标签，
        loads 时按标签还原（JsonPlusSerializer 靠它区分 dataclass/enum/
        基本类型），官方 postgres/sqlite saver 同款通路。base64 是因为
        ES 的 binary 字段只收 base64 字符串。"""
        tag, blob = self.serde.dumps_typed(obj)
        return tag, base64.b64encode(blob).decode("ascii")

    def _load(self, tag: str, b64: str) -> Any:
        """_dump 的逆：按类型标签还原对象。"""
        return self.serde.loads_typed((tag, base64.b64decode(b64)))

    def _writes_for(self, thread_id: str, ns: str,
                    checkpoint_id: str) -> List[PendingWrite]:
        """取某 checkpoint 的全部 pending writes（节点任务写）。

        读回侧的形态是 (task_id, channel, value) 三元组——与写入侧
        put_writes 收到的 (channel, value) 二元组不同（基类协议如此，
        实测踩过：按三元组解包写入侧直接 ValueError）。按 idx 正序保证
        重放顺序=写入顺序；单条 blob 损坏跳过不拖垮整链回放。"""
        resp = self.client.search(index=self.index_name, query={
            "bool": {"filter": [
                {"term": {"type": "write"}},
                {"term": {"thread_id": thread_id}},
                {"term": {"checkpoint_ns": ns}},
                {"term": {"checkpoint_id": checkpoint_id}},
            ]}},
            sort=[{"idx": {"order": "asc"}}], size=200)
        writes: List[PendingWrite] = []
        for hit in resp["hits"]["hits"]:
            src = hit["_source"]
            try:
                writes.append((src["task_id"], src["channel"],
                               self._load(src["type_tag"], src["blob"])))
            except Exception:                       # 单条损坏不拖垮整链回放
                continue
        return writes

    def _row_to_tuple(self, src: dict) -> CheckpointTuple:
        """checkpoint 文档（ES _source）→ CheckpointTuple 的组装器，
        get_tuple/list 共用。四件事：
          ① config：重建 configurable（thread_id/checkpoint_ns/checkpoint_id
             三元组——langgraph 靠它在后续操作中定位这条快照）；
          ② checkpoint 主体 + metadata：两个 blob 分别反序列化；
          ③ parent_config：指向父快照（时间旅行的链）；
          ④ pending_writes：补挂该快照上的节点中间写。"""
        thread_id, ns, cp_id = src["thread_id"], src["checkpoint_ns"], src["checkpoint_id"]
        config = {"configurable": {"thread_id": thread_id,
                                   "checkpoint_ns": ns,
                                   "checkpoint_id": cp_id}}
        parent_config = None
        if src.get("parent_checkpoint_id"):
            parent_config = {"configurable": {
                "thread_id": thread_id, "checkpoint_ns": ns,
                "checkpoint_id": src["parent_checkpoint_id"]}}
        return CheckpointTuple(
            config=config,
            checkpoint=self._load(src["type_tag"], src["blob"]),
            metadata=self._load(src["meta_type_tag"], src["meta_blob"]),
            parent_config=parent_config,
            pending_writes=self._writes_for(thread_id, ns, cp_id),
        )

    # ------------------------------------------------------------------
    # 写：put / put_writes（同步实现，async 桥接见下）
    # ------------------------------------------------------------------
    def put(self, config: RunnableConfig, checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
            new_versions: ChannelVersions) -> RunnableConfig:
        """存一个 checkpoint 快照——图每执行完一个 superstep 调一次。

        _id 用 thread|ns|cp_id 三段拼接：同一快照重复写入是覆盖（幂等），
        不同步骤天然不同 _id。返回更新后的 config（带本快照 id），langgraph
        用它作为下一步的父指针。"""
        thread_id = config["configurable"]["thread_id"]
        ns = config["configurable"].get("checkpoint_ns", "")
        cp_id = checkpoint["id"]
        # 父 checkpoint id 取自传入 config（官方 saver 同款语义：put 收到的
        # config 指向父快照）——回放/时间旅行靠 parent 链逐级上溯
        configurable_parent = config["configurable"].get("checkpoint_id") or ""
        tag, blob = self._dump(checkpoint)
        meta_tag, meta_blob = self._dump(metadata)
        self.client.index(index=self.index_name,
                          id=f"{thread_id}|{ns}|{cp_id}", document={
                              "type": "checkpoint",
                              "thread_id": thread_id, "checkpoint_ns": ns,
                              "checkpoint_id": cp_id,
                              "parent_checkpoint_id": configurable_parent,
                              # 主体与元数据分开序列化：metadata.step/source
                              # 是 list()/时间旅行的高频过滤字段
                              "type_tag": tag, "blob": blob,
                              "meta_type_tag": meta_tag, "meta_blob": meta_blob,
                              # ts 是 ISO 字符串转的 epoch 毫秒辅助列：
                              # 排序兜底 + 人工排查时可读
                              "ts": _iso_to_ms(checkpoint.get("ts", "")),
                              "meta_source": str(metadata.get("source", "")),
                              "meta_step": metadata.get("step"),
                          }, refresh=True)   # 读己之写：下一步循环要能读到本快照
        return {"configurable": {"thread_id": thread_id,
                                 "checkpoint_ns": ns,
                                 "checkpoint_id": cp_id}}

    def put_writes(self, config: RunnableConfig,
                   writes: Sequence[Tuple[str, Any]], task_id: str,
                   task_path: str = "") -> None:
        """存节点中间写（pending writes）。writes 是 (channel, value) 二元组
        （langgraph-checkpoint 2.x 形态；读回侧 CheckpointTuple.pending_writes
        才是 (task_id, channel, value) 三元组）。

        channel 非 None 时先删同 (thread,ns,cp,task,channel) 旧写再插——
        官方 sqlite saver 的 upsert 语义（节点重试覆盖而非追加）。
        多条写合并为一次 bulk + 一次 refresh（逐条 index+refresh 会把
        每个节点任务的写放大成 N 次强制刷新——每问约 24 个 ES 文档里
        writes 占一半，这是主要写入开销）。"""
        thread_id = config["configurable"]["thread_id"]
        ns = config["configurable"].get("checkpoint_ns", "")
        cp_id = config["configurable"]["checkpoint_id"]
        if not cp_id:
            raise ValueError("put_writes 需要 configurable.checkpoint_id")
        channels = {c for c, _ in writes if c is not None}
        if channels:
            self.client.delete_by_query(index=self.index_name, query={
                "bool": {"filter": [
                    {"term": {"type": "write"}},
                    {"term": {"thread_id": thread_id}},
                    {"term": {"checkpoint_ns": ns}},
                    {"term": {"checkpoint_id": cp_id}},
                    {"term": {"task_id": task_id}},
                    {"terms": {"channel": sorted(channels)}},
                ]}},
                conflicts="proceed", refresh=True)
        actions = []
        for idx, (channel, value) in enumerate(writes):
            tag, blob = self._dump(value)
            actions.append({
                "_op_type": "index", "_index": self.index_name,
                "_id": f"{thread_id}|{ns}|{cp_id}|{task_id}|{idx}",
                "type": "write",
                "thread_id": thread_id, "checkpoint_ns": ns,
                "checkpoint_id": cp_id, "task_id": task_id,
                "task_path": task_path or "",
                "idx": idx, "channel": channel or "",
                "type_tag": tag, "blob": blob,
            })
        if actions:
            bulk(self.client, actions, refresh=True)

    # ------------------------------------------------------------------
    # 读：get_tuple / list
    # ------------------------------------------------------------------
    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """取 checkpoint。定位规则（对齐官方 saver）：
          config 带 checkpoint_id → 精确取那一条（时间旅行到指定步骤）
          不带 → 该 thread 最新一条（正常续跑场景）
        "最新"按 checkpoint_id 字典序倒序——id 是 uuid6（时间有序），
        字典序=时间序，不需要额外时间列。"""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            return None
        ns = configurable.get("checkpoint_ns", "")
        cp_id = get_checkpoint_id(config)
        filters = [{"term": {"type": "checkpoint"}},
                   {"term": {"thread_id": thread_id}},
                   {"term": {"checkpoint_ns": ns}}]
        if cp_id:
            filters.append({"term": {"checkpoint_id": cp_id}})
        resp = self.client.search(index=self.index_name, query={
            "bool": {"filter": filters}},
            sort=[{"checkpoint_id": {"order": "desc"}}], size=1)
        hits = resp["hits"]["hits"]
        return self._row_to_tuple(hits[0]["_source"]) if hits else None

    def list(self, config: Optional[RunnableConfig], *, filter: dict = None,
             before: Optional[RunnableConfig] = None,
             limit: int = None) -> Iterator[CheckpointTuple]:
        """按时间倒序列出某 thread 的 checkpoints（时间旅行的数据源）。

        before：只要某快照之前的（aget_state_history 翻页用它）——转成
        checkpoint_id 的 range 查询（uuid6 字典序等价时间序，lt 即"早于"）。
        filter：浅过滤只支持元数据列 source/step（官方 saver 支持任意
        metadata 键；demo 的图不使用该参数，够用即止，docstring 已注明）。"""
        configurable = (config or {}).get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            return
        ns = configurable.get("checkpoint_ns", "")
        filters = [{"term": {"type": "checkpoint"}},
                   {"term": {"thread_id": thread_id}},
                   {"term": {"checkpoint_ns": ns}}]
        if filter:
            for k, v in filter.items():              # 浅过滤：仅元数据列
                if k == "source":
                    filters.append({"term": {"meta_source": str(v)}})
                elif k == "step":
                    filters.append({"term": {"meta_step": v}})
        if before and (before_id := get_checkpoint_id(before)):
            filters.append({"range": {"checkpoint_id": {"lt": before_id}}})
        resp = self.client.search(index=self.index_name, query={
            "bool": {"filter": filters}},
            sort=[{"checkpoint_id": {"order": "desc"}}],
            size=limit or 10)
        for hit in resp["hits"]["hits"]:
            yield self._row_to_tuple(hit["_source"])

    def delete_thread(self, thread_id: str) -> None:
        """删除该 thread 的全部 checkpoints 与 writes（会话注销/清理用；
        checkpoint 与 write 同索引，一条 term 查询全清）。"""
        self.client.delete_by_query(index=self.index_name, query={
            "term": {"thread_id": thread_id}},
            conflicts="proceed", refresh=True)

    # ------------------------------------------------------------------
    # async 面（基类 async 桩无自动桥接，统一 to_thread 包同步实现）
    # ------------------------------------------------------------------
    async def aput(self, config, checkpoint, metadata, new_versions):
        return await asyncio.to_thread(
            self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(self, config, writes, task_id, task_path=""):
        return await asyncio.to_thread(
            self.put_writes, config, writes, task_id, task_path)

    async def aget_tuple(self, config) -> Optional[CheckpointTuple]:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(self, config, *, filter=None, before=None, limit=None):
        # 生成器不能整体 to_thread：逐条桥接（demo 规模下无性能问题）
        for item in await asyncio.to_thread(
                lambda: list(self.list(config, filter=filter,
                                       before=before, limit=limit))):
            yield item

    async def adelete_thread(self, thread_id: str) -> None:
        return await asyncio.to_thread(self.delete_thread, thread_id)
