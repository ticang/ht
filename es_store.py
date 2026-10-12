# -*- coding: utf-8 -*-
"""
es_store.py —— ES 8 存储 + hybrid 三路召回（复刻生产 ElasticVectorSearch7 的 ES8 分支）

真实对照（aichain/vectores/elastic7_vector_search.py）：
  _default_text_mapping(_ik)(:84-116)  索引 mapping：text + text.ik + vector
  add_texts(:709-783)                   uuid4当_id + 批量嵌入 + bulk写入
  _generate_metadata_filter(:119-137)   term/terms → metadata.X.keyword
  _default_vector_query(:155)           ES8 knn（field/k/num_candidates/filter）
  _default_match_query(:190)            match on text.ik（模糊路）
  _default_match_phrase_query(:207)     match_phrase on text（精确路）
  _do_search(:646)                      hits → Document，_score写metadata['score']
  hybrid_search(:572)                   三路召回 + fusion_ranked RRF
  fusion_ranked(:323)                   加权RRF（k=60）

本类与生产的行为差异（均为demo级简化，注释逐处标注）：
  · 每次召回三次独立 search（生产把 filter 拼进各路 DSL，此处等价）
  · 不做查询重试装饰器、不区分ES7/ES8双分支（demo明确用8.15）
"""
import math
import time
import uuid
from typing import Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from langchain_core.documents import Document

import config
from clients import TEIEmbedClient
from config import check_url

# bge-m3 输出维度（生产同样按模型取 dims）
VECTOR_DIMS = 1024


def fusion_ranked(weights, results):
    """加权 RRF —— 生产 elastic7_vector_search.py:323 原样移植（逻辑不变）。

    ① 名次打分：第 idx 路第 i 名 → weights[idx]/(i+60+1)，k=60 压平名次曲线
    ② 跨路累加：同 _id 多路命中分数相加（"双路共识 > 单路第一"）
    ③ ×60 还原量纲（美化，不影响排序）
    ④ 排序键 (new_score, metadata['score'])：RRF分优先，平局用ES原始分

    为什么只用名次不用分数：match/match_phrase 是 BM25 分数量纲、kNN 是
    余弦分数量纲，直接相加无意义；RRF 天然免量纲归一。"""
    map_results: Dict[str, Document] = {}
    for result_idx, result in enumerate(results):
        for i, res in enumerate(result):
            doc_id = res.metadata["_id"]
            score = weights[result_idx] / float(i + 60 + 1)
            previous = map_results.get(doc_id)
            if previous:
                score += float(previous.metadata["new_score"])
            res.metadata["new_score"] = float(score)
            map_results[doc_id] = res
    concat = list(map_results.values())
    for ele in concat:
        ele.metadata["new_score"] = ele.metadata["new_score"] * 60
    concat.sort(key=lambda x: (float(x.metadata["new_score"]),
                               float(x.metadata.get("score", 0))), reverse=True)
    return concat


class ESStore:
    """真 ES 的存储+检索门面。一个索引同时服务双通道（filter 区分）。"""

    def __init__(self, embed_client: TEIEmbedClient, index_name: str = None):
        self.embed = embed_client
        self.index_name = index_name or config.INDEX_NAME
        # ES 连接（初始化即过白名单校验；认证可选）
        check_url(config.ES_URL)
        es_kwargs = {"request_timeout": 30}
        if config.ES_USER and config.ES_PASSWORD:
            es_kwargs["basic_auth"] = (config.ES_USER, config.ES_PASSWORD)
        self.client = Elasticsearch(config.ES_URL, **es_kwargs)

    # ------------------------------------------------------------------
    # 索引管理：mapping 复刻 _default_text_mapping_ik（:106-116）
    # text.ik 子字段需要 IK 插件——es8-ik 镜像自带，生产用 _support_ix_match
    # 探测（:786-795），demo 直接断言插件存在（缺了会建索引失败，fail-fast）。
    # ------------------------------------------------------------------
    def ensure_index(self, reset: bool = False):
        """索引不存在则创建；reset=True 先删（demo 幂等重跑用）。"""
        if reset and self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)
            print(f"[ES] 已删除旧索引 {self.index_name}", flush=True)
        if not self.client.indices.exists(index=self.index_name):
            mapping = {
                "properties": {
                    "text": {                      # 正文，standard 分词
                        "type": "text",
                        "fields": {                # IK 子字段：写入细切、查询粗切
                            "ik": {"type": "text", "analyzer": "ik_max_word",
                                   "search_analyzer": "ik_smart"}}},
                    "vector": {                    # 1024维余弦，ES8原生kNN索引
                        "type": "dense_vector", "dims": VECTOR_DIMS,
                        "index": True, "similarity": "cosine"},
                }
                # metadata 不声明 → 动态映射为 object，各键自动带 .keyword 子字段，
                # metadata.is_faq.keyword / metadata.knowledge_base_id.keyword 过滤即依赖它
            }
            self.client.indices.create(index=self.index_name, mappings=mapping)
            print(f"[ES] 已创建索引 {self.index_name}（text+text.ik+vector{VECTOR_DIMS}维）",
                  flush=True)

    # ------------------------------------------------------------------
    # 入库：复刻 add_texts(:709-783)
    # ------------------------------------------------------------------
    def add_documents(self, docs: List[Document]):
        """① 每条 uuid4 当 _id（检索回来写回 metadata['_id']，RRF去重靠它）
           ② TEI 批量嵌入（生产在ES侧入库前嵌入）
           ③ bulk 写入 {vector, text, metadata}（与生产 _source 结构一致）"""
        if not docs:                                   # 空批保护（解析出 0 块时）
            print("[ES] 无文档可写入，跳过", flush=True)
            return
        ids = [str(uuid.uuid4()) for _ in docs]
        for d, i in zip(docs, ids):
            d.metadata = dict(d.metadata)      # 防御：子块可能共享同一 metadata 字典
            d.metadata["_id"] = i
        t0 = time.perf_counter()
        vectors = self.embed.embed([d.page_content for d in docs])
        t_embed = time.perf_counter() - t0
        t1 = time.perf_counter()
        actions = []
        for i, v, d in zip(ids, vectors, docs):
            meta = dict(d.metadata)
            text = d.page_content
            # 生产 add_texts:762-764 的 original_content 反转：切分时清洗留底的
            # 原版正文在入库时写回 text 字段（嵌入仍是清洗版的向量——向量
            # 干净，而精确/模糊路（match_phrase/match）在原文上匹配，代码块
            # 里的字符串也能被字面命中），且不随 metadata 落库（pop 掉）
            if "original_content" in meta:
                text = meta.pop("original_content")
            actions.append({"_op_type": "index", "_index": self.index_name, "_id": i,
                            "vector": v, "text": text, "metadata": meta})
        ok, _ = bulk(self.client, actions, refresh=True)
        t_bulk = time.perf_counter() - t1
        print(f"[ES] 写入 {ok} 条文档到 {self.index_name}"
              f"（向量{len(vectors[0])}维；嵌入 {t_embed:.2f}s + bulk {t_bulk:.2f}s）",
              flush=True)

    # ------------------------------------------------------------------
    # 过滤 DSL：复刻 _generate_metadata_filter(:119-137)
    # ------------------------------------------------------------------
    @staticmethod
    def _filter_dsl(flt: Optional[dict]) -> List[dict]:
        """标量 → term；list → terms。作用于 metadata.X.keyword 精确匹配。
        FAQ 路传 {'is_faq':'Y'}、两路都传 {'knowledge_base_id':[...]}。"""
        if not flt:
            return []
        conds = []
        for k, v in flt.items():
            if not v:
                continue
            field = f"metadata.{k}.keyword"
            if isinstance(v, list):
                conds.append({"terms": {field: v}})
            else:
                conds.append({"term": {field: v}})
        return conds

    @staticmethod
    def _bool_filter(flt: Optional[dict]) -> dict:
        """三路 DSL 共用的 bool.filter 骨架。"""
        return {"bool": {"filter": ESStore._filter_dsl(flt)}} if flt else None

    # ------------------------------------------------------------------
    # 三路检索 DSL + 统一取回：复刻 _do_search(:646-699)
    # ------------------------------------------------------------------
    def _to_documents(self, response) -> List[Document]:
        """ES 响应 → Document 列表。_score 写入 metadata['score']（:666）、
        ES 的 _id 写入 metadata['_id']（:667）——生产同款，后续 RRF/去重依赖。"""
        documents = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            metadata = dict(source.get("metadata") or {})
            metadata["score"] = hit["_score"]      # BM25分或kNN相似度分（看来源路）
            metadata["_id"] = hit["_id"]
            documents.append(Document(page_content=source["text"], metadata=metadata))
        return documents

    def knn_search(self, query: str, size: int, flt: Optional[dict]) -> List[Document]:
        """向量路 —— 复刻 _default_vector_query 的 ES8 分支(:156-168)：
        kNN 近邻（field=vector, k=size, num_candidates=50）+ bool filter。"""
        query_vector = self.embed.embed_query(query)
        resp = self.client.search(index=self.index_name, knn={
            "field": "vector", "query_vector": query_vector,
            "k": max(size, 1), "num_candidates": 50,
            **({"filter": self._bool_filter(flt)} if flt else {}),
        })
        return self._to_documents(resp)

    def match_phrase_search(self, query: str, size: int, flt: Optional[dict]) -> List[Document]:
        """精确路 —— 复刻 _default_match_phrase_query(:207)：match_phrase on text。
        text 主字段 standard 分词对中文是单字切分，短语匹配≈子串匹配，
        所以能当"完全命中"用（生产 hybrid_search 只取1条并置顶）。"""
        resp = self.client.search(index=self.index_name, query={
            "bool": {"must": [{"match_phrase": {"text": {"query": query}}}],
                     **({"filter": self._bool_filter(flt)} if flt else {})}},
            size=size)
        return self._to_documents(resp)

    def match_search(self, query: str, size: int, flt: Optional[dict]) -> List[Document]:
        """模糊路 —— 复刻 _default_match_query(:190)：match on text.ik。
        IK 词级分词 + OR 语义 + BM25 打分，补"向量漏掉的字面命中"。"""
        resp = self.client.search(index=self.index_name, query={
            "bool": {"must": [{"match": {"text.ik": {"query": query}}}],
                     **({"filter": self._bool_filter(flt)} if flt else {})}},
            size=size)
        return self._to_documents(resp)

    # ------------------------------------------------------------------
    # hybrid：复刻 hybrid_search(:572-643) —— 三路 + 配额 + RRF + 精确置顶
    # ------------------------------------------------------------------
    def hybrid_search(self, query: str, size: int, alpha: float = 0.75,
                      filter: Optional[dict] = None) -> List[Document]:
        """参数与生产一致：size=通道配额（文档路4×top_k/FAQ路2.5×top_k）、
        alpha=0.75 硬编码、filter=metadata条件。"""
        search_type = "FAQ" if filter and filter.get("is_faq") == "Y" else "文档"

        # step1 精确路：只取1条，命中即不参与RRF、直接置顶（:598）
        t0 = time.perf_counter()
        keyword_matched = self.match_phrase_search(query, 1, filter)
        t_exact = time.perf_counter() - t0
        exact_n = len(keyword_matched)                # 后面会往里 append，先记数
        size = size - exact_n

        # step2 配额：有精确命中固定2/1；否则 向量=ceil(size*alpha)、模糊=剩余（:603-611）
        if keyword_matched:
            sparse_size, dense_size = 2, 1
        else:
            sparse_size = math.ceil(size * alpha)
            dense_size = size - sparse_size

        # step3 向量路 / step4 模糊路（各自独立查询）
        t1 = time.perf_counter()
        sparse_matched = self.knn_search(query, sparse_size, filter) if sparse_size > 0 else []
        t_vec = time.perf_counter() - t1
        t2 = time.perf_counter()
        dense_matched = self.match_search(query, dense_size, filter) if dense_size > 0 else []
        t_match = time.perf_counter() - t2

        # step5 RRF 融合（两路BM25/余弦分只决定名次，不直接参与数值比较）
        ordered = fusion_ranked([alpha, 1 - alpha], [sparse_matched, dense_matched])

        # step6 精确命中按 _id 去重后置顶（:626-642）
        doc_ids = [d.metadata["_id"] for d in keyword_matched]
        for d in ordered:
            if d.metadata["_id"] not in doc_ids:
                doc_ids.append(d.metadata["_id"])
                keyword_matched.append(d)

        print(f"    [hybrid][{search_type}] 精确{exact_n}({t_exact*1000:.0f}ms)"
              f" + 向量{len(sparse_matched)}({t_vec*1000:.0f}ms)"
              f" + 模糊{len(dense_matched)}({t_match*1000:.0f}ms)"
              f" → RRF 融合后 {len(keyword_matched)} 条", flush=True)
        return keyword_matched
