# -*- coding: utf-8 -*-
"""
ingest_service.py —— 文件入库服务（复刻 server/tasks/ingest.py 的
file_ingest:86 与 knowledge_faq_batch_ingest:653 两个入口）

生产链路（file_ingest，普通知识）：
    file_ingest(chain, file_path, knowledge_item_id, ...)     # ingest.py:86-148
      ├─ get_upload_file_path(file_path)                      # :102 minio/本地路径
      ├─ metadata = {knowledge_id, workspace, name, product_name,
      │              parent_id, labels, knowledge_base_id}    # :107-115
      ├─ ingest_chain = ChainFactory.get_ingest_chain(chain)  # :117-120
      └─ ingest_chain.ingest_from_file(...)                   # :122-128
           └─ BaseDocumentIngest.file_to_vector               # aichain/ingest/base.py:91
                ├─ get_text_split()       → ParagraphCharacterTextSplitter（demo: text_splitters）
                ├─ get_file_loader(...)   → FILE_LOADER_MAP 分派（demo: loaders）
                ├─ loader.load_and_split(text_splitter)       # langchain 用法
                └─ doc_persistent(docs, metadata)             # :135-158
                     └─ vector_store.from_documents(docs, embedding)
                        （另生成 summary/questions 辅助文档 :153-166，demo 未复刻）

生产链路（knowledge_faq_batch_ingest，FAQ 知识）：
    faq_batch(file, column_question, column_answer, ...)      # ingest.py:653-816
      ├─ 读表：csv→pd.read_csv / docx→handle_faq_dox / excel→pd.read_excel(:683-700)
      ├─ 逐行：question/answer 列取值(:731-732)
      ├─ md5 去重（内容级 exist_md5 + 库级 query_by_md5，:739-755）→ demo 只做内容级
      ├─ metadata = {..., is_faq:'Y', answer}                 # :773-785
      ├─ base_doc = "问题：q\n答案：a"（完整对）               # :787
      ├─ faq_doc  = question（纯问题）                         # :789
      └─ 50 行一批入库 _ingest_knowledge_doc(:719-797)         # demo 一次入库

demo 与生产的差异：
    · 无 DB/minio/celery：状态统计(_adjust_knowledge_file_stats)与异步任务壳省略
    · summary/questions 辅助文档生成（DocumentSummaryGenerator）省略
    · _rebuild_content 的 base64 图片搬运省略（demo 无图片场景）
"""
import hashlib
import os
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
from langchain_core.documents import Document

from file_service import get_upload_file_path
from loaders import get_file_loader, handle_faq_docx
from text_splitters import get_text_split

# 生产 generate_faq_data(ingest.py:643-648)：50 行一批（数据库批量提交粒度，
# demo 的 ES bulk 本身批量，保留常量注明出处）
FAQ_CHUNK_SIZE = 50


def get_text_md5(text: str) -> str:
    """生产 server/utils/file.py file_tools.get_text_md5：内容指纹，
    FAQ 重复导入拦截用（:739）。demo 用 SHA-256（同用途去重指纹，
    规避 MD5 碰撞风险；生产是 md5，此处仅为安全等价替换）。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class IngestService:
    """入库门面 —— 对应生产的 ChainFactory.get_ingest_chain(chain) + ingest_from_file
    + doc_persistent 三层（demo 合并到一个类，es_store.ESStore 承担向量库角色）。"""

    def __init__(self, store):
        """store: ESStore（真 ES 写入 + 嵌入），对应生产的
        vector_store_cls.from_documents(docs, embedding)。"""
        self.store = store

    # ------------------------------------------------------------------
    # 普通知识入库 —— file_ingest(ingest.py:86-148) + file_to_vector(ingest/base.py:91)
    # ------------------------------------------------------------------
    def file_ingest(self, chain: str, file_path: str, knowledge_item_id: str = None,
                    product_name: str = None, text_split: Dict[str, Any] = None,
                    ingest_kwargs: Dict[str, Any] = None, partition_type: str = None,
                    split_type: int = 0, labels: list = None,
                    knowledge_base_id: str = None) -> int:
        """解析任意支持类型的文件并入库，返回入库条数。

        参数与生产 file_ingest(:86-95) 同名同义；demo 的差异：
          · knowledge_item_id 生产必填（查 knowledge_items 表），demo 可空——
            用它模拟生产 metadata['knowledge_id']（每文件一个 uuid）
          · chain：生产按链名走不同 ingest chain（knowledge_base/knowledge_faq/
            graph...），demo 只认 'knowledge_base'，'knowledge_faq' 请走
            faq_batch_ingest（生产 FAQ 也是独立入口 :558）"""
        start_time = __import__('time').time()
        # 生产 :102：get_upload_file_path 把（可能的）minio 对象路径统一成本地
        # 文件；demo 固定本地存储模式（file_service.py 复刻该分支判定）
        file_path = get_upload_file_path(file_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        # 生产 :104-115：查 knowledge_items 表构造 metadata；demo 用参数拼
        item_id = knowledge_item_id or str(uuid.uuid4())
        metadata = {
            "knowledge_id": item_id,
            "name": os.path.splitext(os.path.basename(file_path))[0],
            "product_name": product_name,
            "parent_id": None,
            "labels": labels or [],
            "knowledge_base_id": knowledge_base_id,
        }

        # 生产 :117-128：ingest_chain.ingest_from_file → file_to_vector
        #   ① 切分器（get_text_split 默认父子切分）
        text_splitter = get_text_split(text_split)
        #   ② 类型分派（get_file_loader：pdf/docx/md/xlsx 各归其 loader）
        loader, split = get_file_loader(file_path, partition_type or "default",
                                        knowledge_id=item_id, split_type=split_type)
        #   ③ langchain 标准用法 load_and_split（生产 _load_file :184-187：
        #      split=True → load_and_split(text_splitter)；否则只 load）
        docs = loader.load_and_split(text_splitter=text_splitter) if split else loader.load()
        print(f"[file_ingest] {os.path.basename(file_path)} 解析+切分 → {len(docs)} 块"
              f"（{__import__('time').time() - start_time:.2f}s）", flush=True)
        #   ④ doc_persistent（生产 :135-148：metadata 逐块合并后 from_documents）
        for d in docs:
            d.metadata.update(metadata)
        self.store.add_documents(docs)
        return len(docs)

    # ------------------------------------------------------------------
    # FAQ 批量入库 —— knowledge_faq_batch_ingest(ingest.py:653-816)
    # ------------------------------------------------------------------
    def faq_batch_ingest(self, file_path: str, column_question: int = 1,
                         column_answer: int = 2, product_name: str = None,
                         text_split: Dict[str, Any] = None, workspace: str = "rag-lab",
                         chain: str = "knowledge_faq", labels: list = None,
                         parent_knowledge_id: str = "",
                         knowledge_base_id: str = None) -> int:
        """FAQ 文件（docx/xlsx/csv）→ 双份入库，返回入库条数（base+faq 合计）。

        生产 knowledge_faq_batch_ingest(:653) 的复刻：
          · 读表三分支（:683-700）：csv→read_csv / docx→handle_faq_dox /
            excel→read_excel(可见 sheet)；demo 同样三分支
          · 列号 1-based（:711-714：问题/答案所在列，>20 报错）
          · 逐行取 question/answer，'nan'/空跳过（:731-735）
          · md5 内容级去重（:739-742 的 exist_md5；库级查表 :752-755 demo 无库）
          · metadata 带 is_faq='Y' + answer（:773-785）——FAQ 检索路靠
            is_faq 过滤、命中后答案从 metadata 取（不用再检索答案文本）
          · 双份：base_doc=完整问答对 / faq_doc=纯问题（:787-789）
        """
        ext = os.path.splitext(file_path)[-1].lower()
        data_tables: List[pd.DataFrame] = []
        if ext == '.csv':
            data_tables.append(pd.read_csv(file_path))
        elif ext == '.docx':
            # 生产 :694-696：handle_faq_dox 把"标题=问题/正文=答案"的 docx 转 DataFrame
            pairs = handle_faq_docx(file_path)
            data_tables.append(pd.DataFrame(pairs, columns=['question', 'answer']))
        elif ext in ('.xlsx', '.xls'):
            sheets = pd.read_excel(file_path, sheet_name=None)   # None=全部sheet
            data_tables = list(sheets.values())
        else:
            raise ValueError(f"FAQ 不支持的文件类型: {ext}（生产支持 csv/docx/excel）")

        # 生产 :711-714：列号校验（1-based，≤20）
        column_question_int = int(column_question) if column_question else 1
        column_answer_int = int(column_answer) if column_question else 2
        if column_question_int > 20 or column_answer_int > 20:
            raise ValueError('excel列号不能大于20')

        text_splitter = get_text_split(text_split)
        exist_md5: List[str] = []
        total = 0
        for table in data_tables:
            # 生产 :719：50 行一批（generate_faq_data）；demo 直接整表一批
            base_docs, faq_docs = [], []
            for row in table.values:
                try:
                    question = str(row[column_question_int - 1])
                    answer = str(row[column_answer_int - 1])
                    # 生产 :735：空值/nan 跳过
                    if not (question and answer and question != 'nan' and answer != 'nan'):
                        continue
                    # 生产 :737-742：内容 md5，重复跳过
                    content = f"问题：{question}\n答案：{answer}"
                    md5 = get_text_md5(text=content)
                    if md5 in exist_md5:
                        print(f"[faq_ingest] md5 重复，跳过：{question[:20]}", flush=True)
                        continue
                    exist_md5.append(md5)
                    # 生产 :757-785：知识项 + metadata（is_faq/answer 是关键）
                    item_id = str(uuid.uuid4())
                    metadata = {
                        "knowledge_id": item_id,
                        "workspace": workspace,
                        "name": question,
                        "product_name": product_name,
                        "chain": chain,
                        "labels": labels or [],
                        "parent_id": parent_knowledge_id,
                        "is_faq": "Y",                  # FAQ 检索路的过滤条件
                        "answer": answer,               # 命中后直接取，不再检索
                        "knowledge_base_id": knowledge_base_id,
                    }
                    # 生产 :787-789：双份文档（faq_doc 用同一份 metadata dict）
                    base_docs.append(Document(page_content=content, metadata=dict(metadata)))
                    faq_docs.append(Document(page_content=question, metadata=dict(metadata)))
                except Exception as e:
                    print(f"[faq_ingest] 行解析失败跳过: {e}", flush=True)   # 生产 :794-795
            # 双份各自切分入库（生产 _ingest_knowledge_doc→ingest_faq_from_docs）
            for docs in (base_docs, faq_docs):
                splitted = text_splitter.split_documents(docs)
                self.store.add_documents(splitted)
                total += len(splitted)
        print(f"[faq_ingest] {os.path.basename(file_path)} 双份入库完成：{total} 块",
              flush=True)
        return total
