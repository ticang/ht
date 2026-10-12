# -*- coding: utf-8 -*-
"""
text_splitters.py —— 父子块切分器（复刻 aichain/ingest/text_splitter.py 全文）

生产对照（aichain/ingest/text_splitter.py）：
  ParagraphCharacterTextSplitter(:8-49)   本文件的切分主体
    · parent_chunk_size=1600 / child_chunk_size=400（:9-12）
      ⚠ 生产代码两个尺寸读的是【同一个】环境变量 OPSGPT_PARENT_CHUNK_SIZE，
        即显示设置该变量时 child 会跟着变成同一个值——这是生产的历史笔误，
        demo 原样保留（注释标出），默认值 1600/400 是真实运行的值
    · 切分流程（split_documents :17-49）：
        ① 父块切分：先看原文长度 >1600 且"剥掉代码块后剩下的不足一半"
           （即代码占比>50%）→ 父块尺寸×2 再切（代码整块不易截断）
        ② 子块切分：每个父块再按 400 切子块（检索用子块，命中后还原父块）
        ③ subject 前缀：每个子块正文前拼上标题 subject（"。".join）——
           让子块自带标题语义，问题路（query↔标题）召回更准
        ④ 仅当父块切出 >1 个子块才挂 metadata['parent_text']（单块无需还原）
        ⑤ img_clean/code_clean 清洗（demo 简化为正则剥 ``` 代码块与 <img> 标签，
           生产的 data_cleaner 是更完整的正则集）

为什么是"父子"两块而不是一层：检索时用 400 的小块保证向量语义集中，
生成时还原 1600 的父块保证上下文完整——ElasticVectorSearch7 存的也是子块，
parent_text 只存在于子块 metadata 里（本 demo 的 es_store.add_documents 同构）。

与 langchain 的关系：内部两把 RecursiveCharacterTextSplitter（父子各一把），
对外的 split_documents(List[Document]) → List[Document] 与 langchain 文档切分器
协议一致（loaders 里 load_and_split(text_splitter=...) 直接喂它，同生产
_load_file → loader.load_and_split(text_splitter) 的用法）。
"""
import os
import re
from typing import Iterable, List

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# 生产同款环境变量名（含"两个尺寸读同一变量"的原始笔误，注释见上）
PARENT_CHUNK_SIZE = int(os.getenv('OPSGPT_PARENT_CHUNK_SIZE', 1600))
CHILD_CHUNK_SIZE = int(os.getenv('OPSGPT_PARENT_CHUNK_SIZE', 400))   # noqa: F841 保留笔误


def code_clean(text: str) -> str:
    """剥掉 ``` 围栏代码块 —— 生产 aichain/utils/data_cleaner.code_clean 的简化版。

    用途见 split_documents ①：切父块前先量"非代码内容占比"，代码占大头的
    文档（比如接口文档里大段示例）父块尺寸翻倍，避免代码块被腰斩。"""
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def img_clean(text: str) -> str:
    """剥掉 <img .../> 标签 —— 生产 data_cleaner.img_clean 的简化版。

    图片标签对向量检索是噪声（base64/路径无语义），入库前清掉；
    FAQ 场景的图片以 metadata['answer'] 里的 <img> 保留，检索后由
    knowledge_v3 式的响应组装重新拼回（见 rag_demo 的组装段）。"""
    return re.sub(r"<img\s+[^>]*/?>", "", text)


class ParagraphCharacterTextSplitter:
    """父子块切分器（接口与 langchain TextSplitter 的 split_documents 兼容）。"""

    # 类属性（生产 :9-12 原样）：两把 RecursiveCharacterTextSplitter，chunk_overlap=0
    parent_chunk_size = PARENT_CHUNK_SIZE
    child_chunk_size = CHILD_CHUNK_SIZE
    child_splitter = RecursiveCharacterTextSplitter(chunk_size=child_chunk_size, chunk_overlap=0)
    parent_splitter = RecursiveCharacterTextSplitter(chunk_size=parent_chunk_size, chunk_overlap=0)

    def split_documents(self, documents: Iterable[Document]) -> List[Document]:
        """生产 split_documents(:17-49) 原样移植，五步见模块 docstring。"""
        docs: List[Document] = []
        for parent_doc in documents:
            parent_text = parent_doc.page_content
            # ① 父块切分：超长且代码占比 >50% → 尺寸×2（生产 :23-29）
            if len(parent_text) > self.parent_chunk_size:
                filter_parent = code_clean(parent_text)
                if len(filter_parent) < len(parent_text) / 2:
                    parent_documents = RecursiveCharacterTextSplitter(
                        chunk_size=self.parent_chunk_size * 2, chunk_overlap=0
                    ).split_documents([parent_doc])
                else:
                    parent_documents = self.parent_splitter.split_documents([parent_doc])
            else:
                parent_documents = self.parent_splitter.split_documents([parent_doc])
            # ②③④⑤ 子块切分 + subject 前缀 + parent_text + 清洗（生产 :31-47）
            for doc in parent_documents:
                sub_docs = self.child_splitter.split_documents([doc])
                for _doc in sub_docs:
                    # subject（标题）前缀：子块自带标题语义（生产 :33-34）
                    if "subject" in _doc.metadata and _doc.metadata["subject"]:
                        _doc.page_content = "。".join(
                            [_doc.metadata["subject"], _doc.page_content])
                    # 仅多块父块才需要还原（生产 :38-39）
                    if len(sub_docs) > 1:
                        _doc.metadata["parent_text"] = doc.page_content
                    # img/code 清洗（生产 :40-43，original_content 留底）
                    image_text = img_clean(_doc.page_content)
                    code_text = code_clean(image_text)
                    if code_text != _doc.page_content:
                        _doc.metadata["original_content"] = _doc.page_content
                        _doc.page_content = code_text
                docs.extend(sub_docs)
        return docs


def get_text_split(text_split_config: dict = None):
    """复刻 aichain/document_loader/text_splitter.get_text_split(:77-99)：
    按配置返回切分器，默认（config=None）就是 ParagraphCharacterTextSplitter——
    生产 file_ingest 不传 text_split 时走的正是这个默认值。"""
    if text_split_config is None:
        text_split_config = {
            "cls": "ParagraphCharacterTextSplitter",
        }
    cls = text_split_config.get("cls", None)
    if cls == 'ParagraphCharacterTextSplitter':
        return ParagraphCharacterTextSplitter()
    # 生产另有 ChineseTextSplitter（按中文句读断句）与裸 RecursiveCharacterTextSplitter
    # 两个分支（text_splitter.py:94-98），demo 只用默认父子切分，其余显式不支持
    raise ValueError(f"demo 未复刻的切分器类型: {cls}（生产默认走 ParagraphCharacterTextSplitter）")
