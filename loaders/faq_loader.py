# -*- coding: utf-8 -*-
"""
faq_loader.py —— FAQ docx 解析（复刻 server/tasks/ingest.py 的 handle_faq_dox）

生产对照（server/tasks/ingest.py:624-639）：
    handle_faq_dox：handle_element 产出 (element, current_title, parent_title)
    流；标题（Heading/Title 样式且 text==current_title.text）→ 新问题的 key；
    正文归入当前问题（标题非 Heading 时取父标题兜底，:632-635 是"短正文被
    误判标题"的容错）。最后 {问题: '\n'.join(答案)} → DataFrame。

demo 与生产的差异：
    · 等价实现返回二元组列表 [(question, answer), ...]（DataFrame 化在
      ingest_service 里做，与生产 knowledge_faq_batch_ingest 拿 DataFrame
      遍历的形态衔接）；"标题非 Heading 取父标题"的容错简化为直接
      Heading 判定
    · FAQ 不走 FILE_LOADER_MAP（生产独立入口 knowledge_faq_batch_ingest），
      demo 为"全类型一个入口"挂进 loader 体系，由 ingest_service 的
      chain='knowledge_faq' 分派到它
"""
import os
from typing import Any, List, Tuple

import docx as docx_lib
from langchain_core.documents import Document
from langchain_core.document_loaders import BaseLoader

from .word_loader import _strip_numbering


def handle_faq_docx(filename: str) -> List[Tuple[str, str]]:
    """FAQ docx → [(question, answer), ...]（Heading=问题、正文聚合=答案）。"""
    document = docx_lib.Document(filename)
    doc_data: "dict[str, list]" = {}
    question, answer_body = None, []
    for para in document.paragraphs:
        text = para.text.strip()
        if not text:
            continue
        style_name = para.style.name if para.style else ""
        if style_name.startswith("Heading") or style_name == "Title":
            if question and answer_body:               # flush 上一问
                doc_data.setdefault(question, []).extend(answer_body)
            question, answer_body = _strip_numbering(text) or text, []
        elif question is not None:
            answer_body.append(text)
    if question and answer_body:                       # flush 最后一问（生产 :637）
        doc_data.setdefault(question, []).extend(answer_body)
    return [(q, "\n".join(v)) for q, v in doc_data.items() if v]


class FaqDocumentLoader(BaseLoader):
    """FAQ loader —— 生产 FAQ 不走 FILE_LOADER_MAP（独立入口
    knowledge_faq_batch_ingest），demo 为了"全类型一个入口"也挂进 loader 体系，
    但由 ingest_service.file_ingest 的 chain='knowledge_faq' 分派到它。"""

    def __init__(self, file_path: str, **kwargs: Any):
        self.file_path = file_path

    def load(self) -> List[Document]:
        print(f"[loader] faq: {os.path.basename(self.file_path)}", flush=True)
        pairs = handle_faq_docx(self.file_path)
        # DataFrame(question,answer) 形态：与生产 handle_faq_dox 返回一致，
        # 双份入库（base_doc/faq_doc）在 ingest_service.faq_batch_ingest 做
        import pandas as pd
        df = pd.DataFrame(pairs, columns=['question', 'answer'])
        return [Document(page_content="", metadata={"faq_df": df})]
