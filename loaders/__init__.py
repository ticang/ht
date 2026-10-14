# -*- coding: utf-8 -*-
"""
loaders/ —— word / pdf / md / excel / faq 各类型文档解析包
（复刻 aichain/document_loader/ 的目录形态：每类型一个 loader 模块 + 本文件分派）

生产链路对照（server/tasks/ingest.py file_ingest:86 → aichain/ingest/base.py:91）：
    file_ingest(chain, file_path, ...)
      └─ ingest_chain.ingest_from_file(...)            # aichain/chains/base.py:127
           └─ BaseDocumentIngest.file_to_vector(...)   # aichain/ingest/base.py:91-133
                ├─ get_text_split()                    # 默认 ParagraphCharacterTextSplitter
                ├─ get_file_loader(file_path, partition_type, knowledge_id, split_type)
                │    └─ aichain/document_loader/__init__.py:132（FILE_LOADER_MAP 分派）
                ├─ loader.load_and_split(text_splitter)  ← langchain BaseLoader 标准用法
                └─ doc_persistent(docs) → vector_store.from_documents  ← demo 的 ESStore.add_documents

各类型生产实现 → demo 模块：
    word : WordDocumentLoader(word_document.py:17) → partition_docx(word_partition.py:378)
           标题分块，subject=「父标题,当前标题」           → word_loader.py
    pdf  : UnstructuredPaddlePDFLoader(pdf_loader.py:16)
           全图片页→RapidOCR；否则 pdf2docx 转 docx       → pdf_loader.py
           （demo 主路径升级为 MinerU，pdf2docx 降级保留）
    md   : UnstructuredPaddleMDLoader(md_loader.py:15)
           pandoc 转 docx → partition_docx               → md_loader.py
    excel: MAASExcelLoader→maas_partition_xlsx(excel_loader.py:240)
           表头识别 + 合并单元格展开 + '序号'行主键       → excel_loader.py
    faq  : 独立入口 knowledge_faq_batch_ingest(ingest.py:653)
           →handle_faq_dox(ingest.py:624)
           Heading=问题、正文=答案 → DataFrame → 双份入库 → faq_loader.py
           （faq 不走 FILE_LOADER_MAP，由 chain='knowledge_faq' 区分）

demo 与生产的差异（逐处有注释）：
    · 类型探测：生产 unstructured.detect_filetype + magic，demo 用文件后缀
    · txt/csv/html/ppt 等其余类型：生产 FILE_LOADER_MAP 也有，demo 未复刻
      （报错提示不支持，保持行为显式）
"""
import os
from typing import Tuple

from langchain_core.document_loaders import BaseLoader

from obs import obs                                  # langfuse 阶段埋点（no-op 安全）

from .excel_loader import ExcelDocumentLoader
from .faq_loader import FaqDocumentLoader, handle_faq_docx
from .md_loader import MarkdownDocumentLoader
from .pdf_loader import PdfDocumentLoader
from .word_loader import WordDocumentLoader, partition_docx

__all__ = ["FILE_LOADER_MAP", "get_file_type", "get_file_loader",
           "ExcelDocumentLoader", "FaqDocumentLoader", "MarkdownDocumentLoader",
           "PdfDocumentLoader", "WordDocumentLoader",
           "handle_faq_docx", "partition_docx"]

# ===========================================================================
# 类型分派 —— get_file_loader(aichain/document_loader/__init__.py:132)
# ===========================================================================
FILE_LOADER_MAP = {
    # 生产 :28-109 的 demo 子集；cls/split/kwargs/support_ext_partition 字段同名
    "pdf":  {"cls": PdfDocumentLoader,     "split": True},
    "docx": {"cls": WordDocumentLoader,    "split": True,
             "kwargs": {"mode": "elements"}, "support_ext_partition": True},
    "doc":  {"cls": WordDocumentLoader,    "split": True,
             "kwargs": {"mode": "elements"}, "support_ext_partition": True},
    "md":   {"cls": MarkdownDocumentLoader, "split": True},
    "xlsx": {"cls": ExcelDocumentLoader,   "split": True,
             "kwargs": {"mode": "elements"}, "support_ext_partition": True},
    "xls":  {"cls": ExcelDocumentLoader,   "split": True,
             "kwargs": {"mode": "elements"}, "support_ext_partition": True},
    # 生产另有 txt/csv/json/html/mhtml/ppt(x)/convert_md；demo 未复刻，显式报错
}


def get_file_type(file_path: str) -> str:
    """生产 get_file_type(__init__.py:114-130)：unstructured.detect_filetype+magic
    按内容探测，失败才退回后缀。demo 直接用后缀（注释标差异）。"""
    return os.path.splitext(file_path)[-1][1:].lower()


def get_file_loader(file_path: str, partition_type: str = "default",
                    knowledge_id: str = None,
                    split_type: int = 0) -> Tuple[BaseLoader, bool]:
    """生产 get_file_loader(__init__.py:132-188) 骨架复刻：
      · split_type!=0 → 'convert_md'（预转换 md 路线），demo 未复刻，报错说明
      · 后缀查 FILE_LOADER_MAP，没有 → ValueError（生产 :140-141 同款）
      · support_ext_partition 的类型把 partition_type 传给 loader（:144-147）；
        生产 partition_type!='default' 会动态加载 third_part 插件改写解析
        （word_document.py:53-55），demo 无插件体系，仅透传日志
      · pdf/doc/docx 传 knowledge_id（:169-173，图片 bucket 用）
    返回 (loader, split)：split=True 表示入库前还要过 text_splitter 切分
    （生产 _load_file:184-187 按 split 决定 load_and_split 还是 load）。"""
    if split_type != 0:
        raise ValueError("split_type!=0 的 convert_md 路线 demo 未复刻（生产 :136/:174-178）")
    extension = get_file_type(file_path)
    print(f"[分派] file={os.path.basename(file_path)}, extension={extension}", flush=True)
    if extension not in FILE_LOADER_MAP:
        raise ValueError(
            f"Cannot find loader for file type {extension}"
            f"（生产支持 txt/csv/html/ppt 等，demo 仅 pdf/docx/md/xlsx）")
    loader_cfg = FILE_LOADER_MAP[extension]

    kwargs = {'file_path': file_path}
    if "support_ext_partition" in loader_cfg:
        kwargs['partition_type'] = partition_type
    if extension in ("pdf", "doc", "docx"):
        kwargs['knowledge_id'] = knowledge_id
    kwargs.update(loader_cfg.get('kwargs', {}))
    loader = loader_cfg['cls'](**kwargs)
    # langfuse：类型分派阶段（哪个 loader 接了活——解析全链的第一个节点）
    obs.generation_call(name="dispatch_loader", model=f"loader:{extension}",
                        input={"file": os.path.basename(file_path),
                               "partition_type": partition_type},
                        output={"split": loader_cfg['split']})
    return loader, loader_cfg['split']
