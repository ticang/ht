# -*- coding: utf-8 -*-
"""
pdf_loader.py —— pdf 解析（MinerU 主路径 + pdf2docx 降级）

生产对照（aichain/document_loader/pdf_loader.py:16-71）：
    UnstructuredPaddlePDFLoader：全图片页→RapidOCR 逐页识别（:30-49）；
    文本型→pdf2docx 转成 docx 再走 partition_docx（:56-60）

demo 与生产的差异：
    · 主路径升级为 MinerU 4.x（布局分析+OCR 一体，pdf→markdown→# 标题转
      docx→partition_docx）：图片型/扫描件 PDF 无需额外 OCR 依赖即可解析，
      补齐生产 RapidOCR 对应环节；tier=basic（CPU 单页 ~1-3s）
    · pdf2docx 保留为 MinerU 失败时的降级路径（生产 :56-60 原样，标题靠
      字号启发识别）
    · knowledge_id 同 word：生产用于 OCR 图片回传 minio，demo 形参保留
"""
import os
from typing import Any, List

from langchain_core.documents import Document
from langchain_core.document_loaders import BaseLoader

from .md_loader import _markdown_text_to_docx
from .word_loader import partition_docx


def _mineru_parse_pdf(file_path: str, tier: str = "basic",
                      ocr_mode: str = "auto") -> str:
    """MinerU 4.x 本地解析 pdf → markdown 文本。

    · tier：basic/standard/advanced（demo 用 basic——CPU 单页 ~1s，
      standard/advanced 更慢但版面更细）
    · ocr_mode：auto=文本层差自动走 OCR（MinerU 内置，**图片型 PDF 无需
      额外 OCR 依赖即可解析**——补齐了生产 RapidOCR 的对应环节）
    · 首次运行自动下载模型（modelscope/HF，已在 venv 缓存）
    返回 markdown 字符串（标题行 `#`~`######`，MinerU 的布局分析产出品）。"""
    from mineru import parse as mineru_parse
    result = mineru_parse(file_path, tier=tier, ocr_mode=ocr_mode)
    return result.markdown()


class PdfDocumentLoader(BaseLoader):
    """pdf loader —— 主路径 MinerU，降级路径 pdf2docx（生产 UnstructuredPaddle
    PDFLoader 的文本型分支）。

    主路径：pdf --MinerU--> markdown --(#→Heading 转 docx)--> partition_docx
      · MinerU 的 markdown 标题层级正好接上 demo 的 md 解析路线（与
        MarkdownDocumentLoader 同构的转换函数）
      · 图片型/扫描件 PDF：MinerU 内置 OCR 直接出文本，不再报错
    降级路径：MinerU 不可用（未装包/模型缺失）时走 pdf2docx → partition_docx
    （生产 :56-60 原样），标题靠字号启发识别。

    knowledge_id 同 word：生产用于 OCR 图片回传，demo 形参保留。"""

    def __init__(self, file_path: str, knowledge_id: str = None,
                 mode: str = "single", **kwargs: Any):
        self.file_path = file_path
        self.knowledge_id = knowledge_id
        self.mode = mode

    def load(self) -> List[Document]:
        print(f"[loader] pdf: {os.path.basename(self.file_path)}", flush=True)
        docs_path = self.file_path.replace('.pdf', '.docx')
        try:
            # ---- 主路径：MinerU → markdown → docx → partition_docx ----
            md_text = _mineru_parse_pdf(self.file_path)
            print(f"    [loader] MinerU 解析完成（markdown {len(md_text)} 字）", flush=True)
            _markdown_text_to_docx(md_text, docs_path)
            docs = partition_docx(docs_path, self.knowledge_id)
            for d in docs:
                d.metadata["source"] = os.path.basename(self.file_path)
            return docs
        except Exception as e:
            # ---- 降级路径：pdf2docx（生产 UnstructuredPaddlePDFLoader :56-60）----
            print(f"    [loader] MinerU 失败（{type(e).__name__}: {str(e)[:80]}），"
                  f"降级 pdf2docx", flush=True)
            from pdf2docx import Converter
            cv = Converter(self.file_path)
            cv.convert(docs_path)                      # all pages by default
            cv.close()
            docs = partition_docx(docs_path, self.knowledge_id)
            for d in docs:
                d.metadata["source"] = os.path.basename(self.file_path)
            return docs
