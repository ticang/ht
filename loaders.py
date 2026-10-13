# -*- coding: utf-8 -*-
"""
loaders.py —— word / pdf / md / excel / faq 全类型文档解析
（复刻 aichain/document_loader/ 的 FILE_LOADER_MAP 分派与各 loader 的解析逻辑）

生产链路对照（server/tasks/ingest.py file_ingest:86 → aichain/ingest/base.py:91）：
    file_ingest(chain, file_path, ...)
      └─ ingest_chain.ingest_from_file(...)            # aichain/chains/base.py:127
           └─ BaseDocumentIngest.file_to_vector(...)   # aichain/ingest/base.py:91-133
                ├─ get_text_split()                    # 默认 ParagraphCharacterTextSplitter
                ├─ get_file_loader(file_path, partition_type, knowledge_id, split_type)
                │    └─ aichain/document_loader/__init__.py:132（FILE_LOADER_MAP 分派）
                ├─ loader.load_and_split(text_splitter)  ← langchain BaseLoader 标准用法
                └─ doc_persistent(docs) → vector_store.from_documents  ← demo 的 ESStore.add_documents

各类型生产实现 → demo 复刻：
    pdf  : UnstructuredPaddlePDFLoader(pdf_loader.py:16)
           全图片页→RapidOCR；否则 pdf2docx 转成 docx 再走 partition_docx
    word : WordDocumentLoader(word_document.py:17) → partition_docx(word_partition.py:378)
           标题分块，subject=「父标题,当前标题」
    md   : UnstructuredPaddleMDLoader(md_loader.py:15)
           pandoc 转 docx → partition_docx（与 pdf 殊途同归）
    excel: MAASExcelLoader→maas_partition_xlsx(excel_loader.py:240)
           表头识别 + 合并单元格展开 + '序号'行主键 → 行转陈述句
    faq  : 独立入口 knowledge_faq_batch_ingest(ingest.py:653)→handle_faq_dox(ingest.py:624)
           Heading=问题、正文=答案 → DataFrame(question,answer) → 双份入库
           （faq 不走 FILE_LOADER_MAP，由 file_ingest 的 chain='knowledge_faq' 区分，
            demo 同样在 ingest_service 里分派，见 ingest_service.py）

demo 与生产的差异（逐处有注释）：
    · pdf 全图片页：生产走 RapidOCR，demo 不带 OCR 依赖 → 明确报错提示
    · md 无 pandoc 时：纯 Python 把 # 标题写成 docx Heading（保留"md→docx→标题分块"
      的管线本质；生产环境恒有 pandoc）
    · excel：不提取单元格图片；.xls 老格式不单独支持（生产 ExcelLoader 为
      "列名是值"简版，demo 只复刻 MAAS 主版本，xls 提示转存 xlsx）
    · 类型探测：生产 unstructured.detect_filetype + magic，demo 用文件后缀
    · txt/csv/html/ppt 等其余类型：生产 FILE_LOADER_MAP 也有，demo 未复刻
      （报错提示不支持，保持行为显式）
"""
import os
import re
import shutil
import subprocess
from typing import Any, List, Optional, Tuple

import docx as docx_lib
import openpyxl
from langchain_core.documents import Document
from langchain_core.document_loaders import BaseLoader

from obs import obs                                  # langfuse 阶段埋点（no-op 安全）

# 生产 OPSGPT_DOC_CHUNK_SIZE：excel 行转陈述句时的单块上限（excel_loader.py:14）
DOC_CHUNK_SIZE = int(os.getenv('OPSGPT_DOC_CHUNK_SIZE', 1000))
# 生产 word_partition.py:44 OPSGPT_TITLE_SPLIT：subject 的父/子标题分隔符
TITLE_SPLIT = ","


# ===========================================================================
# ① word 解析 —— partition_docx(word_partition.py:378) + handle_element(:106)
# ===========================================================================
def _strip_numbering(text: str) -> str:
    """剥标题编号（word_partition.py:471 `_paragraph_to_element` 同款正则：
    '1.2 丢弃定时器' → '丢弃定时器'）。"""
    return re.sub(r'^\d+(?:\.\d+){1,4}', '', text).strip()


def _heading_level_by_style(paragraph) -> Optional[int]:
    """样式名判定标题级别（生产 _get_title_level:458 + DOC_STYLE_MAPPING）：
    'Title'→1，'Heading N'→N，其余 None（正文）。"""
    style_name = paragraph.style.name if paragraph.style else ""
    if style_name == "Title":
        return 1
    if style_name.startswith("Heading"):
        m = re.search(r'(\d+)', style_name)
        return int(m.group(1)) if m else 1
    return None


def _paragraph_font_size(paragraph) -> Optional[float]:
    """段落主字号 —— 生产 _get_paragraph_possible_font_size(:489)：取 runs 里
    非空 run 的字号（显式 run 字号优先，无则回退样式字号）。"""
    sizes = [r.font.size.pt for r in paragraph.runs
             if r.text.strip() and r.font.size is not None]
    if sizes:
        return max(set(sizes), key=sizes.count)        # 众数：正文混排时取主流字号
    if paragraph.style and paragraph.style.font and paragraph.style.font.size:
        return paragraph.style.font.size.pt
    return None


def _is_possible_title(text: str, font_size: Optional[float],
                       body_size: Optional[float]) -> bool:
    """无 Heading 样式时的标题启发 —— 生产 _is_possible_title(:637) /
    _is_possible_tile_by_text(:588) 的精简版（pdf2docx 转出的 docx 没有样式，
    生产正是靠这组字号+文本形态启发识别标题）：
      ① 短文本（生产阈值 24 字符）
      ② 不以句尾标点结束（'。；，'等结尾的是句子）
      ③ 字号明显大于正文主字号（>body+0.5）"""
    if not text or len(text) > 24:
        return False
    if text[-1] in "。；;，,．.":
        return False
    if font_size and body_size and font_size > body_size + 0.5:
        return True
    return False


def _body_font_size(document) -> Optional[float]:
    """全文正文主字号：所有段落字号的众数（生产按逐段与前段的比较判定，
    demo 用全局众数，效果等价且更稳）。"""
    sizes = []
    for p in document.paragraphs:
        s = _paragraph_font_size(p)
        if s:
            sizes.append(s)
    if not sizes:
        return None
    return max(set(sizes), key=sizes.count)


def partition_docx(filename: str, knowledge_id: str = None) -> List[Document]:
    """docx → List[Document]，标题分块聚合。

    生产结构（word_partition.py）：handle_element 先产出
    (element, current_title, parent_title) 元组流（每个正文段落都带它所属的
    当前/父标题引用），partition_docx(:399-428) 再把相邻正文聚合、把
    subject 写进 metadata（父≠子时 '父标题,子标题'，:406-409）。

    demo 用单遍聚合等价实现（原 parse_docx 升级版）：
      · 按文档流顺序遍历 body（段落 + 表格混排都按原顺序处理——生产
        handle_element 同样按 body 顺序；表格转文本走 _convert_table_to_text，
        复刻 :542-563）
      · 标题判定 = Heading/Title 样式 ∨ 字号启发（供 pdf2docx 转出的无样式 docx）
      · level≤1 或首个标题 → parent_title；level≥2 → current_title
      · 正文聚合到当前标题下，flush 时写 subject
    knowledge_id：生产用于图片上传 minio 的 bucket（:107），demo 保留形参不使用。
    """
    from docx.table import Table
    from docx.text.paragraph import Paragraph

    document = docx_lib.Document(filename)
    body_size = _body_font_size(document)
    sections: List[Tuple[str, List[str]]] = []          # (subject, [正文段])
    parent_title, current_title, current_body = None, None, []

    def flush():
        if current_title is not None and current_body:
            subject = current_title
            if parent_title and parent_title != current_title:
                subject = TITLE_SPLIT.join([parent_title, current_title])
            sections.append((subject, current_body))
        elif current_body:
            # 兜底：全文无标题（MinerU 的 markdown 无 #、word 无 Heading 且
            # 字号启发未命中）时正文不能丢——用文件名当 subject，等价生产
            # unstructured 对无标题文档的 Title 判定兜底
            sections.append((os.path.splitext(os.path.basename(filename))[0],
                             current_body))

    # 文档流顺序遍历（w:p=段落，w:tbl=表格），保持原始混排顺序
    for child in document.element.body.iterchildren():
        tag = child.tag
        if tag.endswith('}p'):
            para = Paragraph(child, document)
            text = para.text.strip()
            if not text:
                continue
            level = _heading_level_by_style(para)
            if level is None and _is_possible_title(text, _paragraph_font_size(para), body_size):
                level = 1                                # 字号启发命中按一级标题
            if level is not None:
                flush()
                current_body = []
                current_title = _strip_numbering(text) or text
                if level <= 1 or parent_title is None:
                    parent_title = current_title
            else:
                current_body.append(text)
        elif tag.endswith('}tbl'):
            table = Table(child, document)
            table_text = _convert_table_to_text(table)
            if table_text:
                current_body.append(table_text)         # 表格文本并入当前标题正文
    flush()

    # 生产 load()（word_document.py:64-94）：metadata 带 subject/source；
    # 文件名正则 `_后取知识名` 拼 subject 前缀是上传路径约定，demo 用 basename
    return [Document(page_content="\n".join(body),
                     metadata={"subject": subject, "source": os.path.basename(filename)})
            for subject, body in sections]


class WordDocumentLoader(BaseLoader):
    """word loader —— 复刻 WordDocumentLoader(word_document.py:17)。

    生产 mode='elements'（FILE_LOADER_MAP docx 项的 kwargs）：每个聚合后的
    "标题节"一个 Document；load_and_split 用基类默认实现（text_splitter.
    split_documents(self.load())），与生产覆写版(word_document.py:96-105)一致。
    partition_type：生产 !='default' 时动态 import third_part 插件(:53-55)，
    demo 无插件体系，仅保留参数与日志。"""

    def __init__(self, file_path: str, mode: str = "elements",
                 partition_type: str = "default", knowledge_id: str = None, **kwargs: Any):
        self.file_path = file_path
        self.mode = mode
        self.partition_type = partition_type
        self.knowledge_id = knowledge_id

    def load(self) -> List[Document]:
        print(f"[loader] word: {os.path.basename(self.file_path)}"
              f"（partition_type={self.partition_type}）", flush=True)
        return partition_docx(self.file_path, self.knowledge_id)


# ===========================================================================
# ② pdf 解析 —— MinerU（pdf→markdown→docx→标题分块）
#    生产对照 UnstructuredPaddlePDFLoader(pdf_loader.py:16)：全图片页→RapidOCR；
#    文本型→pdf2docx→partition_docx。demo 主路径升级为 MinerU（布局分析+
#    OCR 一体，图片型 PDF 也能解析），pdf2docx 保留为 MinerU 失败时的降级。
# ===========================================================================
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


# ===========================================================================
# ③ md 解析 —— UnstructuredPaddleMDLoader(md_loader.py:15)
# ===========================================================================
def _convert_table_to_text(table) -> str:
    """docx 表格 → 纯文本 —— 复刻生产 _convert_table_to_text(word_partition.py:542-563)：
    每行单元格拼接（内部换行剥掉）、空行跳过（_is_empty_row:537-540）、行间换行。
    demo 的表格来源：MinerU markdown 表格转成的 docx 表格、word 文档原生表格。"""
    lines = []
    for row in table.rows:
        cells = [c.text.replace("\n", "").strip() for c in row.cells]
        if any(cells):                                # 空行跳过（生产 _is_empty_row）
            lines.append("".join(cells))
    return "\n".join(lines)


def _markdown_text_to_docx(md_text: str, docx_path: str):
    """markdown 文本 → docx —— pdf(MinerU) 与 md 两条路线共用的转换。

    结构映射（对照生产解析产物，保证后续 partition_docx 同路处理）：
      · `#`~`######` 标题 → add_heading(level)（标题合并/父子切分依赖它）
      · markdown 表格块（|表头| + |---| 分隔行 + 数据行）→ docx 真表格
        （add_table；partition_docx 侧再按生产 _convert_table_to_text 拉平）
      · 图片行 `![alt](url/base64)` → 跳过（对向量检索是噪声；生产图片走
        minio 提取，demo 不落盘）
      · 其余行（列表/公式/正文）→ add_paragraph 原文
    """
    d = docx_lib.Document()
    lines = md_text.splitlines()
    i = 0
    while i < len(lines):
        text = lines[i].rstrip()
        if not text.strip():
            i += 1
            continue
        # 图片行：跳过（见 docstring）
        if re.match(r'^\s*!\[[^\]]*\]\([^)]*\)\s*$', text):
            i += 1
            continue
        # 标题行
        m = re.match(r'^(#{1,6})\s+(.*)$', text)
        if m:
            d.add_heading(m.group(2), level=len(m.group(1)))
            i += 1
            continue
        # 表格块：|表头| 行 + |---| 分隔行 起头，连续 | 行为一个表
        if text.strip().startswith('|') and i + 1 < len(lines) and \
                re.match(r'^\|[\s:|\-]+\|?$', lines[i + 1].strip()):
            header = [c.strip() for c in text.strip().strip('|').split('|')]
            rows = []
            j = i + 2                                  # 跳过分隔行
            while j < len(lines) and lines[j].strip().startswith('|'):
                rows.append([c.strip() for c in lines[j].strip().strip('|').split('|')])
                j += 1
            tbl = d.add_table(rows=1 + len(rows), cols=len(header))
            try:
                tbl.style = 'Table Grid'
            except Exception:
                pass                                   # 默认模板缺样式时裸表也可
            for c, val in enumerate(header):
                tbl.rows[0].cells[c].text = val
            for r, row in enumerate(rows, start=1):
                for c in range(len(header)):
                    tbl.rows[r].cells[c].text = row[c] if c < len(row) else ''
            i = j
            continue
        if text.strip() in ("---", "***"):
            i += 1
            continue                                   # 分隔线跳过
        d.add_paragraph(text.strip())
        i += 1
    d.save(docx_path)


def _md_to_docx_by_python(md_path: str, docx_path: str):
    """无 pandoc 时的 fallback：读 md 文件 → _markdown_text_to_docx。

    生产环境恒有 pandoc（md_loader.py:20 直接 subprocess.run），demo 机器可能
    没有，故保留"md→docx→partition_docx"管线本质的同时用纯 Python 转换。"""
    with open(md_path, "r", encoding="utf-8") as f:
        _markdown_text_to_docx(f.read(), docx_path)


class MarkdownDocumentLoader(BaseLoader):
    """md loader —— 复刻 UnstructuredPaddleMDLoader._get_elements(md_loader.py:18-22)：
    pandoc -s -o <docx> <md> 转成 docx 后走 partition_docx；
    demo 加了无 pandoc 的纯 Python fallback（见 _md_to_docx_by_python）。"""

    def __init__(self, file_path: str, mode: str = "elements", **kwargs: Any):
        self.file_path = file_path
        self.mode = mode

    def load(self) -> List[Document]:
        print(f"[loader] md: {os.path.basename(self.file_path)}", flush=True)
        docs_path = self.file_path.replace('.md', '.docx')
        if shutil.which("pandoc"):
            # 生产 md_loader.py:20 原样
            subprocess.run(["pandoc", "-s", "-o", docs_path, self.file_path], check=True)
        else:
            print("    [loader] 未安装 pandoc，用纯 Python md→docx fallback"
                  "（生产用 pandoc）", flush=True)
            _md_to_docx_by_python(self.file_path, docs_path)
        docs = partition_docx(docs_path)
        for d in docs:
            d.metadata["source"] = os.path.basename(self.file_path)
        return docs


# ===========================================================================
# ④ excel 解析 —— maas_partition_xlsx(excel_loader.py:240-428)
# ===========================================================================
def _unmerge_and_fill_cells(worksheet):
    """合并单元格展开填充 —— 生产 excel_loader.py:23-32 unmerge_and_fill_cells 原样：
    拆掉合并区域，把左上角的值填进区域内每个格子（否则只有第一格有值，
    行转陈述句会丢数据）。"""
    for merged_cell_range in list(worksheet.merged_cells.ranges):
        merged_cell = merged_cell_range.start_cell
        worksheet.unmerge_cells(range_string=merged_cell_range.coord)
        for row_index, col_index in merged_cell_range.cells:
            worksheet.cell(row=row_index, column=col_index).value = merged_cell.value


def _create_row_doc(sheet_name: str, sheet_num: int, source: str,
                    row_number: int, content_text: str) -> Document:
    """行 → Document —— 生产 _create_element_from_row(excel_loader.py:122-135)：
    metadata 记 page_name(sheet名)/page_number(sheet序号)/filename/row(行号)。"""
    return Document(page_content=content_text, metadata={
        "page_name": sheet_name, "page_number": sheet_num,
        "source": os.path.basename(source), "row": row_number})


def maas_partition_xlsx(filename: str) -> List[Document]:
    """xlsx → 行转陈述句 —— 生产 maas_partition_xlsx(excel_loader.py:240-428) 复刻。

    输出形态（老代码只有"列名是值"，MAAS 版多了行主键维度）：
      表头行：'网络参数'（合并单元格当表头时拼接上方合并值）
      主键列（'序号'起头的连续前缀列，生产 :398-404）：
          "参数名是discardTimer"            ← 行主键自身的陈述句
      数据列：
          "参数名+discardTimer是默认值500毫秒" ← 列名+行主键是值（三元组句子）
    demo 与生产的差异：
      · 不提取单元格内图片与浮动图片（生产 :328-337/:406-417 存盘并记路径）
      · 生产 max_col 先扫前 20 行估算(:294-312)加速，demo 直接读 sheet.max_column
      · xls 老格式（生产 ExcelLoader.partition_xlsx:54 简版"列名是值"）不支持
    """
    workbook = openpyxl.load_workbook(filename, data_only=True)
    elements: List[Document] = []
    try:
        for sheet_num, sheet in enumerate(workbook.worksheets, start=1):
            if sheet.sheet_state != "visible":         # 生产只处理可见 sheet(:42)
                continue
            _unmerge_and_fill_cells(sheet)             # 合并单元格先展开
            max_col = sheet.max_column or 0
            rows = list(sheet.iter_rows(min_col=1, max_col=max_col, values_only=True))

            table_head: List[Optional[str]] = []       # 列名（None/空=待后续行补齐）
            row_head_col = 0                           # 行主键列数（'序号'前缀）
            header_row_seen = False                    # 表头行是否已出现
            head_checked = False                       # 表头是否已解析完成

            def _check_header_done():
                """生产 :391-404：表头无空占位即解析完成，并统计'序号'前缀
                连续列数为主键列宽。"""
                nonlocal row_head_col
                if table_head and all(h not in (None, ' ') for h in table_head):
                    for h in table_head:
                        if h and '序号' in h:
                            row_head_col += 1
                        else:
                            break
                    return True
                return False

            for row in rows:
                # 全空行直接跳过（生产 _is_empty_row:16-20 的语义；首行全空的
                # 表格，真正的表头顺延到下一个非空行）
                if all(v is None for v in row):
                    continue

                if not header_row_seen:
                    # ---- 表头行（第一个非空行）：整行收集列名 ----
                    # 生产 :342-350：有值取值（换行剥掉），空格/合并格占位
                    for value in row:
                        if value is None:
                            table_head.append(' ')
                        else:
                            table_head.append(str(value).replace("\n", ""))
                    header_row_seen = True
                    head_checked = _check_header_done()
                    continue

                # ---- 数据行 / 多行表头补齐行 ----
                content_text = ''
                row_head = ''                          # 本行主键值（如序号）
                for col_count, value in enumerate(row, start=1):
                    if value is None:
                        continue
                    v = str(value).replace("\n", ";")  # 单元格内换行→分号(生产:382)
                    if not head_checked and col_count - 1 < len(table_head) \
                            and table_head[col_count - 1] in (None, ' '):
                        # 表头还没齐：本行值并入表头（生产 get_merged_head :224-237
                        # 的合并多行表头语义——上方合并单元格值拼接）
                        table_head[col_count - 1] = v
                        continue
                    if col_count - 1 >= len(table_head):
                        continue                       # 超出表头宽度的孤儿列跳过
                    head_name = table_head[col_count - 1] or ' '
                    if col_count <= row_head_col:
                        # 主键列：'列名是主键值'（生产 :364-376）
                        row_head = v
                        content_text += f"{head_name}是{v},"
                    else:
                        # 数据列：'列名+行主键是值' 三元组（生产 :378-382）
                        prefix = f"{head_name}+{row_head}" if row_head else head_name
                        content_text += f"{prefix}是{v},"
                if content_text:
                    elements.append(_create_row_doc(
                        sheet.title, sheet_num, filename, len(elements) + 1,
                        content_text.rstrip(',')))
                # 行尾再查一次表头是否补齐（生产每行尾部检查 :391-404）
                if not head_checked:
                    head_checked = _check_header_done()
        return elements
    finally:
        workbook.close()


class ExcelDocumentLoader(BaseLoader):
    """excel loader —— 复刻 MAASExcelLoader(excel_loader.py:430)（xlsx 主版本）。

    FILE_LOADER_MAP 里 xlsx→MAASExcelLoader（表头+行主键语义），
    xls→ExcelLoader（"列名是值"简版 partition_xlsx:54）；demo 统一 MAAS 版。"""

    def __init__(self, file_path: str, mode: str = "elements",
                 partition_type: str = "default", **kwargs: Any):
        self.file_path = file_path
        self.mode = mode
        self.partition_type = partition_type

    def load(self) -> List[Document]:
        if self.file_path.endswith(".xls"):
            raise ValueError(".xls 老格式 demo 未复刻（生产 ExcelLoader 简版），请转存 .xlsx")
        print(f"[loader] excel: {os.path.basename(self.file_path)}", flush=True)
        return maas_partition_xlsx(self.file_path)


# ===========================================================================
# ⑤ faq 解析 —— handle_faq_dox(server/tasks/ingest.py:624)
# ===========================================================================
def handle_faq_docx(filename: str) -> "List[Tuple[str, str]]":
    """FAQ docx → [(question, answer), ...]

    生产 handle_faq_dox(ingest.py:624-639)：handle_element 产出
    (element, current_title, parent_title) 流；标题（Heading/Title 样式且
    text==current_title.text）→ 新问题的 key；正文归入当前问题（标题非
    Heading 时取父标题兜底，:632-635 是"短正文被误判标题"的容错）。
    最后 {问题: '\n'.join(答案)} → DataFrame(columns=['question','answer'])。

    demo 等价实现返回二元组列表（DataFrame 化在 ingest_service 里做，
    与生产 knowledge_faq_batch_ingest 拿 DataFrame 遍历的形态衔接）。"""
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


# ===========================================================================
# ⑥ 类型分派 —— get_file_loader(aichain/document_loader/__init__.py:132)
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
