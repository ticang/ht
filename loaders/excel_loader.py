# -*- coding: utf-8 -*-
"""
excel_loader.py —— excel（xlsx）解析（复刻 excel_loader.py 的 MAAS 主版本）

生产对照（aichain/document_loader/excel_loader.py:240-428）：
    unmerge_and_fill_cells(:23-32)        合并单元格展开填充
    _create_element_from_row(:122-135)    行 → Document（page_name/page_number/
                                          filename/row 四键 metadata）
    get_merged_head(:224-237)             多行表头拼接
    maas_partition_xlsx(:240-428)         表头识别 + '序号'行主键 → 行转陈述句

输出形态（老代码只有"列名是值"，MAAS 版多了行主键维度）：
    主键列（'序号'起头的连续前缀列，:398-404）："参数名是discardTimer"
    数据列："参数名+discardTimer是默认值500毫秒"（列名+行主键是值 三元组）

demo 与生产的差异：
    · 不提取单元格内图片与浮动图片（生产 :328-337/:406-417 存盘并记路径）
    · 生产 max_col 先扫前 20 行估算(:294-312)加速，demo 直接读 sheet.max_column
    · 表头补齐简化为单列补值（生产逐格拼接，语义近似）
    · xls 老格式（生产 ExcelLoader.partition_xlsx:54 简版"列名是值"）不支持
      （生产 OPSGPT_DOC_CHUNK_SIZE 的行转陈述句分块 demo 未复刻，量级小整行一块）
"""
import os
from typing import Any, List, Optional

import openpyxl
from langchain_core.documents import Document
from langchain_core.document_loaders import BaseLoader


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
    """xlsx → 行转陈述句 —— 生产 maas_partition_xlsx(excel_loader.py:240-428) 复刻。"""
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
