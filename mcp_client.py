# -*- coding: utf-8 -*-
"""
mcp_client.py —— 工单查询 MCP 客户端（MCP_QUERY 分支的执行体）

生产对照（aiops-agent-core）：
  AssistantService.stream :1547-1568 的 MCP 分支（_stream_mcp_with_knowledge_
  fallback 语义）：MCP_QUERY → 调 MCP 服务查询 → 成功直接返回；服务失败自动
  降级知识库检索。demo 此前只有"降级"半边（assistant_ask 的占位分支），
  本文件补上"真调用"半边，成功/失败语义与生产对齐：
    · 正常响应（含"查无此单/请提供工单号"这类服务端答复）→ 返回答案字符串，
      不降级——服务正常工作，只是没查到
    · 调用本身失败（超时/连接错/5xx）→ 返回 None，由调用方降级知识检索

双模式（config.MCP_TICKET_URL）：
  · 未配置（默认）：内置 mock 工单数据，零外部依赖即可演示全链路
    （工单号形态与 classifier 分边界规则同源：ZW-/ATRUST- 开头）
  · 已配置：POST {"question": ...} 到该 URL（快速失败——重试 1 次、间隔 1s，
    降级是兜底路径，不吃 clients 层的长重试窗）；host 须在 HTTP_ALLOWED_HOSTS
    白名单内（接真服务时自行加白），复用 clients._post_with_retry 的
    白名单校验与过载退避
"""
import asyncio
import re
import time
from typing import Optional

import config
from clients import _post_with_retry, obs

# 工单号形态与分类 prompt 的边界规则同源（classifier.py §1 的排除项：
# ZW-/ATRUST- 开头的编号归工单流程类）
_TICKET_ID_RE = re.compile(r"(?:ZW-\d{4,}-\d+|ATRUST-[A-Za-z0-9-]+)")

# 内置 mock 工单表（demo 无真 MCP 服务；配 MCP_TICKET_URL 后改走真服务）
MOCK_TICKETS = {
    "ZW-20261012-001": {"title": "PDCP丢包率异常", "status": "处理中",
                        "progress": "60%", "owner": "网络运维二组",
                        "updated": "2026-10-13 16:40"},
    "ZW-20261009-008": {"title": "传输带宽扩容", "status": "已完结",
                        "progress": "100%", "owner": "传输保障组",
                        "updated": "2026-10-11 09:12"},
}


def extract_ticket_id(question: str) -> Optional[str]:
    """从问题里抽工单编号（ZW-/ATRUST- 形态）；抽不到返回 None。
    生产是 MCP 服务端/agent 负责解析参数，demo 在客户端侧正则等价。"""
    m = _TICKET_ID_RE.search(question.upper())
    return m.group(0) if m else None


class McpTicketClient:
    """工单查询客户端 —— mock / http 双模式，接口 query(question) → 答案或 None。"""

    def __init__(self, timeout: int = 15):
        self.base_url = (config.MCP_TICKET_URL or "").rstrip("/")
        self.mode = "http" if self.base_url else "mock"
        self.timeout = timeout
        print(f"[mcp] 工单查询客户端就绪（mode={self.mode}）", flush=True)

    # ------------------------------------------------------------------
    # 查询主体（同步；异步入口 aquery 丢线程池）
    # ------------------------------------------------------------------
    def query(self, question: str) -> Optional[str]:
        """工单/流程查询 → 格式化答案；服务失败返回 None（降级信号）。

        生产对照：MCP 服务正常响应（哪怕"查无此单"）都算成功直答；
        只有服务本身不可用才走降级知识检索。"""
        t0 = time.perf_counter()
        try:
            if self.mode == "mock":
                answer = self._query_mock(question)
            else:
                answer = self._query_http(question)
        except Exception as e:
            # 服务失败（连接/超时/5xx）：None = 降级信号（生产 :1547-1568）
            print(f"[mcp] 查询失败（{type(e).__name__}: {str(e)[:80]}），"
                  f"耗时 {time.perf_counter() - t0:.1f}s", flush=True)
            return None
        print(f"[mcp] 查询成功（mode={self.mode}，"
              f"耗时 {time.perf_counter() - t0:.1f}s）：{answer[:60]}", flush=True)
        return answer

    async def aquery(self, question: str) -> Optional[str]:
        """异步入口 —— requests 同步调用丢线程池（同 TEI 客户端的桥接方式；
        to_thread 复制 contextvars，埋点挂当前 trace）。"""
        with obs.span("mcp_query", input={"question": question[:60],
                                          "mode": self.mode}) as s:
            answer = await asyncio.to_thread(self.query, question)
            if s:
                s.update(output={"answer": answer[:80] if answer else None})
        return answer

    # ------------------------------------------------------------------
    # mock 模式：内置假工单表（含"无编号/查无此单"两类正常答复）
    # ------------------------------------------------------------------
    @staticmethod
    def _query_mock(question: str) -> str:
        ticket_id = extract_ticket_id(question)
        if not ticket_id:
            return ("未在问题中识别到工单编号，无法查询工单进度。"
                    "请提供工单编号（ZW- 或 ATRUST- 开头）后再试。")
        ticket = MOCK_TICKETS.get(ticket_id)
        if not ticket:
            return f"工单系统未查到编号 {ticket_id} 的记录，请确认编号是否正确。"
        return (f"工单 {ticket_id}【{ticket['title']}】\n"
                f"状态：{ticket['status']}（进度 {ticket['progress']}）\n"
                f"处理组：{ticket['owner']}\n"
                f"最近更新：{ticket['updated']}")

    # ------------------------------------------------------------------
    # http 模式：POST {question} → 期望 JSON（answer 字段优先，整包兜底）
    # ------------------------------------------------------------------
    def _query_http(self, question: str) -> str:
        resp = _post_with_retry(self.base_url, retries=1, wait=1.0,
                                json={"question": question}, timeout=self.timeout)
        resp.raise_for_status()                        # 5xx → 异常 → 降级
        data = resp.json()
        if isinstance(data, dict) and data.get("answer"):
            return str(data["answer"])
        import json as _json
        return _json.dumps(data, ensure_ascii=False)
