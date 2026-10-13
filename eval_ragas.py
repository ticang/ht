# -*- coding: utf-8 -*-
"""
eval_ragas.py —— ragas 评估（demo 新增，生产的 only_retrieval 评估模式落地）

生产对照：链路里的 only_retrieval 参数（knowledge.py:82/:92 透传）正是为
ragas 类评估预留——只取检索结果不生成；生产用它做知识库质量回归。demo 把
这一环补全成可运行的 ragas 评估脚本：

  评估对象：rag_demo.knowledges_retrieve（与生产 qa_chain.acall 同构的检索）
  Judge LLM：微信 Deepseek-v4-flash（langchain_openai.ChatOpenAI 兼容接入，
             ragas 原生吃 langchain 模型）
  Embeddings：TEI bge-m3（包成 langchain Embeddings 接口，ragas 的
             answer_relevancy 需要向量）
  指标（ragas 经典四件）：
    context_precision  引用里相关块是否排在前面（判据=ground_truth）
    context_recall     ground_truth 的要点是否被引用覆盖
    faithfulness       答案是否忠于引用（无编造）
    answer_relevancy   答案与问题的相关度（向量相似）
  评估集：6 条问答对，覆盖 word/md/pdf/excel/faq 五种入库类型

运行：
    .venv/Scripts/python eval_ragas.py           # 索引需已入库（先跑 rag_demo/api_server）
输出：各指标均值 + langfuse trace（name=ragas_eval）
"""
import asyncio
import sys

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:                              # pragma: no cover
    pass

from langchain_core.embeddings import Embeddings
from langchain_openai import ChatOpenAI

import config
from clients import build_clients
from es_store import ESStore
from obs import Obs
from rag_demo import build_qa_chain_factory, knowledges_retrieve

KB_ID = "kb-demo"

# 评估集：(问题, 标准答案, 解析类型)——覆盖五种入库来源，出分时按类型分组
# （"ragas 覆盖解析"：不同解析器的检索质量可分别对比）
EVAL_SET = [
    ("PDCP丢弃定时器的默认值是多少？",                # word
     "PDCP丢弃定时器（discardTimer）默认值为500毫秒，取值范围50到3000毫秒。",
     "word"),
    ("A5事件的门限默认值是多少？",                    # md
     "A5事件门限：门限1默认为-100dBm，门限2默认为-105dBm。",
     "md"),
    ("传输时延阈值超过多少会告警？",                  # pdf
     "传输时延阈值默认值为50毫秒，实际时延连续5个周期超过该阈值时触发传输劣化告警。",
     "pdf"),
    ("状态报告周期的取值范围是多少？",                # excel
     "状态报告周期取值范围为100~1000毫秒，默认值200毫秒。",
     "excel"),
    ("如何重置设备的管理员密码？",                    # faq(docx)
     "登录设备Web页面，进入系统工具→配置文件，先备份配置再选择恢复出厂设置；"
     "或使用CLI命令 reset admin-password 进行重置。",
     "faq_docx"),
    ("如何备份设备配置？",                           # faq(excel)
     "登录设备Web页面，进入系统工具→配置文件，点击导出配置并设置加密口令。",
     "faq_excel"),
]


class TEILangchainEmbeddings(Embeddings):
    """TEI 嵌入客户端 → langchain Embeddings 接口（ragas 需要）。
    对应生产的 embedding_factory.get_embeddings()（langchain 生态接入位）。"""

    def __init__(self, tei_client):
        self.tei = tei_client

    def embed_documents(self, texts):
        return self.tei.embed(list(texts))

    def embed_query(self, text):
        return self.tei.embed([text])[0]


def build_judge_llm():
    """微信 OpenAI 兼容接口 → ChatOpenAI（ragas 的 judge）。
    base_url 用 v1 根（ChatOpenAI 自动拼 /chat/completions）。"""
    return ChatOpenAI(
        base_url=config.LLM_BASE_URL, api_key=config.LLM_API_KEY,
        model=config.LLM_MODEL, temperature=0)


async def collect_answers(get_qa_chain, obs) -> list:
    """跑检索拿 (answer, contexts)。trace_scope 让每次检索内部的
    嵌入/三路召回/RRF/去重/重排/生成埋点自动挂到 ragas_eval 的 trace 下
    （ragas 对"检索"环节的完整观测）。"""
    samples = []
    for question, ground_truth, source_type in EVAL_SET:
        print(f"[eval] 检索({source_type}): {question}", flush=True)
        with obs.span("eval_retrieve", input={"question": question,
                                              "source_type": source_type}) as s:
            ret = await knowledges_retrieve(get_qa_chain, question, [KB_ID],
                                            max_recalls=4, min_score=0.5)
            if s:
                s.update(output={"citations": len(ret["source_documents"]),
                                 "titles": [d.title for d in ret["source_documents"][:3]]})
        samples.append({
            "user_input": question,
            "response": ret["result"],
            "retrieved_contexts": [d.content for d in ret["source_documents"]],
            "reference": ground_truth,
            # 分组维度（ragas 的 dataset 允许附加列，evaluate 只取指标所需列）
            "source_type": source_type,
        })
    return samples


def run_ragas(samples: list, judge_llm, embeddings) -> tuple:
    """ragas 评估主体（ragas 0.2.x API）。返回 (总体均值 dict, 逐样本 DataFrame)。
    RunConfig 放宽超时/重试（微信接口偶发慢）。"""
    from datasets import Dataset as HF_Dataset
    from ragas import evaluate, RunConfig
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from ragas.llms import LangchainLLMWrapper
    from ragas.metrics import (answer_relevancy, context_precision,
                               context_recall, faithfulness)
    import pandas as pd

    ds = HF_Dataset.from_list(samples)
    run_config = RunConfig(timeout=300, max_workers=2, max_retries=5)
    result = evaluate(
        dataset=ds,
        metrics=[context_precision, context_recall, faithfulness,
                 answer_relevancy],
        llm=LangchainLLMWrapper(judge_llm),
        embeddings=LangchainEmbeddingsWrapper(embeddings),
        run_config=run_config,
        show_progress=True)
    # EvaluationResult.scores 是 list[dict]（逐样本各指标分，与 samples 同序）
    detail = pd.DataFrame(result.scores)
    detail.insert(0, "source_type", [s["source_type"] for s in samples])
    return detail.mean(numeric_only=True).to_dict(), detail


def main():
    print("=" * 60, flush=True)
    print("ragas 评估：context_precision/recall + faithfulness/answer_relevancy",
          flush=True)
    embedder, reranker, llm = build_clients()
    store = ESStore(embedder)
    if not store.client.indices.exists(index=store.index_name):
        raise SystemExit("索引不存在，请先运行 rag_demo.py --reset 入库")
    get_qa_chain = build_qa_chain_factory(store, embedder, reranker, llm, top_k=4)

    # ① 收集答案与引用（trace_scope 内：检索各阶段埋点自动挂本 trace）
    obs = Obs()
    trace = obs.trace(name="ragas_eval",
                      input={"questions": [q for q, _, _ in EVAL_SET]})
    with obs.trace_scope(trace):
        samples = asyncio.run(collect_answers(get_qa_chain, obs))

    # ② ragas 评估（judge=微信 LLM，embeddings=TEI bge-m3）
    judge = build_judge_llm()
    embeddings = TEILangchainEmbeddings(embedder)
    scores, detail = run_ragas(samples, judge, embeddings)

    # ③ 输出：总体均值 + 按解析类型分组（word/md/pdf/excel/faq 对比）
    print("-" * 60, flush=True)
    obs.generation(trace, name="ragas_scores",
                   output={"overall": scores},
                   model="ragas+Deepseek-v4-flash+bge-m3")
    obs.flush()
    print("[ragas] 指标均值：", flush=True)
    for k, v in scores.items():
        print(f"    {k}: {v:.3f}", flush=True)
    grouped = detail.groupby("source_type").mean(numeric_only=True)
    print("[ragas] 按解析类型分组（每类样本数少，分值仅作参考）：", flush=True)
    for st, row in grouped.iterrows():
        cells = "  ".join(f"{c}={row[c]:.2f}" for c in row.index)
        print(f"    {st:<9} {cells}", flush=True)
    obs.generation(trace, name="ragas_scores_by_type",
                   output=grouped.round(3).to_dict(orient="index"))
    obs.flush()
    print("[完成] 详情见 langfuse UI（trace: ragas_eval）", flush=True)


if __name__ == "__main__":
    main()
