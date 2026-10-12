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

# 评估集：(问题, 标准答案)——覆盖五种入库来源
EVAL_SET = [
    ("PDCP丢弃定时器的默认值是多少？",                # word
     "PDCP丢弃定时器（discardTimer）默认值为500毫秒，取值范围50到3000毫秒。"),
    ("A5事件的门限默认值是多少？",                    # md
     "A5事件门限：门限1默认为-100dBm，门限2默认为-105dBm。"),
    ("传输时延阈值超过多少会告警？",                  # pdf
     "传输时延阈值默认值为50毫秒，实际时延连续5个周期超过该阈值时触发传输劣化告警。"),
    ("状态报告周期的取值范围是多少？",                # excel
     "状态报告周期取值范围为100~1000毫秒，默认值200毫秒。"),
    ("如何重置设备的管理员密码？",                    # faq(docx)
     "登录设备Web页面，进入系统工具→配置文件，先备份配置再选择恢复出厂设置；"
     "或使用CLI命令 reset admin-password 进行重置。"),
    ("如何备份设备配置？",                           # faq(excel)
     "登录设备Web页面，进入系统工具→配置文件，点击导出配置并设置加密口令。"),
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


async def collect_answers(get_qa_chain) -> list:
    """跑检索拿 (answer, contexts) —— 生产评估同样以 only_retrieval=False
    全链路产物（答案+引用）为评估输入。"""
    samples = []
    for question, ground_truth in EVAL_SET:
        print(f"[eval] 检索: {question}", flush=True)
        ret = await knowledges_retrieve(get_qa_chain, question, [KB_ID],
                                        max_recalls=4, min_score=0.5)
        samples.append({
            "user_input": question,
            "response": ret["result"],
            "retrieved_contexts": [d.content for d in ret["source_documents"]],
            "reference": ground_truth,
        })
    return samples


def run_ragas(samples: list, judge_llm, embeddings) -> dict:
    """ragas 评估主体（ragas 0.2.x API：dataset 要 EvaluationDataset/HF Dataset，
    不能传裸 list）。RunConfig 放宽超时/重试（微信接口偶发慢）。"""
    from datasets import Dataset as HF_Dataset
    from ragas import evaluate, RunConfig
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from ragas.llms import LangchainLLMWrapper
    from ragas.metrics import (answer_relevancy, context_precision,
                               context_recall, faithfulness)

    ds = HF_Dataset.from_list(samples)           # list[dict] → HF Dataset
    run_config = RunConfig(timeout=300, max_workers=2, max_retries=5)
    result = evaluate(
        dataset=ds,
        metrics=[context_precision, context_recall, faithfulness,
                 answer_relevancy],
        llm=LangchainLLMWrapper(judge_llm),
        embeddings=LangchainEmbeddingsWrapper(embeddings),
        run_config=run_config,
        show_progress=True)
    # ragas 0.2 的 EvaluationResult.scores 是 list[dict]（逐样本各指标分），
    # 转 DataFrame 后按列取均值即总评分
    import pandas as pd
    return pd.DataFrame(result.scores).mean(numeric_only=True).to_dict()


def main():
    print("=" * 60, flush=True)
    print("ragas 评估：context_precision/recall + faithfulness/answer_relevancy",
          flush=True)
    embedder, reranker, llm = build_clients()
    store = ESStore(embedder)
    if not store.client.indices.exists(index=store.index_name):
        raise SystemExit("索引不存在，请先运行 rag_demo.py --reset 入库")
    get_qa_chain = build_qa_chain_factory(store, embedder, reranker, llm, top_k=4)

    # ① 收集答案与引用
    samples = asyncio.run(collect_answers(get_qa_chain))

    # ② ragas 评估（judge=微信 LLM，embeddings=TEI bge-m3）
    judge = build_judge_llm()
    embeddings = TEILangchainEmbeddings(embedder)
    scores = run_ragas(samples, judge, embeddings)

    # ③ 输出 + langfuse 埋点
    print("-" * 60, flush=True)
    obs = Obs()
    trace = obs.trace(name="ragas_eval", input=[s["user_input"] for s in samples])
    obs.generation(trace, name="ragas_scores", output=scores,
                   model="ragas+Deepseek-v4-flash+bge-m3")
    obs.flush()
    print("[ragas] 指标均值：", flush=True)
    for k, v in scores.items():
        print(f"    {k}: {v:.3f}", flush=True)
    print("[完成] 详情见 langfuse UI（trace: ragas_eval）", flush=True)


if __name__ == "__main__":
    main()
