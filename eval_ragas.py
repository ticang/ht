# -*- coding: utf-8 -*-
"""
eval_ragas.py —— ragas 评估（demo 新增，生产的 only_retrieval 评估模式落地）

生产对照：链路里的 only_retrieval 参数（knowledge.py:82/:92 透传）正是为
ragas 类评估预留——只取检索结果不生成；生产用它做知识库质量回归。demo 把
这一环补全成可持续运转的评估闭环（数据集 → 打分 → 基线对比 → 错题归因）：

  评估对象：rag_demo.knowledges_retrieve（与生产 qa_chain.acall 同构的检索）
  Judge LLM：微信 Deepseek-v4-flash（langchain_openai.ChatOpenAI 兼容接入，
             ragas 原生吃 langchain 模型）
  Embeddings：TEI bge-m3（包成 langchain Embeddings 接口，ragas 的
             answer_relevancy/answer_correctness 需要向量）

  指标分三层（成熟方案的质量/成本/确定性组合）：
    LLM judge（ragas 五件）：
      context_precision  引用里相关块是否排在前面（判据=ground_truth）
      context_recall     ground_truth 的要点是否被引用覆盖
      faithfulness       答案是否忠于引用（无编造）
      answer_relevancy   答案与问题的相关度（向量相似）
      answer_correctness 答案与 ground_truth 的最终正确性（端到端业务指标，
                         四件之外唯一直接对应"答对了没有"）
    确定性硬指标（无 LLM、可复现——期望命中标注 expected_titles 时计算）：
      hit_rate   前k引用里是否召回期望文档（知识文档=文件名/FAQ=问题本身，
                 见 ingest_service.py:98/:197 的 name 规则）
      mrr        首个期望文档排名的倒数均值（衡量排序质量）
    成本：每题端到端延迟（token 消耗未记——WeixinLLM 客户端未暴露 usage，
          生产接入时从响应 usage 字段补上即可）

  评估集：eval_set.jsonl（外置文件，每行 user_input/reference/source_type/
          expected_titles；缺失时从 DEFAULT_EVAL_SET 引导生成，改数据不动代码）
  基线对比：eval_results/<run>.json 落盘每次跑分；baseline.json 为基线，
          自动 diff 总体/分组/逐样本翻转（评估的意义在对比，不在绝对分）

运行：
    .venv/Scripts/python eval_ragas.py                          # 首跑自动建基线
    .venv/Scripts/python eval_ragas.py --run rerank06           # 改配置后重跑，自动 diff
    .venv/Scripts/python eval_ragas.py --set-baseline           # 认可当前结果，重设基线
输出：控制台（指标/分组/延迟/diff）+ eval_results/*.json + langfuse trace
"""
import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(line_buffering=True)
except AttributeError:                              # pragma: no cover
    pass

from langchain_core.embeddings import Embeddings
from langchain_openai import ChatOpenAI

import config
from clients import build_clients
from es_store import ESStore
from obs import obs
from rag_demo import RagComponents, knowledges_retrieve

KB_ID = "kb-demo"
EVAL_SET_PATH = Path(__file__).parent / "eval_set.jsonl"
RESULTS_DIR = Path(__file__).parent / "eval_results"
BASELINE_PATH = RESULTS_DIR / "baseline.json"
PASS_THRESHOLD = 0.5        # answer_correctness ≥ 此值记"通过"（翻转判据）
REGRESSION_TOLERANCE = 0.02  # 指标相对基线跌幅超过此值提示回归（CI 门禁用同款）

# 检索参数快照（评估可比的前提：同一 run 内配置冻结；落盘供跨 run 追溯）
EVAL_CONFIG = {"kb": KB_ID, "max_recalls": 4, "min_score": 0.5, "top_k": 4,
               "llm": config.LLM_MODEL, "embedder": "bge-m3",
               "reranker": "bge-reranker-v2-m3"}

# 引导用默认评估集（eval_set.jsonl 缺失时生成；与样例文档逐一对应：
# word=sample_kb / md=sample_switch / pdf=sample_transport / excel=sample_params /
# faq_docx、faq_excel=两份 FAQ 文件。expected_titles 按 name 规则标注）
DEFAULT_EVAL_SET = [
    ("PDCP丢弃定时器的默认值是多少？",                # word
     "PDCP丢弃定时器（discardTimer）默认值为500毫秒，取值范围50到3000毫秒。",
     "word", ["sample_kb"]),
    ("A5事件的门限默认值是多少？",                    # md
     "A5事件门限：门限1默认为-100dBm，门限2默认为-105dBm。",
     "md", ["sample_switch"]),
    ("传输时延阈值超过多少会告警？",                  # pdf
     "传输时延阈值默认值为50毫秒，实际时延连续5个周期超过该阈值时触发传输劣化告警。",
     "pdf", ["sample_transport"]),
    ("状态报告周期的取值范围是多少？",                # excel（word 文档含部分要点，双期望）
     "状态报告周期取值范围为100~1000毫秒，默认值200毫秒。",
     "excel", ["sample_params", "sample_kb"]),
    ("如何重置设备的管理员密码？",                    # faq(docx)：FAQ 的 name=问题本身
     "登录设备Web页面，进入系统工具→配置文件，先备份配置再选择恢复出厂设置；"
     "或使用CLI命令 reset admin-password 进行重置。",
     "faq_docx", ["如何重置设备的管理员密码？"]),
    ("如何备份设备配置？",                           # faq(excel)
     "登录设备Web页面，进入系统工具→配置文件，点击导出配置并设置加密口令。",
     "faq_excel", ["如何备份设备配置？"]),
]

METRICS = ["context_precision", "context_recall", "faithfulness",
           "answer_relevancy", "answer_correctness"]


# ===========================================================================
# ① 评估集：外置 JSONL（缺失时从内置默认引导生成）
# ===========================================================================
def load_eval_set() -> list:
    """读 eval_set.jsonl → [{user_input, reference, source_type,
    expected_titles}]。文件缺失时用 DEFAULT_EVAL_SET 生成（首次克隆后
    直接可跑）；文件在则完全以文件为准——替换成生产真实问答对即可。"""
    if not EVAL_SET_PATH.exists():
        print(f"[eval] 评估集不存在，按内置默认生成 {EVAL_SET_PATH.name}", flush=True)
        with EVAL_SET_PATH.open("w", encoding="utf-8") as f:
            for q, gt, st, titles in DEFAULT_EVAL_SET:
                f.write(json.dumps({"user_input": q, "reference": gt,
                                    "source_type": st,
                                    "expected_titles": titles},
                                   ensure_ascii=False) + "\n")
    samples = []
    for line in EVAL_SET_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        row = json.loads(line)
        if not row.get("user_input") or not row.get("reference"):
            raise SystemExit(f"评估集行缺少 user_input/reference: {line[:50]}")
        samples.append(row)
    if not samples:
        raise SystemExit("评估集为空")
    return samples


class TEILangchainEmbeddings(Embeddings):
    """TEI 嵌入客户端 → langchain Embeddings 接口（ragas 需要）。
    对应生产的 embedding_factory.get_embeddings()（langchain 生态接入位）。"""

    def __init__(self, tei_client):
        self.tei = tei_client

    def embed_documents(self, texts):
        return self.tei.embed(list(texts))

    def embed_query(self, text):
        return self.tei.embed([text])[0]


# 思考段剥除正则（模块级——ChatOpenAI 是 pydantic 模型，类属性会被收成
# ModelPrivateAttr 占位符导致运行期 AttributeError）
_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL)


class ThinkStrippedChatOpenAI(ChatOpenAI):
    """judge 专用 ChatOpenAI：剥掉推理型模型夹带的 <think>...</think> 段。

    微信 Deepseek-v4-flash 会不定期输出思考段——ragas 按 JSON 解析 judge
    输出，遇到思考段即解析失败，该样本该指标记 NaN（现象：整行指标 nan）。
    与 clients.WeixinLLM 的思考净化（生产 _parse_llm_response 同款）同规则。
    ChatGeneration.text 是 message.content 的只读 property，须改 content。"""

    @staticmethod
    def _strip(result):
        for gen in result.generations:
            content = getattr(gen.message, "content", None)
            if content:
                gen.message.content = _THINK_RE.sub("", content).strip()
        return result

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return self._strip(super()._generate(messages, stop=stop,
                                             run_manager=run_manager, **kwargs))

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        # langchain-openai 的异步路径独立实现，不经 _generate，需分别包
        result = await super()._agenerate(messages, stop=stop,
                                          run_manager=run_manager, **kwargs)
        return self._strip(result)


def build_judge_llm():
    """微信 OpenAI 兼容接口 → ChatOpenAI（ragas 的 judge）。
    base_url 用 v1 根（ChatOpenAI 自动拼 /chat/completions）。
    注意：成熟方案里 judge 应强于被测模型，此处两者同为 Deepseek-v4-flash
    是 demo 的简化（生产接入时换更强的 judge 并先做人工标注校准）。"""
    return ThinkStrippedChatOpenAI(
        base_url=config.LLM_BASE_URL, api_key=config.LLM_API_KEY,
        model=config.LLM_MODEL, temperature=0,
        # 微信接口并发上限 6：openai 客户端层对 429 带指数退避重试
        # （ragas 层的 RunConfig 重试不覆盖 RateLimitError）
        max_retries=5)


# ===========================================================================
# ② 跑预测：逐题过被测链路，收集 answer/contexts + 延迟 + 命中
# ===========================================================================
async def collect_answers(rag: RagComponents, eval_set: list) -> list:
    """跑检索拿 (answer, contexts, latency, hit/mrr)。
    trace_scope 让每次检索内部的嵌入/三路召回/RRF/去重/重排/生成埋点自动
    挂到 ragas_eval 的 trace 下——错题归因时直接点开该题 span 看检索树。"""
    samples = []
    for row in eval_set:
        question, source_type = row["user_input"], row.get("source_type", "-")
        print(f"[eval] 检索({source_type}): {question}", flush=True)
        with obs.span("eval_retrieve", input={"question": question,
                                              "source_type": source_type}) as s:
            t0 = time.time()
            ret = await knowledges_retrieve(rag, question, [KB_ID],
                                            max_recalls=4, min_score=0.5)
            latency = time.time() - t0
            titles = [d.title for d in ret["source_documents"]]
            # 确定性硬指标：期望文档是否被召回/排名几何（无 LLM，可复现）
            expected = row.get("expected_titles") or []
            hit, rank = 0, 0
            for i, t in enumerate(titles, start=1):
                if t in expected:
                    hit, rank = 1, i
                    break
            if s:
                s.update(output={"citations": len(titles),
                                 "hit": hit or None, "rank": rank or None,
                                 "latency_s": round(latency, 2)})
        samples.append({
            "user_input": question,
            "response": ret["result"],
            "retrieved_contexts": [d.content for d in ret["source_documents"]],
            "reference": row["reference"],
            # 附加列（ragas 只取指标所需列，其余原样带过）
            "source_type": source_type,
            "hit": hit if expected else None,        # 未标注期望 → 不参与硬指标
            "rr": 1.0 / rank if rank else (0.0 if expected else None),
            "latency_s": round(latency, 3),
            "titles": titles,
        })
    return samples


# ===========================================================================
# ③ ragas 打分（0.2.x API，五指标含 answer_correctness）
# ===========================================================================
def run_ragas(samples: list, judge_llm, embeddings) -> tuple:
    """ragas 评估主体。返回 (逐样本 DataFrame, )——均值等聚合交给调用方按列
    计算（避免把 latency/hit 等附加列混进指标均值）。RunConfig 放宽超时/
    重试（微信接口偶发慢）。"""
    from datasets import Dataset as HF_Dataset
    from ragas import evaluate, RunConfig
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from ragas.llms import LangchainLLMWrapper
    from ragas.metrics import (answer_correctness, answer_relevancy,
                               context_precision, context_recall, faithfulness)
    import pandas as pd

    ds = HF_Dataset.from_list(samples)
    # max_workers=1：judge 串行——微信接口并发上限 6，ragas 的一个指标任务
    # 内部会连发多次 LLM 调用，并行任务叠加即 429（重试也救不回的会记 NaN）
    run_config = RunConfig(timeout=300, max_workers=1, max_retries=5)
    result = evaluate(
        dataset=ds,
        metrics=[context_precision, context_recall, faithfulness,
                 answer_relevancy, answer_correctness],
        llm=LangchainLLMWrapper(judge_llm),
        embeddings=LangchainEmbeddingsWrapper(embeddings),
        run_config=run_config,
        show_progress=True)
    # EvaluationResult.scores 是 list[dict]（逐样本各指标分，与 samples 同序）
    detail = pd.DataFrame(result.scores)
    # 附加列回填（result.scores 只有指标列；user_input/source_type 对齐样本，
    # hit/rr/latency 用于硬指标与成本报告——逐样本翻转靠 user_input 对齐基线）
    for col in ("user_input", "source_type", "hit", "rr", "latency_s", "titles"):
        detail[col] = [s[col] for s in samples]
    return detail


# ===========================================================================
# ④ 落盘与基线 diff（评估的意义在对比：总体/分组/逐样本翻转三层）
# ===========================================================================
def summarize(detail) -> dict:
    """DataFrame → 落盘/上报用的汇总 dict：五指标均值 + 硬指标 + 平均延迟
    + 按解析类型分组（"ragas 覆盖解析"：不同解析器的检索质量分别对比）。"""
    def _mean(df, col):
        return round(float(df[col].mean()), 4) if col in df else None

    overall = {m: _mean(detail, m) for m in METRICS}
    hard = {"hit_rate": _mean(detail, "hit"), "mrr": _mean(detail, "rr"),
            "avg_latency_s": _mean(detail, "latency_s")}
    by_type = {}
    for st, g in detail.groupby("source_type"):
        by_type[st] = {**{m: _mean(g, m) for m in METRICS},
                       **{"hit_rate": _mean(g, "hit")}}
    samples = [{"user_input": r.user_input, "source_type": r.source_type,
                "pass": bool(r.answer_correctness >= PASS_THRESHOLD)
                        if r.answer_correctness == r.answer_correctness else False,
                "hit": r.hit, "rr": r.rr, "latency_s": r.latency_s,
                "scores": {m: getattr(r, m) for m in METRICS}}
               for r in detail.itertuples()]
    return {"overall": overall, "hard": hard, "by_type": by_type,
            "samples": samples}


def save_run(run_name: str, summary: dict) -> Path:
    """每次跑分落盘 eval_results/<run>.json（含配置快照与逐样本分明细，
    供事后追溯与跨 run diff）。"""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{run_name}.json"
    payload = {"run_name": run_name,
               "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "config": EVAL_CONFIG, **summary}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


def diff_against_baseline(summary: dict) -> dict:
    """与 baseline.json 对比并打印：① 总体指标增减（超容差提示回归，CI
    门禁用同款阈值）② 硬指标 ③ 逐样本翻转（由对变错/由错变对——比均值
    更能定位具体回归案例）。返回摘要 dict 供 langfuse 上报。"""
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    print(f"[diff] 对比基线 {baseline['run_name']}"
          f"（{baseline['created_at']}，config={baseline['config']}）：", flush=True)

    deltas = {}
    for m in METRICS:
        old, new = baseline["overall"].get(m), summary["overall"].get(m)
        if old is None or new is None:
            continue
        d = round(new - old, 4)
        deltas[m] = {"baseline": old, "current": new, "delta": d}
        flag = "  ← 回归超容差" if d < -REGRESSION_TOLERANCE else ""
        print(f"    {m:<20} {old:.3f} → {new:.3f}  ({d:+.3f}){flag}", flush=True)
    regressions = [m for m, v in deltas.items() if v["delta"] < -REGRESSION_TOLERANCE]

    hard = {}
    for k in ("hit_rate", "mrr", "avg_latency_s"):
        old, new = baseline["hard"].get(k), summary["hard"].get(k)
        if old is None or new is None:
            continue
        hard[k] = {"baseline": old, "current": new,
                   "delta": round(new - old, 4)}
        print(f"    {k:<20} {old:.3f} → {new:.3f}  ({new - old:+.3f})", flush=True)

    # 逐样本翻转：answer_correctness 过/不过 的状态变化（按 user_input 对齐）
    base_pass = {s["user_input"]: s["pass"] for s in baseline["samples"]}
    worse, better = [], []
    for s in summary["samples"]:
        old = base_pass.get(s["user_input"])
        if old is None or old == s["pass"]:
            continue
        ac = s["scores"]["answer_correctness"]
        item = (s["user_input"], s["source_type"],
                base_pass[s["user_input"]], s["pass"], ac)
        (worse if s["pass"] is False else better).append(item)
    print(f"[diff] 翻转样本（answer_correctness≥{PASS_THRESHOLD} 记通过）：", flush=True)
    for q, st, old, new, ac in worse:
        print(f"    变差 ✗ [{st}] {q}  (正确性 {ac:.2f})", flush=True)
    for q, st, old, new, ac in better:
        print(f"    变好 ✓ [{st}] {q}  (正确性 {ac:.2f})", flush=True)
    if not worse and not better:
        print("    （无翻转，全部样本通过与基线一致）", flush=True)

    if regressions:
        print(f"[diff] ⚠ 指标回归超容差({REGRESSION_TOLERANCE})："
              f"{', '.join(regressions)} —— CI 门禁应拦截此变更", flush=True)
    return {"baseline_run": baseline["run_name"], "deltas": deltas,
            "hard": hard, "worse": [w[0] for w in worse],
            "better": [b[0] for b in better], "regressions": regressions}


# ===========================================================================
# 主流程
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="ragas 评估（基线对比模式）")
    parser.add_argument("--run", default=None,
                        help="本次 run 名（默认 run_时间戳；建议语义化如 "
                             "chunk800/rerank06，便于对比定位变更）")
    parser.add_argument("--set-baseline", action="store_true",
                        help="把本次结果设为基线（认可当前配置的质量水位）")
    parser.add_argument("--eval-set", default=None,
                        help="评估集 JSONL 路径（默认 eval_set.jsonl）")
    args = parser.parse_args()
    global EVAL_SET_PATH
    if args.eval_set:
        EVAL_SET_PATH = Path(args.eval_set)
    run_name = args.run or f"run_{datetime.now().strftime('%m%d_%H%M%S')}"

    print("=" * 60, flush=True)
    print(f"ragas 评估 run={run_name}：五指标 + 硬指标(hit/mrr) + 基线 diff",
          flush=True)
    eval_set = load_eval_set()
    embedder, reranker, llm = build_clients()
    store = ESStore(embedder)
    if not store.client.indices.exists(index=store.index_name):
        raise SystemExit("索引不存在，请先运行 rag_demo.py --reset 入库")
    rag = RagComponents(store, embedder, reranker, llm, top_k=4)

    # ① 跑预测（trace_scope 内：检索各阶段埋点自动挂本 trace）
    trace = obs.trace(name="ragas_eval",
                      input={"run": run_name,
                             "questions": [r["user_input"] for r in eval_set]})
    with obs.trace_scope(trace):
        samples = asyncio.run(collect_answers(rag, eval_set))

    # ② ragas 评估（judge=微信 LLM，embeddings=TEI bge-m3）
    detail = run_ragas(samples, build_judge_llm(),
                       TEILangchainEmbeddings(embedder))
    summary = summarize(detail)

    # ③ 输出与落盘：总体均值 + 硬指标 + 分组（word/md/pdf/excel/faq 对比）
    print("-" * 60, flush=True)
    print("[ragas] 指标均值：", flush=True)
    for k, v in summary["overall"].items():
        print(f"    {k}: {v:.3f}", flush=True)
    print(f"[ragas] 硬指标：hit_rate={summary['hard']['hit_rate']}, "
          f"mrr={summary['hard']['mrr']}, "
          f"平均延迟={summary['hard']['avg_latency_s']}s", flush=True)
    print("[ragas] 按解析类型分组（每类样本数少，分值仅作参考）：", flush=True)
    short = {"context_precision": "ctx_p", "context_recall": "ctx_r",
             "faithfulness": "faith", "answer_relevancy": "ans_r",
             "answer_correctness": "ans_c"}
    for st, row in summary["by_type"].items():
        cells = "  ".join(f"{short[m]}={row[m]:.2f}" for m in METRICS)
        print(f"    {st:<9} {cells}  hit={row['hit_rate']}", flush=True)
    obs.generation(trace, name="ragas_scores",
                   output={"overall": summary["overall"],
                           "hard": summary["hard"]},
                   model="ragas+Deepseek-v4-flash+bge-m3",
                   metadata={"run": run_name})
    obs.generation(trace, name="ragas_scores_by_type",
                   output=summary["by_type"], metadata={"run": run_name})

    # ④ 基线机制：首跑自动建基线；有基线则 diff；--set-baseline 显式重设
    run_path = save_run(run_name, summary)
    print(f"[ragas] 结果已落盘 {run_path.name}", flush=True)
    if args.set_baseline or not BASELINE_PATH.exists():
        first = not BASELINE_PATH.exists()   # 先记再写（写完文件必然存在）
        BASELINE_PATH.write_text(run_path.read_text(encoding="utf-8"),
                                 encoding="utf-8")
        print(f"[ragas] 基线{'已建立' if first else '已重设'}："
              f"{run_name}（后续 run 自动与之 diff）", flush=True)
        diff = None
    else:
        diff = diff_against_baseline(summary)
        obs.generation(trace, name="ragas_baseline_diff", output=diff,
                       metadata={"run": run_name,
                                 "tolerance": REGRESSION_TOLERANCE})
    obs.flush()
    print("[完成] 详情见 langfuse UI（trace: ragas_eval）与 eval_results/",
          flush=True)


if __name__ == "__main__":
    main()
