# rag-lab 接口调用指南

## 启动项目（从零开始）

### 0. 前置
git lfs install
- Docker Desktop 已运行
- Python ≥ 3.10（demo 用 3.13 验证）

### 1. 创建并激活 venv

```bash
cd rag-lab
python -m venv .venv                    # 创建（只需一次）
source .venv/Scripts/activate           # Git Bash 激活
# PowerShell / CMD：.venv\Scripts\activate
# 退出虚拟环境：deactivate
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

说明：MinerU 在**首次解析 PDF 时**自动下载模型（几分钟，之后走本地缓存）；
依赖里不含 torch（MinerU core 走 ONNX + llama.cpp 后端）。

### 3. 首次部署的一次性准备

```bash
# ① 配置 .env（复制模板后填 LLM key；TEI/ES/langfuse 地址默认本机不用改）
cp .env.example .env    # Windows Git Bash；PowerShell 用 copy

# ② 下载 TEI 的两个 ONNX 模型到 models/（hf-mirror 直链，约 545MB×2，
#    下载完把 model_int8.onnx 重命名为 model.onnx）
B=https://hf-mirror.com
mkdir -p models/bge-m3-onnx/onnx models/bge-reranker-v2-m3-onnx/onnx
for f in config.json tokenizer_config.json special_tokens_map.json sentencepiece.bpe.model tokenizer.json; do
  curl -L -C - -o models/bge-m3-onnx/$f $B/onnx-community/bge-m3-ONNX/resolve/main/$f
  curl -L -C - -o models/bge-reranker-v2-m3-onnx/$f $B/onnx-community/bge-reranker-v2-m3-ONNX/resolve/main/$f; done
for f in modules.json sentence_bert_config.json; do
  curl -L -C - -o models/bge-m3-onnx/$f $B/onnx-community/bge-m3-ONNX/resolve/main/$f; done
mkdir -p models/bge-m3-onnx/1_Pooling
curl -L -C - -o models/bge-m3-onnx/1_Pooling/config.json $B/onnx-community/bge-m3-ONNX/resolve/main/1_Pooling/config.json
curl -L -C - -o models/bge-m3-onnx/onnx/model_int8.onnx $B/onnx-community/bge-m3-ONNX/resolve/main/onnx/model_int8.onnx
curl -L -C - -o models/bge-reranker-v2-m3-onnx/onnx/model_int8.onnx $B/onnx-community/bge-reranker-v2-m3-ONNX/resolve/main/onnx/model_int8.onnx
mv models/bge-m3-onnx/onnx/model_int8.onnx models/bge-m3-onnx/onnx/model.onnx
mv models/bge-reranker-v2-m3-onnx/onnx/model_int8.onnx models/bge-reranker-v2-m3-onnx/onnx/model.onnx
```

### 4. 启动 docker 基础设施（一个 compose 管全部）

```bash
docker compose up -d          # TEI×2 + ES + Kibana + langfuse + postgres
# 就绪检查（全部应返回 200）：
#   curl http://127.0.0.1:8081/health       # TEI 嵌入
#   curl http://127.0.0.1:8082/health       # TEI 重排
#   curl http://127.0.0.1:9200/_cluster/health
#   curl http://127.0.0.1:3000/api/public/health   # langfuse
```

### 5. 启动接口服务

```bash
python api_server.py          # 已激活 venv；127.0.0.1:8000
# 未激活 venv 时等价写法：.venv/Scripts/python api_server.py
```

索引不存在时启动会自动入库五类样例到 `kb-demo`（已存在则跳过，幂等；
强制重建用 `python rag_demo.py --reset`）。交互式文档：http://127.0.0.1:8000/docs

---

## 接口指南

业务接口：**会话问答**（SSE 流式，支持多轮）、**解析入库**（上传→异步解析→
写入 ES）、**检索**（问答）、**反馈**（点赞/点踩）。另有健康检查
`GET /healthz` 与评估脚本 `eval_ragas.py`（见 ⑤）。

服务防护（默认全关，调试方便；上生产建议开启）：

```bash
# .env 配置后重启生效
RAGLAB_API_KEYS=key-a,key-b        # 接口鉴权：请求头 X-API-Key
RAGLAB_RATE_LIMIT_RPM=60           # 限流：每 key 每分钟（未鉴权按客户端 IP）
# 未配置 key 的请求访问受保护接口 → 401；超限 → 429
```

---

## ① 解析入库 `POST /api/v1/knowledge/upload`

multipart 上传知识文件，解析链路：类型分派（pdf/docx/md/xlsx）→
标题分块/行转陈述句 → 父子切分（1600/400）→ ES 写入。

| 参数 | 位置 | 默认 | 说明 |
|---|---|---|---|
| `file` | form 文件 | 必填 | 知识文件。普通知识：`.docx` `.pdf` `.md` `.xlsx`；FAQ：`.docx`（Heading=问题/正文=答案）、`.xlsx`/`.csv`（列=问题/答案） |
| `chain` | form | `knowledge_base` | `knowledge_base`=普通知识；`knowledge_faq`=FAQ 双份入库（is_faq=Y+answer 入 metadata，FAQ 检索路专用） |
| `knowledge_base_id` | form | `kb-demo` | 目标知识库 id，检索时按它过滤 |
| `column_question` | form | `1` | FAQ 表格问题列号（1-based） |
| `column_answer` | form | `2` | FAQ 表格答案列号（1-based） |

```bash
# 普通知识（md）
curl -X POST http://127.0.0.1:8000/api/v1/knowledge/upload \
     -F "file=@uploads/sample_switch.md" \
     -F "chain=knowledge_base" -F "knowledge_base_id=kb-demo"

# FAQ（xlsx，问题在第1列、答案在第2列）
curl -X POST http://127.0.0.1:8000/api/v1/knowledge/upload \
     -F "file=@uploads/sample_faq.xlsx" \
     -F "chain=knowledge_faq" -F "knowledge_base_id=kb-demo"
```

响应（HTTP 202，解析在后台任务执行——大 PDF 的 MinerU 解析单页 1~3s，
同步会卡住请求）：

```json
{"code": 202, "msg": "accepted",
 "data": {"task_id": "3f0c...", "file": "sample_switch.md",
          "chain": "knowledge_base",
          "poll": "/api/v1/knowledge/tasks/3f0c..."}}
```

轮询任务状态（生产任务状态接口的等价物）：

```bash
curl http://127.0.0.1:8000/api/v1/knowledge/tasks/3f0c... -H "X-API-Key: ..."
```

```json
{"code": 200, "msg": "success",
 "data": {"status": "done", "chunks": 3, "replaced": 0, "error": null, ...}}
```

幂等重传：**同名文件 + 同知识库重复上传 = 替换**（先删旧块再入新块，
`replaced` 为删除的旧块数），不会重复入库。

失败时任务 `status=failed`，`error` 带原因（如图片型 PDF 需 OCR、不支持的类型）。

---

## ② 检索 `POST /api/v3/knowledges/retrieve`

意图分流后的知识检索+生成：双通道（文档路/FAQ 路）三路召回
（精确/向量/模糊）→ RRF → 嵌入去重 → bge-reranker 重排 → 合并 → LLM 生成。

| 参数 | 默认 | 说明 |
|---|---|---|
| `query` | 必填 | 用户问题 |
| `knowledges` | `["kb-demo"]` | 检索的知识库 id 列表 |
| `max_recalls` | `4` | 最大召回数（top_k） |
| `min_score` | `0.5` | 重排阈值 |
| `only_retrieval` | `false` | true=只检索不生成（返回引用原文拼接） |
| `search_strategy` | `hybrid` | 预留字段（与生产一致，当前未消费） |

```bash
curl -X POST http://127.0.0.1:8000/api/v3/knowledges/retrieve \
     -H "Content-Type: application/json" \
     -d '{"query":"PDCP丢弃定时器的默认值是多少？","knowledges":["kb-demo"]}'
```

响应（`source_documents` 每条：id=ES 文档 id、content、score=重排分、
title、url=来源文件、metadata）：

```json
{"code": 200, "msg": "success",
 "data": {"result": "PDCP丢弃定时器的默认值是500毫秒。",
          "source_documents": [
            {"id": "0c9a...", "content": "PDCP参数说明,丢弃定时器。PDCP丢弃定时器…",
             "score": 0.999, "title": "sample_kb", "url": "sample_kb.docx",
             "metadata": {"subject": "PDCP参数说明,丢弃定时器", "is_faq": null, ...}}]}}
```

FAQ 命中时 title=问题本身、metadata 带 `is_faq: "Y"` 与完整 `answer`。

---

## ③ 会话问答 `POST /api/v2/assistant_run/stream`（SSE）

意图分流 + 多轮会话的流式接口：分类（知识/工单/预设/闲聊）→ 知识类走 ② 的
检索链路、工单类（ZW-/ATRUST- 编号或工单进度查询）调 MCP、其余直答；
SSE 事件序列 `metadata(run_id) → data[HumanMessage] → data[ToolMessage 引用]
→ data[AIMessage...]`。

**多轮**：客户端携带同一个 `thread_id` 连续提问，服务端把追问改写成独立
问题再检索（"那它的取值范围呢" → "PDCP丢弃定时器的取值范围是多少"）；
不传 `thread_id` 每次都是新会话。会话为进程内存态（重启即清、单 worker）。

```bash
curl -N -X POST http://127.0.0.1:8000/api/v2/assistant_run/stream \
     -H "Content-Type: application/json" \
     -d '{"question":"PDCP丢弃定时器的默认值是多少？","thread_id":"demo-thread-1"}'
# 第二轮（同一 thread_id）：
#    {"question":"那它的取值范围呢？","thread_id":"demo-thread-1"}
```

**工单 MCP**：问题分类为工单类时先调工单服务（默认内置 mock 数据；配
`MCP_TICKET_URL` 接真服务），成功直接返回工单进度；服务失败自动降级知识检索。

---

## ④ 反馈 `POST /api/v1/feedback` + 健康检查

点赞/点踩落盘 `feedback/feedback.jsonl`——"线上反馈 → 离线评估"回流的原始
数据：定期从 `down` 里挑 badcase 补进 `eval_set.jsonl` 再跑 ragas 基线对比。

```bash
curl -X POST http://127.0.0.1:8000/api/v1/feedback -H "Content-Type: application/json" \
     -d '{"question":"PDCP丢弃定时器的默认值是多少？","answer":"500毫秒","rating":"up"}'
```

`GET /healthz`（无鉴权）：组件级健康探测——ES 挂 = 503（核心）、TEI 挂 =
200 degraded（检索可部分降级）。

---

## ⑤ ragas 评估 `eval_ragas.py`

检索质量回归：对评估集逐题跑检索链路 → ragas 打分 → 与基线 diff。
改了切分参数/重排阈值/模型/prompt 之后重跑一次，即可对比质量变化。

前提：docker 基础设施起着（步骤 4）、索引已入库（步骤 5 启动过
api_server 即可）、venv 已激活。judge 全程串行调 LLM，6 题约 3~5 分钟。

```bash
python eval_ragas.py --run first_run                  # 首跑：自动建立基线
# ……修改配置（切分/重排阈值/模型/prompt）……
python eval_ragas.py --run chunk800                   # 重跑：自动与基线 diff
python eval_ragas.py --run chunk800 --set-baseline    # 认可新结果 → 重设基线
```

| 参数 | 说明 |
|---|---|
| `--run 名字` | run 名，默认 `run_时间戳`；建议语义化（如 `chunk800`/`rerank06`），便于对比定位变更 |
| `--set-baseline` | 把本次结果设为基线 `eval_results/baseline.json` |
| `--eval-set 路径` | 评估集 JSONL，默认 `eval_set.jsonl` |

### 指标

| 指标 | 类型 | 评什么 |
|---|---|---|
| context_precision | LLM judge | 引用里相关块是否排在前面（检索精度） |
| context_recall | LLM judge | 标准答案要点是否被引用覆盖（检索覆盖） |
| faithfulness | LLM judge | 答案是否忠于引用（无编造） |
| answer_relevancy | judge+向量 | 答案与问题的相关度 |
| answer_correctness | LLM judge | 答案与标准答案的最终正确性（端到端） |
| hit_rate / mrr | 确定性 | 期望文档是否被召回 / 排名几何（无 LLM，可复现） |
| 平均延迟 | — | 每题端到端耗时 |

### 评估集 `eval_set.jsonl`

每行一题：`user_input`（问题）、`reference`（标准答案——要点写得越全，
context_recall 越真实）、`source_type`（分组维度）、`expected_titles`
（期望命中文档标题：普通知识=文件名去扩展名，FAQ=问题本身；不标则该题
跳过硬指标）。`source_type: "negative"` 是负例（知识库无覆盖）——期望
"明确拒答"而非编造；负例不进总体均值（ragas 对正确拒答的 faithfulness
天然低分），在分组里单独呈现。

```json
{"user_input": "PDCP丢弃定时器的默认值是多少？",
 "reference": "PDCP丢弃定时器（discardTimer）默认值为500毫秒，取值范围50到3000毫秒。",
 "source_type": "word", "expected_titles": ["sample_kb"]}
```

把内置样例换成生产真实问答对（如 FAQ excel 的问题/答案列导出）即成真实评估。

### 输出在哪看

- **控制台**：指标均值 + 按解析类型分组 + 硬指标 + 基线 diff（指标增减、
  跌幅超 0.02 提示回归、逐样本由对变错/由错变对）
- **eval_results/**：`<run>.json` 逐样本分明细与配置快照；`baseline.json` 基线
- **langfuse**（http://127.0.0.1:3000）：trace `ragas_eval`——每题检索树
  （错题归因直接点开看三路召回/重排命中）+ ragas_scores / baseline_diff

> Windows Git Bash 的 curl 传中文 JSON 会乱码，建议用 Python 客户端：
> `requests.post(url, json={...})`
