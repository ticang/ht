# rag-lab 接口调用指南

## 启动项目（从零开始）

### 0. 前置

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

共两个业务接口：**解析入库**（上传文件→解析→写入 ES）与**检索**（问答）。

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

响应：

```json
{"code": 200, "msg": "success",
 "data": {"file": "sample_switch.md", "chain": "knowledge_base",
          "chunks": 3, "duration": 0.61}}
```

失败时 `code=500`，`msg` 带错误原因（如图片型 PDF 需 OCR、不支持的类型）。

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

> Windows Git Bash 的 curl 传中文 JSON 会乱码，建议用 Python 客户端：
> `requests.post(url, json={...})`
