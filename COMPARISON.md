# rag-lab demo 与 aiops 生产链路对照文档

demo（`D:\code\python\aiops\rag-lab\`）是 aiops-chatops / aiops-agent-core 知识库链路的
全真实组件复刻。本文逐环节列出**相同点**（刻意对齐的行号级逻辑）与**差异点**
（未复刻/简化/安全替换的部分及原因）。

生产代码位置约定：
- 【ingest】= `aiops-chatops/server/tasks/ingest.py`
- 【loader】= `aiops-chatops/aichain/document_loader/`
- 【vector】= `aiops-chatops/aichain/vectores/elastic7_vector_search.py`
- 【qa】= `aiops-chatops/aichain/chains/`（base.py / rerank.py / redundant_filter.py）
- 【v3】= `aiops-chatops/server/api/v3/knowledge_v3.py` + `server/service/knowledge.py`
- 【assistant】= `aiops-agent-core/backend/agent_core/service/assistant.py`
- 【classifier】= `aiops-agent-core/backend/agent_core/service/question_classifier.py`

demo 文件位置约定：
- 【demo-loaders】= `rag-lab/loaders.py`
- 【demo-split】= `rag-lab/text_splitters.py`
- 【demo-ingest】= `rag-lab/ingest_service.py`
- 【demo-classifier】= `rag-lab/classifier.py`
- 【demo-main】= `rag-lab/rag_demo.py`
- 【demo-es】= `rag-lab/es_store.py`
- 【demo-clients】= `rag-lab/clients.py`

---

## 〇、总览

| 生产链路 | demo 链路 | 对齐度 |
|---|---|---|
| file_ingest（普通文件） | IngestService.file_ingest | 结构相同，周边系统省略 |
| knowledge_faq_batch_ingest（FAQ） | IngestService.faq_batch_ingest | 核心相同（双份/去重/metadata） |
| create_stream → AssistantService.stream | assistant_ask（单接口） | 分流逻辑相同，去掉 SSE/agent/安全层 |
| knowledges_retrieve（v3） | knowledges_retrieve | 参数与调用形态相同 |
| qa_chain.acall（FixRetrievalQA） | SimpleQAChain / SimpleRetrievalQA | 逐环节同构 |
| ElasticVectorSearch7.hybrid_search | ESStore.hybrid_search | 三路+RRF 相同 |

---

## 一、解析（入库侧）

### 1. 类型分派 get_file_loader

| 项 | 生产【loader/__init__.py:132-188】 | demo【demo-loaders.get_file_loader】 | 异同 |
|---|---|---|---|
| FILE_LOADER_MAP 结构 | cls/split/kwargs/support_ext_partition 四字段 | 同名同义 | ✅同 |
| 支持类型 | pdf/doc(x)/md/xlsx/xls/txt/csv/json/html/mhtml/ppt(x)/convert_md | pdf/doc(x)/md/xlsx/xls(仅报错提示) | ⚠demo 5 类，其余显式 ValueError |
| 类型探测 | unstructured.detect_filetype + magic 按内容探测，失败退后缀 | 直接文件后缀 | ⚠异（demo 简化） |
| knowledge_id 传递 | pdf/doc/docx 传（图片 minio bucket 用） | 同样传（形参保留） | ✅同 |
| split_type≠0（convert_md 预转换路线） | 支持 | 报错未复刻 | ⚠异 |
| partition_type 插件 | ≠'default' 时动态 import third_part.*（WordProblemConversion 等） | 无插件体系，仅透传日志 | ⚠异 |
| split 标志语义 | True→load_and_split(splitter)，False→load() | 相同 | ✅同 |

### 2. word 解析（docx/doc）

| 项 | 生产【loader/word_partition.py】 | demo【demo-loaders.partition_docx】 | 异同 |
|---|---|---|---|
| Heading/Title 样式判级 | _get_title_level(:458) | _heading_level_by_style | ✅同 |
| 标题编号剥离 | `^\d+(?:\.\d+){1,4}`（:471） | 同一正则 | ✅同 |
| subject 组装 | 「父标题,子标题」（TITLE_SPLIT=","，:406-409） | 相同 | ✅同 |
| 字号启发标题 | _get_paragraph_possible_font_size(:489)/_font_size_less_than_paragraph(:508)/_is_possible_tile_by_text(:588)（供 pdf2docx 等无样式 docx） | _is_possible_title（短文本+非句尾标点+字号>正文+0.5）；正文主字号取全局众数 | ⚠简化（保留判定三要素，删 between_text 等启发） |
| 解析结构 | handle_element 先产 (element,current_title,parent_title) 流 → partition_docx(:399-428) 再聚合 | 单遍聚合，语义等价 | ⚠实现不同、结果同构 |
| 表格 | _convert_table_to_html/_text(:520-563) | 未复刻（样例无表格） | ⚠异 |
| 图片 | 提取转 png 上传 minio（knowledge_id bucket） | 未复刻 | ⚠异 |
| 文件名→subject 前缀 | 正则 `_后取知识名` 拼 subject（word_document.py:86-93，上传路径约定） | 用 basename | ⚠异 |
| word_clean 清洗 | aichain/utils/data_cleaner | 未复刻 | ⚠异 |
| mode='elements' | FILE_LOADER_MAP docx kwargs | 同 | ✅同 |

### 3. pdf 解析

| 项 | 生产【loader/pdf_loader.py:16-71】 | demo【demo-loaders.PdfDocumentLoader】 | 异同 |
|---|---|---|---|
| 主解析路线 | 全图片页→RapidOCR；文本型→pdf2docx→partition_docx | **MinerU 4.x**（布局分析+OCR 一体）：pdf→markdown→(#→Heading 转 docx)→partition_docx | ⚠demo 升级为 MinerU（按需求） |
| 图片型/扫描件 | RapidOCR 逐页识别（:30-49） | MinerU 内置 OCR（ocr_mode=auto），无需额外依赖 | ✅能力对齐 |
| 降级路径 | — | MinerU 失败时 pdf2docx→partition_docx（生产 :56-60 原样，字号启发识别标题） | ⚠demo 新增兜底 |
| 无标题文档 | unstructured Title 判定兜底 | 文件名当 subject 兜底（flush 分支） | ✅语义对齐 |
| 解析档位 | — | tier=basic（CPU 单页 ~1-3s；standard/advanced 更细但更慢） | ⚠demo 选型 |
| source 元数据 | metadata_filename 保留原 pdf 名 | 相同（source 改回 pdf 名） | ✅同 |

### 4. md 解析

| 项 | 生产【loader/md_loader.py:15-33】 | demo【demo-loaders.MarkdownDocumentLoader】 | 异同 |
|---|---|---|---|
| 主路线 | `pandoc -s -o <docx> <md>` → partition_docx | 相同（subprocess 同参） | ✅同 |
| 无 pandoc | 不存在此场景（环境恒装） | 纯 Python fallback：# → add_heading，其余行→段落 | ⚠demo 新增兜底（保留"md→docx→标题分块"管线本质） |

### 5. excel 解析（xlsx）

| 项 | 生产【loader/excel_loader.py:240-428 maas_partition_xlsx】 | demo【demo-loaders.maas_partition_xlsx】 | 异同 |
|---|---|---|---|
| 合并单元格 | unmerge_and_fill_cells(:23-32) 拆开填值 | 相同 | ✅同 |
| 表头识别 | 第一行+合并表头拼接（get_merged_head:224-237） | 第一个非空行整行收集，占位列由后续行补齐 | ⚠简化（单列补值替代逐格拼接，语义近似） |
| 行主键 | '序号'前缀连续列 = 主键列宽（:398-404） | 相同 | ✅同 |
| 输出三元组 | `列名+行主键是值`；主键列 `列名是主键值`（:360-382） | 相同 | ✅同 |
| 换行处理 | 单元格内 \n → `;` | 相同 | ✅同 |
| 行→Document | metadata: page_name/page_number/filename/row（_create_element_from_row:122-135） | 相同四键 | ✅同 |
| 可见 sheet | 只处理 visible | 相同 | ✅同 |
| 列宽估算 | 先扫前 20 行估 max_col 加速（:294-312） | 直接 sheet.max_column | ⚠简化 |
| 单元格图片 | SheetImageLoader 提取存盘+浮动图片(:328-337/:406-417) | 未复刻 | ⚠异 |
| .xls 老格式 | ExcelLoader 简版"列名是值"（partition_xlsx:54-120，DOC_CHUNK_SIZE 折块） | 报错提示转存 xlsx | ⚠异 |
| 全空行 | （无显式跳过，靠空 content） | 跳过且表头顺延 | ⚠demo 更稳 |

### 6. faq 解析（三源）

| 项 | 生产【ingest.py:653-816】 | demo【demo-ingest.faq_batch_ingest + demo-loaders.handle_faq_docx】 | 异同 |
|---|---|---|---|
| 三源读表 | csv→read_csv / docx→handle_faq_dox / excel→read_excel(可见 sheet)（:689-700） | 相同三分支（excel 分支读全部 sheet） | ✅同（⚠sheet 可见性过滤简化） |
| docx FAQ 结构 | Heading/Title=问题、正文聚合=答案（handle_faq_dox:624-639） | 相同（含"标题非 Heading 取父标题"的容错语义简化为直接 Heading 判定） | ✅同构 |
| 列号 | 1-based，≤20 校验（:711-714） | 相同 | ✅同 |
| 空值跳过 | 空/'nan' 跳过（:735） | 相同 | ✅同 |
| 内容去重 | get_text_md5（MD5）+ exist_md5（:739-742） | SHA-256 同用途 | ⚠安全等价替换（防碰撞，非生产行为） |
| 库级去重 | query_knowledge_items_by_md5(md5+version)（:752-755） | 无（无 DB） | ⚠异 |
| 关键 metadata | is_faq='Y'、answer、name=question（:773-785） | 相同 | ✅同 |
| 双份入库 | base_doc（"问题：q\n答案：a"）+ faq_doc（纯问题）（:787-789） | 相同 | ✅同 |
| 分批 | 50 行一批 generate_faq_data(:643-648) | 常量保留，一次入库 | ⚠简化 |
| base64 图片 | _rebuild_content 搬运（:377-402） | 未复刻 | ⚠异 |
| 异步/落库 | celery 任务 + knowledge_items 状态流转 | 同步调用 | ⚠异（demo 无任务队列） |

### 7. 切分（父子块）

| 项 | 生产【aichain/ingest/text_splitter.py:8-49】 | demo【demo-split】 | 异同 |
|---|---|---|---|
| 父/子尺寸 | 1600/400 | 相同（含"两尺寸读同一环境变量 OPSGPT_PARENT_CHUNK_SIZE"的历史笔误，注释标出） | ✅同（笔误一并保留） |
| 代码占比>50% | 父块尺寸×2 再切（:23-29） | 相同 | ✅同 |
| subject 前缀 | "。".join([subject, 正文])（:33-34） | 相同 | ✅同 |
| parent_text | 仅多子块父块挂载（:38-39） | 相同 | ✅同 |
| 清洗留底 | img_clean/code_clean + original_content（:40-43） | 相同骨架；正则简化（```围栏 / `<img>` 标签） | ⚠简化 |
| 其他切分器 | ChineseTextSplitter（中文句读）/裸 RecursiveCharacterTextSplitter 可配（text_splitter.py:77-99） | 只默认父子切分，其余显式报错 | ⚠异 |

### 8. 入库持久化

| 项 | 生产【ingest.py file_ingest:86-148 + aichain/ingest/base.py:91-166】 | demo【demo-ingest.file_ingest】 | 异同 |
|---|---|---|---|
| metadata 组装 | knowledge_id/workspace/name/product_name/parent_id/labels/knowledge_base_id（:107-115） | 相同字段（workspace 缺省） | ✅同 |
| 逐块合并 metadata | doc_persistent（:147-148） | 相同 | ✅同 |
| ES 写入结构 | {text, vector, metadata} + uuid4 _id + bulk index + refresh（add_texts:758-776） | 相同（demo-es.add_documents） | ✅同 |
| original_content 反转 | 清洗留底的原版在入库时写回 text 字段（嵌入仍用清洗版，BM25 路在原文匹配，:762-764） | 相同（写入前 pop 并替换 text） | ✅同 |
| 入库分批 | OPSGPT_INGEST_DOCS_CHUNK_SIZE=1000 分批嵌入写入（:721/:727） | 一次全量 | ⚠简化（demo 量级小） |
| text.ik 声明 | store:"false"（:106-115） | 未写（无行为差异） | ⚠声明性差异 |
| 摘要/问题辅助文档 | DocumentSummaryGenerator + DocumentQuestionsGenerator 双写（:153-166） | 未复刻 | ⚠异 |
| minio | get_upload_file_path 下载/用后删除（:102/:142-143） | 本地路径直用 | ⚠异 |
| DB 状态 | status/duration/message 流转与统计增减 | 未复刻 | ⚠异 |

---

## 二、检索（问答侧）

### 1. 意图识别（QuestionClassifier）

| 项 | 生产【classifier】 | demo【demo-classifier】 | 异同 |
|---|---|---|---|
| 9 类枚举 | identity/capability/scope/learning/service/mcp/knowledge_query/greeting/other | 相同 | ✅同 |
| 分类 prompt | 巨型规则+关键词清单+几十条示例（:30-271） | 保留类别定义/优先级/knowledge↔mcp 边界规则，删示例清单 | ⚠瘦身约 70%（分流正确性关键保留） |
| 输出解析 | 小写化→type_mapping，非法落 OTHER（:329-345） | 相同 | ✅同 |
| requires_knowledge | 仅 knowledge_query=True（_get_type_config:435-475） | 相同 | ✅同 |
| OTHER 类直答 | LLM 自由作答 temp=0.7，失败降级预设（:353-416） | 相同 | ✅同 |
| 异常兜底 | 任何异常→OTHER 形态（:426-433） | 相同 | ✅同 |
| 分类模型 | AiopsLLMProvider 的 Qwen2.5-7B（小模型快分类） | 微信 Deepseek-v4-flash（同链路共用） | ⚠异（生产小模型更快更省） |
| 超时 | 30s（:312） | 60s | ⚠放宽（微信接口偶发慢） |
| 预设话术 | preset_responses 平台话术 | demo 自写等价话术（每类一句） | ⚠内容不同、机制同 |

### 2. 会话分流（AssistantService.stream）

| 项 | 生产【assistant.py:1490-1618】 | demo【demo-main.assistant_ask】 | 异同 |
|---|---|---|---|
| 分流顺序 | 安全校验→分类→llm_response→preset→mcp→agent（:1500-1568） | 分类→llm_response→preset→mcp(降级)→知识检索 | ✅同序（安全层除外） |
| 响应形态 | SSE 流式（EventSourceResponse/to_sse，token 级输出，50s 超时 wrapper :1578-1597） | 非流式一次返回 | ⚠异（demo 无流式） |
| 输入安全 | input_defence_by_prompt（:1500） | 跳过（无安全词库） | ⚠异 |
| 输出安全 | get_security_words_regexes 过滤（:1571/:1599-1607） | 跳过 | ⚠异 |
| mcp 分支 | 真实调 MCP 服务，失败降级知识库（:1547-1568） | 无 MCP 服务，直接按降级语义走知识检索（日志标明） | ⚠异（只保留降级路径） |
| 知识检索通路 | langgraph agent astream_state + 知识库转 runable tool（convert_to_runable_tool，return_direct/interrupt_before 可配） | 直连 knowledges_retrieve | ⚠异（少一层 agent/工具编排，检索本体等价） |
| 会话状态 | thread 历史/checkpoint（get_thread_history 等） | 无状态单问 | ⚠异 |
| langfuse 观测 | 埋点/父链传递 | print 日志 | ⚠异 |
| 接口拆分 | /assistant_run/stream 与 /v3/knowledges/retrieve 两个 HTTP 接口 | 合并为一个 assistant_ask() | ⚠按需求合并（内部分层不变） |

### 3. 检索服务（knowledges_retrieve）

| 项 | 生产【v3 + server/service/knowledge.py:95-117】 | demo【demo-main.knowledges_retrieve】 | 异同 |
|---|---|---|---|
| 链组装 | 每请求 ChainFactory.get_qa_chain（:104-111） | 每请求闭包工厂（build_qa_chain_factory） | ✅同构 |
| 调用形态 | acall(query, threshold=min_score, filter={'knowledge_base_id': knowledges})（:113-117） | 相同 | ✅同 |
| 参数语义 | max_recalls→top_k、min_score→threshold、only_retrieval 透传 | 相同 | ✅同 |
| search_strategy | 请求体有、service 未消费 | 同样保留形参未消费 | ✅同（连"未消费"都一致） |
| 响应字段 | result + source_documents（:140-145） | 相同 | ✅同 |
| 条目结构 | id/content/score/title/url/metadata（:102-112） | 相同六字段（dataclass） | ✅同 |
| title 取值 | name 优先、退 source 文件名 | 相同 | ✅同 |
| FAQ 图片 | answer 中 `<img>` 正则提取拼回 result（:126-138） | 相同正则 | ✅同 |
| 引用统计 | HotKnowledges 热点知识落库（:114-124） | 未复刻 | ⚠异 |

### 4. QA 链（双通道 + 压缩管线 + 生成）

| 项 | 生产【qa】 | demo【demo-main】 | 异同 |
|---|---|---|---|
| 双通道超采样 | 文档路 4×top_k、FAQ 路 2.5×top_k（base.py:464/480） | 相同 | ✅同 |
| FAQ 过滤 | +is_faq:'Y' | 相同 | ✅同 |
| langchain 结构 | ContextualCompressionRetriever + DocumentCompressorPipeline | 相同 | ✅同 |
| 嵌入去重 | FixedEmbeddingsRedundantFilter：numpy 相似矩阵、>0.95 丢后者（redundant_filter.py:42-75） | 朴素两两余弦循环，阈值/语义相同 | ⚠实现简化（结果同构） |
| 重排五步 | 打分→降序→覆写 score→threshold→top_n（rerank.py:72-96） | 相同 | ✅同 |
| 重排模型 | bge-m3 的 /rerank 端点（嵌入服务两用） | bge-reranker-v2-m3 专用交叉编码器（int8 ONNX） | ⚠异（demo 用更标准的 reranker 模型） |
| 阈值 | 默认 0.5，双通道可分别环境变量覆盖 | 统一 API 传入 0.5 | ⚠简化 |
| 合并策略 | FAQ top1>0.85 前置/分数混排/内容去重/截 8（:237-279） | 相同 | ✅同 |
| 父块还原 | parent_text 替换+二次去重（_filter_docs:52-64） | 相同 | ✅同 |
| 生成 prompt | "已知信息…无法回答请明说"（knowledge_base_chat 模板） | 相同 | ✅同 |
| only_retrieval | 短路返回检索结果（ragas 评估用） | 相同 | ✅同 |
| LLM | get_llm_with_db 平台模型（可配） | 微信 Deepseek-v4-flash（temp=0） | ⚠异（同为低温） |
| `<think>` 剥离 | _parse_llm_response | 相同 | ✅同 |

### 5. ES 三路召回 + RRF

| 项 | 生产【vector】 | demo【demo-es】 | 异同 |
|---|---|---|---|
| mapping | text + text.ik(ik_max_word/ik_smart) + vector(dense_vector 1024 cosine) | 相同 | ✅同 |
| 精确路 | match_phrase on text，只取 1 条，命中即置顶不参与 RRF（:598） | 相同 | ✅同 |
| 配额 | 有精确命中固定 2/1；否则向量=ceil(size×0.75)、模糊=剩余（:603-611） | 相同 | ✅同 |
| 向量路 | ES8 kNN，num_candidates=50（:155-168） | 相同 | ✅同 |
| 模糊路 | match on text.ik，BM25（:190） | 相同 | ✅同 |
| RRF | weight/(rank+60+1) 跨路累加、×60 还原、平局用 ES 原始分（fusion_ranked:323-361） | 逐行移植 | ✅同 |
| _id 去重置顶 | 精确命中 + RRF 结果合并去重（:626-642） | 相同 | ✅同 |
| score 写回 | _score→metadata['score']、_id→metadata['_id']（:646-699） | 相同 | ✅同 |
| filter DSL | term/terms on metadata.X.keyword（:119-137） | 相同 | ✅同 |
| ES 版本 | ES7/ES8 双分支 + 重试装饰器 | 仅 ES8（8.15） | ⚠简化 |
| 查询执行 | 单次 DSL 拼 filter | 三次独立 search | ⚠实现不同、语义等价 |

---

## 三、基础设施与安全

| 项 | 生产 | demo | 异同 |
|---|---|---|---|
| ES | 8.15 + IK（可能开认证） | 8.15 + IK（es8-kibana 子 compose，384m 堆，无认证） | ✅同版本 |
| 嵌入 | bge-m3 1024 维（GPU 服务） | bge-m3 int8 ONNX（TEI CPU 容器） | ✅同模型 |
| 重排 | bge-m3 /rerank | bge-reranker-v2-m3 int8（TEI CPU 容器） | ⚠模型不同（见 §二.4） |
| LLM | 平台自有（get_llm_with_db） | 微信 OpenAI 兼容接口 | ⚠异 |
| 出站安全 | 内网环境无此机制 | check_url host 白名单（localhost/127.0.0.1/chatapi.weixin.qq.com）+ localhost→127.0.0.1 改写（系统代理坑） | ⚠demo 新增（防 SSRF） |
| 观测 | langfuse + logger | print 分环节日志（耗时/命中数/分数） | ⚠异 |

---

## 四、差异汇总（快速索引）

**结构性省略**（生产有、demo 无，均有注释标注）：
1. 图片链路：docx/pdf 图片提取、excel 单元格图片、minio 上传、base64 搬运
2. 辅助文档双写：摘要（DocumentSummaryGenerator）+ 生成问题（DocumentQuestionsGenerator）
3. DB/celery/minio 周边：知识项状态流转、库级 md5 去重、异步任务、统计增减
4. agent/langgraph 编排：知识库转 runable tool、thread 会话/checkpoint
5. SSE 流式输出、输入/输出安全词过滤、langfuse 观测、热点引用统计
6. 其他文件类型：txt/csv/json/html/mhtml/ppt/convert_md 路线、.xls 简版 excel 解析
7. OCR：pdf 全图片页（RapidOCR）、ChineseTextSplitter 等其他切分器

**安全等价替换**（行为不同、用途相同）：
1. FAQ 去重指纹 MD5 → SHA-256
2. 出站 URL 白名单校验（demo 新增）

**实现简化**（结果同构）：
1. word 解析单遍聚合（生产两遍流式）；字号启发保留三要素
2. excel 表头补齐/列宽估算/多行表头拼接简化；空行处理更稳
3. 嵌入去重 numpy 矩阵 → 朴素循环
4. 分类 prompt 瘦身（类别与边界规则全保留）
5. md 无 pandoc 时 Python fallback；ES 查询三次独立 search

**环境性差异**：
1. 重排模型 bge-m3 两用 → bge-reranker-v2-m3 专用
2. LLM 平台模型 → 微信 Deepseek-v4-flash；分类小模型 Qwen2.5-7B → 同一 Deepseek
3. GPU 嵌入服务 → TEI CPU int8 ONNX（docker 内存受限场景）

---

## 五、v3 增补（SSE / langfuse / ragas / 本地文件存储）

### 1. 文件存储（file_service.py）

| 项 | 生产（file_manage.py:71-89） | demo | 异同 |
|---|---|---|---|
| get_upload_file_path | minio 分支（bucket 下载）+ 本地分支 | 本地分支复刻（存在性 fail-fast）；minio 分支保留骨架报 NotImplementedError | ✅本地分支同 / ⚠minio 不起 |
| 上传目录 | settings.ingest_doc_path | uploads/（样例文件生成于此） | ✅同概念 |
| 调用位 | file_ingest:102 入库前统一转本地路径 | ingest_service.file_ingest 同位置调用 | ✅同 |

### 2. HTTP 接口与 SSE（api_server.py）

| 项 | 生产（assistant_run.py + stream.py） | demo | 异同 |
|---|---|---|---|
| 会话接口 | POST /stream（EventSourceResponse） | POST /api/v2/assistant_run/stream（sse-starlette） | ✅同形态 |
| 请求体 | CreateAssitantRunPayload | question/assistant_uuid/transaction_id 三核心字段 | ⚠demo 取子集 |
| langfuse header | langfuse_parent_trace_id/observation_id 透传 | 同名两个 header，兼作 trace id | ✅同（简化为直接 trace id） |
| SSE 事件序列 | metadata(run_id) → data[HumanMessage] → data[ToolMessage 引用] → data[AIMessage 流] | 完全相同序列；消息 response_metadata 键逐一对照（start_time/model_name/transaction_id/thread_id/assistant_uuid/kb_empty） | ✅同构 |
| 回答流式性 | LLM token 级真流 | LLM 非流式，按 40 字切片同 id 消息块模拟 | ⚠近似 |
| 检索接口 | POST /v3/knowledges/retrieve → {code,msg,data} | POST /api/v3/knowledges/retrieve 同响应结构 | ✅同 |
| 超时 wrapper | 50s agent 超时 + 错误话术（:1578-1597） | 各客户端自身 timeout | ⚠简化 |
| uvicorn/FastAPI | 生产同框架 | 同框架 | ✅同 |

### 3. langfuse 观测（obs.py + docker-compose.langfuse.yml）

| 项 | 生产 | demo | 异同 |
|---|---|---|---|
| 实例 | 云上 langfuse | 自托管 v2 + postgres（compose 起，seed org/project/user/API key） | ⚠自托管（按需求） |
| 埋点方式 | @observe() 装饰器 + langchain callback handler 挂进链（knowledge.py:78-92） | 双轨：手动 trace/generation（分类/检索汇总/评估）+ 同款 CallbackHandler 经 callbacks 通道挂进 qa_chain（chain→双通道 retriever→hybrid retriever 逐级上报） | ✅同机制（callbacks 通道同名同位） |
| 检索各阶段可见性 | langchain run 树：chain/retriever/LLM（LLM 是 langchain 模型自动记录；rerank 等 compressor 非同样不自动出现） | langchain run 树：SimpleRetrievalQA/ContextualCompressionRetriever×2/SimpleHybridRetriever×2；LLM 是手写客户端→以手动 generation（knowledges_retrieve）呈现 | ⚠LLM 粒度实现不同（自动 vs 手动） |
| trace 层级 | 1 trace/请求；分类/检索/生成为子节点 | 相同（question_classification / langchain run 树 / knowledges_retrieve） | ✅同 |
| trace id | transaction_id 兼作 parent id（assistant_run.py:19-20） | transaction_id 直接作 trace id | ✅同语义 |
| 埋点失败影响 | 旁路不影响业务 | 相同（Obs 全方法静默降级） | ✅同 |

### 4. ragas 评估（eval_ragas.py）

| 项 | 生产 | demo | 异同 |
|---|---|---|---|
| 评估入口 | only_retrieval 参数预留（knowledge.py:82/:92），离线跑 | eval_ragas.py 可运行脚本（检索→组装→evaluate） | ⚠demo 落地为脚本 |
| judge LLM | 平台模型 | 微信 Deepseek-v4-flash（ChatOpenAI 兼容接入） | ⚠环境差异 |
| embeddings | 平台嵌入 | TEI bge-m3（包 langchain Embeddings 接口） | ✅同模型 |
| 指标 | —（生产未见固化指标集） | context_precision/recall + faithfulness/answer_relevancy | ⚠demo 选型 |
| 结果上报 | — | langfuse trace（name=ragas_eval） | ⚠demo 新增 |
