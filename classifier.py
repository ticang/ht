# -*- coding: utf-8 -*-
"""
classifier.py —— 问题意图识别（复刻 aiops-agent-core/backend/agent_core/
service/question_classifier.py 的 QuestionClassifier）

生产链路位置（assistant_run.py create_stream → AssistantService.stream）：
    AssistantService.stream(assistant.py:1503-1545)
      └─ _classify_question_with_llm(question, llm)   # assistant.py:2099-2126
           └─ QuestionClassifier().classify_question(question, llm_instance)
                ├─ classification_prompt.format(question=...)  # 巨型规则 prompt
                ├─ LLM 返回类型字符串（30s 超时保护, :312-318）
                ├─ type_mapping → QuestionType 枚举（:333-345）
                └─ _get_type_config → {requires_knowledge, preset_response}（:435-475）
    随后 stream 按结果分流（:1511-1568）：
      · llm_response 有值（OTHER 类已让 LLM 直接作答）→ 直接返回
      · preset_response 有值（greeting/identity/... 预设回复）→ 直接返回
      · MCP_QUERY → MCP 查询+知识库降级（demo 无 MCP 服务，见 rag_demo 注释）
      · 其余（KNOWLEDGE_QUERY）→ 走 agent 知识库检索（→ knowledges_retrieve）

demo 复刻范围与差异：
    · 9 类枚举、输出格式、type_mapping、_get_type_config 全保留
    · 分类 prompt 保留全部类别定义与关键判定规则，删去生产 :44-52 的关键词
      清单和 :55-75/:188-247 的几十条示例（prompt 瘦身约 70%，注释标注）；
      demo 知识域是"PDCP/切换参数"，系统名示例同步替换
    · 超时保护从 30s 降到 60s（微信接口偶发慢，demo 宽松些）；OTHER 类的
      "LLM 自由作答"分支保留（生产 :353-416）
"""
import asyncio
import time
from enum import Enum
from typing import Any, Dict, Optional


class QuestionType(Enum):
    """问题类型枚举 —— 生产 question_classifier.py:12-22 原样。"""
    IDENTITY = "identity"                # 身份识别类（你是谁）
    CAPABILITY = "capability"            # 问题处理能力类
    SCOPE = "scope"                      # 功能范围类
    LEARNING = "learning"                # 学习能力类
    SERVICE = "service"                  # 服务规范类
    MCP_QUERY = "mcp"                    # 工单、流程查询类
    KNOWLEDGE_QUERY = "knowledge_query"  # 知识库查询类（唯一走检索的类型）
    GREETING = "greeting"                # 问候语类型
    OTHER = "other"                      # 其他（LLM 自由作答）


# 预设回复表 —— 生产 agent_core/service/preset_responses.get_preset_response
# （大段平台话术），demo 每类一句等价话术，行为同为"不检索直接返回"
PRESET_RESPONSES = {
    QuestionType.IDENTITY: "我是 rag-lab 知识库问答助手，"
                           "复刻自网络创新服务支撑平台的知识问答链路。",
    QuestionType.CAPABILITY: "我可以回答已入库知识范围内的业务问题"
                             "（docx/pdf/md/excel/faq 多类型知识），闲聊也会尽力一答。",
    QuestionType.SCOPE: "我基于检索到的知识片段作答，检索不到时会明确说不知道，"
                        "不编造答案。",
    QuestionType.LEARNING: "我的知识来自管理员上传的文档（解析入库后可检索），"
                           "对话本身不会改变我的知识。",
    QuestionType.SERVICE: "本 demo 是链路复刻环境；生产平台的反馈渠道此处不可用。",
    QuestionType.GREETING: "你好！我是 rag-lab 知识库问答助手，请问想查询什么？",
}


# 分类提示词 —— 生产 question_classifier.py:30-271 的瘦身版：
# 保留 9 类定义、优先级顺序、knowledge_query 与 mcp 的边界规则（这是分流
# 正确性的关键），删去关键词罗列与几十条 few-shot 示例
CLASSIFICATION_PROMPT = """你是[网络创新服务支撑平台]的问题分类专家，请严格按照以下优先级规则将用户问题分类：

## 分类规则（按优先级匹配）：

### 1. 知识库查询类（最高优先级）
识别规则：咨询运维运营相关业务问题，涉及系统配置、功能使用、操作指导、参数含义等。
特殊规则：
- 问题包含系统名称或应用名（如"PDCP参数"、"切换参数"等专有名词）时归此类
- 排除：问题包含工单编号（ZW-/ATRUST-开头）或明确的工单/流程查询语句时归工单流程类

### 3. 身份识别类
谁、创造者、身份、你的名字、机器人、真人。示例："你是谁"、"你叫什么名字"

### 4. 问题处理能力类
复杂问题、回答不了、不理解、怎么办。示例："你能处理复杂问题吗"

### 5. 功能范围类
能做什么、不能做什么、准确吗。示例："你能做什么"、"回答100%准确吗"

### 6. 学习能力类
学习、知识来源、习惯、更新。示例："你会学习吗"、"你的知识从哪来"

### 7. 服务规范类
区别、隐私、错误反馈、新功能。示例："和ChatGPT有什么区别"

### 8. 问候语类型
你好、您好、哈喽、嗨、早上好。示例："你好"

### 9. 工单、流程查询类
仅当问题唯一核心意图是查询/统计工单数据，或包含具体流程名称的流程咨询。
（含故障、报错、地点、设备、"请协助"等诉求的问题应归知识库查询类，不归此类）
示例："ZW-20261012-001 工单处理到什么进度了"、"查一下我上周提交的工单办到哪一步了"

## 输出要求
请只返回以下8个类型之一（英文分类），不要任何解释，不要返回括号以及括号内的解释说明：
- knowledge_query（知识库查询类）
- identity（身份识别类）
- capability（问题处理能力类）
- scope（功能范围类）
- learning（学习能力类）
- service（服务规范类）
- greeting（问候语类型）
- mcp（工单、流程查询类）

如果完全不匹配以上类型，返回：other

---

用户问题：{question}

分类结果："""


class QuestionClassifier:
    """问题分类器 —— 生产 QuestionClassifier(question_classifier.py:24) 复刻。

    生产 __init__ 里建 AiopsLLMProvider 小模型（Qwen2.5-7B），demo 直接用
    传入的 WeixinLLM（classify_question 的 llm_instance 形参两者同构）。"""

    def __init__(self, llm):
        self.llm = llm

    async def classify_question(self, question: str) -> Dict[str, Any]:
        """分类用户问题 —— 生产 classify_question(:273-433) 的复刻（同步链路
        丢线程池的桥接省略，demo 的 LLM 客户端自带 ainvoke）。

        返回 Dict：
          type                 QuestionType 枚举
          requires_knowledge   是否需要知识库检索（仅 KNOWLEDGE_QUERY 为 True）
          preset_response      预设回复类型（有则不检索直接返回）
          llm_response         OTHER 类时 LLM 直接生成的回答
          classification_confidence  high/low（返回值是否在映射表内）
        异常兜底（生产 :426-433）：分类失败 → OTHER 形态（不带 llm_response，
        由调用方决定降级策略）。"""
        try:
            start_time = time.time()
            prompt = CLASSIFICATION_PROMPT.format(question=question)
            print(f"[意图识别] 问题=\"{question[:30]}\" → 调 LLM 分类"
                  f"（prompt {len(prompt)} 字）", flush=True)
            # 生产 :312-318 的超时保护（30s→demo 60s，微信接口偶发慢）
            try:
                response = await asyncio.wait_for(
                    self.llm.ainvoke(prompt), timeout=60.0)
            except asyncio.TimeoutError:
                raise Exception("LLM request timeout")
            content = response if isinstance(response, str) else str(response)
            classification_result = content.strip().lower()
            print(f"[意图识别] LLM 返回: '{classification_result}'，"
                  f"耗时 {time.time() - start_time:.1f}s", flush=True)

            # 生产 :333-345 的映射表：非法输出一律落 OTHER
            type_mapping = {
                'identity': QuestionType.IDENTITY,
                'capability': QuestionType.CAPABILITY,
                'scope': QuestionType.SCOPE,
                'learning': QuestionType.LEARNING,
                'service': QuestionType.SERVICE,
                'greeting': QuestionType.GREETING,
                'mcp': QuestionType.MCP_QUERY,
                'knowledge_query': QuestionType.KNOWLEDGE_QUERY,
                'other': QuestionType.OTHER,
            }
            question_type = type_mapping.get(classification_result, QuestionType.OTHER)
            result = self._get_type_config(question_type)

            # OTHER 类：LLM 直接作答（生产 :353-416，温度 0.7 自由生成）
            if question_type == QuestionType.OTHER:
                general_prompt = (
                    "你是[网络创新服务支撑平台]的智能助手。虽然你的专业领域是平台相关知识，"
                    "但你可以用友善、专业的方式回答用户的各种问题。\n\n"
                    f"用户问题：{question}\n\n"
                    "回答要求：1.友善专业 2.简洁明了 3.完全无关的问题礼貌引导"
                    "回平台问题 4.不要编造。请回答：")
                print("[意图识别] other 类 → LLM 直接作答（不检索）", flush=True)
                try:
                    llm_response = await asyncio.wait_for(
                        self.llm.ainvoke(general_prompt, temperature=0.7), timeout=60.0)
                    return {
                        "type": question_type,
                        "requires_knowledge": False,
                        "preset_response": None,
                        "llm_response": llm_response if isinstance(llm_response, str)
                                        else str(llm_response),
                        "classification_confidence":
                            "high" if classification_result in type_mapping else "low",
                    }
                except Exception as e:                  # 生产 :408-416：降级预设回复
                    return {"type": question_type, "requires_knowledge": False,
                            "preset_response": "other", "error": str(e)}

            # 其余类型：生产 :418-424 的返回形态
            return {
                "type": question_type,
                "requires_knowledge": result["requires_knowledge"],
                "preset_response": result["preset_response"],
                "classification_confidence":
                    "high" if classification_result in type_mapping else "low",
            }
        except Exception as e:
            # 生产 :426-433：任何异常都兜底成 OTHER（保证接口不挂）
            print(f"[意图识别] 失败，兜底 OTHER: {e}", flush=True)
            return {"type": QuestionType.OTHER, "requires_knowledge": False,
                    "preset_response": None, "error": str(e)}

    @staticmethod
    def _get_type_config(question_type: QuestionType) -> Dict[str, Any]:
        """生产 _get_type_config(:435-475) 原样：只有 KNOWLEDGE_QUERY 需要
        检索；预设回复按类型查表（demo 表见 PRESET_RESPONSES）。"""
        requires = question_type == QuestionType.KNOWLEDGE_QUERY
        preset = None
        if question_type in PRESET_RESPONSES:
            preset = question_type.value               # 返回类型名，话术由调用方查表
        return {"requires_knowledge": requires, "preset_response": preset}

    def get_preset_content(self, response_type: Optional[str]) -> Optional[str]:
        """生产 get_preset_response_content：类型名 → 预设话术。"""
        for qt, text in PRESET_RESPONSES.items():
            if qt.value == response_type:
                return text
        return None
