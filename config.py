# -*- coding: utf-8 -*-
"""
config.py —— 配置加载 + 出站 URL 安全校验

对应真实项目：配置散布在各处环境变量（OPSGPT_*）与 server/config.py；
本 demo 统一收口在这里。

【安全约束】所有对外的 HTTP 调用（TEI/ES 探测/LLM）发起前必须过 check_url：
  ① scheme 仅允许 http/https；
  ② host 必须在白名单 HTTP_ALLOWED_HOSTS（默认即本次明确部署的三个服务：
     localhost 的 TEI/ES + chatapi.weixin.qq.com 的 LLM）；
  ③ 白名单之外的 host 一律抛异常拒绝（防 SSRF：代码里不存在可被诱导访问
     任意内网地址的路径——所有 URL 均来自本配置文件，而非用户输入）。
"""
import os
from urllib.parse import urlsplit

from dotenv import load_dotenv

# 加载 rag-lab/.env（真实密钥只放在 .env，不进代码/示例文件）
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

# 【代理坑修复】Windows 系统代理（如 Clash 7890）开启时：
#   · requests 对 localhost 主机名不走本地绕过（每请求固定慢 ~21s）
#   · langfuse SDK 对 127.0.0.1 的 POST 上报会被代理拦成 502 Bad gateway
# 把本地 host 显式写进 NO_PROXY（requests / langfuse SDK 都认），外网 host
# （chatapi.weixin.qq.com）不受影响仍走代理。
_no_proxy = os.environ.get("NO_PROXY", "")
for _h in ("127.0.0.1", "localhost"):
    if _h not in _no_proxy:
        _no_proxy = f"{_no_proxy},{_h}" if _no_proxy else _h
os.environ["NO_PROXY"] = _no_proxy
os.environ["no_proxy"] = _no_proxy


def require_env(name: str) -> str:
    """读取必填环境变量，缺失直接报错终止（fail-fast）。"""
    v = os.getenv(name)
    if not v:
        raise ValueError(f"环境变量 {name} 未配置（检查 rag-lab/.env）")
    return v


# ---- LLM（微信 OpenAI 兼容接口）----
LLM_BASE_URL = require_env("LLM_BASE_URL")
LLM_API_KEY = require_env("LLM_API_KEY")
LLM_MODEL = require_env("LLM_MODEL")

# ---- ES（docker 已运行的 es8-ik）----
ES_URL = require_env("ES_URL")
ES_USER = os.getenv("ES_USER")          # 可选：开了安全认证才填
ES_PASSWORD = os.getenv("ES_PASSWORD")
INDEX_NAME = os.getenv("INDEX_NAME", "rag_lab_demo")

# ---- TEI（本目录 docker compose 启动）----
TEI_EMBED_URL = require_env("TEI_EMBED_URL")
TEI_RERANK_URL = require_env("TEI_RERANK_URL")

# ---- HTTP host 白名单 ----
ALLOWED_HOSTS = {h.strip() for h in
                 os.getenv("HTTP_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()}


def check_url(url: str) -> str:
    """出站请求前的统一校验：仅 http/https + host 白名单。通过则返回
    （localhost 会被规范成 127.0.0.1，原因见下）。

    所有客户端（clients.py 的 TEI/LLM、es_store.py 的 ES）初始化时调用一次，
    把"允许访问哪里"钉死在配置层，运行期 URL 不再变化。

    为什么 localhost → 127.0.0.1：Windows 开着系统代理（如 Clash 7890）时，
    Python requests 会读注册表代理设置；hostname 写 localhost 不走"本地地址
    绕过"，请求被代理转一圈，实测每请求固定慢 ~21 秒；写 127.0.0.1 则正常
    绕过（~40ms）。ES 官方客户端不读注册表代理，本不受影响，统一改写只为
    消除两套行为。"""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"拒绝非 http/https 的 URL: {url!r}")
    host = parts.hostname or ""
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"拒绝访问白名单之外的 host: {host!r}（白名单: {sorted(ALLOWED_HOSTS)}）")
    if host == "localhost":
        return url.replace("://localhost", "://127.0.0.1", 1)
    return url
