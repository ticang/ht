# -*- coding: utf-8 -*-
"""
file_service.py —— 文件路径服务（复刻 server/service/file_manage.py 的
get_upload_file_path:71-89）+ demo 的本地上传目录约定

生产逻辑（file_manage.py:71-89）：
    get_upload_file_path(file_path)
      · 有 OPSGPT_MINIO_HOST → 对象存储模式：path 倒数第二段当 bucket、
        最后一段当文件名，从 minio 下载到 /tmp/ingest_files 再返回本地路径
      · 无该环境变量 → 本地存储模式：原样返回（文件本来就存本机）

demo 按需求【固定走本地存储模式】：不起 minio，统一把待入库文件放进
rag-lab/uploads/ 目录（对应生产的 settings.ingest_doc_path 上传目录概念），
get_upload_file_path 复刻同样的分支判定（minio 分支保留代码骨架与注释，
未配置时自然走本地——与生产行为完全一致）。

配套改动：rag_demo 的样例文件生成到 uploads/；ingest_service.file_ingest
在解析前调用 get_upload_file_path（对齐生产 ingest.py:102 的调用位）。
"""
import os

# demo 上传目录（生产是 settings.ingest_doc_path 配置的目录）
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")


def ensure_upload_dir() -> str:
    """确保上传目录存在并返回（样例生成/入库前调用）。"""
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    return UPLOAD_DIR


def get_upload_file_path(file_path: str = None) -> str:
    """复刻 file_manage.py:71-89：返回可用的本地文件路径。

    生产语义：入库任务（file_ingest/faq_batch）拿到的 file_path 可能是
    minio 对象路径（bucket/文件名 形式），此处负责统一成"本地已存在的文件"：
      · OPSGPT_MINIO_HOST 已配置 → 下载到 /tmp/ingest_files（生产 :72-87，
        bucket=倒数第二段、文件名=最后一段、minio_client.fget_object）
      · 未配置 → 本地存储模式，原样返回（生产 :89 的 fallthrough）
    demo 不起 minio（需求明确本地模式），minio 分支以注释形式保留骨架；
    本地分支额外做一个存在性检查，缺文件尽早报错（fail-fast，生产的
    存在性检查在 file_to_vector:112-114）。"""
    if os.getenv("OPSGPT_MINIO_HOST"):
        # 生产 minio 分支（demo 未启用，保留骨架对照）：
        #   file_split = file_path.split('/')
        #   file_bucket, file_name = file_split[-2], file_split[-1]
        #   file_destination = '/tmp/ingest_files'  (makedirs)
        #   minio_client.fget_object(file_bucket, file_name, 本地路径)
        raise NotImplementedError(
            "demo 按需求固定本地存储模式（未复刻 minio 分支）；"
            "如需对象存储请取消 OPSGPT_MINIO_HOST 并参照 file_manage.py:72-87")
    # 本地存储模式（生产 :89）：原样返回
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"本地文件不存在: {file_path}（demo 上传目录: {UPLOAD_DIR}）")
    return file_path
