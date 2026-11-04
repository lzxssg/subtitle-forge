"""配置管理：环境变量为默认值，Web 界面可修改并持久化到 SQLite。"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # /app
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", str(BASE_DIR / "config")))
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
MODELS_DIR = Path(os.getenv("MODELS_DIR", str(BASE_DIR / "models")))
STATIC_DIR = Path(__file__).resolve().parent / "static"
DB_PATH = DATA_DIR / "subtitle_forge.db"

CONFIG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# llama-swap 中注册的全部 LLM 模型（转录阶段全部卸载，把显存让给 ASR）
DEFAULT_SWAP_MODELS = "sakura-gpu,qwen3_8b,review-35b,35B-A3B,glm4_9b"

DEFAULTS = {
    "watch_dirs": os.getenv("WATCH_DIRS", "/Media"),
    "scan_interval_minutes": int(os.getenv("SCAN_INTERVAL_MINUTES", "60")),
    "scan_interval_value": int(os.getenv("SCAN_INTERVAL_VALUE", "60")),
    "scan_interval_unit": os.getenv("SCAN_INTERVAL_UNIT", "minutes"),
    "video_exts": os.getenv("VIDEO_EXTS", ".mp4,.mkv,.avi,.mov,.wmv,.flv,.webm,.ts,.m2ts"),
    "asr_model": os.getenv("ASR_MODEL", "small"),
    "asr_device": os.getenv("ASR_DEVICE", "auto"),
    "asr_compute_type": os.getenv("ASR_COMPUTE_TYPE", "int8"),
    "asr_cpu_threads": int(os.getenv("ASR_CPU_THREADS", "8")),
    # v1.8.0: ASR 运行方式 local=容器内 faster-whisper / llama=llama-swap 统一托管（whisper.cpp）
    "asr_provider": os.getenv("ASR_PROVIDER", "local"),
    "llama_swap_config": os.getenv("LLAMA_SWAP_CONFIG", "/config-llama.yaml"),
    "llama_models_dir": os.getenv("LLAMA_MODELS_DIR", "/models-llama"),
    "llama_swap_container": os.getenv("LLAMA_SWAP_CONTAINER", "llama-swap"),
    # v2.0.0: 多模型共存调度——显存总量(MB)达到该值才允许多个大模型同时驻留，低于则严格单模型串行
    "multi_model_vram_mb": int(os.getenv("MULTI_MODEL_VRAM_MB", "16000")),
    "llm_base_url": os.getenv("LLM_BASE_URL", ""),
    "llm_api_key": os.getenv("LLM_API_KEY", ""),
    "llm_model": os.getenv("LLM_MODEL", "qwen2.5:7b"),
    "llm_temperature": float(os.getenv("LLM_TEMPERATURE", "0.3")),
    "source_lang": os.getenv("SOURCE_LANG", "auto"),
    "target_lang": os.getenv("TARGET_LANG", "zh"),
    "max_workers": int(os.getenv("MAX_WORKERS", "1")),
    # v4 新增：任务线程 / 转录并发 / 翻译并发（auto=按 GPU 画像智能推荐）
    "task_workers": int(os.getenv("TASK_WORKERS", os.getenv("MAX_WORKERS", "1"))),
    "transcribe_workers": os.getenv("TRANSCRIBE_WORKERS", "auto"),
    "translate_workers": os.getenv("TRANSLATE_WORKERS", "auto"),
    "smart_queue": os.getenv("SMART_QUEUE", "true").lower() == "true",
    "translate_only": os.getenv("TRANSLATE_ONLY", "false").lower() == "true",
    "skip_existing_subtitle": os.getenv("SKIP_EXISTING_SUBTITLE", "true").lower() == "true",
    "subtitle_suffix": os.getenv("SUBTITLE_SUFFIX", ".zh"),
    "transcript_quality_threshold": int(os.getenv("TRANSCRIPT_QUALITY_THRESHOLD", "60")),
    "translation_quality_threshold": int(os.getenv("TRANSLATION_QUALITY_THRESHOLD", "70")),
    # v2 新增
    "monitor_enabled": os.getenv("MONITOR_ENABLED", "true").lower() == "true",
    "auto_retry_failed": os.getenv("AUTO_RETRY_FAILED", "true").lower() == "true",
    "file_stable_seconds": int(os.getenv("FILE_STABLE_SECONDS", "10")),
    "asr_language": os.getenv("ASR_LANGUAGE", "auto"),
    "translation_speed": os.getenv("TRANSLATION_SPEED", "balanced"),
    # v3 新增：运行模式 manual=手动/scheduled=定时扫描/guard=智能监控；独立校验模型
    "mode": os.getenv("MODE", "manual"),
    "queue_paused": os.getenv("QUEUE_PAUSED", "false").lower() == "true",
    "review_base_url": os.getenv("REVIEW_BASE_URL", ""),
    "review_api_key": os.getenv("REVIEW_API_KEY", ""),
    "review_model": os.getenv("REVIEW_MODEL", ""),
    # v1.4.8: 翻译校对 LLM 抽样条数（越多越准、越慢；规则预检命中时跳过 LLM）
    "review_sample_count": int(os.getenv("REVIEW_SAMPLE_COUNT", "12")),
    # v1.4.6: 两阶段模式 pipeline=边转录边翻译 / staged=先全部转录再集体翻译
    "phase_mode": os.getenv("PHASE_MODE", "pipeline"),
    "sakura_container": os.getenv("SAKURA_CONTAINER", "llama-sakura"),
    # v1.5.0: llama-swap 模型生命周期管理（转录前卸载的模型列表，逗号分隔）
    "llama_swap_models": os.getenv("LLAMA_SWAP_MODELS", DEFAULT_SWAP_MODELS),
    "llama_swap_auto_unload": os.getenv("LLAMA_SWAP_AUTO_UNLOAD", "true").lower() == "true",
    # v1.6.3: 模型下载代理（如 http://127.0.0.1:7890），官方源不可达时经代理下载
    "hf_proxy": os.getenv("HF_PROXY", ""),
    # v1.6.9: 润色作为流水线最后一步（转录→质检→翻译→质检→润色），输出 .zh.rs.srt 并存
    "polish_enabled": os.getenv("POLISH_ENABLED", "false").lower() == "true",
    # v2.1.0: 调度模式
    #   staged   = 单模型·环节串行（全部转录完再集体翻译，同一时刻只驻留一个大模型）
    #   pipeline = 单模型·任务串行（一个任务走完全流程再进下一个）
    #   parallel = 多模型·环节并行（转录/翻译/润色各自并发，多模型共存，需大显存）
    #   auto     = 按显卡显存自动判断（>= multi_model_vram_mb 用 parallel，否则 staged）
    "scheduler_mode": os.getenv("SCHEDULER_MODE", "auto"),
    "polish_workers": os.getenv("POLISH_WORKERS", "auto"),
    # v2.2.0: 润色独立大模型（留空则依次回退 review_model → llm_model）
    "polish_base_url": os.getenv("POLISH_BASE_URL", ""),
    "polish_api_key": os.getenv("POLISH_API_KEY", ""),
    "polish_model": os.getenv("POLISH_MODEL", ""),
    "polish_temperature": float(os.getenv("POLISH_TEMPERATURE", "0.3")),
    # 多模型模式各环节并发数（auto=按显卡型号/显存智能推荐，或 1-8 手选）
    "parallel_transcribe_workers": os.getenv("PARALLEL_TRANSCRIBE_WORKERS", "auto"),
    "parallel_translate_workers": os.getenv("PARALLEL_TRANSLATE_WORKERS", "auto"),
    "parallel_polish_workers": os.getenv("PARALLEL_POLISH_WORKERS", "auto"),
    # 质检未通过字幕归档目录（Web 界面选择映射文件夹，可进子目录；空=留在视频原目录）
    "review_failed_dir": os.getenv("REVIEW_FAILED_DIR", ""),
    "review_archive_enabled": os.getenv("REVIEW_ARCHIVE_ENABLED", "true").lower() == "true",
    # 显卡参数：留空/0 时自动读取 nvidia-smi，可手动覆盖（容器内看不到 GPU 的场景）
    "gpu_model_override": os.getenv("GPU_MODEL_OVERRIDE", ""),
    "gpu_vram_mb_override": int(os.getenv("GPU_VRAM_MB_OVERRIDE", "0")),
    # v1.7.0: 播放媒体 webhook 联动（Jellyfin/Emby/Plex 入库通知）
    "webhook_enabled": os.getenv("WEBHOOK_ENABLED", "false").lower() == "true",
    "webhook_secret": os.getenv("WEBHOOK_SECRET", ""),
    "webhook_events": os.getenv("WEBHOOK_EVENTS", "ItemAdded,ItemUpdated,LibraryNew"),
    "webhook_media_types": os.getenv("WEBHOOK_MEDIA_TYPES", "Movie,Episode"),
    "webhook_path_map": os.getenv("WEBHOOK_PATH_MAP", ""),
}

# llama-swap 中注册的全部 LLM 模型（转录阶段全部卸载，把显存让给 ASR）
DEFAULT_SWAP_MODELS = "sakura-gpu,qwen3_8b,review-35b,35B-A3B,glm4_9b"

# 可被 Web 界面修改的键
EDITABLE_KEYS = [
    "watch_dirs", "scan_interval_minutes", "scan_interval_value", "scan_interval_unit",
    "video_exts",
    "asr_model", "asr_device", "asr_compute_type", "asr_cpu_threads", "asr_language", "asr_provider",
    "llm_base_url", "llm_api_key", "llm_model", "llm_temperature",
    "review_base_url", "review_api_key", "review_model",
    "source_lang", "target_lang", "max_workers", "task_workers",
    "transcribe_workers", "translate_workers", "polish_workers", "smart_queue", "translate_only",
    "polish_enabled",
    "skip_existing_subtitle", "subtitle_suffix",
    "transcript_quality_threshold", "translation_quality_threshold", "review_sample_count",
    "monitor_enabled", "auto_retry_failed", "file_stable_seconds", "translation_speed",
    "phase_mode", "llama_swap_models", "llama_swap_auto_unload", "hf_proxy",
    "multi_model_vram_mb",
    "scheduler_mode", "polish_workers", "polish_base_url", "polish_api_key", "polish_model",
    "parallel_transcribe_workers", "parallel_translate_workers", "parallel_polish_workers",
    "review_failed_dir", "review_archive_enabled",
    "gpu_model_override", "gpu_vram_mb_override",
    "webhook_enabled", "webhook_secret", "webhook_events", "webhook_media_types", "webhook_path_map",
]

# HF 国内镜像加速（模型下载走 hf-mirror.com）
HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://hf-mirror.com")

# faster-whisper 各模型参考大小（MB，用于下载进度估算，误差约 ±5%）
# 镜像端缺失（404）的模型仍保留：下载时会自动尝试 hf-mirror → 官方源（可配代理）。
# large-v3-turbo：distil 蒸馏版，多语言（含韩语），速度约为 large-v3 的 4-5 倍，体积约一半
ASR_MODEL_SIZES_MB = {
    "tiny": 75, "base": 141, "small": 464, "medium": 1512,
    "large-v3": 2950, "large-v3-turbo": 1600,
    "tiny.en": 75, "base.en": 141,
    "small.en": 464, "medium.en": 1512,
}


def get_setting(key, default=None):
    """读取配置：优先 SQLite 持久化值，其次 DEFAULTS（环境变量）默认值（函数内延迟导入避免循环依赖）。"""
    from . import database
    val = database.get_setting(key)
    if val is not None:
        return val
    if key in DEFAULTS:
        return DEFAULTS[key]
    return default


def set_setting(key, value):
    from . import database
    return database.set_setting(key, value)
