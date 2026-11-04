"""设备能力探测与模型推荐，以及失败判定建议。"""
import logging
import os
import re
import subprocess

logger = logging.getLogger("subtitle-forge.device")


def get_cpu_info():
    try:
        import psutil
        cores = psutil.cpu_count(logical=True)
        mem_total = psutil.virtual_memory().total / (1024 ** 3)
        mem_avail = psutil.virtual_memory().available / (1024 ** 3)
        return {"cores": cores, "mem_total_gb": round(mem_total, 1), "mem_avail_gb": round(mem_avail, 1)}
    except Exception:
        return {"cores": os.cpu_count() or 1, "mem_total_gb": 0, "mem_avail_gb": 0}


def get_gpu_info():
    """解析 nvidia-smi，返回 GPU 列表或 None。"""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,memory.used,utilization.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            return None
        gpus = []
        for line in r.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 5:
                gpus.append({
                    "index": parts[0], "name": parts[1],
                    "mem_total_mb": parts[2], "mem_used_mb": parts[3],
                    "util_pct": parts[4],
                })
        return gpus or None
    except Exception:
        return None


def get_device_profile():
    """汇总设备能力画像。"""
    cpu = get_cpu_info()
    gpus = get_gpu_info()
    profile = {
        "cpu": cpu,
        "gpu": gpus,
        "has_gpu": bool(gpus),
        "gpu_mem_total_mb": int(gpus[0]["mem_total_mb"]) if gpus else 0,
    }
    profile["recommended_model"] = recommend_asr_model(profile)
    return profile


def recommend_asr_model(profile=None):
    """根据硬件推荐 faster-whisper 模型。"""
    profile = profile or get_device_profile()
    if profile.get("has_gpu"):
        mem = profile.get("gpu_mem_total_mb", 0)
        if mem >= 16000:
            return "large-v3"
        if mem >= 8000:
            return "large-v3-turbo"
        if mem >= 4000:
            return "medium"
        if mem >= 2000:
            return "small"
        return "base"
    cores = profile.get("cpu", {}).get("cores", 4)
    if cores >= 8:
        return "small"
    return "base"


def recommend_concurrency(profile=None):
    """根据 GPU 型号/显存/占用，智能推荐 转录与翻译并发数。

    规则：
    - 转录（ASR）显存占用随模型增大（small≈1.2G / medium≈3G / large≈6G+）；
    - 翻译（LLM）并发受 LLM 服务显存占用影响（如 Sakura 常驻 7G）；
    - 有 GPU 时先看显存总量，再看当前占用（空闲显存 <2G 时转录并发强制为 1）；
    - 无 GPU（CPU int8 转录）时允许 2 个转录并发利用多核，翻译并发 1。
    """
    profile = profile or get_device_profile()
    transcribe = 1
    translate = 1
    if profile.get("has_gpu"):
        total = profile.get("gpu_mem_total_mb", 0)
        used = 0
        gpus = profile.get("gpu", []) or []
        if gpus:
            try:
                used = int(gpus[0].get("mem_used_mb", 0) or 0)
            except (ValueError, TypeError):
                used = 0
        free = max(0, total - used)
        if total >= 24000:
            transcribe, translate = 2, 2
        elif total >= 16000:
            transcribe, translate = 2, 1
        elif total >= 8000:
            transcribe, translate = 1, 1
        else:
            transcribe, translate = 1, 1
        # 显存紧张兜底：空闲不足 2G 时转录并发必须为 1，防止 OOM
        if free < 2048:
            transcribe = 1
        # 空闲非常充足且显存大 → 转录可提到 3
        if total >= 24000 and free >= 16000:
            transcribe = 3
    else:
        cores = profile.get("cpu", {}).get("cores", 4)
        transcribe = 2 if cores >= 8 else 1
        translate = 1
    return {"transcribe": transcribe, "translate": translate}


# ---------- NVIDIA GeForce 10-50 系智能参数表 ----------
# tier 表示同显存下的推理代差系数：代系越新（架构/算力越高），
# 同显存可承载的 LLM 并发越多。gen 用于兼容比较。
GPU_SERIES_TABLE = [
    {"name": "RTX 50 系", "keywords": ("rtx 50", "rtx50", "geforce rtx 50", "rtx 5090", "rtx 5080", "rtx 5070", "rtx 5060"), "gen": 50, "tier": 5},
    {"name": "RTX 40 系", "keywords": ("rtx 40", "rtx40", "geforce rtx 40", "rtx 4090", "rtx 4080", "rtx 4070", "rtx 4060"), "gen": 40, "tier": 4},
    {"name": "RTX 30 系", "keywords": ("rtx 30", "rtx30", "geforce rtx 30", "rtx 3090", "rtx 3080", "rtx 3070", "rtx 3060", "rtx 3050"), "gen": 30, "tier": 3},
    {"name": "RTX 20 系", "keywords": ("rtx 20", "rtx20", "geforce rtx 20", "rtx 2080", "rtx 2070", "rtx 2060"), "gen": 20, "tier": 2},
    {"name": "GTX 16 系", "keywords": ("gtx 16", "gtx16", "geforce gtx 16", "gtx 1660", "gtx 1650", "gtx 1630"), "gen": 16, "tier": 1},
    {"name": "GTX 10 系", "keywords": ("gtx 10", "gtx10", "geforce gtx 10", "gtx 1080", "gtx 1070", "gtx 1060", "gtx 1050"), "gen": 10, "tier": 1},
]


def _match_gpu_series(name: str):
    """按 GPU 名称匹配 NVIDIA GeForce 10-50 系智能参数；未知型号返回 None。"""
    if not name:
        return None
    n = str(name).lower()
    for s in GPU_SERIES_TABLE:
        if any(k in n for k in s["keywords"]):
            return s
    return None


def recommend_polish_concurrency(profile=None):
    """智能推荐润色并发数（LLM 推理，与翻译共享 GPU）。

    规则：
    - 无 GPU → 1（CPU LLM 吞吐有限，并发无益）；
    - 有 GPU 先按「代系(tier) + 显存总量」给出基准并发；
    - 显存紧张修正：空闲 <2G 强制 1（防 OOM）；空闲 <4G 上限 2；空闲 ≥8G 基准+1；
    - 代系修正：RTX 40/50 系架构新、算力高，同显存可多一路并发。
    返回 {"polish": int, "series": str, "reason": str}。
    """
    profile = profile or get_device_profile()
    if not profile.get("has_gpu"):
        return {"polish": 1, "series": "无 NVIDIA GPU", "reason": "无 GPU：润色走 CPU/无加速，并发保持 1"}
    gpus = profile.get("gpu", []) or []
    name = gpus[0].get("name", "") if gpus else ""
    try:
        total = int(profile.get("gpu_mem_total_mb", 0) or 0)
    except (ValueError, TypeError):
        total = 0
    used = 0
    try:
        used = int(gpus[0].get("mem_used_mb", 0) or 0)
    except (ValueError, TypeError):
        used = 0
    free = max(0, total - used)
    series = _match_gpu_series(name)
    sname = series["name"] if series else "未知 NVIDIA 卡"
    gen = series["gen"] if series else 0

    # 基准并发（按显存总量 + 代系）
    if total >= 24000:
        base = 4 if gen >= 40 else 3
    elif total >= 16000:
        base = 3 if gen >= 40 else 2
    elif total >= 12000:
        base = 2 if gen >= 30 else 1
    elif total >= 8000:
        base = 2 if gen >= 40 else 1
    else:
        base = 1
    # 显存紧张修正
    if free < 2048:
        polish = 1
    elif free < 4096:
        polish = min(2, base)
    elif free >= 8192:
        polish = base + 1
    else:
        polish = base
    polish = max(1, min(4, polish))
    reason = "%s（显存 %.0fG，空闲 %.1fG）推荐润色并发 %d" % (sname, total / 1024, free / 1024, polish)
    return {"polish": polish, "series": sname, "reason": reason}


def recommend_stage_concurrency(profile=None):
    """智能并发（v1.7.0）：每个阶段独立判断并发数，适配分批分阶段批量任务。

    规则：
    - 转录 transcribe：asr_provider=llama（whisper.cpp 托管）为单槽位服务 → 恒 1，
      并发转录会触发 proxy EOF / 502；local faster-whisper 按显存 1~2（>=16G 2）。
    - 翻译 translate：受 LLM 服务 --parallel 能力限制；sakura-gpu / qwen3_8b
      已配 --parallel 2 → 推荐 2；显存空闲 <2G 时降 1。
    - 润色 polish：受润色模型 --parallel 能力限制；qwen3_8b 已配 --parallel 2 → 2；
      显存空闲 <2G 时降 1。
    返回 {"transcribe": int, "translate": int, "polish": int, "reasons": {...}}。
    """
    from .. import config as cfg
    profile = profile or get_device_profile()
    gpus = profile.get("gpu", []) or []
    try:
        total = int(profile.get("gpu_mem_total_mb", 0) or 0)
    except (ValueError, TypeError):
        total = 0
    used = 0
    if gpus:
        try:
            used = int(gpus[0].get("mem_used_mb", 0) or 0)
        except (ValueError, TypeError):
            used = 0
    free = max(0, total - used)
    has_gpu = profile.get("has_gpu", False)
    reasons = {}

    # ---- 转录：llama 托管 whisper.cpp 单槽位，不可并发 ----
    provider = str(cfg.get_setting("asr_provider") or "local").lower()
    if provider == "llama":
        transcribe = 1
        reasons["transcribe"] = "whisper.cpp 托管为单槽位服务，不支持并发转录（并发会 502/EOF）"
    elif has_gpu:
        transcribe = 2 if total >= 16000 else 1
        if total >= 24000:
            transcribe = 3
        reasons["transcribe"] = f"本地 faster-whisper 按显存 {total}MB 推荐并发 {transcribe}"
    else:
        cores = profile.get("cpu", {}).get("cores", 4)
        transcribe = 2 if cores >= 8 else 1
        reasons["transcribe"] = f"CPU 转录按核数 {cores} 推荐并发 {transcribe}"

    # ---- 翻译：按 LLM 服务并发能力 ----
    llm_model = str(cfg.get_setting("llm_model") or "").lower()
    if not has_gpu:
        translate = 1
        reasons["translate"] = "无 GPU，翻译并发保持 1"
    else:
        translate = 1
        if any(k in llm_model for k in ("sakura", "qwen", "35b", "glm")):
            translate = 2
        if total >= 24000:
            translate = max(translate, 3)
        if free < 2048:
            translate = 1
        reasons["translate"] = f"翻译模型 {llm_model or '未知'} 服务能力推荐并发 {translate}（空闲显存 {free}MB）"

    # ---- 润色：按润色模型并发能力 ----
    review_model = str(cfg.get_setting("review_model") or "").lower()
    if not has_gpu:
        polish = 1
        reasons["polish"] = "无 GPU，润色并发保持 1"
    else:
        polish = 1
        if any(k in review_model for k in ("qwen", "35b", "glm")):
            polish = 2
        if free < 2048:
            polish = 1
        reasons["polish"] = f"润色模型 {review_model or '未知'} 服务能力推荐并发 {polish}"

    return {"transcribe": transcribe, "translate": translate, "polish": polish, "reasons": reasons}


def advise_for_failure(error: str) -> dict:
    """任务失败时：结合错误信息与设备能力，判定当前设备能否成功并给建议。"""
    err = error.lower()
    profile = get_device_profile()
    suggestion = ""
    can_succeed = True

    if any(k in err for k in ("cuda", "out of memory", "oom", "no kernel image", "gpu")):
        can_succeed = profile.get("has_gpu", False)
        if profile.get("has_gpu"):
            suggestion = "显存可能不足：请换小模型（base/small）或降低 compute_type（float16→int8），或关闭其他 GPU 占用"
        else:
            suggestion = "当前设备无 NVIDIA GPU：请改用 CPU（ASR_DEVICE=cpu, int8），或使用 small/base 小模型"
    elif any(k in err for k in ("llm", "api", "401", "403", "connection", "timeout", "base_url")):
        suggestion = "翻译服务不可达：检查 LLM_BASE_URL / LLM_API_KEY / LLM_MODEL 配置，确认 Ollama 已启动或 API Key 有效"
        can_succeed = True  # 配置修复后可成功
    elif any(k in err for k in ("ffmpeg", "extract", "wav")):
        suggestion = "音频提取失败：确认 ffmpeg 正常、文件未损坏（可用 ffprobe 检查）"
    elif any(k in err for k in ("no speech", "empty", "转录结果为空")):
        suggestion = "视频可能无语音或音频异常：换 VAD 参数或检查音轨语言；若为无声源请忽略该文件"
    else:
        mem = profile.get("cpu", {}).get("mem_avail_gb", 0)
        if profile.get("has_gpu"):
            suggestion = "GPU 环境，建议模型 large-v3-turbo（显存>=8G）或 small（显存<8G）"
        elif mem and mem < 4:
            suggestion = "当前设备内存偏小（%.1fGB）：建议 ASR_MODEL=base + int8" % mem
            can_succeed = False if "模型" in error and "下载" in error else True
        else:
            suggestion = "CPU 环境可用 small + int8；若质量不足再尝试 medium"

    return {
        "can_succeed": can_succeed,
        "suggestion": suggestion,
        "device_profile": {
            "cpu_cores": profile.get("cpu", {}).get("cores"),
            "mem_avail_gb": profile.get("cpu", {}).get("mem_avail_gb"),
            "has_gpu": profile.get("has_gpu"),
            "gpu_name": profile.get("gpu", [{}])[0].get("name") if profile.get("gpu") else None,
            "recommended_model": profile.get("recommended_model"),
        },
    }
