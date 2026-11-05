"""llama-swap 模型生命周期管理：转录前卸载 LLM 释放显存，翻译/质检按需懒加载。

llama-swap 原生 API（Ollama 兼容扩展，端口与 /v1 相同）：
- POST {base}/api/models/unload/{model}  → 卸载指定模型（返回 OK）
- GET  {base}/api/ps                     → 已加载模型列表
- 无 load API：请求模型时自动加载（懒加载），切换模型自动卸载旧模型。
"""
import logging
import time
from urllib.parse import urlparse

import requests

from .. import config

logger = logging.getLogger("subtitle-forge.llm-manager")

DEFAULT_SWAP_MODELS = config.DEFAULT_SWAP_MODELS
# 转录前全量卸载名单（含润色模型 qwen3_8b，避免与 ASR 抢显存）

_request_timeout = 10


def _swap_models() -> list:
    """返回应纳入显存管理的 llama-swap 模型列表（Web 可配置，逗号分隔）。"""
    raw = config.get_setting("llama_swap_models", DEFAULT_SWAP_MODELS) or DEFAULT_SWAP_MODELS
    models = [m.strip() for m in str(raw).split(",") if m.strip()]
    return models or [m.strip() for m in DEFAULT_SWAP_MODELS.split(",")]


def llama_swap_base() -> str:
    """从 llm_base_url 派生 llama-swap Ollama 根地址（去掉 /v1 后缀）。"""
    base = str(config.get_setting("llm_base_url") or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/v1"):
        base = base[:-3].rstrip("/")
    return base


def loaded_models() -> list:
    """查询 llama-swap 当前已加载模型（GET /api/ps）。失败返回 []。"""
    base = llama_swap_base()
    if not base:
        return []
    try:
        r = requests.get(f"{base}/api/ps", timeout=_request_timeout)
        if r.status_code != 200:
            return []
        data = r.json()
        return [m.get("name") or m.get("model") for m in data.get("models", [])]
    except Exception as e:
        logger.warning("查询 llama-swap 已加载模型失败: %s", e)
        return []


def unload_model(model: str) -> bool:
    """卸载单个模型。支持 llama-swap（/api/models/unload）与原生 ollama（keep_alive=0）。

    返回 True 表示无需/已卸载（调用失败仅告警，不阻断流程）。
    """
    base = llama_swap_base()
    if not base:
        logger.info("未配置 LLM 地址，跳过卸载 %s", model)
        return True
    try:
        r = requests.post(f"{base}/api/models/unload/{model}", timeout=_request_timeout)
        if r.status_code == 200:
            logger.info("llama-swap 模型 %s 已卸载（HTTP 200）", model)
            return True
        if r.status_code == 404:
            # 非 llama-swap 后端（如原生 ollama），改用 keep_alive=0 卸载
            return _unload_via_ollama(base, model)
        logger.warning("卸载 %s 返回 HTTP %s: %s", model, r.status_code, r.text[:200])
        return False
    except Exception as e:
        logger.warning("卸载模型 %s 失败: %s", model, e)
        return False


def _unload_via_ollama(base: str, model: str) -> bool:
    """原生 ollama 卸载：POST /api/generate keep_alive=0（空闲即卸载）。

    404（模型不存在/不认识）视为成功，保证兼容 openai 代理等其他后端不阻断。
    """
    try:
        r = requests.post(
            f"{base}/api/generate",
            json={"model": model, "keep_alive": 0, "prompt": ""},
            timeout=_request_timeout,
        )
        if r.status_code in (200, 404):
            logger.info("ollama 模型 %s keep_alive=0 已生效（HTTP %s）", model, r.status_code)
            return True
        logger.warning("ollama 卸载 %s 返回 HTTP %s: %s", model, r.status_code, r.text[:200])
        return False
    except Exception as e:
        logger.warning("ollama 卸载 %s 失败: %s", model, e)
        return False


def unload_all() -> bool:
    """转录前卸载所有 LLM 模型，释放显存给 ASR。返回是否全部成功（失败不阻断转录）。

    卸载后通过 /api/ps 二次确认；仍有驻留则补卸一次，避免"卸载未生效导致转录 OOM"。
    """
    models = _swap_models()
    logger.info("转录前卸载 LLM 模型: %s", models)
    ok = True
    for m in models:
        if not unload_model(m):
            ok = False
    remain = loaded_models()
    if remain:
        logger.warning("卸载后仍有驻留模型: %s，补卸一次", remain)
        for m in remain:
            unload_model(m)
    return ok


def _resolve_keep_models() -> list:
    """解析校对/润色使用的模型名，返回应保留（不卸载）的 llama-swap server 名。

    保留模型优先 polish_model（润色独立模型），其次 review_model（校对），
    都未配置时回退 llm_model（此时与翻译同模型，不卸载）。
    """
    candidate = str(config.get_setting("polish_model") or "").strip()
    if not candidate:
        candidate = str(config.get_setting("review_model") or "").strip()
    if not candidate:
        candidate = str(config.get_setting("llm_model") or "").strip()
    c = candidate.lower()
    keep = {m for m in _swap_models() if m.lower() in c or c in m.lower()}
    return list(keep)


def unload_others(keep: list | None = None) -> bool:
    """润色前卸载非保留模型（如翻译模型 Sakura），让位给润色/质检模型。

    keep 传 None 时自动解析（review_model / llm_model 对应的 swap 模型）；
    无法识别保留模型时保守跳过卸载，避免误伤正在推理的模型。
    """
    keep = list(keep) if keep is not None else _resolve_keep_models()
    if not keep:
        logger.info("无法识别润色保留模型，跳过卸载（llama-swap 懒加载兜底）")
        return True
    models = [m for m in _swap_models() if m not in keep]
    logger.info("润色前卸载模型（保留 %s）: %s", keep, models)
    ok = True
    for m in models:
        if not unload_model(m):
            ok = False
    return ok


def ensure_asr_vram(required_mb: int = 1024) -> dict:
    """转录前显存守护：查询 llama-swap 已加载模型并尝试卸载，返回卸载明细。"""
    base = llama_swap_base()
    if not base:
        return {"llama_swap": False, "unloaded": [], "reason": "llm_base_url 未配置"}
    loaded = loaded_models()
    unloaded = []
    for m in loaded:
        if unload_model(m):
            unloaded.append(m)
    return {"llama_swap": True, "unloaded": unloaded, "loaded_before": loaded}



# ================= v2.0.0: 智能显存调度 =================
# 单模型模式（显存 < multi_model_vram_mb）：同一时间只允许一种大模型运行，
# 切换阶段时自动卸载其他模型；多模型模式（显存充足）：按空闲显存判断能否共存，
# 放不下时才卸载非目标模型（优先卸 ASR whisper，再按占用从大到小卸 LLM）。

# 模型显存占用估算（MB，Q4 量化近似），仅用于共存预判，非精确值
_VRAM_EST = {
    "whisper": 1200,
    "sakura": 7000,
    "qwen3": 6500,
    "glm": 6500,
    "35b": 16000,
    "llama3": 6000,
}
_RESERVE_MB = 2048  # 保留显存余量，避免加载瞬间 OOM


def gpu_vram_total_mb() -> int:
    """N 卡显存总量（MB），探测失败返回 0。"""
    try:
        from .device_probe import get_gpu_info
        gpus = get_gpu_info() or []
        if not gpus:
            return 0
        return max(int(g.get("mem_total_mb") or 0) for g in gpus)
    except Exception:
        return 0


def gpu_vram_used_mb() -> int:
    """N 卡当前已用显存（MB），探测失败返回 0。"""
    try:
        from .device_probe import get_gpu_info
        gpus = get_gpu_info() or []
        if not gpus:
            return 0
        return max(int(g.get("mem_used_mb") or 0) for g in gpus)
    except Exception:
        return 0


def _estimate_vram(model: str) -> int:
    m = (model or "").lower()
    if m.startswith("whisper"):
        return 1200
    for key, est in _VRAM_EST.items():
        if key in m:
            return est
    return 2048


# ASR 模型显存基准（MB，CTranslate2 float16 近似）：用于转录前预判是否需要腾挪显存
_ASR_VRAM_BASE = {
    "large-v3-turbo": 1600,
    "large-v2": 3100,
    "large-v3": 3200,
    "medium": 1500,
    "small": 800,
    "base": 450,
    "tiny": 250,
}
# 精度系数（相对 float16）
_COMPUTE_FACTOR = {
    "float16": 1.0,
    "int8_float16": 0.62,
    "int8": 0.42,
    "float32": 1.9,
}
# CUDA context、解码 KV/特征缓存等固定开销
_ASR_OVERHEAD_MB = 700


def estimate_asr_vram_mb() -> int:
    """估算当前配置的 ASR 模型加载所需显存（MB，含 CUDA context 余量）。

    无法探测显存时返回 0，调用方应按“无需腾挪”处理，避免误卸载。
    """
    name = str(config.get_setting("asr_model") or "small").lower()
    compute = str(config.get_setting("asr_compute_type") or "float16").lower()
    base = 0
    for key in sorted(_ASR_VRAM_BASE, key=len, reverse=True):
        if key in name:
            base = _ASR_VRAM_BASE[key]
            break
    if not base:
        return 0
    return int(base * _COMPUTE_FACTOR.get(compute, 1.0)) + _ASR_OVERHEAD_MB


def free_vram_mb() -> int:
    """当前可用显存（MB）。探测失败或非 N 卡返回 0。"""
    total = gpu_vram_total_mb()
    if not total:
        return 0
    return max(0, total - gpu_vram_used_mb())


def wait_vram_free(need_mb: int, timeout: int = 20) -> int:
    """等待卸载后显存真正回落（llama-swap 卸载为异步），返回最终空闲显存 MB。

    仅轮询等待，不重复卸载；超时后照常返回，由 ASR 加载侧的 OOM 兜底继续保命。
    """
    if not need_mb:
        return free_vram_mb()
    deadline = time.time() + timeout
    free = free_vram_mb()
    while time.time() < deadline and free < need_mb:
        time.sleep(1.5)
        free = free_vram_mb()
    if free < need_mb:
        logger.warning("等待显存释放超时（%ss）：空闲 %dMB / 需求 %dMB，继续尝试加载 ASR",
                       timeout, free, need_mb)
    return free


def scheduler_mode() -> str:
    """single=严格单模型串行；multi=允许大模型共存。依据 N 卡显存总量智能判断。"""
    total = gpu_vram_total_mb()
    threshold = max(1024, int(config.get_setting("multi_model_vram_mb") or 16000))
    if total and total >= threshold:
        return "multi"
    return "single"


def _stage_target_model(stage: str) -> str:
    """返回该阶段运行的目标模型名（llama-swap server 名）。"""
    if stage == "transcribe":
        return "whisper-%s" % (config.get_setting("asr_model") or "small")
    if stage == "polish":
        # 润色走 polish client（polish_model → review_model → llm_model）
        keep = _resolve_keep_models()
        if keep:
            return keep[0]
    # translate 阶段（及回退）保留翻译模型 llm_model
    return str(config.get_setting("llm_model") or "").strip()


def _prepare_single(stage: str, report: dict) -> dict:
    """低显存：严格单模型串行，切换阶段时卸载其他模型。"""
    if stage == "transcribe":
        unload_all()
        report["actions"].append("卸载全部 LLM，给 ASR 让位（单模型串行）")
    else:
        _unload_whisper()
        keep = _stage_target_model(stage)
        unload_others(keep=[keep] if keep else None)
        report["actions"].append("卸载 ASR 与非保留模型（单模型串行，保留 %s）" % (keep or "全部"))
    return report


def _prepare_multi(stage: str, report: dict) -> dict:
    """超大显存：允许目标模型与已驻留模型共存，放不下才按需卸载。"""
    total = gpu_vram_total_mb()
    used = gpu_vram_used_mb()
    if not total:
        logger.warning("多模型模式但无法探测显存，回退单模型串行")
        return _prepare_single(stage, report)
    target = _stage_target_model(stage)
    need = _estimate_vram(target)
    loaded = loaded_models()
    if used + need + _RESERVE_MB <= total:
        report["actions"].append(
            "显存充足（已用 %dMB + 需求 %dMB <= %dMB），%s 直接加载，不卸载现有模型" % (used, need, total, target or "目标模型"))
        report["kept"] = loaded
        return report
    unloaded = []
    to_try = sorted(loaded, key=_estimate_vram, reverse=True)
    for m in to_try:
        if target and (m.lower() == target.lower() or target.lower() in m.lower()):
            report["kept"].append(m)
            continue
        if unload_model(m):
            unloaded.append(m)
            used = max(0, used - _estimate_vram(m))
        if used + need + _RESERVE_MB <= total:
            break
    report["unloaded"] = unloaded
    report["actions"].append("多模型共存：卸载 %s 以容纳 %s（当前已用 %dMB）" % (unloaded or "无", target or "目标模型", used))
    return report


def prepare_for(stage: str) -> dict:
    """统一调度入口：按显存模式准备目标阶段运行环境。

    stage: transcribe / translate / polish
    - single 模式：低显存严格单模型串行，切换阶段卸载其他模型
    - multi 模式：大显存按空闲显存判断目标模型能否与已驻留模型共存
    """
    mode = scheduler_mode()
    report = {"mode": mode, "stage": stage, "actions": [], "unloaded": [], "kept": []}
    logger.info("调度[%s] 模式=%s", stage, mode)
    if mode == "multi":
        result = _prepare_multi(stage, report)
    else:
        result = _prepare_single(stage, report)
    if stage in ("translate", "polish"):
        result["warmup"] = warmup_stage_model(stage)
    return result


def warmup_stage_model(stage: str, timeout: float = 90.0) -> bool:
    """阶段开始前预热该阶段的目标 LLM，把 llama-swap 冷启动的 502 提前消化。

    仅告警不抛错：预热失败时业务请求仍走自身 6 次重试兜底。
    """
    try:
        from .llm_client import get_client, get_polish_client
    except Exception:
        return False
    try:
        client = get_polish_client() if stage == "polish" else get_client()
        if not client.ready:
            return False
        return client.warmup(timeout=timeout)
    except Exception as e:
        logger.warning("阶段 %s 模型预热异常（忽略）: %s", stage, e)
        return False


def _unload_whisper():
    """卸载 llama 托管模式的 whisper 模型（幂等）。"""
    model_name = str(config.get_setting("asr_model") or "small")
    unload_model("whisper-%s" % model_name)
