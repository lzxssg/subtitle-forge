"""语音转录：faster-whisper（本地）或 llama-swap 托管 whisper.cpp（统一显存调度）。"""
import logging
import os
import subprocess
import threading
import time

import requests

from .. import config
from ..models import Segment
from ..services import llm_manager

logger = logging.getLogger("subtitle-forge.transcribe")

_model_lock = threading.Lock()
_model_cache = {}

# ASR 模型体积估算（MB，fp16 权重 + 运行时开销基准），用于显存预算判断
_ASR_MODEL_MB = {
    "tiny": 200,
    "base": 350,
    "small": 1000,
    "medium": 3200,
    "large-v1": 6400,
    "large-v2": 6400,
    "large-v3": 6400,
    "large": 6400,
    "turbo": 3400,
    "distil": 3400,
}
_COMPUTE_FACTOR = {
    "float16": 1.0,
    "bfloat16": 1.0,
    "float32": 1.9,
    "int8": 0.6,
    "int8_float16": 0.6,
    "int8_float32": 0.6,
}

# 实际生效的加载参数（可能因显存不足被自动降级），供任务元信息展示
_last_effective = {"device": "", "compute_type": "", "note": ""}


def _asr_provider() -> str:
    return str(config.get_setting("asr_provider") or "local").lower()


def _resolve_device():
    device = str(config.get_setting("asr_device") or "auto").lower()
    if device != "auto":
        return device
    try:
        import torch  # noqa
        if torch.cuda.is_available():
            return "cuda"
    except Exception:
        pass
    try:
        import ctranslate2
        devices = ctranslate2.get_cuda_device_count()
        if devices > 0:
            return "cuda"
    except Exception:
        pass
    return "cpu"


def _gpu_mem_mb():
    """返回 (总显存MB, 已用显存MB)，取不到返回 None。"""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total,memory.used", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10)
        if r.returncode != 0 or not r.stdout.strip():
            return None
        total = 0
        used = 0
        for line in r.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 2:
                continue
            try:
                t, u = int(float(parts[0])), int(float(parts[1]))
            except Exception:
                continue
            total = max(total, t)
            used += u
        if total <= 0:
            return None
        return total, used
    except Exception:
        return None


def _est_need_mb(model_name: str, compute_type: str) -> int:
    name = str(model_name or "").lower()
    base = None
    for key in sorted(_ASR_MODEL_MB, key=len, reverse=True):
        if key in name:
            base = _ASR_MODEL_MB[key]
            break
    if base is None:
        base = 3200
    factor = _COMPUTE_FACTOR.get(str(compute_type or "").lower(), 1.0)
    return int(base * factor) + 500  # 留 500MB 运行时余量


def _is_oom(err: Exception) -> bool:
    msg = str(err).lower()
    return ("out of memory" in msg or "cuda failed" in msg or "cublas" in msg
            or "cudnn" in msg or "insufficient memory" in msg or "oom" == msg.strip())


def _build_model(model_name: str, device: str, compute_type: str, cpu_threads: int):
    from faster_whisper import WhisperModel
    if device == "cpu" and compute_type == "float16":
        compute_type = "int8"  # CPU 不支持 float16
    logger.info("加载 ASR 模型 %s (device=%s, compute=%s, cpu_threads=%d)",
                model_name, device, compute_type, cpu_threads)
    local_dir = config.MODELS_DIR / model_name
    if (local_dir / "model.bin").exists():
        logger.info("使用本地模型目录 %s", local_dir)
        return WhisperModel(str(local_dir), device=device, compute_type=compute_type,
                            cpu_threads=cpu_threads)
    return WhisperModel(
        model_name,
        device=device,
        compute_type=compute_type,
        cpu_threads=cpu_threads,
        download_root=str(config.MODELS_DIR),
    )


def _load_with_fallback(model_name: str, device: str, compute_type: str, cpu_threads: int):
    """加载 ASR 模型，显存不足时自动恢复：卸载常驻 LLM → 降精度 → 回退 CPU。

    恢复顺序保证「能跑完优先」：只在 OOM 时逐级降级，不影响显存充足时的原始配置。
    """
    steps = [(device, compute_type)]
    if device == "cuda":
        steps.append((device, "int8_float16"))
        steps.append(("cpu", "int8"))
    elif compute_type != "int8":
        steps.append(("cpu", "int8"))

    last_err = None
    for idx, (dev, ct) in enumerate(steps):
        try:
            model = _build_model(model_name, dev, ct, cpu_threads)
            if idx == 0:
                _last_effective.update({"device": dev, "compute_type": ct, "note": ""})
            else:
                note = f"显存不足，已自动降级为 {dev}/{ct}"
                logger.warning(note)
                _last_effective.update({"device": dev, "compute_type": ct, "note": note})
            return model
        except Exception as e:
            last_err = e
            if not _is_oom(e):
                raise
            gpu = _gpu_mem_mb()
            logger.warning("ASR 加载 OOM（%s/%s，显存 %s）：%s",
                           dev, ct, f"{gpu[1]}/{gpu[0]}MB" if gpu else "未知", str(e)[:160])
            if dev != "cuda":
                continue
            # 首次 OOM：先尝试卸载 llama-swap 常驻 LLM，等显存真正回落再用原参数重试
            if idx == 0:
                try:
                    need = llm_manager.estimate_asr_vram_mb() or 0
                    llm_manager.unload_all()
                    free = llm_manager.wait_vram_free(need, timeout=15) if need else 0
                    logger.warning("已请求 llama-swap 卸载全部 LLM，等待显存释放后重试 ASR 加载"
                                   "（空闲 %dMB / 需求 %dMB）", free, need)
                except Exception as ee:
                    logger.warning("卸载 LLM 失败: %s", ee)
                    time.sleep(3)
                try:
                    model = _build_model(model_name, dev, ct, cpu_threads)
                    _last_effective.update({
                        "device": dev, "compute_type": ct,
                        "note": "显存被常驻 LLM 占用，已自动卸载 LLM 后加载"})
                    logger.info("卸载 LLM 后 ASR 加载成功")
                    return model
                except Exception as e2:
                    last_err = e2
                    if not _is_oom(e2):
                        raise
                    logger.warning("卸载 LLM 后仍 OOM：%s", str(e2)[:160])
    raise RuntimeError(
        f"ASR 模型加载失败（显存不足，已尝试卸载 LLM / 降精度 / 回退 CPU）：{last_err}")


def get_model():
    """懒加载 faster-whisper 模型（线程安全，带缓存，显存不足自动降级）。

    优先加载本地已下载目录（{MODELS_DIR}/{model_name}，含 model.bin），
    否则退回 faster-whisper 自动下载（snapshot 缓存布局）。
    """
    model_name = config.get_setting("asr_model")
    device = _resolve_device()
    compute_type = config.get_setting("asr_compute_type")
    cpu_threads = max(1, int(config.get_setting("asr_cpu_threads") or 8))
    if device == "cpu" and compute_type == "float16":
        compute_type = "int8"  # CPU 不支持 float16
    key = (model_name, device, compute_type)
    with _model_lock:
        if key in _model_cache:
            return _model_cache[key]
        model = _load_with_fallback(model_name, device, compute_type, cpu_threads)
        _model_cache[key] = model
        return model


def release_model():
    """释放 ASR 模型缓存与显存（翻译阶段前调用，避免 ASR 与 LLM 争抢显存）。幂等。

    llama 托管模式下：调用 llama-swap 卸载 whisper-{model}，把显存让给翻译/润色 LLM。
    """
    if _asr_provider() == "llama":
        model_name = config.get_setting("asr_model")
        llm_manager.unload_model(f"whisper-{model_name}")
        return
    with _model_lock:
        if not _model_cache:
            return
        keys = list(_model_cache.keys())
        _model_cache.clear()
        for key in keys:
            model = _model_cache.pop(key, None)
            try:
                del model
            except Exception:
                pass
        # 回收 CUDA 显存
        try:
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass
        try:
            import ctranslate2
            ctranslate2.get_cuda_free_memory()
        except Exception:
            pass
        logger.info("ASR 模型已释放（%s）", keys)


def _run_transcribe(model, wav_path, lang_arg, progress_cb, cancel_check):
    segments_iter, info = model.transcribe(
        wav_path,
        language=lang_arg,
        vad_filter=True,
        vad_parameters={"threshold": 0.15, "min_silence_duration_ms": 500, "speech_pad_ms": 200},
        beam_size=5,
        condition_on_previous_text=False,
    )

    segments = []
    duration = float(getattr(info, "duration", 0) or 0)
    detected_lang = getattr(info, "language", lang_arg) or lang_arg
    last_progress = time.time()
    t0 = time.time()
    last_chars = 0
    char_total = 0   # 增量累计，避免每 0.5s 对全部历史片段求和（O(n^2)）

    for s in segments_iter:
        if cancel_check and cancel_check():
            raise RuntimeError("任务已取消")
        seg = Segment(start=float(s.start), end=float(s.end), text=s.text.strip())
        segments.append(seg)
        char_total += len(seg.text)
        if progress_cb and duration > 0:
            now = time.time()
            if now - last_progress > 0.5:
                # 实时速率：已转录视频秒数 / 墙钟耗时（xRT）
                from ..services import metrics
                metrics.record_transcribe_progress(float(s.end), now - t0)
                metrics.record_tokens(max(0, int((char_total - last_chars) / 1.5)))
                last_chars = char_total
                progress_cb(min(100, float(s.end) / duration * 100))
                last_progress = now
    return segments, duration, detected_lang


def transcribe(wav_path: str, progress_cb=None, cancel_check=None) -> dict:
    """转录 wav → segments 列表 + 元信息。llama 托管模式走 whisper.cpp API。"""
    if _asr_provider() == "llama":
        return _transcribe_via_llamaswap(wav_path, progress_cb, cancel_check)
    model = get_model()
    asr_lang = config.get_setting("asr_language") or "auto"
    lang_arg = None if asr_lang == "auto" else asr_lang

    t0 = time.time()
    try:
        segments, duration, detected_lang = _run_transcribe(
            model, wav_path, lang_arg, progress_cb, cancel_check)
    except Exception as e:
        # 推理阶段 OOM（加载成功但解码爆显存）：释放并按 int8 重新加载重试一次
        if not _is_oom(e):
            raise
        logger.warning("转录推理阶段 OOM：%s，尝试以 int8 重新加载后重试", str(e)[:160])
        release_model()
        model = _load_with_fallback(config.get_setting("asr_model"), _resolve_device(),
                                    "int8_float16", max(1, int(config.get_setting("asr_cpu_threads") or 8)))
        _last_effective["note"] = "推理显存不足，已自动降级为 int8 重跑"
        segments, duration, detected_lang = _run_transcribe(
            model, wav_path, lang_arg, progress_cb, cancel_check)

    elapsed = max(0.1, time.time() - t0)
    total_chars = sum(len(seg.text) for seg in segments)
    est_tokens = max(1, int(total_chars / 1.5))
    return {
        "segments": segments,
        "duration": duration,
        "language": detected_lang,
        "model": config.get_setting("asr_model"),
        "device": _last_effective.get("device") or _resolve_device(),
        "compute_type": _last_effective.get("compute_type") or config.get_setting("asr_compute_type"),
        "device_note": _last_effective.get("note") or "",
        "tokens_per_sec": round(est_tokens / elapsed, 1),
        "elapsed": round(elapsed, 1),
        "chars": total_chars,
    }


def _transcribe_via_llamaswap(wav_path: str, progress_cb=None, cancel_check=None) -> dict:
    """llama 托管模式：调用 llama-swap 的 OpenAI 兼容 /v1/audio/transcriptions 转录。

    whisper.cpp server 由 llama-swap 按需加载（转录前 queue_manager 已卸载全部 LLM 让显存），
    请求完成后由 release_model() 卸载 whisper 让位翻译模型。
    """
    base = llm_manager.llama_swap_base()
    if not base:
        raise RuntimeError("llama 托管模式需要配置 llm_base_url（指向 llama-swap）")
    model_name = config.get_setting("asr_model")
    model_id = f"whisper-{model_name}"
    asr_lang = config.get_setting("asr_language") or "auto"

    data = {"model": model_id, "response_format": "verbose_json"}
    if asr_lang != "auto":
        data["language"] = asr_lang

    result_box = {}

    def _post():
        with open(wav_path, "rb") as f:
            files = {"file": (os.path.basename(wav_path), f, "audio/wav")}
            r = requests.post(
                f"{base}/v1/audio/transcriptions",
                data=data, files=files,
                timeout=(10, 3600),
            )
            if r.status_code != 200:
                raise RuntimeError(f"llama-swap 转录失败 HTTP {r.status_code}: {r.text[:300]}")
            return r.json()

    def _post_worker():
        try:
            result_box["r"] = _post()
        except Exception as e:
            result_box["e"] = e

    t = threading.Thread(target=_post_worker, daemon=True)
    t.start()
    # whisper.cpp 同步返回，无流式进度：请求期间给一个封顶 90% 的假进度，完成跳 100%
    t0 = time.time()
    while t.is_alive():
        if cancel_check and cancel_check():
            raise RuntimeError("任务已取消")
        time.sleep(1)
        if progress_cb:
            progress_cb(min(90.0, (time.time() - t0) * 1.2))
    if "e" in result_box:
        raise result_box["e"]
    j = result_box["r"]

    segs_raw = j.get("segments") or []
    segments = []
    for s in segs_raw:
        text = (s.get("text") or "").strip()
        if not text:
            continue
        segments.append(Segment(start=float(s.get("start", 0)), end=float(s.get("end", 0)), text=text))
    duration = float(j.get("duration") or 0) or (segments[-1].end if segments else 0)
    detected_lang = j.get("language") or asr_lang
    elapsed = max(0.1, time.time() - t0)
    total_chars = sum(len(seg.text) for seg in segments)
    est_tokens = max(1, int(total_chars / 1.5))
    if progress_cb:
        progress_cb(100)
    logger.info("llama 托管转录完成：%d 段，%.1fs（%.1fs 耗时）", len(segments), duration, elapsed)
    return {
        "segments": segments,
        "duration": duration,
        "language": detected_lang,
        "model": model_id,
        "device": "llama-swap (whisper.cpp)",
        "compute_type": "-",
        "device_note": "",
        "tokens_per_sec": round(est_tokens / elapsed, 1),
        "elapsed": round(elapsed, 1),
        "chars": total_chars,
    }
