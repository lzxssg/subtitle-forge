"""SubtitleForge 主入口：FastAPI + 静态界面 + SSE 实时推送。"""
import asyncio
import json
import re
import logging
import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config, database as db
from . import models_download
from .queue_manager import smart_queue, enqueue_path
from .scan_control import SCAN
from .services import device_probe, metrics
from .services.llm_client import get_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("subtitle-forge.main")

# 同时输出到挂载的 logs 目录，供「运行日志」页读取
from pathlib import Path as _P
_log_dir = _P(__file__).resolve().parent.parent / "logs"
try:
    _log_dir.mkdir(parents=True, exist_ok=True)
    _fh = logging.FileHandler(_log_dir / "subtitle-forge.log", encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logger.addHandler(_fh)
except Exception:
    pass

app = FastAPI(title="SubtitleForge", docs_url=None, redoc_url=None)

# SSE 客户端订阅
_sse_clients = set()
_sse_lock = threading.Lock()
_stop_events = []


# ---------- SSE ----------
def _broadcast(event: str, data: dict):
    payload = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    dead = []
    with _sse_lock:
        for q in list(_sse_clients):
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        for q in dead:
            _sse_clients.discard(q)


def _resource_loop():
    """资源快照广播：有前端订阅时每 2 秒采样；无订阅时降频到每 10 秒，避免空转 nvidia-smi。"""
    last = 0.0
    while not _stop_events[0].is_set():
        try:
            with _sse_lock:
                clients = len(_sse_clients)
            now = time.time()
            if clients or now - last >= 10:
                s = metrics.snapshot()
                s["token_rate"] = metrics.token_rate()
                _broadcast("resource", s)
                last = now
        except Exception as e:
            logger.error("resource loop: %s", e)
        time.sleep(2 if clients else 5)


def _scan_interval_minutes() -> int:
    """把 扫描间隔(数值+单位) 换算为分钟。支持 minutes / hours / days。"""
    try:
        val = int(db.get_setting("scan_interval_value") or db.get_setting("scan_interval_minutes") or 60)
    except (TypeError, ValueError):
        val = 60
    unit = str(db.get_setting("scan_interval_unit") or "minutes").lower()
    if unit == "days":
        return max(1, val * 1440)
    if unit == "hours":
        return max(1, val * 60)
    return max(1, val)


@app.on_event("startup")
async def startup():
    db.init_db()
    # 兼容处理：镜像端不可用的 ASR 模型自动回退为 large-v3
    cur_asr = db.get_setting("asr_model") or ""
    if cur_asr and cur_asr not in config.ASR_MODEL_SIZES_MB:
        logger.warning("ASR 模型 %s 镜像端不可用，自动回退为 large-v3", cur_asr)
        db.set_setting("asr_model", "large-v3")
    # 更新 LLM 客户端配置
    get_client().reload()

    # 设备画像
    profile = device_probe.get_device_profile()
    logger.info("设备画像: %s", profile)

    # 队列
    smart_queue.add_listener(lambda e, p: _broadcast(e, p))
    smart_queue.start(int(db.get_setting("task_workers") or db.get_setting("max_workers") or 1))
    # 恢复上次未完成任务：中间状态（容器中断残留）重置为 pending 后重新入队
    with db.db() as conn:
        rows = conn.execute(
            "SELECT id, status FROM tasks WHERE status IN "
            "('pending','polishing','extracting','transcribing','checking','translating','checking_translation','transcribed')"
        ).fetchall()
    phase_mode = db.get_setting("phase_mode", "pipeline") or "pipeline"
    sync_mode = smart_queue.mode() == "sync"
    for r in rows:
        st = r["status"]
        # v1.1.11 sync：阶段中间态原样保留，交回批次闸门按阶段继续
        if sync_mode and st in ("transcribed", "translated", "reviewed"):
            smart_queue.submit(r["id"])
            continue
        if st != "pending":
            if st == "transcribed":
                # staged 翻译阶段任务：保留状态，等 checkpoint 统一启动翻译
                if phase_mode != "staged":
                    smart_queue.submit(r["id"])
                continue
            db.update_task(r["id"], status="pending", progress=0, error="")
        smart_queue.submit(r["id"])
    if rows:
        logger.info("已恢复未完成任务 %d 条", len(rows))

    # 目录监控（内部按 mode 决定是否启用实时监听）
    from .watcher import DirectoryWatcher, scan_dirs
    watcher = DirectoryWatcher()
    watcher.start()
    app.state.watcher = watcher

    # 定时扫描（APScheduler）：仅 scheduled / guard 模式自动定时扫描
    from apscheduler.schedulers.background import BackgroundScheduler
    sched = BackgroundScheduler(timezone="Asia/Shanghai")
    mode = db.get_setting("mode", "manual")
    if mode in ("scheduled", "guard"):
        interval = _scan_interval_minutes()
        sched.add_job(scan_dirs, "interval", minutes=interval, id="scan_job", args=[False])
        sched.start()
        logger.info("定时扫描已启用: 每 %d 分钟", interval)
    else:
        logger.info("当前模式 %s，不启动定时扫描", mode)

    # v1.1.8：「已有字幕」统计兜底刷新（启动 30s 后，若统计缺失或超过 12h 未更新则后台重算）
    try:
        if _need_existing_refresh():
            threading.Timer(30.0, lambda: threading.Thread(
                target=_refresh_existing_stats, daemon=True).start()).start()
    except Exception:
        logger.warning("启动已有字幕统计刷新失败（忽略）", exc_info=True)

    # 资源采样线程
    ev = threading.Event()
    _stop_events.append(ev)
    threading.Thread(target=_resource_loop, daemon=True).start()

    app.state.watcher = watcher
    app.state.scheduler = sched
    logger.info("SubtitleForge 已启动")


@app.on_event("shutdown")
async def shutdown():
    try:
        smart_queue.stop()
    except Exception as e:
        logger.warning("停止队列异常（忽略）: %s", e)
    if hasattr(app.state, "watcher"):
        try:
            app.state.watcher.stop()
        except Exception as e:
            logger.warning("停止目录监听异常（忽略）: %s", e)
    if hasattr(app.state, "scheduler"):
        try:
            app.state.scheduler.shutdown(wait=False)
        except Exception as e:
            logger.warning("停止调度器异常（忽略）: %s", e)
    if _stop_events:
        _stop_events[0].set()


# ---------- 静态界面 ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(config.STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(config.STATIC_DIR)), name="static")


@app.middleware("http")
async def _no_cache_static(request: Request, call_next):
    """静态页面禁用缓存，避免浏览器缓存旧版导致设置页空白"""
    response = await call_next(request)
    if request.url.path in ("/", "/static/index.html") or request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


# ---------- API ----------
class AddTaskBody(BaseModel):
    path: str
    recursive: bool = False


class ConfigBody(BaseModel):
    values: dict


class LLMProbeBody(BaseModel):
    base_url: str = ""
    api_key: str = ""


class LLMTestBody(BaseModel):
    base_url: str = ""
    api_key: str = ""
    model: str = ""


def _normalize_base_url(url: str) -> str:
    """补 /v1：http://host:11434 -> http://host:11434/v1"""
    url = (url or "").strip().rstrip("/")
    if url and "/v1" not in url:
        url = url + "/v1"
    return url


def _llm_headers(api_key: str) -> dict:
    headers = {"Content-Type": "application/json"}
    if api_key and api_key.strip().lower() not in ("", "ollama", "none"):
        headers["Authorization"] = f"Bearer {api_key.strip()}"
    return headers


@app.post("/api/llm/probe")
async def api_llm_probe(body: LLMProbeBody):
    """根据 API 地址拉取可用模型列表（OpenAI 兼容 /v1/models）。"""
    import requests
    base_url = _normalize_base_url(body.base_url)
    if not base_url:
        return {"ok": False, "error": "API 地址不能为空"}
    try:
        resp = requests.get(f"{base_url}/models", headers=_llm_headers(body.api_key), timeout=15)
        if resp.status_code != 200:
            return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        data = resp.json()
        models = [m.get("id", m.get("name", "")) for m in (data.get("data") or data.get("models") or [])]
        models = [m for m in models if m]
        return {"ok": True, "models": models}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


@app.post("/api/llm/test")
async def api_llm_test(body: LLMTestBody):
    """发一个最小 chat 请求，验证 API 连通性与模型可用性。"""
    import requests
    base_url = _normalize_base_url(body.base_url)
    if not base_url:
        return {"ok": False, "error": "API 地址不能为空"}
    if not body.model:
        return {"ok": False, "error": "模型不能为空"}
    payload = {
        "model": body.model,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
        "temperature": 0,
    }
    t0 = time.time()
    try:
        resp = requests.post(
            f"{base_url}/chat/completions",
            headers=_llm_headers(body.api_key),
            json=payload,
            timeout=60,
        )
        elapsed = round(time.time() - t0, 2)
        if resp.status_code != 200:
            return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}", "elapsed": elapsed}
        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        return {"ok": True, "model": body.model, "elapsed": elapsed, "reply": (content or "")[:100]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200], "elapsed": round(time.time() - t0, 2)}


_size_cache = {}   # video_path -> (ts, size)，5 秒内复用，避免列表刷新反复 stat 网络盘
_SIZE_TTL = 5.0


def _file_size(path: str) -> int:
    now = time.time()
    hit = _size_cache.get(path)
    if hit and now - hit[0] < _SIZE_TTL:
        return hit[1]
    try:
        size = Path(path).stat().st_size
        if len(_size_cache) > 5000:
            _size_cache.clear()
        _size_cache[path] = (now, size)
        return size
    except Exception:
        return 0


def _task_row(task, subtitles=None):
    row = dict(task)
    # tasks 行内已含 meta JSON，直接解析，省掉每任务一次 SELECT
    try:
        row["meta"] = json.loads(task["meta"] or "{}")
    except Exception:
        row["meta"] = {}
    if subtitles is None:
        row["subtitles"] = [dict(s) for s in db.get_subtitles(task["id"])]
    else:
        row["subtitles"] = subtitles
    row["file_size"] = _file_size(task["video_path"])
    return row


@app.get("/api/version")
async def api_version():
    """返回后端版本号（v1.1.4 起提供），前端据此显示，避免硬编码漂移。"""
    return {"ok": True, "version": getattr(config, "APP_VERSION", "unknown")}


@app.get("/api/status")
async def api_status():
    profile = device_probe.get_device_profile()
    profile["recommend_concurrency"] = device_probe.recommend_concurrency(profile)
    profile["recommend_polish"] = device_probe.recommend_polish_concurrency(profile)
    cfg = db.get_all_settings()
    return {
        "device": profile,
        "running_tasks": len(smart_queue._running_tasks),
        "llm_ready": get_client().ready,
        "monitor_enabled": bool(cfg.get("monitor_enabled", True)),
        "mode": cfg.get("mode", "manual"),
        "queue_paused": bool(cfg.get("queue_paused", False)),
        "batch": smart_queue.batch_state(),
        "config": cfg,
    }


_existing_refresh_lock = threading.Lock()


def _need_existing_refresh(max_age=12 * 3600):
    try:
        last = float(db.get_setting("scan_last_at") or 0)
    except Exception:
        last = 0.0
    return (time.time() - last) > max_age


def _refresh_existing_stats():
    """v1.1.8：后台轻量重算「已有字幕」数（只做文件名匹配，不查库、不 ffprobe）。"""
    if not _existing_refresh_lock.acquire(blocking=False):
        return {"ok": False, "error": "统计任务正在运行"}
    try:
        from .watcher import count_existing_subtitles
        if SCAN.running():
            return {"ok": False, "error": "扫描进行中，稍后再试"}
        t0 = time.time()
        existing, videos = count_existing_subtitles()
        db.set_setting("scan_existing_count", int(existing))
        db.set_setting("scan_video_count", int(videos))
        from .watcher import LAST_SCAN_SKIP
        db.set_setting("scan_unsupported_count", int(LAST_SCAN_SKIP.get("unsupported") or 0))
        db.set_setting("scan_last_at", time.time())
        logger.info("已有字幕统计刷新完成：%s/%s 个视频已有字幕，用时 %.1fs",
                    existing, videos, time.time() - t0)
        return {"ok": True, "existing": int(existing), "videos": int(videos),
                "elapsed": round(time.time() - t0, 1)}
    except Exception as e:
        logger.warning("已有字幕统计刷新失败: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        _existing_refresh_lock.release()


@app.get("/api/stats")
async def api_stats():
    return db.task_stats()


@app.post("/api/stats/existing/refresh")
async def api_stats_existing_refresh():
    """手动触发「已有字幕」统计重算（后台执行，可重复调用）。"""
    if SCAN.running():
        return {"ok": False, "error": "扫描进行中，稍后再试"}
    threading.Thread(target=_refresh_existing_stats, daemon=True).start()
    return {"ok": True, "started": True}


@app.get("/api/dirs")
async def api_dirs():
    return {"dirs": db.watch_dir_list()}


@app.get("/api/browse")
async def api_browse(path: str = Query("/")):
    """目录浏览：返回 path 下所有子目录列表（目录选择器后端）。

    - path 缺省为 "/"；不以 "/" 开头时自动补 "/"
    - 权限不足/不存在时返回空 dirs，不报错
    """
    p = path if path.startswith("/") else "/" + path
    if not p:
        p = "/"
    parent = None
    if p != "/":
        parent = os.path.dirname(p.rstrip("/"))
        if not parent:
            parent = None
    dirs = []
    denied = False
    if not os.path.exists(p):
        return {"path": p, "parent": parent, "dirs": [], "error": "notfound"}
    if not (os.access(p, os.R_OK) and os.access(p, os.X_OK)):
        return {"path": p, "parent": parent, "dirs": [], "error": "permission"}
    try:
        entries = os.listdir(p)
        dirs = sorted(e for e in entries if os.path.isdir(os.path.join(p, e)))
    except Exception:
        denied = True
    return {"path": p, "parent": parent, "dirs": dirs, "error": ("permission" if denied else None)}


@app.get("/api/roots")
async def api_roots():
    """容器映射根目录：由 WATCH_DIRS 环境变量（逗号分隔）解析，过滤存在的目录。"""
    raw = os.environ.get("WATCH_DIRS", "/Media")
    roots = []
    for p in raw.split(","):
        p = p.strip()
        if p and os.path.isdir(p):
            roots.append(p)
    return {"roots": roots}


class DirBody(BaseModel):
    path: str


@app.post("/api/dirs")
async def api_add_dir(body: DirBody):
    p = Path(body.path).expanduser()
    if not p.is_dir():
        return {"ok": False, "error": f"目录不存在: {p}"}
    dirs = db.add_watch_dir(str(p))
    if hasattr(app.state, "watcher"):
        app.state.watcher.restart()
    return {"ok": True, "dirs": dirs}


@app.delete("/api/dirs")
async def api_del_dir(body: DirBody):
    dirs = db.remove_watch_dir(body.path)
    if hasattr(app.state, "watcher"):
        app.state.watcher.restart()
    return {"ok": True, "dirs": dirs}


@app.get("/api/allow-dirs")
async def api_allow_dirs():
    return {"dirs": db.allow_dir_list()}


@app.post("/api/allow-dirs")
async def api_add_allow_dir(body: DirBody):
    p = Path(body.path).expanduser()
    if not p.is_dir():
        return {"ok": False, "error": f"目录不存在: {p}"}
    dirs = db.add_allow_dir(str(p))
    return {"ok": True, "dirs": dirs}


@app.delete("/api/allow-dirs")
async def api_del_allow_dir(body: DirBody):
    dirs = db.remove_allow_dir(body.path)
    return {"ok": True, "dirs": dirs}


class MonitorBody(BaseModel):
    enabled: bool


@app.post("/api/monitor")
async def api_monitor(body: MonitorBody):
    db.set_setting("monitor_enabled", body.enabled)
    if hasattr(app.state, "watcher"):
        app.state.watcher.restart()
    return {"ok": True, "monitor_enabled": body.enabled}


class ModeBody(BaseModel):
    mode: str  # manual / scheduled / guard


class ModelDownloadBody(BaseModel):
    model: str  # faster-whisper 模型名（如 small / large-v3）
    provider: str = "llama"  # llama=llama-swap 托管 / ollama=ollama 托管


class ModelImportBody(BaseModel):
    src_path: str       # 容器内模型目录绝对路径（需含 model.bin）
    model_name: str = ""  # 可选：导入后的模型名，默认取目录名


@app.get("/api/models/list")
async def api_models_list():
    cfg = db.get_all_settings()
    return {
        "models": models_download.get_models_list(),
        "hf_endpoint": config.HF_ENDPOINT,
        "asr_provider": cfg.get("asr_provider", "local"),
        "llama_managed": [m["name"] for m in models_download.get_models_list() if models_download.is_llama_deployed(m.get("name", ""))],
        "llama_swap_ready": bool(models_download._llama_base()),
    }


@app.post("/api/models/download")
async def api_models_download(body: ModelDownloadBody):
    ok, msg = models_download.start_download(body.model)
    return {"ok": ok, "message": msg, "downloads": models_download.download_status()}


@app.get("/api/models/download/status")
async def api_models_download_status():
    return {"downloads": models_download.download_status()}


# v1.8.0: llama-swap 一键托管 ASR（whisper.cpp）
@app.post("/api/asr/deploy")
async def api_asr_deploy(body: ModelDownloadBody):
    """一键部署转录模型到 llama-swap 托管。"""
    ok, msg = models_download.deploy_to_llama(body.model)
    return {"ok": ok, "message": msg, "deploys": models_download.deploy_status()}


@app.get("/api/asr/deploy/status")
async def api_asr_deploy_status():
    return {"deploys": models_download.deploy_status()}


@app.post("/api/models/import")
async def api_models_import(body: ModelImportBody):
    ok, msg, name = models_download.import_local_model(body.src_path, body.model_name)
    if ok:
        _broadcast("model_update", {"imported": name})
    return {"ok": ok, "message": msg, "name": name}


@app.post("/api/mode")
async def api_set_mode(body: ModeBody):
    """切换运行模式：
    - manual    手动模式：不自动扫描、不实时监听，仅手动下达任务
    - scheduled 定时模式：按 scan_interval 定时扫描，不实时监听
    - guard     智能监控：实时监听 + 定时扫描兜底，无人值守自动处理
    """
    if body.mode not in ("manual", "scheduled", "guard"):
        return {"ok": False, "error": "无效模式，可选 manual/scheduled/guard"}
    db.set_setting("mode", body.mode)

    # 重启 watcher（内部仅在 guard 模式 + monitor_enabled 时真正监听）
    if hasattr(app.state, "watcher"):
        app.state.watcher.restart()

    # 管理定时扫描 job
    from .watcher import scan_dirs
    sched = getattr(app.state, "scheduler", None)
    if sched is not None:
        interval = int(db.get_setting("scan_interval_minutes") or 60)
        try:
            if body.mode in ("scheduled", "guard"):
                try:
                    sched.add_job(scan_dirs, "interval", minutes=interval,
                                  id="scan_job", args=[False])
                except Exception:
                    try:
                        sched.reschedule_job("scan_job", trigger="interval", minutes=interval)
                    except Exception:
                        pass
                if not sched.running:
                    sched.start()
                logger.info("定时扫描已启用: 每 %d 分钟（mode=%s）", interval, body.mode)
            else:
                try:
                    sched.remove_job("scan_job")
                except Exception:
                    pass
                logger.info("定时扫描已停用（mode=%s）", body.mode)
        except Exception as e:
            logger.error("切换模式调度失败: %s", e)

    _broadcast("task_update", {"mode": body.mode})
    return {"ok": True, "mode": body.mode}


class QueuePauseBody(BaseModel):
    paused: bool
    # v1.1.7：恢复队列默认接入任务修复（卸载闲置模型、解死锁、重建线程、预热并续跑）
    force_unload: bool = True


@app.post("/api/queue/pause")
async def api_queue_pause(body: QueuePauseBody):
    """暂停/恢复任务队列。

    暂停：不再领取新任务，并把所有进行中的任务挂起（已完成的环节产物保留，恢复后续跑）。
    恢复（v1.1.7 默认接入任务修复）：走一次 repair —— 强制卸载闲置 LLM 模型释放显存
          → 强杀卡住的提取进程 → 解除批次死锁 → 中间态任务归位 → 重建工作线程
          → 预热工作模型 → 立即续跑。传 force_unload=false 可退化为「仅放行」。
    """
    if body.paused:
        n = smart_queue.pause_all()
    else:
        n = smart_queue.resume_all(force_unload=body.force_unload)
    _broadcast("task_update", {"queue_paused": body.paused, "affected": n})
    resp = {"ok": True, "paused": body.paused, "affected": n,
            "repaired": bool(not body.paused and body.force_unload)}
    if not body.paused and body.force_unload:
        resp["repair"] = smart_queue.repair_state()
    return resp


class RepairBody(BaseModel):
    force_unload: bool = True
    restart_workers: bool = True
    reason: str = "任务修复"


@app.post("/api/queue/repair")
async def api_queue_repair(body: RepairBody = None):
    """任务修复：一键恢复队列并排查显存/死锁问题。

    动作：强制卸载 llama-swap 中当前驻留、但没有任务在用的模型（正在跑翻译/润色的不动）
    → 强杀卡住的音频提取进程 → 解除批次闸门死锁 → 中间态任务归位 → 重建工作线程
    → 重新入队并放行 → 按阶段预热工作模型。
    """
    body = body or RepairBody()
    report = smart_queue.repair(force_unload=body.force_unload,
                                restart_workers=body.restart_workers,
                                reason=body.reason or "任务修复")
    _broadcast("batch_update", smart_queue.batch_state())
    return {"ok": True, "report": report}


@app.get("/api/queue/repair")
async def api_queue_repair_state():
    """只读健康快照：worker 心跳/卡死、队列积压、批次、提取进程，附最近一次修复结果。"""
    return {"ok": True, "health": smart_queue.health(),
            "report": smart_queue.repair_state()}


@app.post("/api/queue/watchdog")
async def api_queue_watchdog_tick():
    """手动触发一次看门狗巡检（自动修复仍受 watchdog_auto_heal 开关与冷却时间约束）。"""
    result = smart_queue._watchdog_tick()
    return {"ok": True, "result": result, "health": smart_queue.health()}


@app.post("/api/tasks/cancel-all")
async def api_cancel_all(purge_scan: bool = True):
    """取消所有任务（清空排队 + 标记活动任务取消）。

    v1.1.6：默认先取消正在运行的扫描任务——否则扫描线程还会继续往队列塞新任务，
    表现为「点了取消，任务又冒出来」。
    """
    scan_cancelled = SCAN.cancel() if purge_scan else 0
    n = smart_queue.cancel_all()
    _broadcast("task_update", {"bulk": "cancelled"})
    _broadcast("scan_update", SCAN.status())
    return {"ok": True, "cancelled": n, "scan_cancelled": scan_cancelled}


@app.post("/api/tasks/clear-all")
async def api_clear_all(purge_scan: bool = True):
    """清空全部任务与字幕记录（不影响已生成字幕文件）。

    v1.1.6：先中止扫描、再中断在跑任务，最后清记录。顺序很重要：
    直接清记录时扫描仍在跑，会立刻重新入队，用户看到的就是「清空没用」。
    """
    scan_cancelled = 0
    if purge_scan:
        scan_cancelled = SCAN.cancel()
        if scan_cancelled:
            # 给扫描线程一点时间退出遍历，避免清完又被塞回来
            await asyncio.sleep(0.6)
    smart_queue.cancel_all()
    n = smart_queue.clear_all()
    _broadcast("task_update", {"bulk": "cleared"})
    _broadcast("scan_update", SCAN.status())
    return {"ok": True, "deleted": n, "scan_cancelled": scan_cancelled}


_ASR_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/\-]{0,63}")


def _safe_asr_name(name):
    """校验 ASR 模型名：仅允许字母数字与 . _ - /，禁止 .. 与路径穿越。

    合法示例：large-v3、medium、mobiuslabsgmbh/faster-whisper-large-v3-turbo。
    """
    name = str(name or "").strip()
    if not name or ".." in name:
        return ""
    return name if _ASR_NAME_RE.fullmatch(name) else ""


def _resolve_enhanced_retry(enhanced, model=""):
    """解析「增强转录」参数：是否启用、用哪个 ASR 模型、精度。

    - enhanced=False 或设置里关闭了增强转录 → (False, "", "")
    - model 为空时取设置 enhanced_asr_model；都为空则退回全局 asr_model
    """
    if not enhanced:
        return False, "", ""
    if not bool(config.get_setting("enhanced_transcribe_enabled", True)):
        return False, "", ""
    name = _safe_asr_name(model)
    if not name:
        name = _safe_asr_name(config.get_setting("enhanced_asr_model"))
    if not name:
        name = _safe_asr_name(config.get_setting("asr_model"))
    compute = str(config.get_setting("enhanced_asr_compute_type") or "").strip()
    return True, name, compute


def _mark_enhanced_retry(task_id, model, compute=""):
    """给任务打上一次性增强转录标记：强制重转 + 指定模型覆盖。"""
    db.update_task_meta(
        task_id,
        asr_model_override=model or "",
        asr_compute_override=compute or "",
        enhanced_retry=bool(model),
        force_retranscribe=bool(model),
        enhanced_requested_at=int(time.time()),
    )


@app.post("/api/tasks/retry-failed")
async def api_retry_failed(enhanced: bool = False, model: str = ""):
    """批量重试所有失败任务。

    enhanced=true 时启用「增强转录」：忽略已有转录结果，用更强的 ASR 模型
    （默认设置项 enhanced_asr_model，如 large-v3）重新转录视频。
    """
    use_enh, enh_model, enh_compute = _resolve_enhanced_retry(enhanced, model)
    failed = db.list_failed_tasks()
    n = 0
    for t in failed:
        db.update_task(t["id"], status="pending", progress=0, error="")
        if use_enh:
            _mark_enhanced_retry(t["id"], enh_model, enh_compute)
        smart_queue.submit(t["id"])
        n += 1
    if n:
        _broadcast("tasks_retried", {"count": n, "enhanced": use_enh, "model": enh_model})
    return {"ok": True, "retried": n, "enhanced": use_enh, "model": enh_model}


@app.get("/api/logs")
async def api_logs(lines: int = 300):
    """读取应用运行日志（末 N 行）。"""
    import os
    log_candidates = [
        os.environ.get("LOG_FILE", ""),
        os.path.join(config.DATA_DIR, "logs", "app.log"),
        os.path.join("/usr/local/apps/@appdata/subtitleforge", "logs", "app.log"),
        os.path.join(str(Path(__file__).resolve().parent.parent), "logs", "subtitle-forge.log"),
    ]
    for p in log_candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8", errors="replace") as f:
                    content = f.readlines()
                return {"ok": True, "path": p, "lines": content[-int(lines):]}
            except Exception as e:
                return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "日志文件不存在"}


@app.post("/api/cache/clear")
async def api_clear_cache():
    """清理临时音频缓存（task_*.wav）。

    v1.1.4: 保留正在运行任务的音频，避免手动清理打断转写；返回实际释放字节数。
    """
    audio_dir = Path(config.DATA_DIR) / "audio"
    removed = 0
    freed = 0
    skipped = 0
    running = set(getattr(smart_queue, "_running_tasks", set()))
    if audio_dir.exists():
        for f in audio_dir.glob("task_*.wav"):
            try:
                task_id = int(f.stem[len("task_"):])
            except ValueError:
                task_id = None
            if task_id in running:
                skipped += 1
                continue
            try:
                size = f.stat().st_size
                f.unlink()
                removed += 1
                freed += size
            except Exception:
                pass
    return {"ok": True, "removed": removed, "freed": freed, "skipped_running": skipped}


@app.get("/api/cache/status")
async def api_cache_status():
    """音频缓存现状与自动清理配置（v1.1.4）。"""
    audio_dir = Path(config.DATA_DIR) / "audio"
    files = 0
    total = 0
    if audio_dir.exists():
        for f in audio_dir.glob("task_*.wav"):
            try:
                total += f.stat().st_size
                files += 1
            except Exception:
                pass
    try:
        enabled, on_done, hours = smart_queue._audio_cache_cfg()
    except Exception:
        enabled, on_done, hours = True, True, 24
    return {"ok": True, "files": files, "bytes": total,
            "cleanup_enabled": enabled, "cleanup_on_done": on_done,
            "retention_hours": hours,
            "last_sweep": getattr(smart_queue, "_last_cache_sweep", {})}


@app.post("/api/subtitles/needs-review/delete")
async def api_delete_needs_review_subtitles():
    """一键删除所有质检未通过（needs_review）的字幕文件与记录。"""
    subs = db.list_needs_review_subtitles()
    deleted_files = 0
    deleted_records = 0
    for s in subs:
        try:
            p = Path(s["srt_path"])
            if p.exists() and p.is_file():
                p.unlink()
                deleted_files += 1
        except Exception as e:
            logger.warning("删除字幕文件失败 %s: %s", s["srt_path"], e)
        db.delete_subtitle(s["id"])
        deleted_records += 1
    if deleted_records:
        _broadcast("task_update", {"bulk": "needs_review_deleted"})
    return {"ok": True, "deleted_files": deleted_files, "deleted_records": deleted_records}


@app.post("/api/tasks/{task_id}/subtitles/delete")
async def api_delete_task_subtitles(task_id: int):
    """删除单个任务产出的字幕文件与记录（任务本身保留，可用于重新生成）。"""
    t = db.get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    busy = ("pending", "extracting", "transcribing", "checking", "translating",
            "checking_translation", "polishing")
    if t["status"] in busy:
        return {"ok": False, "error": "任务正在执行，请先取消后再删除字幕"}
    subs = (db.subtitles_map([task_id]) or {}).get(task_id, [])
    paths = [s["srt_path"] for s in subs]
    for s in subs:
        db.delete_subtitle(s["id"])
    # 兜底：本工具在视频同目录生成的中间/润色字幕
    video = Path(t["video_path"])
    paths += [str(video.with_suffix(".transcript.srt")), str(video.with_suffix(".zh.rs.srt"))]
    deleted_files = 0
    seen = set()
    for p in paths:
        if not p or p in seen:
            continue
        seen.add(p)
        try:
            fp = Path(p)
            if fp.is_file():
                fp.unlink()
                deleted_files += 1
        except Exception as e:
            logger.warning("删除字幕文件失败 %s: %s", p, e)
    # 清除字幕相关 meta，避免增强重试时复用旧转录结果或残留质检提示
    db.update_task_meta(task_id, transcript_score=None, transcript_lang=None,
                        enhanced_note=None, enhanced_flow=None,
                        polish_model=None, polish_tokens_per_sec=None)
    _broadcast("task_update", {"id": task_id, "subtitles_deleted": deleted_files})
    return {"ok": True, "deleted_files": deleted_files, "deleted_records": len(subs)}


@app.get("/api/tasks")
async def api_tasks(limit: int = 20, offset: int = 0, q: str = "", view: str = "all", batch: int = 1):
    """任务列表：支持视图过滤、关键词搜索与服务端分页。

    - view：all/cur/done/failed/review/transcribe/check/translate/polish/history
    - q：片名/路径关键词，非空时忽略视图，在全部任务中搜索
    - batch=1：当前队列与各环节队列仅返回本批次任务
    """
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))
    view = (view or "all").lower()
    batch_ids = None
    if batch and not (q or "").strip() and view in db.BATCH_VIEWS:
        try:
            batch_ids = smart_queue.batch_state().get("members") or None
        except Exception:
            batch_ids = None
    rows, total = db.query_tasks(view=view, q=q, limit=limit, offset=offset, batch_ids=batch_ids)
    subs = db.subtitles_map([t["id"] for t in rows])
    return {"tasks": [_task_row(t, subs.get(t["id"], [])) for t in rows],
            "total": total, "limit": limit, "offset": offset}


@app.post("/api/tasks")
async def api_add_task(body: AddTaskBody):
    p = Path(body.path).expanduser()
    if not p.exists():
        return {"ok": False, "error": f"路径不存在: {p}"}
    if p.is_dir():
        # 扫描目录下视频（可选递归）
        added = 0
        skipped_duration = 0
        stats = {}
        ext_set = {e.strip().lower() for e in str(db.get_setting("video_exts")).split(",") if e.strip()}
        files = p.rglob("*") if body.recursive else p.glob("*")
        for f in files:
            if f.is_file() and f.suffix.lower() in ext_set:
                tid = enqueue_path(str(f), source="manual", stats=stats)
                if tid:
                    added += 1
        skipped_duration = int((stats or {}).get("skipped_duration") or 0)
        return {"ok": True, "added": added, "skipped_duration": skipped_duration,
                "limit_minutes": (stats or {}).get("limit_minutes")}
    # v1.1.7：单文件入队前先判时长，命中上限直接说明原因，不再静默返回空 task_id
    from .watcher import duration_exceeded, max_video_minutes
    over, dur = duration_exceeded(str(p))
    if over:
        return {"ok": False,
                "error": f"视频时长 {dur / 60:.0f} 分钟，超过上限 "
                         f"{max_video_minutes()} 分钟（可在设置中调整「最长视频时长」）"}
    tid = enqueue_path(str(p), source="manual", duration=dur)
    return {"ok": True, "task_id": tid}


# ---------- 扫描任务（后台线程 + 可取消） ----------
_scan_bcast_lock = threading.Lock()
_scan_bcast_ts = {}


def _scan_broadcast(job, min_interval: float = 0.8):
    """进度广播节流：扫描按文件数回调很密，0.8s 内只推一次，避免刷爆 SSE。"""
    now = time.time()
    with _scan_bcast_lock:
        if now - _scan_bcast_ts.get(job.id, 0) < min_interval:
            return
        _scan_bcast_ts[job.id] = now
    _broadcast("scan_update", SCAN.status())


def _launch_scan(job, runner):
    """在后台线程执行扫描任务，结束后写入终态并广播给前端。

    v1.1.8：扫描结束把「已有字幕」跳过数落盘，供首页「已有字幕」统计展示。
    """
    def _target():
        try:
            from .watcher import reset_scan_skip
            reset_scan_skip()
        except Exception:
            pass
        try:
            runner(job)
            SCAN.finish(job, "cancelled" if job.cancel_requested else "done")
        except Exception as e:
            logger.exception("扫描任务 %s(%s) 执行失败", job.label, job.id)
            SCAN.finish(job, "failed", str(e))
        finally:
            try:
                from .watcher import LAST_SCAN_SKIP
                db.set_setting("scan_existing_count", int(LAST_SCAN_SKIP.get("existing") or 0))
                db.set_setting("scan_unsupported_count", int(LAST_SCAN_SKIP.get("unsupported") or 0))
                db.set_setting("scan_last_at", time.time())
                logger.info("扫描跳过明细：已有字幕 %s / 超长 %s / 进行中 %s / 不支持格式 %s",
                            LAST_SCAN_SKIP.get("existing"), LAST_SCAN_SKIP.get("duration"),
                            LAST_SCAN_SKIP.get("active"), LAST_SCAN_SKIP.get("unsupported"))
            except Exception:
                logger.warning("写入扫描跳过统计失败（忽略）", exc_info=True)
            _broadcast("scan_update", SCAN.status())

    threading.Thread(target=_target, name=f"scan-{job.id}", daemon=True).start()
    _broadcast("scan_update", SCAN.status())


@app.post("/api/scan")
async def api_scan():
    """扫描全部监控目录（后台执行，可取消）。"""
    from .watcher import scan_dirs
    dirs = db.watch_dir_list()
    if not dirs:
        return {"ok": False, "error": "监控目录未配置"}
    job = SCAN.start("scan_all", dirs)
    if job is None:
        return {"ok": False, "error": "已有扫描任务在运行，请先取消或等它跑完",
                "running": SCAN.running()}

    def _runner(j):
        stats = {}
        try:
            from .watcher import max_video_minutes
            j.limit_minutes = max_video_minutes()
        except Exception:
            pass

        def _p(p, a, cur):
            SCAN.progress(j, processed=p, enqueued=a, current=cur,
                          skipped=stats.get("skipped_duration"))
            _scan_broadcast(j)

        added = scan_dirs(True, cancel_cb=lambda: j.cancel_requested,
                          on_progress=_p, dirs=dirs, stats=stats)
        j.enqueued = added
        j.skipped = int(stats.get("skipped_duration") or 0)
        j.processed = max(j.processed, 0)

    _launch_scan(job, _runner)
    return {"ok": True, "started": True, "job_id": job.id, "kind": job.kind,
            "label": job.label, "dirs": job.dirs}


def _run_multi_dir_scan(job, fn_name, label):
    """多监控目录逐个扫描：单个目录被取消即整体停止。"""
    from . import queue_manager
    fn = getattr(queue_manager, fn_name)
    for d in job.dirs:
        if job.cancel_requested:
            return
        if not os.path.isdir(d):
            continue
        # 扫描期间被移除的监控目录直接跳过（对应「取消文件夹勾选」的预期）
        try:
            live = {x.strip().rstrip("/") for x in
                    str(db.get_setting("watch_dirs")).split(",") if x.strip()}
            if live and d.strip().rstrip("/") not in live:
                logger.info("扫描跳过已移除的监控目录: %s", d)
                continue
        except Exception:
            pass
        base_p, base_e, base_s, base_t = (job.processed, job.enqueued,
                                          job.skipped, job.total)

        def _cb(processed, enqueued, current, _bp=base_p, _be=base_e,
                _bs=base_s, _bt=base_t):
            SCAN.progress(job, processed=_bp + (processed or 0),
                          enqueued=_be + (enqueued or 0), current=str(current or ""))
            _scan_broadcast(job)

        e, s, t = fn(d, cancel_cb=lambda: job.cancel_requested, on_progress=_cb)
        job.enqueued = base_e + e
        job.skipped = base_s + s
        job.total = base_t + t


def _start_scan_job(kind, label, fn_name):
    dirs = db.watch_dir_list()
    if not dirs:
        return {"ok": False, "error": "监控目录未配置"}
    job = SCAN.start(kind, dirs)
    if job is None:
        return {"ok": False, "error": "已有扫描任务在运行，请先取消或等它跑完",
                "running": SCAN.running()}
    # v1.1.7：把本次生效的「最长视频时长」上限带到扫描状态里，前端可展示超长排除
    try:
        from .watcher import max_video_minutes
        job.limit_minutes = max_video_minutes()
    except Exception:
        pass
    _launch_scan(job, lambda j: _run_multi_dir_scan(j, fn_name, label))
    return {"ok": True, "started": True, "job_id": job.id, "kind": job.kind,
            "label": job.label, "dirs": job.dirs}


@app.post("/api/polish/scan")
async def api_polish_scan():
    """一键扫描润色（后台执行，可取消）：扫描监控目录下所有中文字幕，批量入队润色。"""
    return _start_scan_job("polish", "一键润色", "scan_polish_jobs")


@app.post("/api/scan/transcribe")
async def api_scan_transcribe():
    """一键扫描只转录（后台执行，可取消）：尚无转录字幕的视频批量入队，只转录不翻译。"""
    return _start_scan_job("transcribe", "一键转录", "scan_transcribe_jobs")


@app.post("/api/scan/translate")
async def api_scan_translate():
    """一键扫描转录字幕只翻译（后台执行，可取消）：已有外文字幕的视频批量入队只翻译。"""
    return _start_scan_job("translate", "一键翻译", "scan_translate_jobs")


class ScanCancelBody(BaseModel):
    job_id: str = ""
    purge: bool = False        # 同时清理本次扫描入队、尚未开始的任务
    purge_all: bool = False    # 清理所有未开始的任务（不区分来源）
    delete: bool = False       # 清理时直接删除记录（默认仅标记 cancelled）


@app.get("/api/scan/status")
async def api_scan_status():
    """扫描任务状态：运行中的进度 + 最近一次结束结果。"""
    return SCAN.status()


@app.post("/api/scan/cancel")
async def api_scan_cancel(body: ScanCancelBody = None):
    """取消正在运行的扫描任务；可选连带清理扫描已入队但尚未开始的任务。"""
    body = body or ScanCancelBody()
    n = SCAN.cancel(body.job_id or None)
    purged = 0
    if body.purge or body.purge_all:
        sources = None
        if not body.purge_all:
            sources = {"scan", "scan_transcribe", "scan_translate", "scan_polish"}
        purged = smart_queue.clear_pending(sources=sources, delete=body.delete)
    _broadcast("scan_update", SCAN.status())
    _broadcast("task_update", {"bulk": "scan_cancelled"})
    return {"ok": True, "cancelled": n, "purged": purged,
            "running": SCAN.running()}


@app.post("/api/tasks/{task_id}/pause")
async def api_pause(task_id: int):
    t = db.get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    if t["status"] not in ("pending", "transcribed", "translated", "reviewed"):
        return {"ok": False, "error": f"状态 {t['status']} 不可暂停（仅排队中或待下一阶段的任务可暂停）"}
    db.update_task_meta(task_id, paused_from=t["status"])
    db.update_task(task_id, status="paused", error="")
    _broadcast("task_update", {"id": task_id, "status": "paused", "progress": t["progress"]})
    return {"ok": True}


@app.post("/api/tasks/{task_id}/resume")
async def api_resume(task_id: int):
    t = db.get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    if t["status"] != "paused":
        return {"ok": False, "error": f"状态 {t['status']} 不可恢复"}
    # v1.1.11：恢复时回到暂停前的阶段状态（避免 sync 中间态任务被重置成 pending 重跑转录）
    try:
        meta = db.task_meta(task_id)
    except Exception:
        meta = {}
    back = (meta.get("paused_from") or "pending")
    if back not in ("pending", "transcribed", "translated", "reviewed"):
        back = "pending"
    db.update_task(task_id, status=back, progress=t["progress"] or 0, error="")
    db.update_task_meta(task_id, paused_from="")
    smart_queue.submit(task_id)
    _broadcast("task_update", {"id": task_id, "status": back, "progress": t["progress"]})
    return {"ok": True}


@app.post("/api/tasks/{task_id}/cancel")
async def api_cancel(task_id: int):
    t = db.get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    if t["status"] in ("done", "failed", "cancelled"):
        return {"ok": True, "status": t["status"]}
    db.update_task(task_id, status="cancelled", error="用户取消")
    _broadcast("task_update", {"id": task_id, "status": "cancelled", "progress": t["progress"]})
    return {"ok": True}


@app.post("/api/tasks/{task_id}/retry")
async def api_retry(task_id: int, enhanced: bool = False, model: str = ""):
    """重试单个任务。enhanced=true 时用增强转录模型重新转录（忽略已有转录结果）。"""
    t = db.get_task(task_id)
    if not t:
        return {"ok": False, "error": "任务不存在"}
    running = ("pending", "extracting", "transcribing", "checking", "translating",
               "checking_translation", "polishing")
    if t["status"] in running:
        return {"ok": False, "error": f"任务状态 {t['status']} 不可重试"}
    # 已完成任务默认不重试，但允许「增强重试」：用更强模型重新转录并覆盖现有字幕
    if t["status"] == "done" and not enhanced:
        return {"ok": False, "error": "任务已完成，如需重新生成请使用增强重试"}
    use_enh, enh_model, enh_compute = _resolve_enhanced_retry(enhanced, model)
    db.update_task(task_id, status="pending", progress=0, error="")
    if use_enh:
        _mark_enhanced_retry(task_id, enh_model, enh_compute)
    smart_queue.submit(task_id)
    _broadcast("task_update", {"id": task_id, "status": "pending", "progress": 0})
    return {"ok": True, "enhanced": use_enh, "model": enh_model}


@app.delete("/api/tasks/clear-failed")
async def api_clear_failed():
    """一键删除所有失败/已取消任务记录（保留成功记录）。"""
    n = db.clear_failed_tasks()
    _broadcast("task_update", {"status": "cleared_failed", "deleted": n})
    return {"ok": True, "deleted": n}


@app.delete("/api/tasks/{task_id}")
async def api_delete(task_id: int):
    db.delete_task(task_id)
    _broadcast("task_update", {"id": task_id, "status": "deleted"})
    return {"ok": True}


@app.get("/api/config")
async def api_get_config():
    return db.get_all_settings()


@app.get("/api/scheduler")
async def api_scheduler():
    """调度状态：当前模式、显存画像、自动判断结果与各环节推荐并发。"""
    from .services import llm_manager
    profile = device_probe.get_device_profile()
    try:
        vram = llm_manager.gpu_vram_total_mb()
    except Exception:
        vram = 0
    threshold = int(db.get_setting("multi_model_vram_mb") or 16000)
    auto = "parallel" if (vram and vram >= threshold) else "staged"
    try:
        rec = device_probe.recommend_stage_concurrency()
    except Exception:
        rec = {}
    return {
        "mode": smart_queue.mode(),
        "configured": db.get_setting("scheduler_mode") or "auto",
        "auto": auto,
        "vram_total_mb": vram,
        "vram_threshold_mb": threshold,
        "profile": profile,
        "recommend": rec,
        "workers": smart_queue.worker_count(),
        "queue_paused": bool(db.get_setting("queue_paused", False)),
    }


@app.get("/api/gpu/profile")
async def api_gpu_profile():
    """显卡/CPU 画像与推荐配置（前端设置页展示）。"""
    return device_probe.get_device_profile()


@app.post("/api/config")
async def api_set_config(body: ConfigBody):
    for k, v in body.values.items():
        if k not in config.EDITABLE_KEYS:
            continue
        if k == "batch_size":
            # 同批次任务数量限制：1-100
            try:
                v = max(1, min(100, int(float(v))))
            except (TypeError, ValueError):
                v = 20
        elif k == "max_video_minutes":
            # v1.1.7：最长视频时长（分钟），0 = 不限制，上限 2880（48h）
            try:
                v = max(0, min(2880, int(float(v))))
            except (TypeError, ValueError):
                v = 240
        elif k == "enhanced_transcribe_enabled":
            v = str(v).strip().lower() in ("1", "true", "yes", "on")
        elif k == "enhanced_asr_model":
            # 增强转录模型名：白名单字符 + 禁止 ..；非法值忽略（保留原设置）
            v = _safe_asr_name(v)
            if not v:
                continue
        elif k == "enhanced_asr_compute_type":
            v = str(v).strip()
            if v not in ("", "float16", "int8", "int8_float16", "int8_float32", "float32"):
                v = ""
        db.set_setting(k, v)
    get_client().reload()
    # 若监控目录变更则重启 watcher
    if hasattr(app.state, "watcher"):
        app.state.watcher.restart()
    if hasattr(app.state, "scheduler"):
        interval = _scan_interval_minutes()
        try:
            app.state.scheduler.reschedule_job("scan_job", trigger="interval", minutes=interval)
        except Exception:
            pass
    # 调度模式 / 并发变更：按模式重算工作线程与阶段并发门
    workers = smart_queue.worker_count()
    if len(smart_queue._workers) != workers:
        smart_queue.stop()
        time.sleep(0.5)
        smart_queue.start(workers)
    else:
        smart_queue.apply_limits()
    return {"ok": True, "config": db.get_all_settings()}


@app.get("/api/resources")
async def api_resources():
    s = dict(metrics.get_latest())
    # history 与 latest 指向同一批 dict，必须逐项浅拷贝后挂载，否则自引用导致序列化递归
    s["history"] = [dict(x) for x in metrics.get_history(300)]
    s["token_rate"] = metrics.token_rate()
    return s


@app.get("/api/events")
async def api_events(request: Request):
    """SSE 事件流。"""
    q = asyncio.Queue(maxsize=200)
    with _sse_lock:
        _sse_clients.add(q)

    async def gen():
        try:
            # 立即推一次当前状态
            yield "event: resource\ndata: " + json.dumps(
                {**metrics.get_latest(), "token_rate": metrics.token_rate()}, ensure_ascii=False) + "\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            with _sse_lock:
                _sse_clients.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------- 播放媒体 webhook 联动（Jellyfin / Emby / Plex） ----------
def _webhook_path_map_apply(path: str) -> str:
    """应用路径映射：/media=/mnt/media1,/movies=/mnt/media2 形式，源前缀 → 目标前缀。"""
    raw = db.get_setting("webhook_path_map", "") or ""
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        src, dst = pair.split("=", 1)
        src = src.strip().rstrip("/")
        dst = dst.strip()
        if not src or not dst:
            continue
        if path == src or path.startswith(src + "/"):
            return dst + path[len(src):]
    return path


def _webhook_parse(body: dict) -> dict:
    """从 Jellyfin/Emby/Plex webhook payload 提取入库事件。返回 {event, media_type, path} 或 None。"""
    # Jellyfin / Emby（官方 Webhook 插件 JSON）
    event = body.get("Event") or body.get("event") or body.get("NotificationType") or ""
    item = body.get("Item") or body.get("item") or {}
    path = item.get("Path") or item.get("path") or ""
    media_type = item.get("Type") or item.get("type") or ""
    if path:
        return {"event": event, "media_type": media_type, "path": path}
    # Plex（form 编码，payload 为 JSON 字符串，路由层已解包）
    meta = body.get("Metadata") or {}
    if not event:
        event = body.get("event") or ""
    if meta:
        media_type = meta.get("librarySectionType") or ""
        for m in meta.get("Media") or []:
            for part in m.get("Part") or []:
                if part.get("file"):
                    return {"event": event, "media_type": media_type, "path": part["file"]}
    return None


def _webhook_should_handle(parsed: dict) -> str:
    """返回 '' 表示可处理，否则返回被过滤原因。"""
    enabled = db.get_setting("webhook_enabled", False)
    if not enabled or enabled == "false":
        return "webhook 未启用"
    events = [e.strip().lower() for e in str(db.get_setting("webhook_events", "")).split(",") if e.strip()]
    types = [t.strip().lower() for t in str(db.get_setting("webhook_media_types", "")).split(",") if t.strip()]
    ev = (parsed.get("event") or "").lower()
    tp = (parsed.get("media_type") or "").lower()
    if events and ev and ev not in events:
        return f"事件 {parsed.get('event')} 不在白名单"
    if types and tp and tp not in types:
        return f"媒体类型 {parsed.get('media_type')} 不在白名单"
    return ""


@app.post("/api/webhook/jellyfin")
async def api_webhook_jellyfin(request: Request):
    """Jellyfin / Emby / Plex 入库通知 → 字幕入队。"""
    try:
        if request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
            form = await request.form()
            raw = form.get("payload", "")
            body = json.loads(raw) if raw else {}
        else:
            body = await request.json()
    except Exception:
        return {"ok": False, "error": "payload 解析失败"}
    # 可选共享密钥校验
    secret = db.get_setting("webhook_secret", "") or ""
    if secret:
        got = request.headers.get("X-Webhook-Secret", "") or request.query_params.get("token", "")
        if got != secret:
            return {"ok": False, "error": "密钥校验失败"}
    parsed = _webhook_parse(body)
    if not parsed:
        return {"ok": True, "handled": False, "reason": "未识别到媒体路径"}
    reason = _webhook_should_handle(parsed)
    if reason:
        logger.info("webhook 忽略: %s (%s)", reason, parsed.get("path"))
        return {"ok": True, "handled": False, "reason": reason}
    path = _webhook_path_map_apply(parsed["path"])
    if not os.path.exists(path):
        return {"ok": True, "handled": False, "reason": f"路径不存在: {path}"}
    # 视频文件才入队；目录（剧集季/合集）递归入队
    ext_set = {e.strip().lower() for e in str(db.get_setting("video_exts", "")).split(",") if e.strip()}
    p = Path(path)
    tid = None
    if p.is_file() and p.suffix.lower() in ext_set:
        tid = enqueue_path(str(p), source="webhook")
    elif p.is_dir():
        added = 0
        for f in p.rglob("*"):
            if f.is_file() and f.suffix.lower() in ext_set:
                if enqueue_path(str(f), source="webhook"):
                    added += 1
        tid = f"dir+{added}"
    else:
        return {"ok": True, "handled": False, "reason": f"非视频文件: {path}"}
    logger.info("webhook 入队: %s → task=%s", path, tid)
    return {"ok": True, "handled": True, "task_id": tid, "path": path}


@app.post("/api/webhook/test")
async def api_webhook_test(body: dict):
    """手动测试：模拟一条入库通知。body: {path, event?, media_type?}"""
    path = (body.get("path") or "").strip()
    if not path:
        return {"ok": False, "error": "请填写测试路径"}
    parsed = {"event": body.get("event") or "ItemAdded", "media_type": body.get("media_type") or "Movie", "path": path}
    reason = _webhook_should_handle(parsed)
    if reason:
        return {"ok": True, "handled": False, "reason": reason}
    mapped = _webhook_path_map_apply(path)
    if not os.path.exists(mapped):
        return {"ok": True, "handled": False, "reason": f"路径不存在: {mapped}"}
    ext_set = {e.strip().lower() for e in str(db.get_setting("video_exts", "")).split(",") if e.strip()}
    p = Path(mapped)
    if p.is_file() and p.suffix.lower() in ext_set:
        tid = enqueue_path(str(p), source="webhook-test")
        return {"ok": True, "handled": True, "task_id": tid, "path": str(p)}
    if p.is_dir():
        added = 0
        for f in p.rglob("*"):
            if f.is_file() and f.suffix.lower() in ext_set:
                if enqueue_path(str(f), source="webhook-test"):
                    added += 1
        return {"ok": True, "handled": True, "added": added, "path": str(p)}
    return {"ok": True, "handled": False, "reason": "非视频文件"}


if __name__ == "__main__":
    import uvicorn
    port = int(config.DEFAULTS.get("WEB_PORT", 8310))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, log_level="info")


@app.get("/api/scheduler/status")
async def api_scheduler_status():
    """返回当前显存调度模式与模型驻留状态（v2.0.0 智能调度）。"""
    from .services import llm_manager
    gpus = device_probe.get_gpu_info() or []
    loaded = llm_manager.loaded_models()
    return {
        "mode": llm_manager.scheduler_mode(),
        "gpu_count": len(gpus),
        "gpu": gpus[0] if gpus else None,
        "vram_total_mb": llm_manager.gpu_vram_total_mb(),
        "vram_used_mb": llm_manager.gpu_vram_used_mb(),
        "loaded_models": loaded,
        "multi_model_vram_mb": config.get_setting("multi_model_vram_mb"),
        "desc": "single=低显存严格单模型串行（切换阶段自动卸载） / multi=大显存多模型共存（按空闲显存智能判断）",
    }
