"""目录监控：Watchdog(inotify) 实时监听 + APScheduler 定时全量扫描兜底。

- 监听指定目录，新视频文件落盘（写入完成/移动入目录）后自动入队
- 定时扫描：兜底处理容器重启后的旧文件、网络挂载（CIFS）inotify 失效场景
- 自动跳过已在处理/已完成的文件、已存在目标字幕的文件
"""
import fnmatch
import logging
import os
import re
import threading
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from . import config, database as db

logger = logging.getLogger("subtitle-forge.watcher")

# 中文字幕特征（按文件名识别，覆盖 .zh/.zhs/.chs/.cht 等常见写法）
_ZH_SUB_RE = re.compile(r"\.(zh|zhs|chs|cht|zh-hans|zh-hant)\.", re.I)


def is_video_file(path: str, exts: str) -> bool:
    ext = Path(path).suffix.lower()
    return ext in [e.strip().lower() for e in exts.split(",") if e.strip()]


def max_video_minutes() -> int:
    """视频时长上限（分钟）；0 或非法值表示不限制。"""
    try:
        return max(0, int(db.get_setting("max_video_minutes") or 0))
    except Exception:
        return 0


def duration_seconds(video_path: str) -> float:
    """ffprobe 读取视频时长（秒），失败返回 0（延迟导入避免与 pipeline 循环依赖）。"""
    try:
        from .pipeline.extract_audio import probe_duration
        return float(probe_duration(video_path) or 0.0)
    except Exception:
        return 0.0


def duration_exceeded(video_path: str, limit_minutes: int = None):
    """判断视频是否超过时长上限，返回 (是否超限, 时长秒数)。

    v1.1.7：默认上限取设置 max_video_minutes（默认 240 分钟，0 = 不限制）。
    上限为 0 时不探测、直接放行；探测失败（返回 0）同样放行，避免因 ffprobe
    异常把正常视频挡在队列外。
    """
    limit = max_video_minutes() if limit_minutes is None else max(0, int(limit_minutes))
    if limit <= 0:
        return False, 0.0
    dur = duration_seconds(video_path)
    if dur <= 0:
        return False, 0.0
    return (dur > limit * 60), dur



_SUB_EXTS = (".srt", ".ass", ".ssa", ".vtt", ".sub")


def _dir_listing(directory: str, dir_cache: dict = None):
    """列目录（带缓存）。批量扫描同一目录的成百上千视频时，避免逐个文件 glob。"""
    if dir_cache is not None and directory in dir_cache:
        return dir_cache[directory]
    try:
        names = os.listdir(directory)
    except OSError:
        names = []
    if dir_cache is not None:
        dir_cache[directory] = names
    return names


def _find_subtitles(video_path: str, dir_cache: dict = None):
    """同目录下与视频同名的所有字幕文件（含 .srt/.ass/.ssa/.vtt/.sub）。"""
    p = Path(video_path)
    if not p.exists():
        return []
    stem = p.stem
    names = _dir_listing(str(p.parent), dir_cache)
    subs = []
    for name in names:
        if not name.startswith(stem):
            continue
        if name.lower().endswith(_SUB_EXTS):
            subs.append(p.parent / name)
    return sorted(subs)


def classify_video(video_path: str, dir_cache: dict = None):
    """扫描分类：
    - ("skip", 原因)        ：已有中文字幕或非视频 → 跳过
    - ("translate_only", src)：有外文字幕（如日语）→ 只翻译该字幕
    - ("full", "")          ：无任何字幕 → 完整流水线（转录→质检→翻译→质检）
    """
    if not is_video_file(video_path, db.get_setting("video_exts")):
        if Path(video_path).suffix.lower() in OTHER_VIDEO_EXTS:
            return ("skip", "不支持的视频格式")
        return ("skip", "非视频文件")
    name = Path(video_path).name
    if name.startswith(".") or ".part" in name or name.endswith(".download"):
        return ("skip", "下载中/临时文件")
    subs = _find_subtitles(video_path, dir_cache)
    if not subs:
        return ("full", "")
    target_suffix = db.get_setting("subtitle_suffix") or ".zh"
    for s in subs:
        base = s.name.lower()
        # 目标中文字幕（如 *.zh.srt / *.zhs.srt / *.cht.ass 等）
        if _ZH_SUB_RE.search(base) or (target_suffix.lower() + ".") in base.replace(".vtt", "").replace(".ass", "").replace(".ssa", "").replace(".sub", ""):
            return ("skip", f"已存在中文字幕 {s.name}")
    # 有非中文字幕 → 只翻译
    src = subs[0].name
    return ("translate_only", f"已有外文字幕 {src}")


def count_existing_subtitles(dirs=None, cancel_cb=None):
    """v1.1.8：轻量统计媒体库「已有中文字幕」的视频数。

    只做文件名匹配（不查库、不 ffprobe），用于首页「已有字幕」统计的兜底刷新。
    返回 (existing, videos)。
    """
    if dirs is None:
        dirs = [d.strip() for d in str(db.get_setting("watch_dirs")).split(",") if d.strip()]
    ext_set = {e.strip().lower() for e in str(db.get_setting("video_exts")).split(",") if e.strip()}
    dir_cache = {}
    existing = videos = unsupported = 0
    for d in dirs:
        root = Path(d)
        if not root.exists():
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            if cancel_cb is not None and cancel_cb():
                dir_cache.clear()
                LAST_SCAN_SKIP["unsupported"] = unsupported
                return existing, videos
            for fname in filenames:
                _sfx = Path(fname).suffix.lower()
                if _sfx not in ext_set:
                    if _sfx in OTHER_VIDEO_EXTS:
                        LAST_SCAN_SKIP["unsupported"] += 1
                    continue
                videos += 1
                action, reason = classify_video(os.path.join(dirpath, fname), dir_cache)
                if action == "skip" and str(reason).startswith("已存在中文字幕"):
                    existing += 1
    dir_cache.clear()
    LAST_SCAN_SKIP["unsupported"] = unsupported
    return existing, videos


def should_skip(video_path: str, dir_cache: dict = None, active_paths=None) -> bool:
    """判断是否应跳过：非视频 / 已有中文字幕 / 已有进行中任务。

    active_paths 传入时用内存集合判断（全量扫描时一次取全，避免逐文件查库）。
    """
    action, _reason = classify_video(video_path, dir_cache)
    if action == "skip":
        return True
    if active_paths is not None:
        if video_path in active_paths:
            return True
        return False
    active = db.get_task_by_path(video_path, active_only=True)
    if active:
        return True
    return False


class MediaHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        self._maybe_enqueue(event.src_path, delay=True)

    def on_moved(self, event):
        if event.is_directory:
            return
        if hasattr(event, "dest_path") and event.dest_path:
            self._maybe_enqueue(event.dest_path, delay=True)

    def on_modified(self, event):
        # 某些写入方式只触发 modified
        if event.is_directory:
            return
        self._maybe_enqueue(event.src_path, delay=True)

    def _maybe_enqueue(self, path: str, delay: bool = False):
        def _do():
            if not path or not os.path.exists(path):
                return
            # 等待文件写入完成（大小稳定）
            try:
                size1 = os.path.getsize(path)
                time.sleep(5)
                size2 = os.path.getsize(path)
                if size1 != size2:
                    return  # 仍在写入，交给后续 modified/扫描
            except OSError:
                return
            if should_skip(path):
                return
            from .queue_manager import enqueue_path
            enqueue_path(path, source="watch")

        if delay:
            t = threading.Thread(target=_do, daemon=True)
            t.start()
        else:
            _do()


class DirectoryWatcher:
    def __init__(self, on_scan_callback=None):
        self.observer = None
        self.watched_dirs = []
        self.on_scan_callback = on_scan_callback
        self._scheduler = None
        self._last_size = {}

    def _stable(self, path: str) -> bool:
        """文件大小 10 秒内不再变化视为写入完成（用于扫描兜底）。"""
        try:
            size = os.path.getsize(path)
        except OSError:
            return False
        key = path
        prev = self._last_size.get(key)
        self._last_size[key] = size
        if prev is None or abs(prev - size) < 1024:
            return True
        return False

    def _should_watch(self) -> bool:
        """仅 guard（智能监控）模式启用实时目录监听；manual/scheduled 下不自动抓取。"""
        return config.get_setting("mode", "manual") == "guard" and config.get_setting("monitor_enabled", True)

    def start(self):
        if not self._should_watch():
            logger.info("当前模式不启用实时监听（mode=%s）", config.get_setting("mode", "manual"))
            return
        dirs = [d.strip() for d in str(db.get_setting("watch_dirs")).split(",") if d.strip()]
        self._update_watches(dirs)

    def _update_watches(self, dirs):
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)
        self.observer = Observer()
        handler = MediaHandler()
        for d in dirs:
            p = Path(d)
            if not p.exists():
                logger.warning("监控目录不存在: %s", d)
                continue
            try:
                self.observer.schedule(handler, str(p), recursive=True)
                self.watched_dirs.append(str(p))
                logger.info("开始监控目录: %s", p)
            except Exception as e:
                logger.error("监控目录失败 %s: %s", d, e)
        self.observer.daemon = True
        self.observer.start()

    def restart(self):
        # 仅 guard 模式 + 实时监测开关开启时运行
        if not self._should_watch():
            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=5)
            self.watched_dirs = []
            logger.info("当前模式不启用实时监听，停止目录监听")
            return
        dirs = [d.strip() for d in str(db.get_setting("watch_dirs")).split(",") if d.strip()]
        self.watched_dirs = []
        self._update_watches(dirs)

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)


# v1.1.8：扫描跳过明细（进程级，由 main 在每次扫描开始前 reset、结束后落盘）
# v1.1.9：常见但未被 video_exts 白名单覆盖的容器/编码，扫描时计入「不支持」
OTHER_VIDEO_EXTS = {
    ".rmvb", ".rm", ".vob", ".m4v", ".3gp", ".3g2", ".asf", ".mpg", ".mpeg",
    ".f4v", ".ogv", ".mts", ".m2v", ".dv", ".wtv", ".divx", ".xvid", ".avs",
    ".ra", ".ram", ".dat", ".bik", ".amv", ".mtv",
}
LAST_SCAN_SKIP = {"existing": 0, "duration": 0, "active": 0, "unsupported": 0}


def reset_scan_skip():
    for k in LAST_SCAN_SKIP:
        LAST_SCAN_SKIP[k] = 0


def scan_dirs(force=False, cancel_cb=None, on_progress=None, dirs=None, stats=None):
    """全量扫描监控目录，把新视频入队。返回新增数量。

    v1.1.6：支持取消与进度上报——
    - cancel_cb() 返回 True 时立即停止遍历（已入队的任务保留，由调用方决定是否清理）；
    - on_progress(processed, added, current_path) 每处理一个目录 / 每 200 个文件回调一次；
      v1.1.7 起每 200 个文件额外附加 stats 快照供前端展示超长跳过数。
    - dirs 显式传入时只扫这些目录（默认取 watch_dirs 设置）。
    - stats 传入字典时回填统计：skipped_duration（超长跳过数）、limit_minutes（生效上限）。
    """
    from .queue_manager import enqueue_path
    # 手动模式禁止自动扫描（用户手动触发时 force=True 除外）
    if not force and config.get_setting("mode", "manual") == "manual":
        logger.info("手动模式不自动扫描，跳过 scan_dirs")
        return 0
    if dirs is None:
        dirs = [d.strip() for d in str(db.get_setting("watch_dirs")).split(",") if d.strip()]
    ext_set = {e.strip().lower() for e in str(db.get_setting("video_exts")).split(",") if e.strip()}
    active_paths = db.active_task_paths()   # 一次查全，扫描时内存判断
    dir_cache = {}                          # 目录列表缓存，避免同目录重复 listdir
    limit_minutes = max_video_minutes()     # v1.1.7：视频时长上限（0=不限制）
    skipped_duration = 0
    added = 0
    processed = 0
    cancelled = False

    def _stats():
        if stats is not None:
            stats["skipped_duration"] = skipped_duration
            stats["limit_minutes"] = limit_minutes

    _stats()

    def _cancelled():
        if cancel_cb is None:
            return False
        try:
            return bool(cancel_cb())
        except Exception:
            return False

    for d in dirs:
        root = Path(d)
        if not root.exists():
            continue
        # v1.1.6：扫描过程中用户移除了监控目录（取消勾选/删除），
        # 尚未开始遍历的目录立即跳过，不再入队 —— 旧实现在扫描启动时就固定了目录快照
        try:
            live = {x.strip().rstrip("/") for x in
                    str(db.get_setting("watch_dirs")).split(",") if x.strip()}
            if live and str(d).strip().rstrip("/") not in live:
                logger.info("扫描跳过已移除的监控目录: %s", d)
                continue
        except Exception:
            pass
        for dirpath, _dirnames, filenames in os.walk(root):
            if _cancelled():
                cancelled = True
                break
            for fname in filenames:
                processed += 1
                if processed % 200 == 0:
                    if _cancelled():
                        cancelled = True
                        break
                    if on_progress:
                        on_progress(processed, added, os.path.join(dirpath, fname))
                _sfx = Path(fname).suffix.lower()
                if _sfx not in ext_set:
                    if _sfx in OTHER_VIDEO_EXTS:
                        LAST_SCAN_SKIP["unsupported"] += 1
                    continue
                full = os.path.join(dirpath, fname)
                # v1.1.8：区分跳过原因 —— 已有中文字幕单独计数用于「已有字幕」统计
                action, reason = classify_video(full, dir_cache)
                if action == "skip":
                    if str(reason).startswith("已存在中文字幕"):
                        LAST_SCAN_SKIP["existing"] += 1
                    _stats()
                    continue
                if active_paths is not None:
                    if full in active_paths:
                        LAST_SCAN_SKIP["active"] += 1
                        continue
                elif db.get_task_by_path(full, active_only=True):
                    LAST_SCAN_SKIP["active"] += 1
                    continue
                # v1.1.7：超过时长上限的视频不进入队列
                over, dur = duration_exceeded(full, limit_minutes)
                if over:
                    skipped_duration += 1
                    LAST_SCAN_SKIP["duration"] += 1
                    logger.info("跳过超长视频（%.0f 分钟 > %s 分钟上限）: %s",
                                dur / 60.0, limit_minutes, full)
                    _stats()
                    continue
                if enqueue_path(full, source="scan",
                                duration=(dur if limit_minutes > 0 else None),
                                limit_minutes=limit_minutes):
                    added += 1
                _stats()
            if on_progress:
                on_progress(processed, added, dirpath)
            if cancelled:
                break
        if cancelled:
            break
    dir_cache.clear()
    if cancelled:
        logger.info("扫描已被取消：已检查 %s 个文件，入队 %s 个", processed, added)
        return added
    # 失败任务自动重试（每轮扫描最多重试一轮，限制次数防死循环）
    if db.get_setting("auto_retry_failed"):
        from .queue_manager import smart_queue
        for t in db.list_failed_tasks():
            retry_count = db.task_meta(t["id"]).get("retry_count", 0)
            if retry_count >= 3:
                continue
            db.update_task_meta(t["id"], retry_count=retry_count + 1)
            db.update_task(t["id"], status="pending", progress=0, error="")
            smart_queue.submit(t["id"])
            added += 1
    return added
