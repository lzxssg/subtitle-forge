"""ffmpeg 提取音频：视频 → 16kHz 单声道 wav（whisper 最优输入）。

v1.1.2 稳定性修复（队列假死根因）：
  旧实现用 `for line in proc.stdout` 阻塞读取，同时 stderr=PIPE 从不消费。
  一旦 ffmpeg 写满 stderr 管道（64KB），或因 ffmpeg 异常退出后 stdout 写端被
  其子进程继承未释放，worker 线程就会永久阻塞在管道读取上，队列从此不再消费
  任何任务（表现为「进行中 0 / 等待几百条 / 队列不动」，只能重启容器）。
  现改为：stderr 落临时文件、stdout 交给读线程投递、主循环按 proc.poll() 轮询，
  并附带空闲超时、总耗时超时、取消回调与外部强制终止（ACTIVE_PROCS）能力。
"""
import logging
import queue as _queue
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from .. import config

logger = logging.getLogger("subtitle-forge.extract")

FFMPEG = "ffmpeg"

# 正在运行的 ffmpeg 进程登记表：task_id -> Popen（供看门狗 / 任务修复强制终止）
ACTIVE_PROCS = {}
_ACTIVE_LOCK = threading.Lock()


def _register(task_id, proc):
    if task_id is None:
        return
    with _ACTIVE_LOCK:
        ACTIVE_PROCS[task_id] = proc


def _unregister(task_id, proc):
    if task_id is None:
        return
    with _ACTIVE_LOCK:
        if ACTIVE_PROCS.get(task_id) is proc:
            ACTIVE_PROCS.pop(task_id, None)


def kill_task(task_id) -> bool:
    """强制终止指定任务正在运行的 ffmpeg（找不到进程返回 False）。"""
    with _ACTIVE_LOCK:
        proc = ACTIVE_PROCS.get(task_id)
    if proc is None:
        return False
    try:
        if proc.poll() is None:
            proc.kill()
            logger.warning("已强制终止任务 %s 的 ffmpeg 进程(pid=%s)", task_id, proc.pid)
            return True
    except Exception as e:
        logger.warning("终止任务 %s 的 ffmpeg 失败: %s", task_id, e)
    return False


def kill_all() -> int:
    """强制终止所有正在运行的 ffmpeg，返回终止数量。"""
    with _ACTIVE_LOCK:
        items = list(ACTIVE_PROCS.items())
    n = 0
    for tid, proc in items:
        try:
            if proc.poll() is None:
                proc.kill()
                n += 1
                logger.warning("已强制终止卡住的 ffmpeg(pid=%s, task=%s)", proc.pid, tid)
        except Exception:
            pass
    return n


def kill_tasks(task_ids) -> int:
    """只终止指定任务的 ffmpeg，返回终止数量（用于保留正常在跑的任务）。"""
    n = 0
    for tid in set(task_ids):
        if kill_task(tid):
            n += 1
    return n


def active_task_ids() -> list:
    with _ACTIVE_LOCK:
        return [tid for tid, p in ACTIVE_PROCS.items() if p.poll() is None]


def running_count() -> int:
    with _ACTIVE_LOCK:
        return sum(1 for p in ACTIVE_PROCS.values() if p.poll() is None)


def probe_duration(video_path: str) -> float:
    """用 ffprobe 获取视频时长（秒）。失败返回 0。"""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, text=True, timeout=60)
        return float(r.stdout.strip() or 0)
    except Exception:
        return 0.0


def extract_audio(video_path: str, out_wav: str, progress_cb=None, cancel_cb=None,
                  task_id=None, idle_timeout: float = 300.0,
                  total_timeout: float = None) -> str:
    """提取音频到 16k 单声道 wav，带实时进度回调。返回 wav 路径。

    - idle_timeout：连续多久没有任何进度输出就判定卡死并强杀（默认 300s）
    - total_timeout：整段提取的总耗时上限，缺省按视频时长推算
    - cancel_cb：返回 True 表示任务已被暂停/取消，立即杀掉 ffmpeg 并抛错
    """
    out_wav = str(out_wav)
    Path(out_wav).parent.mkdir(parents=True, exist_ok=True)
    total = probe_duration(video_path)
    if total_timeout is None:
        total_timeout = max(600.0, total * 4 + 300) if total > 0 else 3600.0
    cmd = [
        FFMPEG, "-y", "-v", "error", "-nostats",
        "-threads", "0",
        "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000",
        "-c:a", "pcm_s16le",
        "-progress", "pipe:1",
        out_wav,
    ]
    logger.info("提取音频: %s", video_path)
    # stderr 落临时文件：既避免管道写满导致的死锁，又保留报错文本
    err_file = tempfile.TemporaryFile(mode="w+b")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err_file,
                            text=False, bufsize=0)
    _register(task_id, proc)

    lines = _queue.Queue()

    def _reader():
        try:
            for raw in proc.stdout:
                lines.put(raw.decode("utf-8", "ignore"))
        except Exception:
            pass
        finally:
            lines.put(None)

    threading.Thread(target=_reader, name=f"ffmpeg-reader-{task_id or 0}",
                     daemon=True).start()

    start = time.time()
    last_beat = start
    state = {"eof": False}

    def _handle(line):
        nonlocal last_beat
        line = line.strip()
        if line.startswith("out_time_ms=") or line.startswith("out_time_us="):
            try:
                ms = int(line.split("=", 1)[1])
            except (ValueError, IndexError):
                return
            last_beat = time.time()
            if progress_cb and total > 0:
                now = time.time()
                if now - state.get("cb_ts", 0) >= 0.5:
                    state["cb_ts"] = now
                    try:
                        progress_cb(min(100, ms / 1000 / total * 100))
                    except Exception:
                        pass

    def _drain():
        while True:
            try:
                line = lines.get_nowait()
            except _queue.Empty:
                return
            if line is None:
                state["eof"] = True
                return
            _handle(line)

    err_text = ""
    try:
        while True:
            if proc.poll() is not None:
                _drain()
                break
            try:
                line = lines.get(timeout=1.0)
            except _queue.Empty:
                line = ""
            if line is None:
                # stdout 已 EOF：等进程退出（管道被继承未释放时也不会死等）
                state["eof"] = True
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break
            if line:
                _handle(line)
            if cancel_cb is not None:
                try:
                    cancelled = bool(cancel_cb())
                except Exception:
                    cancelled = False
                if cancelled:
                    proc.kill()
                    raise RuntimeError("任务已暂停/取消，已终止音频提取")
            now = time.time()
            if now - last_beat > idle_timeout:
                proc.kill()
                raise RuntimeError(
                    f"ffmpeg 提取音频超过 {int(idle_timeout)} 秒无任何进度，已强制终止"
                    f"（疑似存储 IO 卡死或视频文件损坏）")
            if now - start > total_timeout:
                proc.kill()
                raise RuntimeError(
                    f"ffmpeg 提取音频总耗时超过 {int(total_timeout)} 秒，已强制终止")
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        _unregister(task_id, proc)
        try:
            err_file.seek(0)
            err_text = err_file.read().decode("utf-8", "ignore")
        except Exception:
            err_text = ""
        finally:
            err_file.close()

    if proc.returncode not in (0, None):
        raise RuntimeError(f"ffmpeg 提取音频失败(code {proc.returncode}): {err_text[-500:]}")
    return out_wav
