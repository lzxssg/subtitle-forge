"""资源监控采样：CPU / 内存 / GPU / 磁盘，以及最近任务 Token 速率与转录实时速率。

CPU / 内存读取宿主机全局视图（/proc），GPU 由 nvidia-smi 全卡采样（已是全局）。
"""
import logging
import os
import subprocess
import threading
import time

logger = logging.getLogger("subtitle-forge.metrics")

_lock = threading.Lock()
_snapshots = []          # [{ts, cpu, mem, gpu_util, gpu_mem, disk}]
_token_window = []       # [{ts, tokens}] 用于计算 token/s
_transcribe_rate = 0.0   # 最近一次转录实时倍率（每秒转录的视频秒数 xRT）


def _host_cpu_percent():
    """宿主机全局 CPU 利用率（读 /proc/stat，避免容器 cgroup 视图）。"""
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        if not line.startswith("cpu"):
            return 0.0
        fields = [int(x) for x in line.split()[1:8]]
        total = sum(fields)
        idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
        now = (total, idle, time.time())
        key = getattr(_host_cpu_percent, "_last", None)
        _host_cpu_percent._last = now
        if not key or now[2] - key[2] < 0.5:
            return getattr(_host_cpu_percent, "_value", 0.0)
        dt_total = now[0] - key[0]
        dt_idle = now[1] - key[1]
        pct = 0.0
        if dt_total > 0:
            pct = max(0.0, min(100.0, (1 - dt_idle / dt_total) * 100))
        _host_cpu_percent._value = pct
        return pct
    except Exception:
        return 0.0


def _host_mem():
    """宿主机全局内存（读 /proc/meminfo）。"""
    try:
        out = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    out["total_kb"] = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    out["avail_kb"] = int(line.split()[1])
                if "total_kb" in out and "avail_kb" in out:
                    break
        if "total_kb" not in out:
            return None
        total = out["total_kb"] * 1024
        used = max(0, total - out.get("avail_kb", 0) * 1024)
        return {"used": used, "total": total, "pct": round(used / total * 100, 1) if total else 0}
    except Exception:
        return None


def psutil_disk_usage(path="/"):
    """磁盘占用（os.statvfs，容器视角即数据卷挂载所在文件系统）。"""
    st = os.statvfs(path)
    total = st.f_blocks * st.f_frsize
    free = st.f_bavail * st.f_frsize
    used = total - free
    return type("DU", (), {
        "used": used,
        "free": free,
        "percent": round(used / total * 100, 1) if total else 0,
    })()


def _gpu_util():
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total,power.draw",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5)
        if r.returncode != 0:
            return None
        out = []
        for line in r.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                power_w = None
                if len(parts) >= 4 and parts[3] and parts[3] != "N/A":
                    try:
                        power_w = float(parts[3])
                    except ValueError:
                        power_w = None
                out.append({"util": int(parts[0]), "mem_used_mb": int(parts[1]), "mem_total_mb": int(parts[2]), "power_w": power_w})
        return out or None
    except Exception:
        return None


def snapshot():
    """采集一次资源快照（CPU/内存为宿主机全局，GPU 为全卡）。"""
    cpu = _host_cpu_percent()
    mem = _host_mem() or {"used": 0, "total": 1, "pct": 0}
    disk = None
    try:
        disk = psutil_disk_usage()
    except Exception:
        pass
    gpus = _gpu_util()
    s = {
        "ts": time.time(),
        "cpu": cpu,
        "mem_used_gb": round(mem["used"] / 1024 ** 3, 2),
        "mem_total_gb": round(mem["total"] / 1024 ** 3, 2),
        "mem_pct": mem["pct"],
        "gpu": gpus,
        "disk_used_gb": round(disk.used / 1024 ** 3, 1) if disk else 0,
        "disk_free_gb": round(disk.free / 1024 ** 3, 1) if disk else 0,
        "disk_pct": disk.percent if disk else 0,
        "transcribe_rate": transcribe_rate(),
    }
    with _lock:
        _snapshots.append(s)
        if len(_snapshots) > 600:  # 保留 10 分钟（1s 间隔）
            _snapshots.pop(0)
    return s


def get_latest():
    # 返回副本：调用方（如 /api/resources）会往快照里追加 history 等字段，
    # 若返回内部对象引用，history 中会包含自身，JSON 序列化时无限递归。
    if _snapshots:
        return dict(_snapshots[-1])
    return dict(snapshot())


def get_history(seconds=300):
    cutoff = time.time() - seconds
    with _lock:
        return [s for s in _snapshots if s["ts"] >= cutoff]


def record_tokens(count: int):
    global _token_window
    with _lock:
        _token_window.append({"ts": time.time(), "tokens": count})
        cutoff = time.time() - 60
        _token_window = [t for t in _token_window if t["ts"] >= cutoff]


def token_rate():
    """近 60 秒 token/s（翻译+转录实时吞吐，全局）。

    任务取消/结束后不再有新 token 记录，超过 8 秒即判定已停止并清零，
    避免旧窗口残留导致 Token 卡片速度虚高（GPU 已空闲仍显示旧速率）。
    """
    global _token_window
    with _lock:
        if not _token_window:
            return 0.0
        last_ts = _token_window[-1]["ts"]
        if time.time() - last_ts > 8.0:
            _token_window = []  # 清空过期窗口，下一轮直接归零
            return 0.0
        if len(_token_window) < 2:
            return 0.0
        dt = _token_window[-1]["ts"] - _token_window[0]["ts"]
        total = sum(t["tokens"] for t in _token_window)
        if dt <= 0:
            return 0.0
        return round(total / dt, 2)


def record_transcribe_progress(transcribed_sec: float, wall_sec: float):
    """记录转录实时倍率：xRT = 已转录视频秒数 / 墙钟耗时。"""
    global _transcribe_rate
    if wall_sec <= 0:
        return
    with _lock:
        _transcribe_rate = round(transcribed_sec / wall_sec, 2)


def transcribe_rate():
    """最近一次转录实时倍率（每秒转录的视频秒数，全局）。"""
    with _lock:
        return _transcribe_rate


def sampler_loop(stop_event):
    """后台采样线程：每秒一次。"""
    while not stop_event.is_set():
        try:
            snapshot()
        except Exception:
            pass
        time.sleep(1)
