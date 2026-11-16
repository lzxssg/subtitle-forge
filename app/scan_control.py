"""扫描任务控制：长耗时目录扫描的登记、进度上报与取消（v1.1.6）。

背景：手动模式点「扫描全部 / 一键转录 / 一键翻译 / 一键润色」时，扫描在后台线程遍历
整个监控目录（数万文件、几分钟到几十分钟）。旧实现没有任何中断入口：

- 前端只有一个 toast，页面一刷新就彻底失联；
- 扫描中移除监控目录 / 取消文件夹勾选，都不会影响已经跑起来的 os.walk；
- 扫描仍在不停往队列里塞任务，「清空任务」清完又被塞满。

本模块提供进程级 ScanController：登记一个扫描任务、上报进度、支持取消。
watcher.scan_dirs 与 queue_manager.scan_*_jobs 在主循环里轮询 cancel_cb 实现可中断。
"""
import logging
import threading
import time
import uuid
from collections import deque

logger = logging.getLogger("subtitle-forge.scan")

KIND_LABELS = {
    "scan_all": "扫描全部",
    "transcribe": "一键转录",
    "translate": "一键翻译",
    "polish": "一键润色",
}

_FINISHED_KEEP = 8


class ScanJob:
    """一次扫描任务的运行态（线程内累加，前端只读快照）。"""

    def __init__(self, kind: str, dirs=None):
        self.id = uuid.uuid4().hex[:12]
        self.kind = kind
        self.label = KIND_LABELS.get(kind, kind)
        self.dirs = list(dirs or [])
        self.started_at = time.time()
        self.finished_at = None
        self.status = "running"          # running / done / cancelled / failed
        self.error = ""
        self.processed = 0               # 已检查文件数
        self.enqueued = 0                # 已入队任务数
        self.skipped = 0
        self.limit_minutes = 0           # v1.1.7：本次扫描生效的「最长视频时长」上限（分钟，0=不限）
        self.total = 0                   # 已知总量（部分扫描可给出）
        self.current = ""                # 当前处理路径
        self._cancel = threading.Event()

    # ---------- 取消 ----------
    def request_cancel(self) -> bool:
        self._cancel.set()
        return True

    @property
    def cancel_requested(self) -> bool:
        return self._cancel.is_set()

    # ---------- 快照 ----------
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "label": self.label,
            "dirs": self.dirs,
            "status": self.status,
            "cancelling": self.status == "running" and self.cancel_requested,
            "error": self.error,
            "processed": self.processed,
            "enqueued": self.enqueued,
            "skipped": self.skipped,
            "limit_minutes": self.limit_minutes,
            "total": self.total,
            "current": self.current,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed": round((self.finished_at or time.time()) - self.started_at, 1),
        }


class ScanController:
    """进程级扫描任务表：同一时间只允许一个扫描在跑，避免重复遍历同一批目录。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._jobs = {}
        self._finished = deque(maxlen=_FINISHED_KEEP)

    # ---------- 生命周期 ----------
    def start(self, kind: str, dirs=None):
        """登记并返回 ScanJob；已有扫描在运行时返回 None。"""
        with self._lock:
            running = [j for j in self._jobs.values() if j.status == "running"]
            if running:
                return None
            job = ScanJob(kind, dirs)
            self._jobs[job.id] = job
            logger.info("扫描任务开始: %s(%s) 目录=%s", job.label, job.id, job.dirs)
            return job

    def progress(self, job: ScanJob, processed=None, enqueued=None, skipped=None,
                 total=None, current=None):
        if job is None:
            return
        if processed is not None:
            job.processed = processed
        if enqueued is not None:
            job.enqueued = enqueued
        if skipped is not None:
            job.skipped = skipped
        if total is not None:
            job.total = total
        if current is not None:
            job.current = current

    def finish(self, job: ScanJob, status="done", error=""):
        if job is None:
            return
        with self._lock:
            job.status = status
            job.error = error or ""
            job.finished_at = time.time()
            job.current = ""
            self._jobs.pop(job.id, None)
            self._finished.append(job)
        logger.info("扫描任务结束: %s(%s) status=%s 检查=%s 入队=%s 跳过=%s 耗时=%.1fs",
                    job.label, job.id, status, job.processed, job.enqueued,
                    job.skipped, (job.finished_at or time.time()) - job.started_at)

    # ---------- 取消 ----------
    def cancel(self, job_id: str = None) -> int:
        """取消指定扫描（缺省取消全部运行中的扫描），返回被请求取消的数量。"""
        n = 0
        with self._lock:
            for job in list(self._jobs.values()):
                if job.status != "running":
                    continue
                if job_id and job.id != job_id:
                    continue
                if not job.cancel_requested:
                    job.request_cancel()
                    n += 1
                    logger.info("收到取消请求: %s(%s)", job.label, job.id)
        return n

    def has_running(self) -> bool:
        with self._lock:
            return any(j.status == "running" for j in self._jobs.values())

    # ---------- 查询 ----------
    def running(self) -> list:
        with self._lock:
            return [j.to_dict() for j in self._jobs.values() if j.status == "running"]

    def last(self) -> dict:
        with self._lock:
            return self._finished[-1].to_dict() if self._finished else None

    def recent(self, limit: int = 3) -> list:
        with self._lock:
            return [j.to_dict() for j in list(self._finished)[-limit:]]

    def status(self) -> dict:
        return {"ok": True, "running": self.running(), "last": self.last(),
                "recent": self.recent()}


SCAN = ScanController()
