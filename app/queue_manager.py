"""智能任务队列：单并发/多并发执行、失败重试、去重、优先级。

队列状态流转:
  pending → extracting → transcribing → checking → translating → checking_translation → done
  任一步失败 → failed（附失败判定建议）；可手动 retry 重新入队
  staged（两阶段）模式: pending → extracting → transcribing → checking → transcribed → translating → done

v2.1.0 调度模式（scheduler_mode）:
  staged   单模型·环节串行：全部任务转录完再集体翻译，同一时刻只驻留一个大模型
  pipeline 单模型·任务串行：一个任务走完全流程再进下一个
  parallel 多模型·环节并行：转录/翻译/润色各自独立并发，多模型同时驻留（大显存）
  质检未通过的字幕按设置转移到「质检未通过字幕目录」，任务标记失败、不再进入下一环节
"""
import json
import logging
import os
import queue
import subprocess
import threading
import time
import traceback

from . import config, database as db
from .pipeline import quality_check, extract_audio, transcribe, translate, polish
from .services import device_probe, llm_manager

logger = logging.getLogger("subtitle-forge.queue")

# 转录相关状态（staged 模式下 Sakura 需要让出显存）
TRANSCRIBE_STATUSES = ("pending", "extracting", "transcribing", "checking")
TRANSCRIBED = "transcribed"
TRANSLATING = "translating"

STAGE_WEIGHTS = {
    "extracting": (0.02, 0.06),       # 权重范围 (起始, 结束)
    "transcribing": (0.06, 0.56),
    "checking": (0.56, 0.61),
    "translating": (0.61, 0.96),
    "checking_translation": (0.96, 0.98),
    "polishing": (0.98, 1.0),
}


class _StageGate:
    """可动态调整并发数的计数门（信号量替代，支持运行中修改并发数）。

    用于按阶段限流：转录阶段最多 transcribe_workers 个并发，
    翻译阶段最多 translate_workers 个并发，防止 GPU 显存抢占导致降速。
    """

    def __init__(self, limit: int = 1):
        self._limit = max(1, int(limit))
        self._count = 0
        self._cond = threading.Condition()

    def set_limit(self, limit: int):
        with self._cond:
            self._limit = max(1, int(limit))
            self._cond.notify_all()

    def acquire(self):
        with self._cond:
            while self._count >= self._limit:
                self._cond.wait()
            self._count += 1

    def release(self):
        with self._cond:
            self._count = max(0, self._count - 1)
            self._cond.notify()


class SmartQueue:
    def __init__(self):
        self._q = queue.Queue()
        self._workers = []
        self._stop = False
        self._running_tasks = set()
        # v2.3.0 同批次任务数量限制：
        #   _waiting       等候队列（尚未获准进入当前批次的任务 id，FIFO）
        #   _batch_members 当前批次成员（已放行，含排队中/运行中/等待下一环节）
        #   _in_flight     已投递到执行队列或正在执行的任务 id
        self._batch_lock = threading.RLock()
        self._waiting = []
        self._waiting_set = set()
        self._batch_members = []
        self._in_flight = set()
        self._batch_no = 0
        self._requeue = set()        # 处理中再次 submit（同批次下一环节）的待重投任务
        self._admit_lock = threading.Lock()
        self._admit_timer = None     # 放行去抖：扫描批量入队时先攒满等候队列再放行
        # staged 两阶段模式控制
        self._sakura_stopped = False      # 转录阶段已停 Sakura
        self._translation_started = False  # 翻译阶段已启动 Sakura 并提交 transcribed
        self._translate_submitted = set()  # 已提交翻译的任务 id，防重复入队
        self._event_hooks = []  # (event_type, payload) 回调
        # 阶段并发门：转录 / 翻译 各自限流（auto=按 GPU 画像智能推荐，可在 Web 界面调整）
        # 注：初始化为 1，真实值在 start()→apply_limits()（数据库初始化后）解析
        self._gates = {
            "transcribing": _StageGate(1),
            "translating": _StageGate(1),
            "polishing": _StageGate(1),
        }
        # 模型组互斥：asr / translate / review 三组互斥（同组可并发叠加）。
        # 低显存单模型模式下，跨任务处于不同阶段时会互相卸载对方正在使用的 LLM，
        # 导致在途请求 502（upstream exited）。模型组保证同一时间只有一组模型在跑。
        self._model_cond = threading.Condition()
        self._model_group = None
        self._model_ref = 0
        # 进度节流：转录按字幕段回调可达上千次，节流后避免同量级 SQLite 写事务与 SSE 广播
        self._prog_lock = threading.Lock()
        self._prog_last = {}   # task_id -> (ts, progress)
        self._prog_min_interval = 0.8   # 秒
        self._prog_min_delta = 1.0      # 百分点

    @staticmethod
    def _resolve_workers(key: str) -> int:
        """解析 workers 配置：auto/空 → 按设备画像推荐；数字 → 直接使用。"""
        from .services.device_probe import recommend_concurrency, recommend_polish_concurrency

        def _auto():
            if key == "polish_workers":
                return recommend_polish_concurrency()["polish"]
            rec = recommend_concurrency()
            return rec["transcribe"] if key == "transcribe_workers" else rec["translate"]

        val = db.get_setting(key)
        if val is None or str(val).strip().lower() in ("", "auto", "0"):
            return _auto()
        try:
            return max(1, int(val))
        except (ValueError, TypeError):
            return _auto()

    def add_listener(self, cb):
        self._event_hooks.append(cb)

    def _emit(self, event_type, payload=None):
        for cb in self._event_hooks:
            try:
                cb(event_type, payload)
            except Exception:
                pass

    def _emit_progress(self, task_id, progress):
        """进度写入 DB 并实时广播给前端（供进度条按秒刷新）。

        节流：同一任务距上次落库不足 0.8s 且进度变化不足 1 个百分点时直接跳过，
        避免转录逐段回调造成与字幕段数同量级的写事务；进度回退或到达 100% 时强制写入。
        """
        try:
            p = float(progress)
        except (TypeError, ValueError):
            return
        now = time.time()
        with self._prog_lock:
            last = self._prog_last.get(task_id)
            if (last and p < 100 and p >= last[1]
                    and now - last[0] < self._prog_min_interval
                    and p - last[1] < self._prog_min_delta):
                return
            self._prog_last[task_id] = (now, p)
        db.update_task(task_id, progress=p)
        self._emit("task_update", {"id": task_id, "progress": p})

    def _clear_progress_state(self, task_id):
        with self._prog_lock:
            self._prog_last.pop(task_id, None)

    # ---------- v2.1.0 调度模式 ----------
    def mode(self) -> str:
        """当前调度模式：staged / pipeline / parallel。

        配置为 auto 或空时按显卡显存自动判断：
        显存总量 >= multi_model_vram_mb → parallel（多模型共存），否则 staged（单模型环节串行）。
        """
        m = str(db.get_setting("scheduler_mode") or "auto").strip().lower()
        if m in ("staged", "pipeline", "parallel"):
            return m
        try:
            return "parallel" if llm_manager.scheduler_mode() == "multi" else "staged"
        except Exception:
            return "staged"

    def is_parallel(self) -> bool:
        return self.mode() == "parallel"

    @staticmethod
    def _resolve_parallel(stage: str) -> int:
        """多模型模式各环节并发数：auto/空 → 按显卡型号与显存智能推荐。"""
        from .services.device_probe import recommend_stage_concurrency
        val = db.get_setting("parallel_%s_workers" % stage)
        if val is None or str(val).strip().lower() in ("", "auto", "0"):
            try:
                return max(1, int(recommend_stage_concurrency().get(stage, 1)))
            except Exception:
                return 1
        try:
            return max(1, min(8, int(val)))
        except (TypeError, ValueError):
            return 1

    def worker_count(self) -> int:
        """工作线程数：单模型恒为 1（严格串行），多模型按各环节并发取最大值。"""
        if not self.is_parallel():
            return 1
        base = max(2, self._resolve_parallel("transcribe"),
                   self._resolve_parallel("translate"), self._resolve_parallel("polish"))
        return min(8, base)

    def apply_limits(self):
        """按调度模式调整阶段并发门。

        单模型模式三个阶段并发恒为 1，保证同一时刻只有一个大模型驻留显存；
        多模型模式按 parallel_transcribe/translate/polish_workers 放开并发。
        """
        mode = self.mode()
        if mode == "parallel":
            tw = self._resolve_parallel("transcribe")
            tlw = self._resolve_parallel("translate")
            plw = self._resolve_parallel("polish")
        else:
            tw = tlw = plw = 1
        if config.get_setting("asr_provider", "local") == "llama":
            tw = 1  # whisper-server 单槽位：llama 托管模式转录并发恒为 1
        self._gates["transcribing"].set_limit(tw)
        self._gates["translating"].set_limit(tlw)
        self._gates["polishing"].set_limit(plw)
        logger.info("调度模式=%s 阶段并发: 转录=%d 翻译=%d 润色=%d", mode, tw, tlw, plw)

    def _enter_stage(self, stage: str):
        self._gates[stage].acquire()

    def _exit_stage(self, stage: str):
        self._gates[stage].release()

    def _enter_model_group(self, group: str):
        """占用模型组：asr=转录 / translate=翻译 / review=校对+润色。

        同组可叠加并发；不同组互斥等待，保证同一时刻只有一组模型驻留显存，
        避免多任务跨阶段互相卸载对方在途请求的 LLM 模型。
        """
        if self.is_parallel():
            return  # 多模型模式：模型共存由显存调度器接管，不做组互斥
        with self._model_cond:
            while self._model_group is not None and self._model_group != group:
                self._model_cond.wait()
            self._model_group = group
            self._model_ref += 1

    def _exit_model_group(self, group: str):
        if self.is_parallel():
            return
        with self._model_cond:
            self._model_ref = max(0, self._model_ref - 1)
            if self._model_ref == 0:
                self._model_group = None
                self._model_cond.notify_all()

    def start(self, worker_count: int = None):
        self._stop = False
        if worker_count is None:
            worker_count = int(db.get_setting("task_workers") or db.get_setting("max_workers") or 1)
        if not self.is_parallel():
            worker_count = 1  # 单模型模式：严格串行，避免多任务抢占同一张显卡
        for i in range(max(1, worker_count)):
            t = threading.Thread(target=self._run_loop, daemon=True, name=f"worker-{i}")
            t.start()
            self._workers.append(t)
        self.apply_limits()
        logger.info("队列已启动，workers=%d", len(self._workers))

    def stop(self):
        self._stop = True
        self._q.put(None)  # 唤醒

    # ---------- v2.3.0 同批次任务数量限制（批次闸门） ----------
    BATCH_ACTIVE_STATUSES = ("pending", "extracting", "transcribing", "checking",
                             "transcribed", "translating", "checking_translation", "polishing")
    BATCH_END_STATUSES = ("done", "failed", "cancelled", "skipped", "paused")

    def batch_size(self) -> int:
        """同批次任务数量上限（1-100，默认 20）。"""
        try:
            n = int(float(db.get_setting("batch_size") or 20))
        except (TypeError, ValueError):
            n = 20
        return max(1, min(100, n))

    def batch_state(self) -> dict:
        """当前批次状态快照（供前端渲染「当前队列 / 等候队列」）。"""
        with self._batch_lock:
            members = list(self._batch_members)
            return {
                "size": self.batch_size(),
                "batch_no": self._batch_no,
                "members": members,
                # 真正被工作线程领走在跑的任务（前端置顶展示），已放行但还在等线程的不算
                "running": [t for t in members if t in self._running_tasks],
                "queued": [t for t in members if t not in self._running_tasks],
                "waiting": len(self._waiting),
            }

    def _dispatch(self, task_id: int):
        """把任务投递到执行队列（调用方需持有 _batch_lock）。"""
        if task_id in self._in_flight:
            return
        self._in_flight.add(task_id)
        self._q.put(task_id)

    def _admit_batch(self):
        """当前批次全部结束后，按 batch_size 从等候队列放行下一批。

        只有上一批成员全部处于终态（done/failed/cancelled/skipped/paused）时才放行，
        上一批没跑完（含失败任务）绝不会提前进入下一批。
        """
        with self._batch_lock:
            if self._batch_members:
                return
            size = self.batch_size()
            admitted = []
            while self._waiting and len(admitted) < size:
                tid = self._waiting.pop(0)
                self._waiting_set.discard(tid)
                t = db.get_task(tid)
                if not t or t["status"] not in self.BATCH_ACTIVE_STATUSES:
                    continue  # 已结束/已删除/已暂停的任务直接跳过，不占批次名额
                admitted.append(tid)
            # 等候队列为空但仍有「已转录待翻译」的历史任务（如两阶段模式残留）时，
            # 一并纳入本批，避免它们永远排不进批次
            if not self._waiting and len(admitted) < size and self._phase_mode() == "staged":
                for r in db.list_tasks_by_status([TRANSCRIBED]):
                    if len(admitted) >= size:
                        break
                    tid = r["id"]
                    if tid in self._batch_members or tid in self._waiting_set or tid in admitted:
                        continue
                    admitted.append(tid)
            if not admitted:
                return
            self._batch_no += 1
            self._batch_members = admitted
            logger.info("第 %d 批放行 %d 个任务（批次上限 %d），等候队列剩余 %d",
                        self._batch_no, len(admitted), size, len(self._waiting))
            for tid in admitted:
                self._dispatch(tid)
            state = self.batch_state()
        self._emit("batch_update", state)

    def _on_task_finished(self, task_id: int):
        """单个任务处理结束（含失败/取消）：释放批次名额，必要时放行下一批。"""
        redispatch = False
        with self._batch_lock:
            self._in_flight.discard(task_id)
            pending_next = task_id in self._requeue
            self._requeue.discard(task_id)
            t = db.get_task(task_id)
            st = t["status"] if t else None
            in_batch = task_id in self._batch_members
            if in_batch and st in self.BATCH_ACTIVE_STATUSES and pending_next:
                # 同一批次内进入下一环节（如 staged：转录→翻译），继续占本批名额
                self._dispatch(task_id)
                pending_next = False
            elif in_batch and (st is None or st in self.BATCH_END_STATUSES):
                self._batch_members.remove(task_id)
            redispatch = pending_next
        if redispatch:
            # 不在批次内但处理中又提交过：重新走批次闸门排队
            self.submit(task_id)
        self._admit_batch()

    def _schedule_admit(self, delay: float = 0.8):
        """放行去抖：一键扫描会连续 submit 上百条，攒一小会儿再按 batch_size 放行，
        避免「第一条任务刚入队就立刻放行、导致整批只跑 1 个」。
        """
        with self._admit_lock:
            if self._admit_timer is not None and self._admit_timer.is_alive():
                return
            timer = threading.Timer(delay, self._admit_batch)
            timer.daemon = True
            self._admit_timer = timer
            timer.start()

    def _sweep_batch(self):
        """清理批次中已结束/已暂停/已删除的成员（覆盖排队中被取消、删除等情况）。"""
        empty = False
        with self._batch_lock:
            if not self._batch_members:
                return
            alive = []
            for tid in self._batch_members:
                t = db.get_task(tid)
                if not t or t["status"] in self.BATCH_END_STATUSES:
                    self._in_flight.discard(tid)
                    continue
                alive.append(tid)
            if len(alive) != len(self._batch_members):
                self._batch_members = alive
            empty = not alive
        if empty:
            self._admit_batch()

    def submit(self, task_id: int):
        """任务入队：先进等候队列，由批次闸门按 batch_size 决定何时真正执行。

        - 已在当前批次内（例如 staged 模式转录完成、等待翻译阶段）→ 重新投递执行
        - 否则进入等候队列，等当前批次全部结束后再放行
        """
        with self._batch_lock:
            if task_id in self._batch_members:
                if task_id in self._in_flight:
                    # 正被工作线程处理（环节内部提交下一阶段）：等收尾时重新投递，避免同一任务被两条线程同时跑
                    self._requeue.add(task_id)
                else:
                    t = db.get_task(task_id)
                    if t and t["status"] in self.BATCH_ACTIVE_STATUSES:
                        self._dispatch(task_id)
                return
            if task_id in self._waiting_set:
                return
            self._waiting.append(task_id)
            self._waiting_set.add(task_id)
        self._schedule_admit()

    def pause_all(self) -> int:
        """暂停队列：停止领取新任务，并把所有进行中的任务置为 paused。

        在途任务会在下一个进度回调处中断（取消/暂停共用同一中断检查），
        已完成环节的产物（.transcript.srt 等）保留，恢复时自动续跑，不重复转录。
        返回受影响的任务数。
        """
        db.set_setting("queue_paused", True)
        n = 0
        with db.db() as conn:
            rows = conn.execute(
                "SELECT id FROM tasks WHERE status NOT IN ('done','failed','cancelled','paused')"
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE tasks SET status='paused', error='用户暂停队列', updated_at=? WHERE id=?",
                    (time.time(), r["id"]))
                n += 1
        self._emit("queue_paused", {"paused": True, "count": n})
        # 等候队列里的任务同样已置为 paused，清空等候队列，恢复时统一重新入队
        with self._batch_lock:
            while True:
                try:
                    tid = self._q.get_nowait()
                except queue.Empty:
                    break
                if tid is not None:
                    self._in_flight.discard(tid)
            self._waiting.clear()
            self._waiting_set.clear()
        self._sweep_batch()
        logger.info("队列已暂停，%d 个任务挂起", n)
        return n

    def resume_all(self) -> int:
        """恢复队列：清除暂停标记，把所有 paused 任务重新入队。"""
        db.set_setting("queue_paused", False)
        rows = db.list_tasks_by_status(["paused"])
        for r in rows:
            db.update_task(r["id"], status="pending", error="")
            self.submit(r["id"])
        self._emit("queue_paused", {"paused": False, "count": len(rows)})
        logger.info("队列已恢复，%d 个任务重新入队", len(rows))
        return len(rows)

    def cancel_all(self) -> int:
        """取消所有任务：清空排队队列 + 标记所有活动任务为 cancelled。返回受影响数量。"""
        count = 0
        with self._batch_lock:
            while True:
                try:
                    tid = self._q.get_nowait()
                    if tid is not None:
                        self._in_flight.discard(tid)
                    count += 1
                except queue.Empty:
                    break
            self._waiting.clear()
            self._waiting_set.clear()
        now = time.time()
        with db.db() as conn:
            rows = conn.execute(
                "SELECT id FROM tasks WHERE status NOT IN ('done','failed','cancelled')"
            ).fetchall()
            for r in rows:
                conn.execute(
                    "UPDATE tasks SET status='cancelled', error='用户取消全部任务', updated_at=? WHERE id=?",
                    (now, r["id"]),
                )
                count += 1
        self._running_tasks.clear()
        self._sweep_batch()
        return count

    def clear_all(self) -> int:
        """清空全部任务与字幕记录（仅删除记录，不影响已生成字幕文件）。返回删除任务数。"""
        with self._batch_lock:
            self._waiting.clear()
            self._waiting_set.clear()
            self._batch_members.clear()
            self._in_flight.clear()
            while True:
                try:
                    self._q.get_nowait()
                except queue.Empty:
                    break
        with db.db() as conn:
            conn.execute("DELETE FROM subtitles")
            cur = conn.execute("DELETE FROM tasks")
            return cur.rowcount

    def _run_loop(self):
        while not self._stop:
            # 队列暂停时不领取新任务（进行中任务不受影响）
            if db.get_setting("queue_paused", False):
                time.sleep(1)
                continue
            # staged 模式：动态管理 LLM 容器显存（转录时停 Sakura，全部转录完再启动翻译）
            if self._phase_mode() == "staged" and not self.is_parallel():
                self._staged_checkpoint()
            try:
                task_id = self._q.get(timeout=2)
            except queue.Empty:
                # 空闲时清理批次中已取消/暂停/删除的成员，避免批次无法收尾
                self._sweep_batch()
                continue
            if task_id is None:
                continue
            try:
                # staged 模式：转录类任务执行前立即停 LLM 容器释放显存
                if self._phase_mode() == "staged" and not self.is_parallel():
                    t = db.get_task(task_id)
                    if t and t["status"] in TRANSCRIBE_STATUSES and not self._sakura_stopped:
                        if self._sakura_running():
                            if self._stop_sakura():
                                self._sakura_stopped = True
                self._process(task_id)
            except Exception as e:
                logger.error("任务 %s 处理异常: %s", task_id, e)
                traceback.print_exc()
                self._fail(task_id, f"内部异常: {e}", suggestion="请查看容器日志")
            finally:
                self._running_tasks.discard(task_id)
                self._clear_progress_state(task_id)
                # 批次收尾：本任务结束（含失败）后释放名额；整批结束才放行下一批
                self._on_task_finished(task_id)

    # ---------- staged 两阶段模式 ----------
    def _phase_mode(self):
        return db.get_setting("phase_mode", "pipeline") or "pipeline"

    @staticmethod
    def _docker_api(method, path, timeout=15):
        """通过 /var/run/docker.sock 调 Docker API（容器内无 docker CLI）。"""
        sock = "/var/run/docker.sock"
        if not os.path.exists(sock):
            return None
        try:
            r = subprocess.run(
                ["curl", "-s", "-X", method, "--unix-socket", sock,
                 f"http://localhost{path}"],
                capture_output=True, text=True, timeout=timeout)
            if r.returncode != 0:
                return None
            out = r.stdout.strip()
            return out if out else None
        except Exception:
            return None

    @staticmethod
    def _managed_by_llamaswap():
        """llama-swap 按需加载：llm_base_url 指向 llama-swap 根地址即视为托管（容器名不再作为判据）。"""
        return bool(llm_manager.llama_swap_base())

    def _sakura_running(self):
        try:
            if self._managed_by_llamaswap():
                return False  # llama-swap 按需加载，视为未驻留，无需手动停
            name = db.get_setting("sakura_container", "llama-sakura") or "llama-sakura"
            out = self._docker_api("GET", f"/containers/{name}/json")
            if not out:
                return True  # 无法检查时保守假设在运行，不擅自停容器
            import json as _json
            return _json.loads(out).get("State", {}).get("Running", False)
        except Exception:
            return True

    def _stop_sakura(self):
        """转录阶段停止 LLM 容器，释放显存给大 ASR 模型。"""
        try:
            if self._managed_by_llamaswap():
                # v1.5.0: llama-swap 按需加载，改为卸载全部 LLM 模型释放显存
                logger.info("LLM 由 llama-swap 管理，卸载模型释放显存")
                llm_manager.unload_all()
                return True
            name = db.get_setting("sakura_container", "llama-sakura") or "llama-sakura"
            self._docker_api("POST", f"/containers/{name}/stop", timeout=90)
            # 确认已停止
            if not self._sakura_running():
                logger.info("已停止 LLM 容器 %s（转录阶段释放显存）", name)
                return True
            logger.warning("停止 %s 未生效", name)
            return False
        except Exception as e:
            logger.warning("停止 LLM 容器失败: %s", e)
            return False

    def _wait_translation_idle(self, task_id, timeout=300):
        """转录前等待其他任务的翻译/校对阶段结束，避免卸载 LLM 打断在途请求。

        单并发（默认）下立即通过；多并发时最多等待 timeout 秒。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            active = db.count_by_status(("translating", "checking_translation", "polishing"))
            mine = db.get_task(task_id)
            if mine and mine["status"] in ("translating", "checking_translation", "polishing"):
                active = max(0, int(active or 0) - 1)
            if active <= 0:
                return
            time.sleep(2)
        logger.warning("等待其他任务翻译空闲超时（%ss），仍尝试卸载 LLM", timeout)

    @staticmethod
    def _take_asr_override(task_id):
        """取出并清除任务级「增强转录」模型覆盖（一次性生效）。

        增强重试时 main.py 会把 asr_model_override / asr_compute_override 写入任务 meta；
        这里在真正转录前取出并立刻清空，保证只影响本次重试，失败后再点普通重试会回到默认模型。
        返回 (model_name, compute_type)，无覆盖时返回 (None, None)。
        """
        try:
            meta = db.task_meta(task_id) or {}
        except Exception:
            return None, None
        model = str(meta.get("asr_model_override") or "").strip()
        compute = str(meta.get("asr_compute_override") or "").strip()
        if not model and not compute:
            return None, None
        try:
            # enhanced_flow 保留到任务结束：增强流程产出的字幕不再判失败，直接落视频原目录
            db.update_task_meta(task_id, asr_model_override="", asr_compute_override="",
                                force_retranscribe=False, enhanced_retry=False,
                                enhanced_flow=True)
        except Exception:
            logger.warning("清除任务 %s 增强转录标记失败", task_id, exc_info=True)
        logger.info("任务 %s 启用增强转录：模型 %s，精度 %s", task_id,
                    model or "默认", compute or "默认")
        return (model or None), (compute or None)

    @staticmethod
    def _is_enhanced_flow(task_id):
        """本任务是否由「增强转录」发起。

        增强流程的定位是「用更强模型抢救失败任务」：产出的字幕直接落视频原目录，
        质检不通过也不再判失败、不归档到质检未通过目录。
        """
        try:
            return bool(db.task_meta(task_id).get("enhanced_flow"))
        except Exception:
            return False

    def _pre_transcribe_unload(self, task_id, model_name=None, compute_type=None):
        """转录前显存守护：按实际空闲显存决定是否卸载常驻 LLM，给 ASR 让位。

        - 多模型模式：交给 llm_manager 按空闲显存自行判断。
        - 单模型模式：即使 llama_swap_auto_unload 关闭，只要空闲显存不够当前
          ASR 模型（型号+精度）加载，仍强制卸载，避免先 OOM 再自愈白等一轮。
        - model_name / compute_type：增强转录覆盖模型，显存按该模型真实需求估算。
        """
        if self.is_parallel():
            return  # 多模型模式：由 llm_manager 按空闲显存决定是否卸载，不强制清空
        if not self._managed_by_llamaswap():
            return
        auto = bool(config.get_setting("llama_swap_auto_unload", True))
        need = llm_manager.estimate_asr_vram_mb(model_name, compute_type)
        free = llm_manager.free_vram_mb()
        forced = bool(free) and free < need
        if not auto and not forced:
            logger.info("llama_swap_auto_unload 已关闭，且空闲显存 %dMB ≥ ASR 需求 %dMB，跳过卸载",
                        free, need)
            return
        if forced and not auto:
            logger.warning("llama_swap_auto_unload 已关闭，但空闲显存 %dMB < ASR 需求 %dMB，"
                           "强制卸载 LLM 让位（避免 ASR 加载 OOM）", free, need)
        self._wait_translation_idle(task_id)
        llm_manager.prepare_for("transcribe")
        free = llm_manager.wait_vram_free(need)
        logger.info("转录前显存守护：空闲 %dMB / ASR 需求 %dMB", free, need)

    def _start_sakura(self):
        """翻译阶段启动 LLM 容器并等待就绪。"""
        try:
            if self._managed_by_llamaswap():
                logger.info("LLM 由 llama-swap 管理，跳过容器启动")
                return True
            name = db.get_setting("sakura_container", "llama-sakura") or "llama-sakura"
            self._docker_api("POST", f"/containers/{name}/start", timeout=30)
            base = db.get_setting("llm_base_url", "") or ""
            health_url = ""
            if base:
                from urllib.parse import urlparse
                p = urlparse(base)
                health_url = f"{p.scheme}://{p.netloc}/health"
            for _ in range(120):  # 最多等 4 分钟
                time.sleep(2)
                try:
                    if health_url:
                        r = subprocess.run(["curl", "-s", "-m", "2", health_url],
                                           capture_output=True, text=True, timeout=5)
                        if r.returncode == 0 and '"ok"' in r.stdout:
                            logger.info("LLM 容器 %s 已就绪", name)
                            return True
                    else:
                        out = self._docker_api("GET", f"/containers/{name}/json")
                        if out:
                            import json as _json
                            st = _json.loads(out).get("State", {})
                            if st.get("Running") and (st.get("Health", {}).get("Status") == "healthy" or st.get("Health") is None):
                                logger.info("LLM 容器 %s 已就绪", name)
                                return True
                except Exception:
                    pass
            logger.info("LLM 容器 %s 未探测到就绪，继续（调用端会重试）", name)
            return True
        except Exception as e:
            logger.warning("启动 LLM 容器失败: %s", e)
            return False

    def _batch_member_ids(self) -> set:
        with self._batch_lock:
            return set(self._batch_members)

    def _batch_status_counts(self) -> dict:
        """统计当前批次成员的状态分布（staged 阶段切换只看当前批次，不受等候队列影响）。"""
        with self._batch_lock:
            members = list(self._batch_members)
        counts = {}
        for tid in members:
            t = db.get_task(tid)
            if t:
                counts[t["status"]] = counts.get(t["status"], 0) + 1
        return counts

    def _staged_checkpoint(self):
        """staged 模式阶段切换（仅针对当前批次）：
        - 转录阶段：存在待转录/转录中任务时，若 Sakura 在运行则停止以释放显存。
        - 翻译阶段：无转录任务且存在 transcribed 待翻译时，启动 Sakura 并提交翻译。
        - 全部转录/翻译结束：复位阶段标志，使后续批次仍能重新触发翻译。
        """
        try:
            counts = self._batch_status_counts()
            transcribing = sum(counts.get(s, 0) for s in TRANSCRIBE_STATUSES)
            waiting = counts.get(TRANSCRIBED, 0)
            translating = (counts.get(TRANSLATING, 0) + counts.get("checking_translation", 0)
                           + counts.get("polishing", 0))
            if transcribing > 0:
                if not self._sakura_stopped and self._sakura_running():
                    if self._stop_sakura():
                        self._sakura_stopped = True
                # 新一轮转录开始：允许转录完成后再次触发翻译阶段
                self._translation_started = False
                self._translate_submitted.clear()
            elif waiting > 0:
                if not self._translation_started:
                    if self._start_sakura():
                        self._translation_started = True
                if self._translation_started:
                    members = self._batch_member_ids()
                    rows = [r for r in db.list_tasks_by_status([TRANSCRIBED]) if r["id"] in members]
                    queued = 0
                    for r in rows:
                        tid = r["id"]
                        if tid in self._translate_submitted:
                            continue
                        self._translate_submitted.add(tid)
                        self.submit(tid)
                        queued += 1
                    if queued:
                        logger.info("转录阶段完成，%d 条任务进入翻译阶段（本轮提交 %d 条）",
                                    len(rows), queued)
            elif translating <= 0:
                # 没有任何转录/翻译任务在跑：复位，等待下一批
                self._translation_started = False
                self._translate_submitted.clear()
        except Exception as e:
            logger.warning("staged checkpoint 异常: %s", e)

    # ---------- 主流程 ----------
    @staticmethod
    def _interrupt_check(task_id) -> bool:
        """流水线中断检查：任务被取消或暂停时返回 True（下游抛异常跳出当前环节）。"""
        try:
            return db.get_task(task_id)["status"] in ("cancelled", "paused")
        except Exception:
            return False

    def _process(self, task_id: int):
        task = db.get_task(task_id)
        if not task or task["status"] in ("cancelled", "paused", "done", "failed"):
            return
        self._running_tasks.add(task_id)
        try:
            cfg = db.get_all_settings()
            video_path = task["video_path"]

            # 一键润色任务：直接处理字幕文件，不参与视频流水线分流
            task_mode = db.task_meta(task_id).get("mode", "")
            if task_mode == "polish":
                self._polish_only(task_id, video_path, cfg)
                return
            # v1.7.0: 一键扫描只转录模式：转录完成即结束，不翻译
            if task_mode == "transcribe_only":
                self._transcribe_only(task_id, video_path, cfg)
                return

            if not _file_exists(video_path):
                self._fail(task_id, "视频文件不存在（可能已被移动/删除）",
                          suggestion="检查监控目录与文件挂载")
                return

            # v2.4.0 增强转录：强制重新转录时，忽略「已存在目标字幕」与断点续跑
            meta_now = db.task_meta(task_id)
            force_retranscribe = bool(meta_now.get("force_retranscribe"))

            # 已有目标字幕 → 直接完成
            suffix = cfg["subtitle_suffix"]
            target_srt = _srt_path(video_path, suffix)
            if (not force_retranscribe) and cfg["skip_existing_subtitle"] and _file_exists(target_srt):
                db.update_task(task_id, status="done", stage="skipped", progress=100,
                               error="已存在目标字幕，跳过")
                self._emit("task_done", {"id": task_id, "status": "done", "note": "skip_existing"})
                llm_manager.unload_all()

            # 扫描分类模式：有外文字幕 → 只翻译该字幕（不再转录）；无字幕 → 完整流水线
            task_mode = db.task_meta(task_id).get("mode", "")
            if cfg["translate_only"] or task_mode == "translate_only":
                self._translate_only(task_id, video_path, target_srt, cfg)
                return

            # staged 模式翻译阶段：任务已有转录结果，直接翻译
            if task["status"] == TRANSCRIBED:
                self._translate_from_transcript(task_id, video_path, target_srt, cfg)
                return

            # 断点续跑：暂停中断后恢复的任务，已有质检通过的转录字幕则跳过重复转录
            if (not force_retranscribe) and meta_now.get("transcript_score") is not None:
                from pathlib import Path as _P2
                tsrt = str(_P2(video_path).with_suffix(".transcript.srt"))
                if _file_exists(tsrt):
                    logger.info("任务 %s 复用已有转录字幕，跳过转录环节", task_id)
                    self._translate_from_transcript(task_id, video_path, target_srt, cfg)
                    return

            self._full_pipeline(task_id, video_path, target_srt, cfg)
        except Exception as e:
            # 取消/暂停导致的中断：不记为失败，保留断点
            try:
                st = db.get_task(task_id)["status"]
            except Exception:
                st = None
            if st in ("cancelled", "paused"):
                logger.info("任务 %s 已%s，中断执行", task_id, "取消" if st == "cancelled" else "暂停")
                self._emit("task_update", {"id": task_id, "status": st})
                return
            logger.exception("任务执行失败")
            self._fail_with_advice(task_id, str(e))
        finally:
            self._running_tasks.discard(task_id)

    def _full_pipeline(self, task_id, video_path, target_srt, cfg):
        # 1) 提取音频
        self._set_stage(task_id, "extracting", 1)
        wav = _wav_path(video_path, task_id)
        extract_audio.extract_audio(
            video_path, wav,
            progress_cb=lambda p: self._emit_progress(task_id, _overall("extracting", p)))
        duration = extract_audio.probe_duration(video_path)

        # 2) 转录（v1.7.0: 套阶段并发门，转录并发受 transcribe_workers 限制）
        self._set_stage(task_id, "transcribing", 6)
        self._enter_stage("transcribing")
        self._enter_model_group("asr")
        # v2.4.0 增强转录：取出本任务的一次性模型覆盖（如 large-v3）
        ov_model, ov_compute = self._take_asr_override(task_id)
        try:
            self._pre_transcribe_unload(task_id, ov_model, ov_compute)  # v1.5.0: 转录前卸载 LLM，让位 ASR
            result = transcribe.transcribe(
                wav,
                progress_cb=lambda p: self._emit_progress(task_id, _overall("transcribing", p)),
                cancel_check=lambda: self._interrupt_check(task_id),
                asr_model=ov_model,
                compute_type=ov_compute,
            )
        finally:
            self._exit_model_group("asr")
            self._exit_stage("transcribing")
        segments = result["segments"]
        if not segments:
            self._fail_with_advice(task_id, "转录结果为空（视频可能无语音或音频异常）")
            return
        db.update_task_meta(task_id, duration=duration,
                            detected_lang=result["language"], asr_model=result["model"],
                            asr_device=result["device"],
                            asr_compute_type=result.get("compute_type", ""),
                            asr_note=result.get("device_note", ""))
        if result.get("device_note"):
            logger.warning("任务 %s ASR 运行参数回退：%s", task_id, result["device_note"])
        # 转录字幕落盘到视频同目录（日语原文字幕，供用户查看与后续翻译/校对）
        from pathlib import Path
        transcript_srt = str(Path(video_path).with_suffix(".transcript.srt"))
        from .pipeline.srt_utils import write_srt
        write_srt(segments, transcript_srt)

        # 3) 转录质检
        self._set_stage(task_id, "checking", 56)
        check = quality_check.check_transcript(segments, duration, result["language"])
        db.update_task_meta(task_id, transcript_score=check.score,
                            transcribe_tokens_per_sec=round(result.get("tokens_per_sec", 0), 1),
                            transcribe_time_s=round(result.get("elapsed", 0), 1),
                            source_chars=result.get("chars", 0))
        enhanced_flow = self._is_enhanced_flow(task_id)
        if not check.passed:
            logger.warning("转录质检未通过: %s", check.issues)
            if enhanced_flow:
                # v1.1.0 增强流程放行：字幕留在视频同目录，不归档、不判失败，继续往下走
                logger.warning("任务 %s 为增强转录流程，质检未通过（%.1f 分）也放行落盘",
                               task_id, check.score)
                db.create_subtitle(task_id, video_path, transcript_srt, "transcript",
                                   status="needs_review", lang=result["language"], score=check.score,
                                   report={"issues": check.issues[:20],
                                           "suggestions": check.suggestions[:10],
                                           "enhanced_released": True})
                db.update_task_meta(task_id,
                                    enhanced_note=f"转录质检未通过（{check.score:.0f} 分），"
                                                  f"增强流程已放行并保留在视频原目录")
            else:
                archived = self._archive_review_failed(task_id, video_path, transcript_srt, "transcript")
                db.create_subtitle(task_id, video_path, archived, "transcript",
                                   status="needs_review", lang=result["language"], score=check.score,
                                   report={"issues": check.issues[:20], "suggestions": check.suggestions[:10]})
                self._fail(task_id, "转录质检未通过",
                           suggestion=("转录字幕已归档到「质检未通过字幕目录」" if archived != transcript_srt
                                       else "转录字幕已保存在视频同目录") + "，可人工查看修正后重新入队")
                return
        else:
            # 质检通过：不再写质检建议（仅质检未通过的字幕保留 issues/suggestions）
            db.create_subtitle(task_id, video_path, transcript_srt, "transcript",
                               status="ok", lang=result["language"], score=check.score,
                               report={"passed": True})

        # 源语言 == 目标语言 则无需翻译
        if result["language"] == cfg["target_lang"]:
            import shutil
            shutil.copyfile(transcript_srt, target_srt)
            db.update_task(task_id, status="done", stage="done", progress=100,
                           error="源语言已为目标语言，直接使用转录字幕")
            db.create_subtitle(task_id, video_path, target_srt, "translated",
                               lang=cfg["target_lang"], score=check.score,
                               report={"note": "source==target, no translation"})
            self._emit("task_done", {"id": task_id, "status": "done"})
            llm_manager.unload_all()

        # staged 两阶段模式：转录完成即停，等待集体翻译
        if self._phase_mode() == "staged":
            transcribe.release_model()  # 释放 ASR 显存，让位给翻译阶段 LLM
            db.update_task(task_id, status=TRANSCRIBED, stage="transcribed", progress=56,
                           error="转录完成，等待翻译阶段")
            self._emit("task_transcribed", {"id": task_id, "status": TRANSCRIBED})
            return

        # 4) 翻译
        self._translate_core(task_id, video_path, target_srt, cfg, segments, result["language"])

    def _translate_core(self, task_id, video_path, target_srt, cfg, segments, src_lang):
        """翻译 + 校对 + 落盘（流水线模式与 staged 翻译阶段共用）。"""
        self._set_stage(task_id, "translating", 61)
        transcribe.release_model()  # 翻译前释放本地 ASR 缓存
        # 翻译阶段占用 translate 模型组（与 asr/review 互斥，避免跨任务互相卸载）
        self._enter_model_group("translate")
        try:
            llm_manager.prepare_for("translate")  # v2.0.0: 智能调度（单模型串行/多模型共存）
            self._enter_stage("translating")  # v1.7.0: 翻译并发受 translate_workers 限制
            try:
                trans_result = translate.translate_segments(
                    segments,
                    src_lang=src_lang,
                    tgt_lang=cfg["target_lang"],
                    progress_cb=lambda p: self._emit_progress(task_id, _overall("translating", p)),
                    cancel_check=lambda: self._interrupt_check(task_id),
                )
            finally:
                self._exit_stage("translating")
        finally:
            self._exit_model_group("translate")
        db.update_task_meta(
            task_id,
            tokens_in=trans_result.get("tokens_in", 0),
            tokens_out=trans_result.get("tokens_out", 0),
            translate_tokens_per_sec=round(trans_result.get("tokens_per_sec", 0), 2),
            translate_time_s=round(trans_result.get("elapsed", 0), 1),
            untranslated_ratio=round(float(trans_result.get("untranslated_ratio") or 0), 3),
        )
        # 防护：模型未真正翻译（大量原文回显）时直接判失败，避免产出「假中文字幕」
        ratio = float(trans_result.get("untranslated_ratio") or 0)
        if ratio >= 0.3:
            from .pipeline.srt_utils import write_srt
            write_srt(segments, target_srt)
            if self._is_enhanced_flow(task_id):
                # 增强流程放行：字幕直接留在视频原目录，不归档、不判失败，跳过后继校对与润色
                db.create_subtitle(task_id, video_path, target_srt, "translated",
                                   status="needs_review", lang=cfg["target_lang"], score=0,
                                   report={"issues": [f"翻译未生效：{ratio:.0%} 条目仍为原文"],
                                           "suggestions": ["检查翻译模型（llm_model）是否支持该语言对，或改用更强模型重跑"],
                                           "enhanced_released": True})
                db.update_task_meta(task_id,
                                    enhanced_note=f"翻译未生效（{ratio:.0%} 条目仍为原文），"
                                                  f"增强流程已放行，字幕保留在视频原目录")
                db.update_task(task_id, status="done", stage="done", progress=100, error="")
                self._emit("task_done", {"id": task_id, "status": "done"})
                llm_manager.unload_all()
                return
            archived = self._archive_review_failed(task_id, video_path, target_srt, "translated")
            db.create_subtitle(task_id, video_path, archived, "translated",
                               status="needs_review", lang=cfg["target_lang"], score=0,
                               report={"issues": [f"翻译未生效：{ratio:.0%} 条目仍为原文"],
                                       "suggestions": ["检查翻译模型（llm_model）是否支持该语言对，或改用更强模型重跑"]})
            self._fail(task_id, f"翻译未生效（{ratio:.0%} 条目仍为原文）",
                       suggestion="字幕未真正翻译，已归档到「质检未通过字幕目录」，建议更换翻译模型后重跑")
            return

        # 翻译校对（review 模型组：与翻译/asr 互斥）
        self._set_stage(task_id, "checking_translation", 96)
        self._enter_model_group("review")
        try:
            # 环节切换：卸载翻译模型（Sakura），让位给校对模型（review/llm 模型懒加载）
            llm_manager.prepare_for("polish")
            review = quality_check.review_translation(segments, trans_result.get("model_name", cfg["llm_model"]))
            db.update_task_meta(task_id, translation_score=review.score)
            released = False
            if not review.passed:
                logger.warning("翻译校对未通过: %s", review.issues)
                # 落盘翻译字幕（保留供用户查看），任务标记失败不再进入润色队列
                from .pipeline.srt_utils import write_srt
                write_srt(segments, target_srt)
                if self._is_enhanced_flow(task_id):
                    # v1.1.0 增强流程放行：字幕留在视频原目录，不归档、不判失败，继续润色
                    db.create_subtitle(task_id, video_path, target_srt, "translated",
                                       status="needs_review", lang=cfg["target_lang"], score=review.score,
                                       report={"issues": review.issues[:20],
                                               "suggestions": review.suggestions[:10],
                                               "enhanced_released": True})
                    db.update_task_meta(task_id,
                                        enhanced_note=f"翻译质检未通过（{review.score:.0f} 分），"
                                                      f"增强流程已放行并保留在视频原目录")
                    released = True
                else:
                    archived = self._archive_review_failed(task_id, video_path, target_srt, "translated")
                    db.create_subtitle(task_id, video_path, archived, "translated",
                                       status="needs_review", lang=cfg["target_lang"], score=review.score,
                                       report={"issues": review.issues[:20], "suggestions": review.suggestions[:10]})
                    self._fail(task_id, "翻译质检未通过",
                               suggestion=("翻译字幕已归档到「质检未通过字幕目录」" if archived != target_srt
                                           else "翻译字幕已保存在视频同目录") + "，可人工修改后重新入队")
                    return
            # 落盘最终字幕
            from .pipeline.srt_utils import write_srt
            write_srt(segments, target_srt)
            # 质检通过（后续润色完工）：不写质检建议，仅失败字幕保留
            if not released:
                db.create_subtitle(task_id, video_path, target_srt, "translated",
                                   status="ok", lang=cfg["target_lang"], score=review.score,
                                   report={"passed": True})
            # v1.6.9: 润色作为流水线最后一步（仅对译文润色，不覆盖原翻译）
            if cfg.get("polish_enabled", False):
                self._polish_step(task_id, video_path, cfg, segments)
        finally:
            self._exit_model_group("review")
        db.update_task(task_id, status="done", stage="done", progress=100,
                       error="")
        self._emit("task_done", {"id": task_id, "status": "done"})
        llm_manager.unload_all()

    def _translate_from_transcript(self, task_id, video_path, target_srt, cfg):
        """staged 翻译阶段：读取已转录字幕，直接翻译。"""
        try:
            from pathlib import Path
            transcript_srt = str(Path(video_path).with_suffix(".transcript.srt"))
            from .pipeline.srt_utils import load_srt
            segments = load_srt(transcript_srt)
            if not segments:
                self._fail(task_id, "转录结果文件缺失，无法翻译",
                          suggestion="请重试该任务（重新转录）")
                return
            src_lang = db.task_meta(task_id).get("detected_lang") or cfg["source_lang"]
            self._translate_core(task_id, video_path, target_srt, cfg, segments, src_lang)
        except Exception as e:
            logger.exception("staged 翻译阶段失败")
            self._fail_with_advice(task_id, str(e))

    def _translate_only(self, task_id, video_path, target_srt, cfg):
        """仅翻译已有外文字幕（跳过转录）。"""
        from .pipeline.srt_utils import load_srt, write_srt
        self._set_stage(task_id, "translating", 10)
        ext_srt = _find_existing_subtitle(video_path)
        if not ext_srt:
            self._fail_with_advice(task_id, "未找到可翻译的外文字幕（translate_only 模式）")
            return
        segments = load_srt(ext_srt)
        if not segments:
            self._fail_with_advice(task_id, "字幕文件为空")
            return
        # 翻译阶段占用 translate 模型组（与 asr/review 互斥）
        self._enter_model_group("translate")
        try:
            llm_manager.prepare_for("translate")
            self._enter_stage("translating")
            try:
                res = translate.translate_segments(
                    segments, src_lang="auto", tgt_lang=cfg["target_lang"],
                    progress_cb=lambda p: self._emit_progress(task_id, p * 0.95 + 5),
                    cancel_check=lambda: self._interrupt_check(task_id),
                )
            finally:
                self._exit_stage("translating")
        finally:
            self._exit_model_group("translate")
        # 校对 + 润色（review 模型组）
        self._enter_model_group("review")
        try:
            # 环节切换：卸载翻译模型（Sakura），让位给校对模型（review/llm 模型懒加载）
            llm_manager.prepare_for("polish")
            review = quality_check.review_translation(segments, res.get("model_name", cfg["llm_model"]))
            write_srt(segments, target_srt)
            # 通过则不留质检建议，未通过才保留
            db.create_subtitle(task_id, video_path, target_srt, "translated",
                               status="ok" if review.passed else "needs_review",
                               lang=cfg["target_lang"], score=review.score,
                               report=({"passed": True} if review.passed else
                                       {"issues": review.issues[:20],
                                        "suggestions": review.suggestions[:10]}))
            # v1.6.9: 润色作为流水线最后一步（仅对译文润色，不覆盖原翻译）
            if cfg.get("polish_enabled", False):
                self._polish_step(task_id, video_path, cfg, segments)
        finally:
            self._exit_model_group("review")
        db.update_task(task_id, status="done", stage="done", progress=100, error="")
        self._emit("task_done", {"id": task_id, "status": "done"})
        llm_manager.unload_all()

    # ---------- 一键只转录（v1.7.0） ----------
    def _transcribe_only(self, task_id, video_path, cfg):
        """一键扫描只转录：提取音频 → 转录 → 质检 → 落盘 .transcript.srt，不翻译。"""
        from pathlib import Path
        from .pipeline.srt_utils import write_srt

        # 1) 提取音频
        self._set_stage(task_id, "extracting", 1)
        wav = _wav_path(video_path, task_id)
        extract_audio.extract_audio(
            video_path, wav,
            progress_cb=lambda p: self._emit_progress(task_id, _overall("extracting", p)))
        duration = extract_audio.probe_duration(video_path)

        # 2) 转录（套阶段并发门，受 transcribe_workers 限制）
        self._set_stage(task_id, "transcribing", 6)
        self._enter_stage("transcribing")
        self._enter_model_group("asr")
        ov_model, ov_compute = self._take_asr_override(task_id)
        try:
            self._pre_transcribe_unload(task_id, ov_model, ov_compute)
            result = transcribe.transcribe(
                wav,
                progress_cb=lambda p: self._emit_progress(task_id, _overall("transcribing", p)),
                cancel_check=lambda: self._interrupt_check(task_id),
                asr_model=ov_model,
                compute_type=ov_compute,
            )
        finally:
            self._exit_model_group("asr")
            self._exit_stage("transcribing")
        segments = result["segments"]
        if not segments:
            self._fail_with_advice(task_id, "转录结果为空（视频可能无语音或音频异常）")
            return
        db.update_task_meta(task_id, duration=duration,
                            detected_lang=result["language"], asr_model=result["model"],
                            asr_device=result["device"])
        # 转录字幕落盘到视频同目录（.transcript.srt，供后续只翻译扫描识别）
        transcript_srt = str(Path(video_path).with_suffix(".transcript.srt"))
        write_srt(segments, transcript_srt)

        # 3) 转录质检
        self._set_stage(task_id, "checking", 56)
        check = quality_check.check_transcript(segments, duration, result["language"])
        db.update_task_meta(task_id, transcript_score=check.score,
                            transcribe_tokens_per_sec=round(result.get("tokens_per_sec", 0), 1),
                            transcribe_time_s=round(result.get("elapsed", 0), 1),
                            source_chars=result.get("chars", 0))
        db.create_subtitle(task_id, video_path, transcript_srt, "transcript",
                           status="ok" if check.passed else "needs_review",
                           lang=result["language"], score=check.score,
                           report={"issues": check.issues[:20], "suggestions": check.suggestions[:10]})
        db.update_task(task_id, status="done", stage="done", progress=100, error="")
        self._emit("task_done", {"id": task_id, "status": "done"})
        llm_manager.unload_all()

    # ---------- 润色（v1.6.9） ----------
    def _review_hints(self, task_id):
        """收集本任务质检未通过字幕的 issues/suggestions，作为润色 prompt 的修复指引。

        质检通过的字幕不再记录建议，因此正常情况下返回空列表；
        只有存在 needs_review 记录时才注入，避免对已通过内容做多余改写。
        """
        hints = []
        try:
            for row in db.get_subtitles(task_id) or []:
                if row["status"] != "needs_review":
                    continue
                rep = row["report"]
                try:
                    rep = json.loads(rep) if isinstance(rep, str) else (rep or {})
                except Exception:
                    continue
                if not isinstance(rep, dict):
                    continue
                for key in ("issues", "suggestions"):
                    for item in (rep.get(key) or []):
                        t = str(item).strip()
                        if t and t not in hints:
                            hints.append(t)
        except Exception:
            logger.debug("收集质检建议失败（忽略，按无建议润色）", exc_info=True)
        return hints[:12]

    def _polish_step(self, task_id, video_path, cfg, segments):
        """流水线最后一步：把校对后的译文润色为地道中文，输出 .zh.rs.srt 与原翻译并存。"""
        if not segments:
            return
        self._set_stage(task_id, "polishing", 98)
        self._enter_stage("polishing")
        self._enter_model_group("review")
        # 环节切换：卸载翻译/质检等非润色模型，让位给润色模型（llama-swap 懒加载目标模型）
        llm_manager.prepare_for("polish")
        hints = self._review_hints(task_id)
        try:
            res = polish.polish_segments(
                segments,
                progress_cb=lambda p: self._emit_progress(task_id, _overall("polishing", p)),
                cancel_check=lambda: self._interrupt_check(task_id),
                hints=hints,
            )
        except Exception as e:
            logger.warning("润色失败（保留原翻译，任务仍完成）: %s", e)
            return
        finally:
            self._exit_model_group("review")
            self._exit_stage("polishing")
        db.update_task_meta(task_id,
                            polish_model=res.get("model_name", ""),
                            polish_tokens_in=res.get("tokens_in", 0),
                            polish_tokens_out=res.get("tokens_out", 0),
                            polish_tokens_per_sec=round(res.get("tokens_per_sec", 0), 2),
                            polish_filled=res.get("filled", 0))
        from .pipeline.srt_utils import write_srt
        rs_srt = _polish_path(video_path, cfg.get("subtitle_suffix", ".zh"))
        write_srt(segments, rs_srt)
        db.create_subtitle(task_id, video_path, rs_srt, "polished",
                           status="ok", lang=cfg.get("target_lang", "zh"),
                           score=100, report={"filled": res.get("filled", 0),
                                              "total": res.get("total", 0)})

    def _polish_only(self, task_id, srt_path, cfg):
        """一键扫描润色：仅润色一个已有中文字幕文件，输出 .zh.rs.srt 并存。"""
        from .pipeline.srt_utils import load_srt, write_srt
        rs_srt = _polish_path(srt_path, cfg.get("subtitle_suffix", ".zh"))
        if _file_exists(rs_srt):
            db.update_task(task_id, status="done", stage="skipped", progress=100,
                           error="已有润色字幕，跳过")
            self._emit("task_done", {"id": task_id, "status": "done", "note": "skip_existing"})
            llm_manager.unload_all()
        if not _file_exists(srt_path):
            self._fail(task_id, "字幕文件不存在", suggestion="可能已被移动/删除")
            return
        segments = load_srt(srt_path)
        if not segments:
            self._fail(task_id, "字幕文件为空或解析失败")
            return
        db.update_task_meta(task_id, polish_total=len(segments))
        self._set_stage(task_id, "polishing", 1)
        self._enter_model_group("review")
        try:
            # 环节切换：卸载翻译/质检等非润色模型，让位给润色模型（llama-swap 懒加载目标模型）
            llm_manager.prepare_for("polish")
            hints = self._review_hints(task_id)
            try:
                res = polish.polish_segments(
                    segments,
                    progress_cb=lambda p: self._emit_progress(task_id, p),
                    cancel_check=lambda: self._interrupt_check(task_id),
                    hints=hints,
                )
            except Exception as e:
                logger.exception("一键润色失败")
                self._fail_with_advice(task_id, f"润色失败: {e}")
                return
        finally:
            self._exit_model_group("review")
        db.update_task_meta(task_id,
                            polish_model=res.get("model_name", ""),
                            polish_tokens_in=res.get("tokens_in", 0),
                            polish_tokens_out=res.get("tokens_out", 0),
                            polish_tokens_per_sec=round(res.get("tokens_per_sec", 0), 2),
                            polish_filled=res.get("filled", 0))
        write_srt(segments, rs_srt)
        db.create_subtitle(task_id, srt_path, rs_srt, "polished",
                           status="ok", lang=cfg.get("target_lang", "zh"),
                           score=100, report={"filled": res.get("filled", 0),
                                              "total": res.get("total", 0)})
        db.update_task(task_id, status="done", stage="done", progress=100, error="")
        self._emit("task_done", {"id": task_id, "status": "done"})
        llm_manager.unload_all()

    # ---------- 工具方法 ----------
    def _archive_review_failed(self, task_id, video_path, srt_path, kind):
        """质检未通过的字幕 → 转移到设置指定的「质检未通过字幕目录」。

        支持跨盘符/跨挂载点（同设备 rename，跨设备 copy2+unlink 由 shutil.move 自动处理）。
        目录结构: <归档根>/<视频名>/<字幕文件名>，同名视频互不覆盖。
        返回归档后的路径（未配置或失败时返回原路径）。
        """
        import shutil
        from pathlib import Path as _P3
        if not db.get_setting("review_archive_enabled", True):
            return srt_path
        dest_root = str(db.get_setting("review_failed_dir") or "").strip()
        if not dest_root:
            return srt_path
        try:
            os.makedirs(dest_root, exist_ok=True)
            tgt_dir = os.path.join(dest_root, _P3(video_path).stem)
            os.makedirs(tgt_dir, exist_ok=True)
            dst = os.path.join(tgt_dir, os.path.basename(srt_path))
            if os.path.abspath(srt_path) == os.path.abspath(dst):
                return srt_path
            if os.path.exists(srt_path):
                if os.path.exists(dst):
                    os.remove(dst)
                shutil.move(srt_path, dst)
            db.update_task_meta(task_id, review_archive_dir=tgt_dir)
            logger.info("质检未通过字幕已归档: %s → %s", srt_path, dst)
            return dst
        except Exception as e:
            logger.warning("归档质检未通过字幕失败(%s): %s", kind, e)
            return srt_path

    def _set_stage(self, task_id, stage, base_progress):
        db.update_task(task_id, status=stage, stage=stage, progress=base_progress)
        self._emit("task_update", {"id": task_id, "status": stage, "progress": base_progress})

    def _fail(self, task_id, error, suggestion=""):
        db.update_task(task_id, status="failed", error=error[:500], stage="failed")
        self._emit("task_failed", {"id": task_id, "error": error, "suggestion": suggestion})

    def _fail_with_advice(self, task_id, error):
        """失败时探测设备能力，给出『当前设备能否成功』判定与建议。"""
        advice = device_probe.advise_for_failure(error)
        db.update_task_meta(task_id, device_advice=advice)
        self._fail(task_id, error, suggestion=advice.get("suggestion", ""))


def _overall(stage, inner_progress):
    """阶段内进度换算为整体百分比（0~100，与前端进度条、_set_stage 基数同一尺度）。

    STAGE_WEIGHTS 存的是比例（0.06/0.56 ...），此处统一乘 100 输出百分比；
    否则转录/翻译期间整体进度只有 0.0x~0.5x，前端按 'xx%' 显示后几乎不动。
    """
    lo, hi = STAGE_WEIGHTS[stage]
    return round((lo + (hi - lo) * inner_progress / 100) * 100, 2)


def _file_exists(p):
    import os
    return os.path.exists(p)


def _wav_path(video_path, task_id):
    import os
    return os.path.join(config.DATA_DIR, "audio", f"task_{task_id}.wav")


def _srt_path(video_path, suffix):
    from pathlib import Path
    return str(Path(video_path).with_suffix(suffix + ".srt"))


def _polish_path(video_path, suffix):
    """润色产物路径：xxx.mkv → xxx.zh.rs.srt；xxx.zh.srt → xxx.zh.rs.srt（不叠加后缀）。"""
    from pathlib import Path
    p = Path(video_path)
    suffix = suffix or ".zh"
    name = p.name
    # 传入的已是该语言字幕（如 xxx.zh.srt）：先剥离语言后缀，避免生成 xxx.zh.zh.rs.srt
    if name.lower().endswith((suffix + ".srt").lower()):
        base = name[: -len(suffix + ".srt")]
        return str(p.with_name(base + suffix + ".rs.srt"))
    return str(p.with_suffix(suffix + ".rs.srt"))


def scan_polish_jobs(directory):
    """一键扫描：找 directory 下所有待润色的中文字幕（.zh.srt 且无对应 .zh.rs.srt），批量入队。

    返回 (enqueued, skipped, total)：入队数 / 已有润色跳过数 / 中文字幕总数。
    """
    from pathlib import Path
    target_suffix = db.get_setting("subtitle_suffix")
    rs_suffix = target_suffix + ".rs"
    total = enqueued = skipped = 0
    for srt in sorted(Path(directory).rglob("*.srt")):
        name = srt.name.lower()
        if not name.endswith(target_suffix + ".srt") or name.endswith(rs_suffix + ".srt"):
            continue
        total += 1
        rs = srt.with_suffix(rs_suffix + ".srt")
        if rs.exists():
            skipped += 1
            continue
        if _file_exists(str(srt)):
            if db.get_task_by_path(str(srt), active_only=True):
                skipped += 1
                continue
            task_id = db.create_task(str(srt), srt.name)
            db.update_task_meta(task_id, source="scan_polish", mode="polish")
            if not db.get_setting("queue_paused", False):
                smart_queue.submit(task_id)
            smart_queue._emit("task_new", {"id": task_id, "path": str(srt),
                                           "name": srt.name})
            enqueued += 1
    return enqueued, skipped, total


def scan_transcribe_jobs(directory):
    """一键扫描只转录：找 directory 下所有尚无转录字幕的视频，只转录不翻译。

    返回 (enqueued, skipped, total)：入队数 / 已跳过数 / 视频总数。
    """
    from pathlib import Path
    from .watcher import is_video_file
    total = enqueued = skipped = 0
    for f in sorted(Path(directory).rglob("*")):
        if not f.is_file() or not is_video_file(str(f), db.get_setting("video_exts")):
            continue
        total += 1
        # 已有转录字幕 / 已有中文字幕 / 进行中任务 → 跳过
        if _file_exists(str(Path(f).with_suffix(".transcript.srt"))):
            skipped += 1
            continue
        if _file_exists(_srt_path(str(f), db.get_setting("subtitle_suffix"))):
            skipped += 1
            continue
        if db.get_task_by_path(str(f), active_only=True):
            skipped += 1
            continue
        task_id = db.create_task(str(f), f.name)
        db.update_task_meta(task_id, source="scan_transcribe", mode="transcribe_only")
        if not db.get_setting("queue_paused", False):
            smart_queue.submit(task_id)
        smart_queue._emit("task_new", {"id": task_id, "path": str(f), "name": f.name})
        enqueued += 1
    return enqueued, skipped, total


def scan_translate_jobs(directory):
    """一键扫描转录字幕只翻译：找 directory 下已有外文字幕（任意语言）的视频，只翻译不转录。

    返回 (enqueued, skipped, total)：入队数 / 已跳过数 / 视频总数。
    """
    from pathlib import Path
    from .watcher import is_video_file
    total = enqueued = skipped = 0
    for f in sorted(Path(directory).rglob("*")):
        if not f.is_file() or not is_video_file(str(f), db.get_setting("video_exts")):
            continue
        total += 1
        # 已有目标中文字幕 → 跳过
        if _file_exists(_srt_path(str(f), db.get_setting("subtitle_suffix"))):
            skipped += 1
            continue
        ext_srt = _find_existing_subtitle(str(f))
        if not ext_srt:
            skipped += 1
            continue
        if db.get_task_by_path(str(f), active_only=True):
            skipped += 1
            continue
        task_id = db.create_task(str(f), f.name)
        db.update_task_meta(task_id, source="scan_translate", mode="translate_only",
                            translate_note=f"已有外文字幕 {Path(ext_srt).name}")
        if not db.get_setting("queue_paused", False):
            smart_queue.submit(task_id)
        smart_queue._emit("task_new", {"id": task_id, "path": str(f), "name": f.name})
        enqueued += 1
    return enqueued, skipped, total


def _find_existing_subtitle(video_path):
    """查找视频同目录下的外文字幕（非目标语言后缀）。

    优先选择明确的日文/外文字幕（含 .ja/.jp/.en 或同名无后缀），
    排除中文字幕与容器生成的目标字幕。
    """
    import re as _re
    from pathlib import Path
    p = Path(video_path)
    target = _srt_path(video_path, db.get_setting("subtitle_suffix"))
    candidates = sorted(p.parent.glob(p.stem + "*.srt"))
    # 排除中文与目标后缀
    zh_re = _re.compile(r"\.(zh|zhs|chs|cht|zh-hans|zh-hant)\.", _re.I)
    cands = [c for c in candidates if str(c) != target and not zh_re.search(c.name)]
    if not cands:
        return None
    # 优先级：含 ja/jp/en 的 → 同名无后缀 → 其他
    def rank(c):
        n = c.name.lower()
        if ".ja." in n or ".jp." in n or n.endswith(".ja.srt"):
            return 0
        if ".en." in n or n.endswith(".en.srt"):
            return 1
        if c.stem == p.stem:
            return 2
        return 3
    return str(sorted(cands, key=rank)[0])


smart_queue = SmartQueue()


def enqueue_path(video_path: str, source: str = "manual"):
    """入队一个新视频：去重 + 创建任务 + 提交队列。返回 task_id 或 None。

    扫描分类（v1.4.8）：
    - 已有中文字幕 → skip（不入队）
    - 有外文字幕（如日语）→ 任务标记 translate_only，只翻译不转录
    - 无字幕 → 完整流水线（转录→质检→翻译→质检）
    """
    from .watcher import should_skip, classify_video
    if not _file_exists(video_path):
        return None
    if should_skip(video_path):
        return None
    action, reason = classify_video(video_path)
    if action == "skip":
        return None
    existing = db.get_task_by_path(video_path, active_only=True)
    if existing:
        return existing["id"]
    from pathlib import Path
    task_id = db.create_task(video_path, Path(video_path).name)
    meta = {"source": source, "mode": action,
            "duration": round(extract_audio.probe_duration(video_path), 1)}  # 电影总时长（秒）
    if action == "translate_only":
        meta["translate_note"] = reason
    db.update_task_meta(task_id, **meta)
    # 队列暂停时不提交（等待恢复后统一处理）；否则正常入队
    if not db.get_setting("queue_paused", False):
        smart_queue.submit(task_id)
    smart_queue._emit("task_new", {"id": task_id, "path": video_path, "name": Path(video_path).name})
    return task_id
