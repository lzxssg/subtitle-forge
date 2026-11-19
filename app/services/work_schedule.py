"""工作时段调度（v1.1.16）：给任务队列加「每日工作窗口 + 循环休息」的时间闸门。

解决两个真实痛点：
1. 显卡长期 7×24 跑 ASR/翻译，累计热应力对焊点、供电、风扇都不友好；
   按节奏回落到待机温度，比一直恒温更利于延长寿命。
2. 居民峰谷电价（如江西 22:00-次日 08:00 为谷段）内运行，电费更省。

设计原则：
- 只管「自己关的灯」：只有本模块暂停的队列才会被自动恢复；
  用户手动点「暂停队列」不会被它偷偷放开。
- 幂等 tick：每 30 秒算一次「此刻应该在跑还是该停」，
  与数据库里的 queue_paused 比对，不一致才动手，重复执行无副作用。
- 跨夜窗口支持：`22:00-08:00` 视为同一个完整时段，相位从窗口起点开始累计。
"""
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from .. import database as db

logger = logging.getLogger("subtitle-forge.work_schedule")

TZ = timezone(timedelta(hours=8))  # Asia/Shanghai，容器内统一用固定偏移，避免 tzdata 依赖
TICK_SECONDS = 30


def _parse_hm(text: str):
    """'07:30' -> 450（分钟）。非法输入返回 None。"""
    try:
        hh, mm = str(text).strip().replace("：", ":").split(":")
        hh, mm = int(hh), int(mm)
        if 0 <= hh <= 24 and 0 <= mm < 60:
            if hh == 24 and mm != 0:
                return None
            return hh * 60 + mm
    except Exception:
        pass
    return None


def parse_windows(raw) -> list:
    """把配置解析成 [(start_min, end_min), ...]。

    支持三种写法：
    - JSON 数组：`[["22:00","08:00"], ["12:00","14:00"]]`
    - 逗号/分号分隔：`22:00-08:00,12:00-14:00`
    - 纯文本多行（前端 textarea）：每行一个 `22:00-08:00`
    起止相同或解析失败一律忽略；空列表表示「全天允许」。
    """
    windows = []
    if isinstance(raw, (list, tuple)):
        items = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return []
        if text.startswith("["):
            import json
            try:
                items = json.loads(text)
            except Exception:
                items = []
        else:
            items = [x for x in text.replace("\n", ",").replace("；", ",").replace("，", ",")
                     .replace(";", ",").split(",") if x.strip()]
    for item in items:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            s, e = _parse_hm(item[0]), _parse_hm(item[1])
        else:
            part = str(item).strip().replace("~", "-").replace("—", "-")
            if "-" not in part:
                continue
            s_text, e_text = part.split("-", 1)
            s, e = _parse_hm(s_text), _parse_hm(e_text)
        if s is None or e is None:
            continue
        if s == e:
            continue  # 起止相同视为无效，避免 quer 语义歧义
        windows.append((s % 1440, e % 1440))
    return windows


def _in_window(minute: int, windows: list):
    """返回 (是否在窗口内, 所属窗口索引)。空窗口列表视为全天允许。"""
    if not windows:
        return True, -1
    for idx, (start, end) in enumerate(windows):
        if start < end:
            if start <= minute < end:
                return True, idx
        else:  # 跨夜，如 22:00-08:00
            if minute >= start or minute < end:
                return True, idx
    return False, -2


def _window_key(dt: datetime, idx: int, windows: list) -> str:
    """给「当前所在窗口」一个稳定标识，用于判断是否进入了新的一轮窗口。

    跨夜窗口在午夜前后必须算同一个 key，否则 00:00 一到相位会被重置。
    """
    if idx < 0:
        return f"all-{dt.date().isoformat()}"
    start, end = windows[idx]
    if start > end and dt.hour * 60 + dt.minute < end:
        anchor = (dt - timedelta(days=1)).date()  # 凌晨属于前一天开始的跨夜窗口
    else:
        anchor = dt.date()
    return f"w{idx}-{anchor.isoformat()}"


class WorkScheduler:
    def __init__(self):
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        # 相位状态
        self._window_key = None
        self._seg_start = 0.0          # 本段连续工作起点
        self._rest_until = 0.0         # 本轮休息结束时刻
        # 归属标记：True 表示队列是「本模块暂停的」，False 表示用户自己暂停的
        self._owned_pause = False
        self._last_state = "unknown"
        self._last_reason = ""
        self._last_tick = 0.0

    # ---------- 生命周期 ----------
    def start(self, interval: int = TICK_SECONDS):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._interval = max(10, int(interval or TICK_SECONDS))
        self._thread = threading.Thread(target=self._loop, name="work-schedule", daemon=True)
        self._thread.start()
        logger.info("工作时段调度器已启动（tick=%ss）", self._interval)

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as e:
                logger.error("工作时段调度异常: %s", e, exc_info=True)
            self._stop.wait(self._interval)

    # ---------- 配置 ----------
    @staticmethod
    def load_config() -> dict:
        def _bool(key, default=False):
            v = db.get_setting(key, default)
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes", "on")

        def _int(key, default, lo, hi):
            try:
                return max(lo, min(hi, int(db.get_setting(key, default) or default)))
            except (TypeError, ValueError):
                return default

        return {
            "enabled": _bool("work_schedule_enabled", False),
            "windows": parse_windows(db.get_setting("work_schedule_windows", "") or ""),
            "work_minutes": _int("work_schedule_work_minutes", 180, 0, 24 * 60),
            "rest_minutes": _int("work_schedule_rest_minutes", 30, 0, 12 * 60),
            "unload_on_rest": _bool("work_schedule_unload_on_rest", True),
        }

    # ---------- 单次决策 ----------
    def _plan(self, now: float, cfg: dict) -> dict:
        """纯逻辑：算出此刻应「运行」还是「暂停」，以及原因和下次切换时间。"""
        dt = datetime.fromtimestamp(now, TZ)
        minute = dt.hour * 60 + dt.minute
        inside, idx = _in_window(minute, cfg["windows"])
        next_change = None
        reason = ""

        if not inside:
            # 下一个窗口开始时刻
            if cfg["windows"]:
                best = None
                for start, _end in cfg["windows"]:
                    delta = (start - minute) % 1440
                    if delta == 0:
                        delta = 1440
                    if best is None or delta < best:
                        best = delta
                if best is not None:
                    next_change = now + best * 60
            return {"run": False, "reason": "off_window", "next_change": next_change}

        key = _window_key(dt, idx, cfg["windows"])
        if key != self._window_key:
            # 进入新窗口：相位归零
            self._window_key = key
            self._seg_start = now
            self._rest_until = 0.0

        if self._rest_until > now:
            return {"run": False, "reason": "cycle_rest", "next_change": self._rest_until}

        work_sec = cfg["work_minutes"] * 60
        if cfg["work_minutes"] > 0 and cfg["rest_minutes"] > 0 and (now - self._seg_start) >= work_sec:
            # 工作时长到点，进入休息
            self._rest_until = now + cfg["rest_minutes"] * 60
            self._seg_start = self._rest_until
            logger.info("连续工作 %d 分钟，进入 %d 分钟休息", cfg["work_minutes"], cfg["rest_minutes"])
            return {"run": False, "reason": "cycle_rest", "next_change": self._rest_until}

        # 下次切换点：工作段到点 / 当前窗口结束
        candidates = []
        if cfg["work_minutes"] > 0 and cfg["rest_minutes"] > 0:
            candidates.append(self._seg_start + work_sec)
        if idx >= 0:
            _start, end = cfg["windows"][idx]
            end_dt = dt.replace(hour=end // 60 % 24, minute=end % 60, second=0, microsecond=0)
            if end_dt <= dt:
                end_dt += timedelta(days=1)
            candidates.append(end_dt.timestamp())
        if candidates:
            next_change = min(candidates)
        return {"run": True, "reason": "", "next_change": next_change}

    def tick(self) -> dict:
        """执行一次决策。返回状态快照（供 API 展示）。"""
        from ..queue_manager import smart_queue

        cfg = self.load_config()
        now = time.time()
        with self._lock:
            if not cfg["enabled"]:
                if self._owned_pause and bool(db.get_setting("queue_paused")):
                    try:
                        smart_queue.resume_all(force_unload=False)
                        logger.info("工作时段调度已关闭，恢复此前由调度暂停的队列")
                    except Exception as e:
                        logger.warning("恢复队列失败: %s", e)
                self._owned_pause = False
                self._last_state, self._last_reason = "disabled", ""
                self._last_tick = now
                return self.status()

            plan = self._plan(now, cfg)
            cur_paused = bool(db.get_setting("queue_paused"))

            if plan["run"]:
                if not cur_paused and self._owned_pause:
                    # 用户手动恢复了队列：交出所有权，避免下一 tick 又被调度暂停
                    self._owned_pause = False
                if cur_paused and self._owned_pause:
                    try:
                        smart_queue.resume_all(force_unload=True)
                        logger.info("进入工作时段，恢复队列（含模型重载）")
                    except Exception as e:
                        logger.warning("恢复队列失败: %s", e)
                    self._owned_pause = False
                elif cur_paused:
                    # 用户手动暂停 → 尊重用户，不自动放开
                    self._last_state, self._last_reason = "manual_paused", ""
                    self._last_tick = now
                    return self.status()
                self._last_state, self._last_reason = "running", ""
            else:
                if not cur_paused:
                    try:
                        n = smart_queue.pause_all()
                        self._owned_pause = True
                        logger.info("工作时段调度暂停队列（%s），挂起 %d 个任务", plan["reason"], n)
                    except Exception as e:
                        logger.warning("暂停队列失败: %s", e)
                elif not self._owned_pause:
                    # 已经是暂停态但归用户所有，保持不动
                    self._last_state, self._last_reason = "manual_paused", ""
                    self._last_tick = now
                    return self.status()
                else:
                    self._owned_pause = True
                self._last_state, self._last_reason = (
                    "off_window" if plan["reason"] == "off_window" else "cycle_rest"), plan["reason"]

                if cfg["unload_on_rest"] and plan["reason"] == "cycle_rest":
                    self._schedule_unload()

            self._last_tick = now
            self._last_next_change = plan.get("next_change")
            return self.status()

    def _schedule_unload(self):
        """休息时卸载驻留的大模型，进一步降低功耗与温度；延迟 60s 等任务安全挂起。"""
        def _do():
            try:
                from . import llm_manager
                llm_manager.unload_all()
                logger.info("周期休息：已请求卸载驻留大模型")
            except Exception as e:
                logger.warning("休息卸载模型失败（忽略）: %s", e)
        threading.Timer(60.0, _do).start()

    # ---------- 状态 ----------
    def status(self) -> dict:
        cfg = self.load_config()
        now = time.time()
        next_change = getattr(self, "_last_next_change", None)
        dt = datetime.fromtimestamp(now, TZ)
        minute = dt.hour * 60 + dt.minute
        inside, _idx = _in_window(minute, cfg["windows"])
        # 主动算一次下次切换，供前端展示（不改相位）
        if cfg["enabled"] and next_change is None:
            if not inside and cfg["windows"]:
                best = min(((s - minute) % 1440) or 1440 for s, _e in cfg["windows"])
                next_change = now + best * 60
            elif inside and cfg["rest_minutes"] > 0 and cfg["work_minutes"] > 0:
                if self._rest_until > now:
                    next_change = self._rest_until
                else:
                    next_change = self._seg_start + cfg["work_minutes"] * 60
        return {
            "enabled": cfg["enabled"],
            "windows": [f"{s // 60:02d}:{s % 60:02d}-{e // 60:02d}:{e % 60:02d}" for s, e in cfg["windows"]],
            "work_minutes": cfg["work_minutes"],
            "rest_minutes": cfg["rest_minutes"],
            "unload_on_rest": cfg["unload_on_rest"],
            "state": self._last_state,
            "reason": self._last_reason,
            "owned_pause": self._owned_pause,
            "queue_paused": bool(db.get_setting("queue_paused")),
            "seg_start": self._seg_start,
            "rest_until": self._rest_until,
            "next_change_at": next_change,
            "next_change_text": datetime.fromtimestamp(next_change, TZ).strftime("%m-%d %H:%M") if next_change else "",
            "now": now,
        }


work_schedule = WorkScheduler()
