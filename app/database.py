"""SQLite 存储层：任务表、字幕表、设置表。"""
import json
import sqlite3
import threading
import time
from contextlib import contextmanager

from . import config

_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_path TEXT NOT NULL,
    video_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending/extracting/transcribing/checking/translating/checking_translation/done/failed/cancelled
    stage TEXT NOT NULL DEFAULT '',
    progress REAL NOT NULL DEFAULT 0,          -- 0-100
    error TEXT DEFAULT '',
    meta TEXT DEFAULT '{}',                    -- json: duration, model, tokens, quality...
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_path ON tasks(video_path);

CREATE TABLE IF NOT EXISTS subtitles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    video_path TEXT NOT NULL,
    srt_path TEXT NOT NULL,
    kind TEXT NOT NULL,                        -- transcript / translated
    status TEXT NOT NULL DEFAULT 'ok',         -- ok / needs_review / failed
    lang TEXT DEFAULT '',
    score REAL DEFAULT 0,
    report TEXT DEFAULT '{}',
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


_local = threading.local()
_settings_cache = {}
_settings_lock = threading.Lock()


def get_conn():
    """线程内复用连接，避免高频写入（进度/统计）反复建连与设置 PRAGMA 的开销。"""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        return conn
    conn = sqlite3.connect(str(config.DB_PATH), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # WAL 下 NORMAL 只在 checkpoint 时 fsync，写入吞吐明显优于默认 FULL，且不会损坏库
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA cache_size=-16384")   # 16MB 页缓存
    conn.execute("PRAGMA temp_store=MEMORY")
    _local.conn = conn
    return conn


def close_conn():
    """关闭当前线程连接（供退出/测试使用）。"""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


def invalidate_settings_cache():
    with _settings_lock:
        _settings_cache.clear()


@contextmanager
def db():
    conn = get_conn()
    try:
        with _lock:
            yield conn
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def init_db():
    with db() as conn:
        conn.executescript(SCHEMA)
        for k, v in config.DEFAULTS.items():
            conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                         (k, json.dumps(v, ensure_ascii=False)))
        _migrate(conn)


def _migrate(conn):
    """历史配置迁移（幂等）：旧键 → 新键，保留用户已选值。"""
    pairs = [
        ("error_dir", "review_failed_dir"),
        ("error_archive_enabled", "review_archive_enabled"),
    ]
    for old, new in pairs:
        try:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (old,)).fetchone()
            if row:
                conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (new, row["value"]))
                conn.execute("DELETE FROM settings WHERE key=?", (old,))
        except Exception:
            pass


def get_setting(key, default=None):
    """读取配置（带进程内缓存）。配置写少读多，缓存可消除每任务/每请求的重复查询。"""
    with _settings_lock:
        if key in _settings_cache:
            return _settings_cache[key]
    try:
        with db() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if not row:
            val = default if default is not None else config.DEFAULTS.get(key)
        else:
            try:
                val = json.loads(row["value"])
            except Exception:
                val = row["value"]
        with _settings_lock:
            _settings_cache[key] = val
        return val
    except Exception:
        # 启动早期表尚未初始化：返回默认值，避免模块导入阶段崩溃
        if default is not None:
            return default
        return config.DEFAULTS.get(key)


def set_setting(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False)))
    with _settings_lock:
        _settings_cache.pop(key, None)


def get_all_settings():
    with db() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    out = {}
    for r in rows:
        try:
            out[r["key"]] = json.loads(r["value"])
        except Exception:
            out[r["key"]] = r["value"]
    for k, v in config.DEFAULTS.items():
        out.setdefault(k, v)
    with _settings_lock:
        _settings_cache.update(out)
    return out


def create_task(video_path: str, video_name: str):
    now = time.time()
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO tasks(video_path, video_name, status, progress, created_at, updated_at) "
            "VALUES (?, ?, 'pending', 0, ?, ?)",
            (video_path, video_name, now, now))
        return cur.lastrowid


def update_task(task_id: int, **fields):
    fields["updated_at"] = time.time()
    cols = ", ".join(f"{k}=?" for k in fields)
    with db() as conn:
        conn.execute(f"UPDATE tasks SET {cols} WHERE id=?", (*fields.values(), task_id))


def get_task(task_id: int):
    with db() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()


def get_task_by_path(video_path: str, active_only=False):
    with db() as conn:
        if active_only:
            return conn.execute(
                "SELECT * FROM tasks WHERE video_path=? AND status NOT IN ('done','failed','cancelled') ORDER BY id DESC LIMIT 1",
                (video_path,)).fetchone()
        return conn.execute(
            "SELECT * FROM tasks WHERE video_path=? ORDER BY id DESC LIMIT 1",
            (video_path,)).fetchone()


def list_tasks(limit=200, offset=0):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM tasks ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()


def count_tasks():
    with db() as conn:
        return conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]


def count_by_status(statuses):
    """统计指定状态集合的任务数。"""
    if not statuses:
        return 0
    ph = ",".join("?" * len(statuses))
    with db() as conn:
        return conn.execute(
            f"SELECT COUNT(*) FROM tasks WHERE status IN ({ph})", tuple(statuses)).fetchone()[0]


def active_task_paths():
    """所有进行中任务的视频路径集合（全量扫描时一次性判断，避免逐文件查库）。"""
    with db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT video_path FROM tasks "
            "WHERE status NOT IN ('done','failed','cancelled')").fetchall()
    return {r["video_path"] for r in rows}


def list_tasks_by_status(statuses, limit=5000):
    if not statuses:
        return []
    ph = ",".join("?" * len(statuses))
    with db() as conn:
        return conn.execute(
            f"SELECT * FROM tasks WHERE status IN ({ph}) ORDER BY id ASC LIMIT ?",
            tuple(statuses) + (limit,)).fetchall()


def delete_task(task_id: int):
    with db() as conn:
        conn.execute("DELETE FROM subtitles WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))


def clear_failed_tasks() -> int:
    """删除所有失败/已取消的任务记录（含关联字幕），保留成功记录。返回删除条数。"""
    with db() as conn:
        cur = conn.execute("DELETE FROM tasks WHERE status IN ('failed','cancelled')")
        conn.execute("DELETE FROM subtitles WHERE task_id NOT IN (SELECT id FROM tasks)")
        return cur.rowcount


def create_subtitle(task_id, video_path, srt_path, kind, status="ok", lang="", score=0, report=None):
    with db() as conn:
        conn.execute(
            "INSERT INTO subtitles(task_id, video_path, srt_path, kind, status, lang, score, report, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, video_path, srt_path, kind, status, lang, score,
             json.dumps(report or {}, ensure_ascii=False), time.time()))


def get_subtitles(task_id):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM subtitles WHERE task_id=? ORDER BY id", (task_id,)).fetchall()


def subtitles_map(task_ids, chunk=400):
    """批量取多个任务的字幕（一次查询，避免任务列表 N+1 查询）。"""
    ids = [int(t) for t in task_ids if t is not None]
    out = {}
    if not ids:
        return out
    with db() as conn:
        for i in range(0, len(ids), chunk):
            part = ids[i:i + chunk]
            ph = ",".join("?" * len(part))
            rows = conn.execute(
                f"SELECT * FROM subtitles WHERE task_id IN ({ph}) ORDER BY id", tuple(part)).fetchall()
            for r in rows:
                out.setdefault(r["task_id"], []).append(dict(r))
    return out


def list_needs_review_subtitles(limit=5000):
    """所有质检未通过（needs_review）的字幕记录。"""
    with db() as conn:
        return conn.execute(
            "SELECT * FROM subtitles WHERE status='needs_review' ORDER BY id DESC LIMIT ?",
            (limit,)).fetchall()


def delete_subtitle(subtitle_id: int):
    with db() as conn:
        conn.execute("DELETE FROM subtitles WHERE id=?", (subtitle_id,))


def task_stats():
    """任务统计面板：累计/今日/进行中/等待/完成/跳过/不支持/失败/取消。

    单条聚合查询取代原先 8 次 COUNT(*) 全表扫描。
    """
    now = time.time()
    day_start = time.mktime(time.localtime(now)[:3] + (0, 0, 0) + (-1, -1, -1))
    with db() as conn:
        r = conn.execute(
            "SELECT COUNT(*) AS total,"
            " SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS today,"
            " SUM(CASE WHEN status='done' AND created_at >= ? THEN 1 ELSE 0 END) AS today_done,"
            " SUM(CASE WHEN status IN ('pending','extracting','transcribing','checking','translating','checking_translation') THEN 1 ELSE 0 END) AS active,"
            " SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,"
            " SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,"
            " SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,"
            " SUM(CASE WHEN status='done' AND (stage='skipped' OR error LIKE '%跳过%') THEN 1 ELSE 0 END) AS skipped,"
            " SUM(CASE WHEN status='failed' AND error LIKE '%不支持%' THEN 1 ELSE 0 END) AS unsupported,"
            " SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS waiting"
            " FROM tasks", (day_start, day_start)).fetchone()
    keys = ("total", "today", "today_done", "active", "done",
            "failed", "cancelled", "skipped", "unsupported", "waiting")
    return {k: int(r[k] or 0) for k in keys}


def list_failed_tasks(limit=200):
    with db() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status='failed' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def watch_dir_list():
    """监控目录列表（watch_dirs 逗号分隔字符串 → list）。"""
    v = get_setting("watch_dirs")
    if isinstance(v, list):
        return v
    return [d.strip() for d in str(v or "").split(",") if d.strip()]


def add_watch_dir(path: str):
    dirs = watch_dir_list()
    if path not in dirs:
        dirs.append(path)
        set_setting("watch_dirs", ",".join(dirs))
    return dirs


def remove_watch_dir(path: str):
    dirs = watch_dir_list()
    dirs = [d for d in dirs if d != path]
    set_setting("watch_dirs", ",".join(dirs))
    return dirs


def allow_dir_list():
    """可访问文件夹列表（allow_dirs 逗号分隔字符串 → list）。"""
    v = get_setting("allow_dirs")
    if isinstance(v, list):
        return v
    return [d.strip() for d in str(v or "").split(",") if d.strip()]


def add_allow_dir(path: str):
    dirs = allow_dir_list()
    if path not in dirs:
        dirs.append(path)
        set_setting("allow_dirs", ",".join(dirs))
    return dirs


def remove_allow_dir(path: str):
    dirs = allow_dir_list()
    dirs = [d for d in dirs if d != path]
    set_setting("allow_dirs", ",".join(dirs))
    return dirs


def task_meta(task_id):
    with db() as conn:
        row = conn.execute("SELECT meta FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not row:
            return {}
        try:
            return json.loads(row["meta"] or "{}")
        except Exception:
            return {}


def update_task_meta(task_id, **kv):
    meta = task_meta(task_id)
    meta.update(kv)
    update_task(task_id, meta=json.dumps(meta, ensure_ascii=False))
