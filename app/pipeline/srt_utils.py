"""SRT 字幕解析与生成工具。"""
import re
from pathlib import Path

from ..models import Segment

TIME_RE = re.compile(
    r"(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})\s*-->\s*(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})"
)


def _to_seconds(h, m, s, ms):
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def _fmt_time(seconds: float) -> str:
    seconds = max(0, seconds)
    ms = int(round((seconds - int(seconds)) * 1000))
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def parse_srt(text: str) -> list:
    """解析 SRT 文本为 Segment 列表。"""
    segments = []
    blocks = re.split(r"\n\s*\n", text.strip())
    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip() != ""]
        if not lines:
            continue
        idx = 0
        # 时间行
        time_line = None
        text_lines = []
        for i, ln in enumerate(lines):
            if "-->" in ln:
                time_line = ln
                idx = i
                break
        if time_line is None:
            continue
        m = TIME_RE.search(time_line)
        if not m:
            continue
        start = _to_seconds(m.group(1), m.group(2), m.group(3), m.group(4))
        end = _to_seconds(m.group(5), m.group(6), m.group(7), m.group(8))
        text_lines = lines[idx + 1:]
        seg = Segment(start=start, end=end, text="\n".join(text_lines))
        segments.append(seg)
    for i, seg in enumerate(segments, 1):
        seg.index = i
    return segments


def segments_to_srt(segments: list) -> str:
    """Segment 列表 → SRT 文本（优先 translated 字段）。"""
    out = []
    for i, seg in enumerate(segments, 1):
        text = seg.translated if seg.translated else seg.text
        out.append(f"{i}\n{_fmt_time(seg.start)} --> {_fmt_time(seg.end)}\n{text}\n")
    return "\n".join(out)


def write_srt(segments: list, path) -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = segments_to_srt(segments)
    path.write_text(content, encoding="utf-8-sig")
    return str(path)


def load_srt(path) -> list:
    return parse_srt(Path(path).read_text(encoding="utf-8-sig", errors="ignore"))
