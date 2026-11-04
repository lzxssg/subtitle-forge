"""ffmpeg 提取音频：视频 → 16kHz 单声道 wav（whisper 最优输入）。"""
import logging
import subprocess
import time
from pathlib import Path

from .. import config

logger = logging.getLogger("subtitle-forge.extract")

FFMPEG = "ffmpeg"


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


def extract_audio(video_path: str, out_wav: str, progress_cb=None) -> str:
    """提取音频到 16k 单声道 wav，带实时进度回调。返回 wav 路径。

    - -threads 0：ffmpeg 自动多线程解码
    - -progress pipe:1：输出处理进度（out_time_ms），按视频总时长换算百分比
    """
    out_wav = str(out_wav)
    Path(out_wav).parent.mkdir(parents=True, exist_ok=True)
    total = probe_duration(video_path)
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
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    last_t = time.time()
    for line in proc.stdout:
        line = line.strip()
        if line.startswith("out_time_ms="):
            try:
                ms = int(line.split("=", 1)[1])
                if progress_cb and total > 0:
                    now = time.time()
                    if now - last_t >= 0.5:
                        progress_cb(min(100, ms / 1000 / total * 100))
                        last_t = now
            except (ValueError, IndexError):
                pass
    proc.wait()
    if proc.returncode != 0:
        err = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"ffmpeg 提取音频失败: {err[-500:]}")
    return out_wav
