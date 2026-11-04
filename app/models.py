"""轻量数据模型（dataclass），用于流水线内部传递，数据库层直接用 SQLite Row。"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Segment:
    """字幕段。"""
    start: float          # 秒
    end: float            # 秒
    text: str = ""
    translated: str = ""
    index: int = 0

    def as_dict(self):
        return {
            "start": self.start,
            "end": self.end,
            "text": self.text,
            "translated": self.translated,
            "index": self.index,
        }


@dataclass
class MediaFile:
    path: str
    name: str
    ext: str
    duration: Optional[float] = None
    has_embedded_subtitle: bool = False
    existing_subtitles: list = field(default_factory=list)


@dataclass
class QualityReport:
    """质检报告。"""
    score: float = 0            # 0-100
    passed: bool = True
    issues: list = field(default_factory=list)
    suggestions: list = field(default_factory=list)
    details: dict = field(default_factory=dict)
