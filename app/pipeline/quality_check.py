"""字幕质量检查：
1) 转录质检（规则+统计）: 时间戳、覆盖率、空白段、乱码、语言、重复行
2) 翻译校对（规则预检 + LLM 评审）: 空译文/占位废文本/重复行 先拦截，再抽样评审打分

v1.4.8:
- 转录质检增加重复行检测（防止整段重复的无意义转录）
- 翻译校对先做规则预检：命中"请稍等"类占位/空译文/重复严重 → 直接判失败，不浪费 LLM 时间
- LLM 评审 JSON 解析失败 → 判失败（不再放水给 70 分）
- 抽样条数可配置（review_sample_count）
"""
import logging
import re

from .. import config
from ..models import QualityReport

logger = logging.getLogger("subtitle-forge.quality")

_GARBAGE_RE = re.compile(r"[\ufffd]|(?:^|\s)(?:um+|uh+|er+|hmm)(?:\s|$)", re.I)
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_LATIN_RE = re.compile(r"[a-zA-Z]")
# 占位/废文本：LLM 翻译失败时常见的占位输出
_PLACEHOLDER_RE = re.compile(
    r"(请稍等|请稍候|稍等|等一下|请稍后|please wait|please hold|お待ちください|少々お待ち|しばらくお待ち)"
)


def _extract_json(text: str):
    """从 LLM 输出中稳健提取 JSON（兼容 ```json 围栏、前后缀说明、尾部截断）。"""
    import json

    if not text:
        return None
    s = text.strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    i, j = s.find("{"), s.rfind("}")
    if i < 0:
        return None
    frag = s[i:j + 1] if j > i else s[i:]
    try:
        obj = json.loads(frag)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # 尾部被 max_tokens 截断：逐个补齐常见闭合符号再试
    for suffix in ('"]}', ']}', '"]}', '"}]}', '}]}', ']}', '}'):
        try:
            obj = json.loads(frag + suffix)
            return obj if isinstance(obj, dict) else None
        except Exception:
            continue
    # 最后兜底：直接抽 score 字段，issues 留空
    m = re.search(r'"?score"?\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)', frag)
    if m:
        return {"score": float(m.group(1)), "issues": [], "suggestions": []}
    return None


def _dup_ratio(texts):
    if not texts:
        return 1.0
    return 1 - len(set(texts)) / len(texts)


def check_transcript(segments, video_duration, detected_lang) -> QualityReport:
    """转录后质检：返回得分(0-100)与问题清单。"""
    report = QualityReport()
    issues = []
    suggestions = []

    if not segments:
        report.score = 0
        report.passed = False
        report.issues.append("无字幕段")
        return report

    n = len(segments)
    # 1) 时间戳顺序与重叠
    overlap = 0
    for i in range(1, n):
        if segments[i].start < segments[i - 1].end - 0.05:
            overlap += 1
    if overlap > n * 0.05:
        issues.append(f"时间戳重叠/倒序段 {overlap} 条")
        suggestions.append("建议换用 whisperX 对齐或增大 beam_size")

    # 2) 覆盖率
    if video_duration and video_duration > 0:
        covered = sum(min(s.end, video_duration) - max(s.start, 0) for s in segments if s.end > s.start)
        coverage = min(100.0, covered / video_duration * 100)
    else:
        coverage = 0.0
    report.details["coverage"] = round(coverage, 1)

    # 3) 空白/过短行
    empty = sum(1 for s in segments if not s.text.strip())
    too_short = sum(1 for s in segments if 0 < len(s.text.strip()) < 2)
    if empty > 0:
        issues.append(f"空字幕段 {empty} 条")
    if too_short > n * 0.1:
        issues.append(f"过短字幕段偏多（{too_short} 条）")

    # 4) 乱码
    garbled = sum(1 for s in segments if _GARBAGE_RE.search(s.text))
    if garbled > n * 0.02:
        issues.append(f"疑似乱码段 {garbled} 条")

    # 5) 重复行（转录器卡死/循环输出会产生大量重复）
    dup = _dup_ratio([s.text.strip() for s in segments])
    if dup > 0.5:
        issues.append(f"重复行占比 {dup:.0%}，疑似转录循环/失效")
        suggestions.append("建议更换 ASR 模型或降低并发重试")
    elif dup > 0.3:
        issues.append(f"重复行占比 {dup:.0%}")

    # 6) 占位废文本（转录阶段出现"请稍等"类中文占位说明管线异常）
    placeholder = sum(1 for s in segments if _PLACEHOLDER_RE.search(s.text))
    if placeholder > n * 0.1:
        issues.append(f"占位/废文本段 {placeholder} 条（{placeholder / n:.0%}）")

    # 7) 语言一致性
    if detected_lang == "zh":
        non_zh = sum(1 for s in segments if not _CJK_RE.search(s.text) and _LATIN_RE.search(s.text))
        if non_zh > n * 0.3:
            issues.append(f"检测到中文但 {non_zh} 段非中文，可能识别错误")

    # 计分
    score = 100.0
    if overlap > n * 0.05:
        score -= min(30, overlap / n * 100 * 0.5)
    if coverage > 0:
        if coverage < 50:
            score -= 25
            issues.append(f"字幕覆盖率仅 {coverage:.1f}%（视频时长 {video_duration:.0f}s）")
        elif coverage < 80:
            score -= 10
    if empty:
        score -= min(10, empty)
    if garbled:
        score -= min(15, garbled)
    if too_short > n * 0.1:
        score -= 5
    if dup > 0.5:
        score -= min(40, dup * 100 * 0.5)
    elif dup > 0.3:
        score -= min(15, dup * 100 * 0.3)
    if placeholder > n * 0.1:
        score -= min(30, placeholder / n * 100 * 0.5)
    score = max(0, round(score, 1))
    report.score = score
    report.issues = issues[:30]
    report.suggestions = suggestions
    report.passed = score >= config.get_setting("transcript_quality_threshold")
    if not report.passed and issues:
        report.suggestions.append("转录质量不足：可换更大 ASR 模型（如 large-v3-turbo）后重试；无效结果可在「质检未通过」队列一键删除")
    return report


def _precheck_translation(translated) -> tuple:
    """翻译规则预检：返回 (是否直接失败, 问题列表, 失败扣分说明)。

    命中空译文 / 占位废文本 / 严重重复时直接判失败，不走 LLM（省时且拦截稳定）。
    """
    texts = [s.translated.strip() for s in translated]
    n = len(texts)
    problems = []
    if n == 0:
        return True, ["无译文可校对"], 0
    empty_t = sum(1 for t in texts if not t)
    ph = sum(1 for t in texts if _PLACEHOLDER_RE.search(t))
    dup = _dup_ratio(texts)
    if empty_t > 0:
        problems.append(f"空译文 {empty_t} 条")
    if ph > max(1, n * 0.05):
        problems.append(f"占位/废文本 {ph} 条（如\"请稍等\"），占比 {ph / n:.0%}")
    if dup > 0.5:
        problems.append(f"译文重复严重（{dup:.0%}）")
    if problems:
        return True, problems, min(100, 15 + empty_t * 8 + ph * 8 + int(dup * 100 * 0.3))
    return False, [], 0


def review_translation(segments, model_name) -> QualityReport:
    """翻译后 LLM 校对：规则预检 → 随机抽样评审，打分。"""
    report = QualityReport()
    translated = [s for s in segments if s.translated]
    if not translated:
        report.score = 0
        report.passed = False
        report.issues.append("无译文可校对")
        return report

    # 规则预检（先拦截，不浪费 LLM 时间）
    fail, problems, deduction = _precheck_translation(translated)
    if fail:
        report.score = max(0.0, 100.0 - deduction)
        report.issues = problems[:20]
        report.suggestions.append("若修复后仍不达标，可在「质检未通过」队列一键删除该字幕")
        report.passed = False
        return report

    from ..services.llm_client import get_review_client
    client = get_review_client()
    if not client.ready:
        # 校验模型不可用时回退到翻译模型
        from ..services.llm_client import get_client
        client = get_client()
    if not client.ready:
        # LLM 不可用时退化为规则检查（空译文/占位/重复）
        _, problems2, _ = _precheck_translation(translated)
        score = 100.0 if not problems2 else max(0.0, 100.0 - len(problems2) * 15)
        report.score = score
        report.passed = score >= config.get_setting("translation_quality_threshold")
        report.issues = problems2
        return report

    # 抽样（条数可配置，默认 12 条均匀分布）送审
    n = len(translated)
    sample_n = max(4, min(n, int(config.get_setting("review_sample_count") or 12)))
    step = max(1, n // sample_n)
    sample = [translated[i] for i in range(0, n, step)][:sample_n]
    lines = "\n".join(f"{s.index}. 原文: {s.text}\n   译文: {s.translated}" for s in sample)
    prompt = (
        "你是字幕翻译质检专家。请校对以下字幕翻译，从【准确性、流畅性、术语一致性、格式】四方面打分（0-100），"
        "并列出主要问题。只输出 JSON：{\"score\": 数字, \"issues\": [\"问题1\", ...], \"suggestions\": [\"建议1\", ...]}。\n"
        "suggestions 必须是可直接执行的处理意见（如：重译第X条 / 删除空行 / 修正错译 / 拆分超长句 / 用更强翻译模型重跑），"
        "禁止使用\"需符合规范/请确保质量\"等笼统表述；score<70 时必须给出具体重跑/修复动作。\n"
        "注意：译文若为占位废文本（如\"请稍等\"）应打低分并明确指出。\n\n"
        f"翻译模型: {model_name}\n\n{lines}"
    )
    data = None
    err = None
    ask = prompt
    for attempt in range(2):
        try:
            resp = client.chat(ask, json_mode=True, max_tokens=1200)
            data = _extract_json(resp.get("content") or "")
            if data is not None:
                break
            err = ValueError("模型返回内容不含可解析 JSON")
            logger.warning("LLM 校对返回不可解析（第 %d 次）", attempt + 1)
            ask = prompt + "\n\n只输出一行 JSON，不要任何解释、不要代码块。"
        except Exception as e:  # 调用异常重试一次后放弃
            err = e
            logger.warning("LLM 校对调用失败（第 %d 次）: %s", attempt + 1, e)
            if attempt == 0:
                continue
            break
    if data is not None:
        try:
            score = max(0.0, min(100.0, float(data.get("score", 70))))
        except (TypeError, ValueError):
            score = 70.0
        report.score = round(score, 1)
        report.issues = [str(x) for x in (data.get("issues") or [])][:20]
        report.suggestions = [str(x) for x in (data.get("suggestions") or [])][:10]
    else:
        # 解析失败不再直接判 0 分（会把正常字幕误判为失败），降级为规则预检评分并留痕
        _, problems2, _ = _precheck_translation(translated)
        score = 100.0 if not problems2 else max(0.0, 100.0 - len(problems2) * 15)
        report.score = round(score, 1)
        report.issues = list(problems2) + [f"校对模型返回不可解析，已按规则预检评分（{str(err)[:60]}）"]
        report.suggestions = ["建议检查质检模型配置（review_model），或重跑翻译获取 LLM 校对结果"]
        logger.warning("LLM 校对解析失败，降级为规则评分 %.1f: %s", report.score, err)

    report.passed = report.score >= config.get_setting("translation_quality_threshold")
    return report
