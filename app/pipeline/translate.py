"""LLM 翻译：OpenAI 兼容接口，带上下文窗口与术语一致性，统计 token 速率。"""
import logging
import re
import threading
import time

from .. import config
from ..models import Segment
from .srt_utils import _fmt_time

logger = logging.getLogger("subtitle-forge.translate")

# 中文检测（判断是否需要翻译）
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def _is_context_error(e) -> bool:
    """判断异常是否为上下文超限（Context size exceeded 类），需要拆半而不是重试。"""
    s = str(e).lower()
    return "context" in s and ("exceed" in s or "too large" in s or "size" in s or "token" in s)


class _ProgressPacer:
    """批次请求进行期间：按秒平滑推进进度 + token 速率心跳。

    请求真实完成前按估算匀速爬升（到批次占比的 95% 封顶），
    请求完成后 stop() 由主流程校正到真实值。DB 写入走全局锁，线程安全。
    """

    def __init__(self, progress_cb, base_done, batch_len, total, est_tokens=0, est_seconds=60.0):
        self._stop = threading.Event()
        self._progress_cb = progress_cb
        self._base_done = base_done
        self._batch_len = batch_len
        self._total = max(1, total)
        self._est_tokens = max(0, int(est_tokens))
        self._est_seconds = max(10.0, float(est_seconds))
        self._start = time.time()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        per_sec = self._est_tokens / self._est_seconds
        last = 0.0
        while not self._stop.is_set():
            now = time.time()
            frac = min(1.0, (now - self._start) / self._est_seconds)
            # 平滑推进：基准 + 本批占比 * 95%（预留 5% 由真实完成校正）
            shown_done = self._base_done + frac * self._batch_len * 0.95
            pct = shown_done / self._total * 100
            if self._progress_cb:
                self._progress_cb(min(99.0, pct))
            if self._est_tokens > 0 and now - last >= 1.0:
                from ..services import metrics
                metrics.record_tokens(max(1, int(per_sec)))
                last = now
            self._stop.wait(0.5)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1.0)


def _batch_by_tokens(segments, token_budget: int, max_items: int = 0):
    """按 token 预算分批：防止单次请求过大被 max_tokens 截断。

    内层逻辑：单任务内部按 token 估算切批（预算内包含输入+输出余量），
    超长单条（超过预算 60%）单独成批，避免拖累整批。
    max_items>0 时同时限制单批条数，防止短句密集导致输出侧上下文超限。
    """
    batch = []
    used = 0
    for seg in segments:
        est = _estimate_tokens(seg.text)
        # 超长单条：单独成批
        if est >= token_budget * 0.6:
            if batch:
                yield batch
                batch, used = [], 0
            yield [seg]
            continue
        if batch and (used + est > token_budget or (max_items and len(batch) >= max_items)):
            yield batch
            batch, used = [], 0
        batch.append(seg)
        used += est
    if batch:
        yield batch


def _estimate_tokens(text: str) -> int:
    """粗略估算 token 数：CJK 字符≈1 token/字，其他语言≈4 字符/token。"""
    if not text:
        return 0
    cjk = len(re.findall(r"[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]", text))
    other = max(0, len(text) - cjk)
    return int(cjk + other / 4) + 1


def _batch_size_for_speed(speed: str) -> int:
    """翻译速度档位 → 单批 token 预算（内层请求上限，防止超长截断）：
    快速=2400 少轮询 / 均衡=1800 / 精确=1200 细粒度更稳。"""
    s = str(speed or "balanced").lower()
    if s in ("fast", "快速"):
        return 2400
    if s in ("precise", "精确"):
        return 1200
    return 1800


def _translate_batch_smart(batch, prompt_builder):
    """翻译并以「有效译文占比」自检，译文缺失/原文回显时拆半重译。

    小批失败是模型行为问题（如 Sakura 长列表只输出前几条），拆小后成功率显著提升；
    只剩单条时再试一次纯文本兜底 prompt，仍失败则保留原文。
    """
    filled, resp = _translate_batch_with_retry(batch, prompt_builder)
    if len(batch) <= 1:
        return filled, resp
    same = sum(1 for s in batch if s.translated and s.translated.strip() == s.text.strip())
    bad = filled < len(batch) * 0.8 or same > max(1, int(len(batch) * 0.3))
    if not bad:
        return filled, resp
    logger.warning("翻译批次(%d条) 有效译文不足（有效=%d, 原文回显=%d），拆半重译",
                   len(batch), filled, same)
    for s in batch:
        s.translated = ""
    mid = len(batch) // 2
    f1, r1 = _translate_batch_smart(batch[:mid], prompt_builder)
    f2, r2 = _translate_batch_smart(batch[mid:], prompt_builder)
    return f1 + f2, (r1 or r2)


def _translate_batch_with_retry(batch, prompt_builder, attempt_outer=0):
    """外层重试 + 失败细化拆分（内层降粒度重试）。

    策略：整批失败 → 二等分递归重试，直到单条；单条仍失败则保留原文。
    """
    if len(batch) == 0:
        return 0
    from ..services.llm_client import get_client
    client = get_client()
    if not client.ready:
        raise RuntimeError("LLM 未配置（llm_base_url/llm_api_key/llm_model），请在界面设置后重试")
    for attempt in range(3):
        try:
            logger.info("翻译请求：%d 条", len(batch))
            resp = client.chat(prompt_builder(batch))
            logger.info("翻译响应原文(前500): %r | segs=%s", (resp.get("content") or "")[:500],
                        [s.index for s in batch[:5]])
            filled, total = _parse_response(resp["content"], batch)
            logger.info("翻译批次(%d条) OK filled=%d/%d len_resp=%d", len(batch), filled, total,
                        len(resp.get("content") or ""))
            return filled, resp
        except Exception as e:
            if _is_context_error(e) and len(batch) > 1:
                # 上下文超限：重试无意义，直接拆半递归，逐级降粒度直到单条
                logger.warning("翻译批次(%d条) 上下文超限，拆半提交: %s", len(batch), str(e)[:120])
                mid = len(batch) // 2
                f1, r1 = _translate_batch_with_retry(batch[:mid], prompt_builder, attempt_outer + 1)
                f2, r2 = _translate_batch_with_retry(batch[mid:], prompt_builder, attempt_outer + 1)
                return f1 + f2, (r1 or r2)
            logger.warning("翻译批次(%d条) 第%d次失败: %s", len(batch), attempt + 1, e)
            time.sleep(2 * (attempt + 1))
    # 整批 3 次失败 → 细化拆分：一半一半递归，降低单批难度
    if len(batch) > 1:
        mid = len(batch) // 2
        f1, r1 = _translate_batch_with_retry(batch[:mid], prompt_builder, attempt_outer + 1)
        f2, r2 = _translate_batch_with_retry(batch[mid:], prompt_builder, attempt_outer + 1)
        return f1 + f2, (r1 or r2)
    # 单条仍失败：保留原文
    return 0, None


def _build_prompt(batch, src_lang, tgt_lang, ctx_before, ctx_after):
    lines = []
    for seg in batch:
        lines.append(f"[{seg.index}] {seg.text}")
    ctx = ""
    if ctx_before:
        ctx = f"\n前文参考:\n{ctx_before}\n"
    if ctx_after:
        ctx += f"\n后文参考:\n{ctx_after}\n"
    prompt = (
        f"你是专业影视字幕翻译。把以下字幕从{src_lang}翻译成{tgt_lang}。\n"
        f"要求: 1) 只输出翻译结果,每行格式为 [序号] 译文; 2) 口语化、自然、符合{tgt_lang}表达习惯; "
        f"3) 保留专有名词; 4) 不要添加解释; 5) 若原句为语气词/拟声词可意译或省略。\n"
        f"每条字幕长度尽量与原文接近。\n"
        f"{ctx}"
        f"待翻译字幕:\n" + "\n".join(lines)
    )
    return prompt


def _parse_response(text, batch):
    """解析模型输出 [n] 译文 到对应 Segment。

    兼容三种输出：带 [序号]、无序号但行数一致、单条纯译文。
    原文被原样回显（未翻译）不计入有效译文，交由上层拆批重译。
    """
    mapping = {}
    pat = re.compile(r"^\s*\[?(\d+)\]?\s*[.:：、]?\s*(.+)$")
    for line in text.splitlines():
        m = pat.match(line.strip())
        if m:
            idx = int(m.group(1))
            mapping[idx] = m.group(2).strip()
    if not mapping:
        outs = [l.strip() for l in (text or "").splitlines() if l.strip()]
        outs = [re.sub(r"^\[?\d+\]?[.:：、]?\s*", "", l) for l in outs]
        outs = [o for o in outs if o]
        if len(batch) == 1 and outs:
            mapping[batch[0].index] = " ".join(outs)
        elif outs and len(outs) == len(batch):
            for seg, out in zip(batch, outs):
                mapping[seg.index] = out
    filled = 0
    for seg in batch:
        val = mapping.get(seg.index, "").strip()
        if val and val != seg.text.strip():  # 与原文完全一致视为未翻译
            seg.translated = val
            filled += 1
        elif val:
            logger.debug("第 %s 条译文与原文一致，视为未翻译", seg.index)
    return filled, len(batch)


def _is_chinese(text):
    """判断文本是否主要为中文（日语汉字/假名不应误判为中文）。"""
    if not text:
        return False
    han = len(re.findall(r"[\u4e00-\u9fff]", text))
    kana = len(re.findall(r"[\u3040-\u30ff\u31f0-\u31ff]", text))
    if han == 0:
        return False
    # 含大量假名视为日语，不跳过翻译；纯汉字/低假名视为中文
    return kana / max(1, han) < 0.2


def translate_segments(segments, src_lang="auto", tgt_lang="zh", progress_cb=None, cancel_check=None):
    from ..services.llm_client import get_client
    client = get_client()
    if not client.ready:
        raise RuntimeError("LLM 未配置（llm_base_url/llm_api_key/llm_model），请在界面设置后重试")

    # 自动语言：判断源语言是否已为目标语言
    if src_lang == "auto":
        sample = "".join(s.text for s in segments[:50])
        if tgt_lang == "zh" and _is_chinese(sample):
            for s in segments:
                s.translated = s.text
            return {"model_name": "skip", "tokens_in": 0, "tokens_out": 0,
                    "tokens_per_sec": 0, "elapsed": 0, "skipped": True}

    total = len(segments)
    done = 0
    tokens_in = 0
    tokens_out = 0
    start = time.time()
    budget = _batch_size_for_speed(config.get_setting("translation_speed"))
    batches = list(_batch_by_tokens(segments, budget))
    n_batches = len(batches)
    used_model = config.get_setting("llm_model")

    for bi, batch in enumerate(batches):
        if cancel_check and cancel_check():
            raise RuntimeError("任务已取消")
        ctx_before = "\n".join(f"[{s.index}] {s.text}" for s in batches[bi - 1][-4:]) if bi > 0 else ""
        ctx_after = "\n".join(f"[{s.index}] {s.text}" for s in batches[bi + 1][:2]) if bi + 1 < n_batches else ""

        def _pb(b, _sb=ctx_before, _sa=ctx_after):
            return _build_prompt(b, src_lang, tgt_lang, _sb, _sa)

        # 估算本批输出 token 与预计耗时，用于请求期间的进度/速率平滑推进
        est_in = sum(_estimate_tokens(s.text) for s in batch)
        est_out = max(1, int(est_in * 1.1))
        est_seconds = max(15.0, est_out / 30.0)  # 按约 30 tok/s 估算，快慢由真实结果校正
        pacer = None
        if progress_cb:
            pacer = _ProgressPacer(progress_cb, done, len(batch), total, est_tokens=est_out, est_seconds=est_seconds)
            pacer.start()
        try:
            filled, resp = _translate_batch_smart(batch, _pb)
        finally:
            if pacer:
                pacer.stop()
        if resp:
            tokens_in += resp.get("prompt_tokens", 0) or 0
            tokens_out += resp.get("completion_tokens", 0) or 0
            # 实时 token 速率（全局监控）：只统计输出 token（生成速度）
            from ..services import metrics
            metrics.record_tokens(resp.get("completion_tokens", 0) or 0)
        if filled < len(batch):
            logger.warning("翻译批 %d/%d 未完全翻译：%d/%d 条有效", bi + 1, n_batches, filled, len(batch))
        if filled == 0:
            # 整批彻底失败：保留原文，继续后续（保证任务不中断）
            for seg in batch:
                seg.translated = seg.text
        done += len(batch)
        if progress_cb:
            progress_cb(done / total * 100)

    elapsed = time.time() - start
    tps = tokens_out / elapsed if elapsed > 0 else 0
    # 未填充的段兜底用原文
    for seg in segments:
        if not seg.translated:
            seg.translated = seg.text
    # 统计「未真正翻译」比例（译文与原文完全相同），供上层判定任务是否可信
    untranslated = sum(1 for s in segments
                       if s.translated.strip() == s.text.strip() and len(s.text.strip()) > 1)
    ratio = round(untranslated / max(1, len(segments)), 3)
    if ratio > 0:
        logger.warning("译文与原文相同 %d/%d 条（%.0f%%）", untranslated, len(segments), ratio * 100)
    return {
        "model_name": used_model,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "tokens_per_sec": tps,
        "elapsed": elapsed,
        "untranslated_ratio": ratio,
    }
