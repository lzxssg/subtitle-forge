"""LLM 字幕润色：把翻译后的中文字幕润色为自然地道的中文，保持原意不变。

- 输出 .zh.rs.srt（与翻译 .zh.srt 并存，不替换原翻译）
- 复用 translate 的 token 分批估算与 [n] 行解析
- 使用独立润色模型（polish_model），未配置时依次回退 review_model → llm_model
- 若质检未通过记录了问题清单，润色时把这些质检建议一并注入 prompt（针对性修复）
- 上下文超限时自动拆半提交，全部生成后合并
"""
import logging
import time

from .translate import _ProgressPacer, _batch_by_tokens, _is_context_error, _parse_response

logger = logging.getLogger("subtitle-forge.polish")


# 当前润色任务的质检建议（由 queue_manager 注入，供 _build_prompt 使用）
_POLISH_HINTS = []


def set_hints(hints):
    """注入质检问题/建议，润色 prompt 会据此重点修复。"""
    global _POLISH_HINTS
    if isinstance(hints, (list, tuple)):
        _POLISH_HINTS = [str(h).strip() for h in hints if str(h or "").strip()][:12]
    else:
        _POLISH_HINTS = []


def _build_prompt(batch):
    lines = "\n".join(f"[{seg.index}] {seg.text}" for seg in batch)
    hint_txt = ""
    if _POLISH_HINTS:
        hint_txt = (
            "质检反馈（仅针对下列问题做修复，未涉及的行只做语言润色，不要改动语义）:\n- "
            + "\n- ".join(_POLISH_HINTS) + "\n"
        )
    return (
        "以下是已翻译成中文的影视字幕，请润色为自然地道的中文表达。\n"
        "要求: 1) 只输出润色结果,每行格式为 [序号] 润色后文本; 2) 保持原意不变,只优化表达让语句更自然流畅; "
        "3) 口语化、符合中文语境,贴合角色说话习惯; 4) 保留专有名词; 5) 不要添加解释或改写剧情; "
        "6) 每条字幕长度与原文接近; 7) 不得增删条目、不得改变序号。\n"
        + hint_txt +
        "待润色字幕:\n" + lines
    )


def _parse_polish(text, batch):
    """解析润色输出 [n] 文本。输入本身就是中文，原样返回也算已处理。"""
    import re
    mapping = {}
    pat = re.compile(r"^\s*\[?(\d+)\]?\s*[.:：、]?\s*(.+)$")
    for line in (text or "").splitlines():
        m = pat.match(line.strip())
        if m:
            mapping[int(m.group(1))] = m.group(2).strip()
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
        if val:
            seg.translated = val
            filled += 1
    return filled, len(batch)


def _batch_with_retry(batch, attempt_outer=0):
    """外层重试 + 失败细化拆分（整批失败 → 二等分递归，单条失败保留原译文）。

    上下文超限错误直接拆半，不做无效重试。
    """
    from ..services.llm_client import get_polish_client
    client = get_polish_client()
    if not client.ready:
        raise RuntimeError("润色模型未配置（polish_model / review_model / llm_model 均不可用），请在设置页填写润色模型后重试")
    for attempt in range(3):
        try:
            # 每次按当前批次重建 prompt：拆半重试时若复用整批 prompt，子批会解析错位
            resp = client.chat(_build_prompt(batch))
            filled, _ = _parse_polish(resp["content"], batch)
            logger.info("润色批次(%d条) filled=%d len_resp=%d", len(batch), filled,
                        len(resp.get("content") or ""))
            return filled, resp
        except Exception as e:
            if _is_context_error(e) and len(batch) > 1:
                logger.warning("润色批次(%d条) 上下文超限，拆半提交: %s", len(batch), str(e)[:120])
                mid = len(batch) // 2
                f1, r1 = _batch_with_retry(batch[:mid], attempt_outer + 1)
                f2, r2 = _batch_with_retry(batch[mid:], attempt_outer + 1)
                return f1 + f2, (r1 or r2)
            logger.warning("润色批次(%d条) 第%d次失败: %s", len(batch), attempt + 1, e)
            time.sleep(2 * (attempt + 1))
    if len(batch) > 1:
        mid = len(batch) // 2
        f1, r1 = _batch_with_retry(batch[:mid], attempt_outer + 1)
        f2, r2 = _batch_with_retry(batch[mid:], attempt_outer + 1)
        return f1 + f2, (r1 or r2)
    return 0, None


def _batch_smart(batch, attempt_outer=0):
    """润色并自检：整批有效条目不足 80% 时清空本批并拆半重润色（小批成功率更高）。"""
    filled, resp = _batch_with_retry(batch, attempt_outer)
    if len(batch) <= 1:
        return filled, resp
    if filled >= len(batch) * 0.8:
        return filled, resp
    logger.warning("润色批次(%d条) 有效仅 %d 条，拆半重润色", len(batch), filled)
    for s in batch:
        s.translated = s.text
    mid = len(batch) // 2
    f1, r1 = _batch_smart(batch[:mid], attempt_outer + 1)
    f2, r2 = _batch_smart(batch[mid:], attempt_outer + 1)
    return f1 + f2, (r1 or r2)


def polish_segments(segments, progress_cb=None, cancel_check=None, hints=None):
    """把 segments 的译文文本润色为自然中文（就地更新 translated 字段）。

    返回: {"model_name", "tokens_in", "tokens_out", "tokens_per_sec", "elapsed", "filled", "total"}
    """
    from .. import config
    from ..services.llm_client import get_polish_client
    client = get_polish_client()
    set_hints(hints)
    if not client.ready:
        raise RuntimeError("润色模型未配置（polish_model / review_model / llm_model 均不可用），请在设置页填写润色模型后重试")

    # 润色对象：已有译文用译文，无译文则用原文兜底
    for s in segments:
        if not s.translated:
            s.translated = s.text

    total = len(segments)
    done = 0
    filled = 0
    tokens_in = 0
    tokens_out = 0
    start = time.time()
    # 润色模型上下文通常小于翻译模型：预算保守（1000），并限制单批条数，
    # 防止短句密集导致输出侧上下文超限（整批 500 → 拆半递归直到单条）。
    # review-35b 已拉大到 16K ctx，1000 输入 + 4096 输出仍在安全范围。
    budget = int(config.get_setting("polish_batch_tokens") or 1000)
    max_items = int(config.get_setting("polish_batch_items") or 80)
    batches = list(_batch_by_tokens(segments, budget, max_items=max_items))
    n_batches = len(batches)

    for batch in batches:
        if cancel_check and cancel_check():
            raise RuntimeError("任务已取消")
        # 估算本批输出 token 与预计耗时，请求期间平滑推进进度/速率
        est_in = sum(len(s.text) for s in batch)
        est_out = max(1, int(est_in * 1.2))
        est_seconds = max(15.0, est_out / 30.0)
        pacer = None
        if progress_cb:
            pacer = _ProgressPacer(progress_cb, done, len(batch), total, est_tokens=est_out, est_seconds=est_seconds)
            pacer.start()
        try:
            f, resp = _batch_smart(batch)
        finally:
            if pacer:
                pacer.stop()
        filled += f
        if resp:
            tokens_in += resp.get("prompt_tokens", 0) or 0
            tokens_out += resp.get("completion_tokens", 0) or 0
            from ..services import metrics
            # 只统计输出 token（生成速度），排除 prompt 预填充导致的虚高
            metrics.record_tokens(resp.get("completion_tokens", 0) or 0)
        done += len(batch)
        if progress_cb:
            progress_cb(done / total * 100 if total else 100)

    elapsed = time.time() - start
    tps = tokens_out / elapsed if elapsed > 0 else 0
    return {
        "model_name": client.model,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "tokens_per_sec": tps,
        "elapsed": elapsed,
        "filled": filled,
        "total": total,
    }
