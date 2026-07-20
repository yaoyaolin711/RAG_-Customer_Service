"""
FAQ 自动挖掘跟踪器 — 记录每次高质量对话的 Q&A，供批量挖掘使用。

设计：
  - Redis Sorted Set 统计问题频率（ZINCRBY）
  - Redis List 存储最近的 answer 样本（保留最近 N 条）
  - 非阻塞调用，不影响主回复延迟
"""
from __future__ import annotations

import json
import logging
from typing import Any

import redis

from settings import REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT
from services.qa_normalize import normalize_question

logger = logging.getLogger(__name__)

_KEY_PREFIX = "faq:mining:"
_FREQ_KEY = _KEY_PREFIX + "freq"           # Sorted Set: {norm} → count
_SAMPLE_KEY = _KEY_PREFIX + "sample:{norm}"  # List: 最近的 answer 样本 JSON
_MAX_SAMPLES_PER_QUESTION = 10


def _client():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB,
        decode_responses=True, protocol=2,
        socket_connect_timeout=2, socket_timeout=2,
    )


def log_turn(
    user_message: str,
    assistant_answer: str,
    answer_confidence: float,
    route: str,
    reply_mode: str,
) -> None:
    """在每次高质量对话轮次后调用，记录 Q&A 样本。"""
    norm = normalize_question(user_message)
    if not norm or len(norm) < 2:
        return
    if answer_confidence < 0.75:
        return
    if route == "casual_chat":
        return

    try:
        r = _client()
        r.zincrby(_FREQ_KEY, 1, norm)
        sample = json.dumps({
            "raw": user_message[:200],
            "answer": assistant_answer[:500],
            "confidence": answer_confidence,
            "route": route,
            "reply_mode": reply_mode,
        }, ensure_ascii=False)
        r.lpush(_SAMPLE_KEY.format(norm=norm), sample)
        r.ltrim(_SAMPLE_KEY.format(norm=norm), 0, _MAX_SAMPLES_PER_QUESTION - 1)
        r.expire(_SAMPLE_KEY.format(norm=norm), 86400 * 7)
    except Exception:
        logger.debug("FAQ 挖掘记录失败（不影响主流程）", exc_info=True)


def get_top_candidates(min_count: int = 3, top_n: int = 50) -> list[dict[str, Any]]:
    """获取达到频次阈值的问题候选列表。"""
    try:
        r = _client()
        items = r.zrevrangebyscore(_FREQ_KEY, "+inf", min_count, start=0, num=top_n, withscores=True)
    except Exception:
        logger.exception("读取 FAQ 挖掘候选失败")
        return []

    results = []
    for norm, count in items:
        samples = []
        try:
            raw_list = r.lrange(_SAMPLE_KEY.format(norm=norm), 0, -1)
            for s in raw_list:
                samples.append(json.loads(s))
        except Exception:
            pass
        if samples:
            results.append({
                "norm": norm,
                "count": int(count),
                "samples": samples,
            })
    return results


def clear() -> None:
    """清空所有挖掘数据。"""
    try:
        r = _client()
        for key in r.scan_iter(match=_KEY_PREFIX + "*", count=500):
            r.delete(key)
    except Exception:
        pass
