"""
FAQ 自动挖掘 — 读取追踪器中的高频问题，提取最佳答案，写入 FAQ 存储。

流程：
  1. 读取 freq ≥ threshold 的问题候选
  2. 对每个候选，取置信度最高的 answer
  3. 检查是否已存在于 FAQ（去重）
  4. 不存在 → 生成问法变体 → MySQL insert → Redis sync
"""
from __future__ import annotations

import logging
from typing import Any

from services.faq_mining_tracker import get_top_candidates
from services.faq_store_mysql import get_mysql_faq_store
from services.faq_store_redis import get_redis_faq_store
from services.qa_variants import build_question_variants, build_search_text

logger = logging.getLogger(__name__)

# 默认主类/类型（自动挖掘的新问答归属）
_DEFAULT_MAIN_CLASS = "自动挖掘"
_DEFAULT_QA_TYPE = "通用"


def _best_sample(samples: list[dict]) -> dict | None:
    """从样本中选出置信度最高的那条。"""
    if not samples:
        return None
    samples = [s for s in samples if s.get("answer") and len(s["answer"]) >= 3]
    if not samples:
        return None
    return max(samples, key=lambda s: float(s.get("confidence", 0)))


def _already_exists(norm: str) -> bool:
    """检查归一化问法是否已在 FAQ 中。"""
    if not norm:
        return True
    try:
        redis = get_redis_faq_store()
        return redis.resolve_norm(norm) is not None
    except Exception:
        return True


def _is_good_candidate(sample: dict) -> bool:
    """过滤不适合入库的样本。"""
    route = sample.get("route", "")
    reply_mode = sample.get("reply_mode", "")
    if route == "casual_chat":
        return False
    if reply_mode == "handoff":
        return False
    answer = (sample.get("answer") or "").strip()
    raw = (sample.get("raw") or "").strip()
    if len(answer) < 5 or len(raw) < 3:
        return False
    return True


def mine_candidates(min_count: int = 3, max_items: int = 20) -> list[dict[str, Any]]:
    """执行一次挖掘，返回新入库的 FAQ 列表。"""
    candidates = get_top_candidates(min_count=min_count, top_n=max_items)
    if not candidates:
        logger.info("没有达到频次阈值的 FAQ 候选 (min_count=%s)", min_count)
        return []

    mysql = get_mysql_faq_store()
    redis = get_redis_faq_store()
    inserted = []

    for cand in candidates:
        norm = cand["norm"]
        samples = cand["samples"]

        # 已存在则跳过
        if _already_exists(norm):
            logger.debug("跳过已存在的 FAQ: %s", norm)
            continue

        best = _best_sample(samples)
        if not best or not _is_good_candidate(best):
            logger.debug("样本质量不足，跳过: %s", norm)
            continue

        raw_question = best["raw"]
        answer = best["answer"]

        # 用 raw_question 作为 sub_class 生成问法变体
        # 提取简短的关键词作为 sub_class（取 raw_question 的前 20 字）
        sub_class = raw_question[:20]
        main_class = _DEFAULT_MAIN_CLASS
        qa_type = _DEFAULT_QA_TYPE
        variants = build_question_variants(main_class, qa_type, sub_class)
        # 确保用户原文也在 variants 中
        if raw_question not in variants:
            variants.append(raw_question)
        search_text = build_search_text(main_class, qa_type, sub_class, variants)
        question_text = f"{raw_question[:50]}"

        row = {
            "main_class": main_class,
            "qa_type": qa_type,
            "sub_class": sub_class,
            "question_text": question_text,
            "question_variants": variants,
            "search_text": search_text,
            "answer": answer,
            "source": "auto_mined",
        }

        try:
            faq_id = mysql.insert_one(row)
            row["id"] = faq_id
            redis.insert_faq(row)
            logger.info("新增 FAQ #%s: %s -> %s", faq_id, norm, answer[:40])
            inserted.append({**row, "id": faq_id, "norm": norm, "count": cand["count"]})
        except Exception as e:
            logger.error("FAQ 入库失败 %s: %s", norm, e)

    logger.info("挖掘完成: %s 新 FAQ 入库", len(inserted))
    return inserted
