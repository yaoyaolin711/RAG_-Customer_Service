"""Redis FAQ 问题索引（全量不过期）+ 答案缓存。

仅操作 faq:qa:* 键，不清理会话等其他项目键。
"""

from __future__ import annotations

import json
from typing import Any

import redis

from settings import (
    QA_ANSWER_TTL_SECONDS,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)

# 问题索引 / 元数据：不过期
KEY_INDEX = "faq:qa:index"  # JSON list[{id, question_text, search_text, variants, ...}]
KEY_NORM = "faq:qa:norm:{norm}"  # faq_id
KEY_ITEM = "faq:qa:item:{id}"  # JSON meta（可不含 answer）
KEY_ANSWER = "faq:qa:answer:{id}"  # 答案正文（可 TTL）
KEY_USER_HIT = "faq:qa:user:{norm}"  # 用户问法命中后的答案缓存


class RedisFaqStore:
    def __init__(self):
        self._client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            decode_responses=True,
            protocol=2,
            socket_connect_timeout=2,
            socket_timeout=2,
        )

    def ping(self) -> bool:
        return self._client.ping() is True

    def clear_index(self) -> None:
        """只删除 faq:qa:*，不动 rag:session:* 等键。"""
        pipe = self._client.pipeline()
        n = 0
        for key in self._client.scan_iter(match="faq:qa:*", count=500):
            pipe.delete(key)
            n += 1
            if n % 500 == 0:
                pipe.execute()
                pipe = self._client.pipeline()
        if n % 500 != 0 or n == 0:
            pipe.execute()

    def load_all_questions(self, rows: list[dict[str, Any]], *, preload_answers: bool = True) -> int:
        """全量写入问题索引（不过期）；可选同时写入答案。"""
        self.clear_index()
        index: list[dict[str, Any]] = []
        pipe = self._client.pipeline()
        for row in rows:
            faq_id = int(row["id"])
            question_text = str(row["question_text"])
            search_text = str(row.get("search_text") or question_text)
            variants = row.get("question_variants") or []
            if isinstance(variants, str):
                try:
                    variants = json.loads(variants)
                except json.JSONDecodeError:
                    variants = [variants]
            if not isinstance(variants, list):
                variants = []
            variants = [str(v).strip() for v in variants if str(v).strip()]

            item = {
                "id": faq_id,
                "main_class": str(row.get("main_class") or ""),
                "qa_type": str(row.get("qa_type") or ""),
                "sub_class": str(row.get("sub_class") or ""),
                "question_text": question_text,
                "search_text": search_text,
                "question_variants": variants,
                "source": str(row.get("source") or ""),
            }
            index.append(item)
            pipe.set(KEY_ITEM.format(id=faq_id), json.dumps(item, ensure_ascii=False))
            for norm in _question_norms(question_text, search_text, item, variants):
                if norm:
                    pipe.set(KEY_NORM.format(norm=norm), str(faq_id))
            answer = str(row.get("answer") or "")
            if preload_answers and answer:
                pipe.set(KEY_ANSWER.format(id=faq_id), answer)
        pipe.set(KEY_INDEX, json.dumps(index, ensure_ascii=False))
        pipe.execute()
        return len(index)

    def get_index(self) -> list[dict[str, Any]]:
        raw = self._client.get(KEY_INDEX)
        if not raw:
            return []
        data = json.loads(raw)
        return data if isinstance(data, list) else []

    def resolve_norm(self, norm_question: str) -> int | None:
        if not norm_question:
            return None
        hit = self._client.get(KEY_USER_HIT.format(norm=norm_question))
        if hit:
            try:
                return int(json.loads(hit)["id"])
            except Exception:
                try:
                    return int(hit)
                except Exception:
                    pass
        raw = self._client.get(KEY_NORM.format(norm=norm_question))
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def get_item(self, faq_id: int) -> dict[str, Any] | None:
        raw = self._client.get(KEY_ITEM.format(id=faq_id))
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None

    def get_answer(self, faq_id: int) -> str | None:
        raw = self._client.get(KEY_ANSWER.format(id=faq_id))
        return raw if raw else None

    def set_answer(self, faq_id: int, answer: str, *, ttl: int | None = None) -> None:
        key = KEY_ANSWER.format(id=faq_id)
        if ttl and ttl > 0:
            self._client.setex(key, ttl, answer)
        else:
            self._client.set(key, answer)

    def insert_faq(self, row: dict[str, Any]) -> bool:
        """增量添加一条 FAQ 到 Redis 索引（MySQL 已写入后调用）。"""
        faq_id = int(row.get("id", 0))
        if not faq_id:
            return False
        question_text = str(row.get("question_text") or "")
        search_text = str(row.get("search_text") or question_text)
        variants = row.get("question_variants") or []
        if isinstance(variants, str):
            try:
                variants = json.loads(variants)
            except json.JSONDecodeError:
                variants = [variants]
        variants = [str(v).strip() for v in variants if str(v).strip()]

        item = {
            "id": faq_id,
            "main_class": str(row.get("main_class") or ""),
            "qa_type": str(row.get("qa_type") or ""),
            "sub_class": str(row.get("sub_class") or ""),
            "question_text": question_text,
            "search_text": search_text,
            "question_variants": variants,
            "source": str(row.get("source") or ""),
        }
        pipe = self._client.pipeline()
        pipe.set(KEY_ITEM.format(id=faq_id), json.dumps(item, ensure_ascii=False))
        for norm in _question_norms(question_text, search_text, item, variants):
            if norm:
                pipe.set(KEY_NORM.format(norm=norm), str(faq_id))
        answer = str(row.get("answer") or "")
        if answer:
            pipe.set(KEY_ANSWER.format(id=faq_id), answer)
        # 更新全量索引列表
        index = self.get_index()
        if not any(x.get("id") == faq_id for x in index):
            index.append(item)
        pipe.set(KEY_INDEX, json.dumps(index, ensure_ascii=False))
        pipe.execute()
        return True

    def cache_user_hit(
        self,
        norm_question: str,
        faq_id: int,
        answer: str,
        *,
        ttl: int | None = None,
    ) -> None:
        """用户问法命中后写入，便于下次 exact。"""
        if not norm_question:
            return
        expire = ttl if ttl is not None else QA_ANSWER_TTL_SECONDS
        payload = json.dumps({"id": faq_id, "answer": answer}, ensure_ascii=False)
        key = KEY_USER_HIT.format(norm=norm_question)
        if expire and expire > 0:
            self._client.setex(key, expire, payload)
        else:
            self._client.set(key, payload)
        self.set_answer(faq_id, answer, ttl=None)


def _question_norms(
    question_text: str,
    search_text: str,
    item: dict[str, Any],
    variants: list[str],
) -> list[str]:
    from services.qa_normalize import normalize_question

    values = [
        question_text,
        item.get("sub_class") or "",
        f"{item.get('main_class')}|{item.get('qa_type')}|{item.get('sub_class')}",
        *variants,
    ]
    # search_text 含多问法，整段归一化通常过长，不作为 exact key
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        n = normalize_question(v)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


_redis_store: RedisFaqStore | None = None


def get_redis_faq_store() -> RedisFaqStore:
    global _redis_store
    if _redis_store is None:
        _redis_store = RedisFaqStore()
    return _redis_store
