"""MySQL FAQ 全量存储（独立库 MYSQL_FAQ_DATABASE，与会话库隔离）。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pymysql

from settings import MYSQL_FAQ_DATABASE, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER

_DDL_DB = (
    f"CREATE DATABASE IF NOT EXISTS `{MYSQL_FAQ_DATABASE}` "
    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
)

_DDL_TABLE = """
CREATE TABLE IF NOT EXISTS faq_qa_pairs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  main_class VARCHAR(128) NOT NULL COMMENT '主类',
  qa_type VARCHAR(64) NOT NULL COMMENT '类型：售前/售中/售后等',
  sub_class VARCHAR(256) NOT NULL COMMENT '副类/主题',
  question_text VARCHAR(512) NOT NULL COMMENT '标准展示问/主题键',
  question_variants JSON NOT NULL COMMENT '可匹配问法列表',
  search_text VARCHAR(1024) NOT NULL COMMENT 'BM25 检索文本（不含话术正文）',
  answer TEXT NOT NULL COMMENT '答案/话术',
  source VARCHAR(255) NOT NULL DEFAULT '',
  hit_count INT NOT NULL DEFAULT 0,
  created_at VARCHAR(32) NOT NULL,
  updated_at VARCHAR(32) NOT NULL,
  KEY idx_faq_main_sub (main_class, sub_class(128)),
  KEY idx_faq_hit_count (hit_count),
  KEY idx_faq_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _connect(*, database: str | None = None):
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=3,
        read_timeout=10,
        write_timeout=10,
    )


def _parse_variants(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")
    text = str(raw).strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(data, list):
        return [str(x).strip() for x in data if str(x).strip()]
    return []


def _row_from_db(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["question_variants"] = _parse_variants(out.get("question_variants"))
    return out


class MySQLFaqStore:
    def __init__(self):
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with _connect(database=None) as conn:
            with conn.cursor() as cur:
                cur.execute(_DDL_DB)
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(_DDL_TABLE)

    def replace_all(self, rows: list[dict[str, Any]]) -> int:
        """全量替换 FAQ（清空本库表后重建；不影响其他库）。"""
        now = datetime.now().isoformat(timespec="seconds")
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE faq_qa_pairs")
                sql = """
INSERT INTO faq_qa_pairs (
  main_class, qa_type, sub_class, question_text, question_variants,
  search_text, answer, source, hit_count, created_at, updated_at
) VALUES (
  %(main_class)s, %(qa_type)s, %(sub_class)s, %(question_text)s, CAST(%(question_variants)s AS JSON),
  %(search_text)s, %(answer)s, %(source)s, 0, %(created_at)s, %(updated_at)s
)
"""
                for row in rows:
                    variants = row.get("question_variants") or []
                    if not isinstance(variants, list):
                        variants = _parse_variants(variants)
                    payload = {
                        "main_class": row["main_class"],
                        "qa_type": row["qa_type"],
                        "sub_class": row["sub_class"],
                        "question_text": row["question_text"],
                        "question_variants": json.dumps(variants, ensure_ascii=False),
                        "search_text": row["search_text"],
                        "answer": row["answer"],
                        "source": row.get("source", ""),
                        "created_at": now,
                        "updated_at": now,
                    }
                    cur.execute(sql, payload)
        return len(rows)

    def list_all(self) -> list[dict[str, Any]]:
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT id, main_class, qa_type, sub_class, question_text, question_variants,
       search_text, answer, source, hit_count
FROM faq_qa_pairs
ORDER BY id
"""
                )
                return [_row_from_db(r) for r in cur.fetchall()]

    def get_by_id(self, faq_id: int) -> dict[str, Any] | None:
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
SELECT id, main_class, qa_type, sub_class, question_text, question_variants,
       search_text, answer, source, hit_count
FROM faq_qa_pairs WHERE id=%s
""",
                    (faq_id,),
                )
                row = cur.fetchone()
                return _row_from_db(row) if row else None

    def incr_hit(self, faq_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE faq_qa_pairs SET hit_count=hit_count+1, updated_at=%s WHERE id=%s",
                    (now, faq_id),
                )

    def insert_one(self, row: dict[str, Any]) -> int:
        """新增一条 FAQ 记录（不含查重，由调用方保证）。返回自增 id。"""
        now = datetime.now().isoformat(timespec="seconds")
        variants = row.get("question_variants") or []
        if not isinstance(variants, list):
            variants = _parse_variants(variants)
        payload = {
            "main_class": row["main_class"],
            "qa_type": row["qa_type"],
            "sub_class": row["sub_class"],
            "question_text": row["question_text"],
            "question_variants": json.dumps(variants, ensure_ascii=False),
            "search_text": row.get("search_text") or row["question_text"],
            "answer": row["answer"],
            "source": row.get("source", "auto_mined"),
            "created_at": now,
            "updated_at": now,
        }
        sql = """
INSERT INTO faq_qa_pairs (
  main_class, qa_type, sub_class, question_text, question_variants,
  search_text, answer, source, hit_count, created_at, updated_at
) VALUES (
  %(main_class)s, %(qa_type)s, %(sub_class)s, %(question_text)s, CAST(%(question_variants)s AS JSON),
  %(search_text)s, %(answer)s, %(source)s, 0, %(created_at)s, %(updated_at)s
)
"""
        with _connect(database=MYSQL_FAQ_DATABASE) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, payload)
                return int(cur.lastrowid)


_store: MySQLFaqStore | None = None


def get_mysql_faq_store() -> MySQLFaqStore:
    global _store
    if _store is None:
        _store = MySQLFaqStore()
    return _store
