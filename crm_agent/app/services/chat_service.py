"""统一聊天服务 — 意图识别 + 分流 Agent 入口。"""

from __future__ import annotations

import logging
from typing import Any

from app.agents.unified_reply import UnifiedReplyAgent
from app.api.schemas import ChatMode
from app.config import config
from app.services.chat_history import save_chat_turn
from services.models import RouteType, SessionSnapshot, SessionStatus, WeChatMessageResponse, ReplyMode
from services.session_status import detect_end_trigger
from vectorstore import check_milvus_connection

logger = logging.getLogger(__name__)

_unified_agent: UnifiedReplyAgent | None = None
_SUMMARY_BATCH_SIZE = 5


def _get_unified_agent() -> UnifiedReplyAgent:
    global _unified_agent
    if _unified_agent is None:
        check_milvus_connection()
        _unified_agent = UnifiedReplyAgent()
    return _unified_agent


def _build_final_summary(user_text: str, answer_text: str) -> str:
    return f"{user_text[:80]} / {answer_text[:120]}"


def _load_prior_summaries(user_id: str) -> list[dict]:
    """从 Redis session 中提取已生成的早期摘要。"""
    try:
        from services.session_store_redis import get_redis_session_store
        store = get_redis_session_store()
        stable_id = f"session_{user_id}"
        snap = store.get(stable_id)
        if not snap:
            return []
        summaries = []
        for turn in snap.turns:
            summary = turn.get("summary", "")
            if summary:
                summaries.append({
                    "start_msg_id": turn.get("turn_index", 0),
                    "end_msg_id": turn.get("turn_index", 0),
                    "summary": summary,
                })
        return summaries
    except Exception:
        logger.debug("加载 prior_summaries 失败", exc_info=True)
        return []


def _summarize_old_turns(snap: SessionSnapshot):
    """为超出 recent_window 的早期轮次生成摘要，直接修改 snap.turns。"""
    _recent_limit = config.context.get("recent_history_limit", 10)
    if len(snap.turns) <= _recent_limit:
        return

    old_turns = snap.turns[:len(snap.turns) - _recent_limit]
    to_summarize = [t for t in old_turns if not t.get("summary")]
    if not to_summarize:
        return

    from app.llm import llm
    for i in range(0, len(to_summarize), _SUMMARY_BATCH_SIZE):
        batch = to_summarize[i:i + _SUMMARY_BATCH_SIZE]
        text = "; ".join(
            f"问: {t['user_message'][:100]} 答: {t['assistant_message'][:100]}"
            for t in batch
        )
        try:
            response = llm.invoke([
                {"role": "system", "content": "用一句话概括这段客服对话的核心内容（20字以内）"},
                {"role": "user", "content": text},
            ], agent_name="unified_reply")
            if isinstance(response, dict):
                content = (response.get("choices", [{}])[0]
                          .get("message", {}).get("content", ""))
            else:
                content = str(response)
            batch[0]["summary"] = content.strip().strip('"\'')[:100]
            logger.info(f"摘要已生成: batch {i // _SUMMARY_BATCH_SIZE + 1} -> {batch[0]['summary']}")
        except Exception:
            logger.warning(f"摘要生成失败: batch {i // _SUMMARY_BATCH_SIZE + 1}")


def _persist_session_turn(
    *,
    user_id: str,
    message: str,
    output: dict[str, Any],
) -> dict[str, Any]:
    """同步 Redis 会话 + MySQL 归档；失败不影响主回复。"""
    meta: dict[str, Any] = {
        "session_id": "",
        "session_status": "",
        "session_end_reason": "",
    }
    try:
        from services.session_store_mysql import get_mysql_session_archive
        from services.session_store_redis import get_redis_session_store

        store = get_redis_session_store()
        # API 侧用稳定 key session_{user_id}，与 UnifiedReplyAgent context 对齐
        stable_id = f"session_{user_id}"
        existing = store.get(stable_id)
        if existing and existing.user_id == user_id and existing.status == SessionStatus.OPEN:
            snap = existing
        else:
            snap = SessionSnapshot(session_id=stable_id, user_id=user_id, channel="api")
            snap.touch()

        route = output.get("route", "")
        reply_mode = output.get("reply_mode", "")
        answer = output.get("result", "")
        answer_confidence = float(output.get("answer_confidence") or 0.0)
        needs_handoff = bool(output.get("needs_handoff"))

        snap.append_turn(
            user_message=message,
            assistant_message=answer,
            intent=output.get("intent", ""),
            intent_confidence=float(output.get("intent_confidence") or 0.0),
            route=route,
            reply_mode=reply_mode,
            answer_confidence=answer_confidence,
            needs_handoff=needs_handoff,
        )
        snap.last_intent = output.get("intent", "")
        snap.last_route = route
        snap.last_reply_mode = reply_mode
        snap.last_answer_confidence = answer_confidence
        snap.last_needs_handoff = needs_handoff

        # 复用 detect_end_trigger：拼一个轻量 response
        try:
            route_enum = RouteType(route) if route else RouteType.FALLBACK
        except ValueError:
            route_enum = RouteType.FALLBACK
        try:
            reply_enum = ReplyMode(reply_mode) if reply_mode in {m.value for m in ReplyMode} else ReplyMode.CASUAL
        except ValueError:
            reply_enum = ReplyMode.CASUAL

        pseudo = WeChatMessageResponse(
            user_id=user_id,
            route=route_enum,
            reply_mode=reply_enum,
            answer=answer,
            needs_handoff=needs_handoff,
            answer_confidence=answer_confidence,
        )
        decision = detect_end_trigger(user_text=message, response=pseudo)
        if decision is not None:
            snap.status = decision.status
            snap.end_reason = decision.reason
            snap.ended_at = snap.ended_at or snap.updated_at
            snap.final_summary = _build_final_summary(message, answer)

        _summarize_old_turns(snap)
        store.update(snap)
        try:
            archive = get_mysql_session_archive()
            if decision is not None:
                archive.finalize_session(snap)
            else:
                archive.upsert_session(snap)
        except Exception:
            logger.exception("MySQL 会话归档失败（不影响本次回复）")

        meta["session_id"] = snap.session_id
        meta["session_status"] = snap.status.value
        meta["session_end_reason"] = snap.end_reason
    except Exception:
        logger.exception("Redis/会话状态同步失败（不影响本次回复）")
    return meta


def _serialize_agent_result(
    result: dict,
    *,
    user_id: str,
    buyer_name: str,
    session_key: str,
    message: str,
    session_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output = result.get("output", {})
    sources = output.get("sources", [])
    answer = output.get("result", "")
    reply_mode = output.get("reply_mode", "no_hit")
    session_meta = session_meta or {}
    display_name = buyer_name or None

    return {
        "count": len(sources),
        "mode": "agent",
        "answer": answer,
        "user_id": user_id,
        "buyer_name": display_name,
        "talent_id": display_name,  # 兼容旧字段
        "session_key": session_key or None,
        "route": output.get("route", "unified_agent"),
        "reply_mode": reply_mode,
        "rag_hit": output.get("rag_hit", False),
        "history_count": output.get("history_count", 0),
        "sources": sources,
        "tools_used": output.get("tools_used", []),
        "intent": output.get("intent", ""),
        "intent_confidence": output.get("intent_confidence", 0.0),
        "action": output.get("action", ""),
        "ticket_id": output.get("ticket_id"),
        "tag_upgrade": None,
        "answer_confidence": output.get("answer_confidence", 0.0),
        "answer_supported": output.get("answer_supported", False),
        "needs_handoff": output.get("needs_handoff", False),
        "confidence_reason": output.get("confidence_reason", ""),
        "query_strategy": output.get("query_strategy", ""),
        "query_strategy_name": output.get("query_strategy_name", ""),
        "query_strategy_reason": output.get("query_strategy_reason", ""),
        "session_id": session_meta.get("session_id", ""),
        "session_status": session_meta.get("session_status", ""),
        "session_end_reason": session_meta.get("session_end_reason", ""),
        "received": {"direction": "receive", "message": message},
        "reply": {"direction": "send", "message": answer},
        "success": result.get("success", False),
    }


def _build_ctx_from_body(
    message: str, user_id: str = "buyer_demo_001",
    buyer_name: str = "", talent_id: str = "",
    session_key: str = "", contact_username: str = "",
    tool_loop: bool = False,
) -> dict[str, Any]:
    text = message.strip()
    profile_name = (buyer_name or talent_id or "").strip() or user_id
    contact = (session_key or contact_username or "").strip() or user_id
    _ctx_cfg = config.context
    prior_summaries = _load_prior_summaries(user_id)
    return {
        "message": text,
        "session_key": contact,
        "contact_username": contact,
        "buyer_profile": f"昵称/ID：{profile_name}",
        "talent_profile": f"昵称/ID：{profile_name}",
        "session_id": f"session_{user_id}",
        "history_limit": _ctx_cfg.get("history_limit", 50),
        "recent_history_limit": _ctx_cfg.get("recent_history_limit", 10),
        "prior_summaries": prior_summaries,
        "l2_summary": prior_summaries[-1] if prior_summaries else None,
        "tool_loop": tool_loop,
    }


def handle_chat(
    message: str,
    mode: ChatMode = ChatMode.AUTO,
    user_id: str = "buyer_demo_001",
    user_tag: str = "B",
    buyer_name: str = "",
    talent_id: str = "",
    session_key: str = "",
    contact_username: str = "",
    tool_loop: bool = False,
) -> dict[str, Any]:
    """
    统一 Agent 处理入口。
    user_tag 保留兼容，已不再参与路由（店铺买家一视同仁）。
    buyer_name / session_key 为推荐字段；talent_id / contact_username 为兼容旧字段。
    """
    _ = (mode, user_tag)
    text = message.strip()
    if not text:
        raise ValueError("消息不能为空")

    profile_name = (buyer_name or talent_id or "").strip() or user_id
    contact = (session_key or contact_username or "").strip() or user_id
    _ctx = _build_ctx_from_body(
        message=text, user_id=user_id,
        buyer_name=buyer_name, talent_id=talent_id,
        session_key=session_key, contact_username=contact_username,
        tool_loop=tool_loop,
    )
    contact = _ctx["session_key"]
    user_id = _ctx["session_id"].replace("session_", "")
    agent = _get_unified_agent()
    result = agent.invoke({
        "task": f"用户新消息：{text}",
        "context": _ctx,
    })

    if not result.get("success"):
        raise RuntimeError(result.get("error") or "Agent 处理失败")

    output = result.get("output") or {}
    answer = output.get("result", "")
    try:
        save_chat_turn(
            contact_username=contact,
            self_username=user_id,
            incoming_message=text,
            outgoing_message=answer,
        )
    except Exception:
        logger.exception("写入历史对话库失败（不影响本次回复）")

    session_meta = _persist_session_turn(user_id=user_id, message=text, output=output)

    # 非阻塞记录 FAQ 挖掘样本
    try:
        from services.faq_mining_tracker import log_turn
        log_turn(
            user_message=text,
            assistant_answer=answer,
            answer_confidence=float(output.get("answer_confidence") or 0),
            route=output.get("route", ""),
            reply_mode=output.get("reply_mode", ""),
        )
    except Exception:
        logger.debug("FAQ 挖掘记录异常（不影响本次回复）")

    return _serialize_agent_result(
        result,
        user_id=user_id,
        buyer_name=profile_name if (buyer_name or talent_id) else "",
        session_key=contact,
        message=text,
        session_meta=session_meta,
    )


def get_health() -> dict[str, Any]:
    milvus_status = "connected"
    try:
        check_milvus_connection()
    except Exception as e:
        milvus_status = f"error: {e}"

    redis_status = "unknown"
    try:
        from services.session_store_redis import get_redis_session_store

        redis_status = "connected" if get_redis_session_store().ping() else "error"
    except Exception as e:
        redis_status = f"error: {e}"

    return {
        "count": 0,
        "status": "ok" if milvus_status == "connected" else "degraded",
        "milvus": milvus_status,
        "redis": redis_status,
        "llm": "configured",
        "agent": "unified_reply",
    }


def get_meta() -> dict[str, Any]:
    from settings import (
        ANSWER_CONFIDENCE_THRESHOLD,
        INTENT_MODEL_ADAPTER_PATH,
        KB_DOC_DISPLAY_NAME,
        LLM_MODEL_NAME,
        RAG_COLLECTION_NAME,
        RAG_RELEVANCE_THRESHOLD,
        RERANK_ENABLED,
    )

    return {
        "count": 4,
        "version": "3.1.0",
        "agent": "unified_reply",
        "llm_model": LLM_MODEL_NAME,
        "collection": RAG_COLLECTION_NAME,
        "relevance_threshold": RAG_RELEVANCE_THRESHOLD,
        "answer_confidence_threshold": ANSWER_CONFIDENCE_THRESHOLD,
        "rerank_enabled": RERANK_ENABLED,
        "kb_doc": KB_DOC_DISPLAY_NAME,
        "intent_model": INTENT_MODEL_ADAPTER_PATH,
        "modes": ["agent", "rag", "talent", "auto"],
        "routes": [
            "rag_agent",
            "transaction",
            "complaint_handoff",
            "manual_handoff",
            "casual_chat",
            "fallback",
        ],
        "intents": ["咨询类", "交易类", "投诉类", "其他类"],
        "reply_modes": ["rag", "no_hit", "casual", "transaction", "handoff"],
        "tools": ["intent_classifier", "search_knowledge_base", "answer_confidence", "query_strategy"],
        "flow": "intent_classify → route → query_strategy → rag/handler/llm → confidence/handoff → session",
        "sla_note": "目标尽快回复买家（约 15 秒内）",
    }
