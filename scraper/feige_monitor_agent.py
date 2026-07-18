"""
飞鸽消息自动化监控与 AI 回复 - 独立 Playwright Agent

设计思路（API 拦截驱动）：
  1. 启动浏览器 → 加载登录态 → 进入 Pigeon IM 页面
  2. 通过 page.on('response') 监听消息 API 的返回
  3. 自适应解析响应 JSON 中消息结构
  4. 发现新消息 → LLM 生成回复 → 填入 #im-input-box → 发送
  5. 通过 ReplyHistory 去重，避免反复回复

使用流程:
  # 第 1 步：分析 API（登录后等消息到达，自动推荐最优 endpoint）
  python -m scraper.feige_monitor_agent --store sulida --analyze

  # 第 2 步：启动监控
  python -m scraper.feige_monitor_agent --store sulida --endpoint "/api/message/poll"
"""

import json
import os
import re
import time
import random
import logging
import threading
import concurrent.futures
from datetime import datetime
from typing import Optional, Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

# ============================================================
# URL 常量
# ============================================================

BUYIN_HOME = "https://buyin.jinritemai.com"
PIGEON_IM_URL = "https://im.jinritemai.com/pc_seller_v2/main/workspace"
DAREN_SQUARE_URL = "https://buyin.jinritemai.com/dashboard/servicehall/daren-square"
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# API 关键字评分 — 用于 --analyze 自动发现消息 API
# ============================================================

API_URL_KEYWORDS = {
    "message": 3, "im": 3, "chat": 3, "conversation": 3,
    "poll": 2, "sync": 2, "push": 2, "notice": 2,
    "list": 1, "new": 1, "unread": 3,
}

API_JSON_KEYS = {
    "content": 2, "sender": 2, "from_user": 2, "from": 2,
    "role": 2, "message": 2, "msg": 2,
    "timestamp": 1, "send_time": 1, "create_time": 1,
    "unread": 2, "contact": 2, "nickname": 1, "name": 1,
    "conversation_id": 2, "session_id": 2,
}

# 消息相关 API 路径特征（正则，用于拦截匹配 + 默认 endpoint 回退）
API_MESSAGE_PATTERNS = [
    r"/chat/api/backstage/conversation/get_current_conversation_list",
    r"/api/.*(?:message|msg|im|chat|conversation)",
    r"/message/?(?:poll|sync|list|push|new)",
    r"/im/?(?:poll|sync|push|list)",
]

# ============================================================
# JS 脚本 — MutationObserver：监听左侧列表新会话
# ============================================================

INJECT_SIDEBAR_OBSERVER_SCRIPT = """
() => {
    if (window.__observerActive) return;
    window.__observerActive = true;
    window.__pendingConversations = [];
    window.__observerState = {};

    setInterval(function() {
        var items = document.querySelectorAll('[data-kora="conversation"]');
        items.forEach(function(item) {
            var titleEl = item.querySelector('[title]:not(sup)');
            var name = titleEl ? titleEl.getAttribute('title') : '';
            if (!name) return;

            var os = window.__observerState[name] || (window.__observerState[name] = {});

            var allText = (item.textContent || '').trim();
            var m = allText.match(/\\d+\\u79d2|\\u521a\\u521a|\\d+\\u5206\\u949f|(?:^|\\D)(\\d{1,2}:\\d{2})(?:\\D|$)/);
            var timeText = m ? (m[1] || m[0] || '').trim() : '';
            if (!timeText) return;

            var isRelative = /[\\u79d2\\u5206\\u949f]|\\u521a\\u521a/.test(timeText);
            var prevIsRelative = os.prevIsRelative;

            // 消息预览：取第二个 [data-btm] 的文本
            var btms = item.querySelectorAll('[data-btm]');
            var preview = '';
            if (btms.length >= 2) {
                preview = (btms[1].textContent || '').trim();
            } else {
                preview = allText.replace(timeText, '').trim();
            }
            var prevPreview = os.prevPreview || '';

            if (timeText) {
                console.log('[OBSERVER]', JSON.stringify({
                    name: name,
                    isRelative: isRelative,
                    prevIsRelative: prevIsRelative,
                    preview: preview,
                    prevPreview: prevPreview,
                    timeText: timeText,
                    prevTimeText: os.prevTimeText || ''
                }));
            }

            // 绝对→相对 (新消息)  或  相对时间内内容变化 (又一条)
            if (isRelative && (!prevIsRelative || preview !== prevPreview)) {
                console.log('[OBSERVER] PUSH by relative:', name);
                window.__pendingConversations.push(name);
            }

            os.prevIsRelative = isRelative;
            os.prevTimeText = timeText;
            os.prevPreview = preview;
        });
    }, 1000);
}
"""

# ============================================================
# JS 脚本 — 点击指定会话
# ============================================================

CLICK_CONTACT_SCRIPT = """
(name) => {
    // 1. 精确 title 属性匹配
    var el = document.querySelector('[title="' + name + '"]');
    if (el) { el.click(); return true; }

    // 2. 遍历 [data-kora="conversation"] 按 title 匹配
    var items = document.querySelectorAll('[data-kora="conversation"]');
    for (var i = 0; i < items.length; i++) {
        var titleEl = items[i].querySelector('[title]:not(sup)');
        if (titleEl && (titleEl.getAttribute('title') || '').indexOf(name) !== -1) {
            titleEl.closest('[data-kora="conversation"]') ? items[i].click() : titleEl.click();
            return true;
        }
    }

    // 3. 旧结构兜底
    items = document.querySelectorAll('.msgItemWrap, [class*="contactCard"], [class*="session"], li, [role="listitem"]');
    if (items.length === 0) items = document.querySelectorAll('div:has(img)');
    for (var i = 0; i < items.length; i++) {
        if ((items[i].textContent || '').trim().indexOf(name) !== -1) {
            items[i].click();
            return true;
        }
    }

    return false;
}
"""

# ============================================================
# JS 脚本 — 提取聊天消息（仅用于兜底验证）
# ============================================================

EXTRACT_MESSAGES_SCRIPT = """
() => {
    var results = [];
    var panel = document.querySelector('.messageList') ||
                 document.querySelector('[class*="message-list"]') ||
                 document.querySelector('[class*="chat-content"]') ||
                 document.querySelector('[class*="chat-body"]');
    if (!panel) return results;
    var items = panel.querySelectorAll('.msgItemWrap, [class*="message-item"], [class*="msg-item"], [class*="chat-message"]');
    if (items.length === 0) items = panel.querySelectorAll(':scope > div > div, :scope > div');

    for (var i = 0; i < items.length; i++) {
        var item = items[i];
        var msgType = 'text';

        // 消息唯一 ID（优先取 messageCard 上的 data-id，兜底取 data-qa-message-id）
        var msgId = '';
        var msgCard = item.querySelector('[class*="messageCard"]');
        if (msgCard) msgId = msgCard.getAttribute('data-id') || '';
        if (!msgId) msgId = item.getAttribute('data-qa-message-id') || '';
        if (!msgId) msgId = item.getAttribute('data-id') || '';

        // 转接通知（transfer_staff_）
        var itemId = item.getAttribute('data-id');
        if (itemId && itemId.indexOf('transfer_staff_') === 0) {
            var text = (item.textContent || '').trim();
            var timeMatch = text.match(/\\d{1,2}:\\d{2}(?::\\d{2})?/);
            results.push({
                sender: 'system',
                content: text.substring(0, 500),
                timestamp: timeMatch ? timeMatch[0] : '',
                msg_type: 'transfer',
                image_url: '',
                msg_id: msgId,
            });
            continue;
        }

        // 发送方: 用稳定 class messageIsMe / messageNotMe，兜底检查 item 自身 class
        var isMe = item.querySelector('.messageIsMe, [class*="self"], [class*="mine"], .msg--right, .msg-right, [class*="align-right"]');
        var isOther = item.querySelector('.messageNotMe, [class*="other"], [class*="opposite"], .msg--left, .msg-left, [class*="align-left"]');
        if (!isMe && !isOther) {
            var cls = item.className || '';
            if (/self|mine|right/i.test(cls)) isMe = item;
            else if (/other|opposite|left/i.test(cls)) isOther = item;
            else {
                var flexRow = item.querySelector('[style*="flex-direction: row"]');
                if (flexRow && flexRow.children.length >= 2) {
                    isOther = flexRow.children[0].querySelector('img') ? item : null;
                    if (!isOther) isMe = item;
                } else {
                    continue;
                }
            }
        }

        // 内容: 优先检测商品卡片
        var card = item.querySelector('.chatd-card');
        var content = '';
        if (card) {
            msgType = 'product_card';
            var parts = [];
            var nameSpans = card.querySelectorAll('.pigeon-card-place-holder-text .content span');
            var nameParts = [];
            for (var ni = 0; ni < nameSpans.length; ni++) {
                var nt = (nameSpans[ni].textContent || '').trim();
                if (nt) nameParts.push(nt);
            }
            if (nameParts.length) parts.push(nameParts.join(''));
            var priceInter = card.querySelector('.chatd-price-price-inter');
            var priceDecimal = card.querySelector('.chatd-price-price-decimal');
            if (priceInter) {
                var p = '￥' + (priceInter.textContent || '').trim();
                if (priceDecimal) p += '.' + (priceDecimal.textContent || '').trim();
                parts.push(p);
            }
            var phs = card.querySelectorAll('.pigeon-card-place-holder-text .content.max-line');
            if (phs.length >= 2) {
                var sales = (phs[1].textContent || '').trim();
                if (sales) parts.push(sales);
            }

            // // 保障标签
            // var tagSpans = card.querySelectorAll('span[style*="border-width"]');
            // var tags = [];
            // for (var ti = 0; ti < tagSpans.length; ti++) {
            //     var tv = (tagSpans[ti].textContent || '').trim();
            //     if (tv) tags.push(tv);
            // }
            // if (tags.length) parts.push(tags.join(', '));

            // // 保障 +N
            // var countEl = card.querySelector('[class*="tag-group-left-count"]');
            // if (countEl) {
            //     var ct = (countEl.textContent || '').trim();
            //     if (ct) parts.push(ct);
            // }

            // 物流
            var allCardSpans = card.querySelectorAll('.pigeon-card-place-holder-text .content span');
            for (var si = 0; si < allCardSpans.length; si++) {
                var st = (allCardSpans[si].textContent || '').trim();
                if (st.indexOf('预计') !== -1 && st.indexOf('发货') !== -1) {
                    parts.push(st);
                    break;
                }
            }

            // 购买意向检测
            var inviteBtn = card.querySelector('.pigeon-card-button-list button span');
            if (inviteBtn && inviteBtn.textContent.trim() === '邀请下单') {
                parts.push('[消费者发送了此商品，有购买意向]');
            }

            content = parts.join(' ');
        }

        // 视频/图片/表情消息兜底
        if (!content) {
            var imgs = item.querySelectorAll('img');
            var imgSrc = '';
            for (var gi = 0; gi < imgs.length; gi++) {
                var cls = imgs[gi].className || '';
                if (/avatar|icon/i.test(cls)) continue;
                var src = (imgs[gi].getAttribute('src') || '').trim();
                if (src.length > 30 || /emoji|sticker|gif/i.test(src)) {
                    imgSrc = src;
                    break;
                }
            }
            if (imgSrc) {
                if (imgSrc.indexOf('base64') !== -1) {
                    content = '[视频]';
                    msgType = 'video';
                } else {
                    content = '[表情]';
                    msgType = 'emoji';
                }
            } else {
                var picEl = item.querySelector('img[alt="图片"]');
                if (picEl) {
                    content = '[图片]';
                    msgType = 'image';
                }
            }
        }

        // 非卡片: 取所有 span 中文本最长的那个（跳过空 span 和仅含时间/名字的）
        if (!content) {
            var spans = item.querySelectorAll('span');
            for (var j = 0; j < spans.length; j++) {
                var t = (spans[j].textContent || '').trim();
                if (t.length >= 1 && t.length > content.length) {
                    if (!/^\\d{1,2}:\\d{2}$/.test(t)) {
                        content = t;
                    }
                }
            }
        }

        // 时间: 正则匹配 HH:MM
        var text = (item.textContent || '').trim();
        var timeMatch = text.match(/\\d{1,2}:\\d{2}(?::\\d{2})?/);
        var timestamp = timeMatch ? timeMatch[0] : '';

        if (!content) continue;

        results.push({
            sender: isMe ? 'me' : 'other',
            content: content.substring(0, 500),
            timestamp: timestamp,
            msg_type: msgType,
            image_url: '',
            msg_id: msgId,
        });
    }
    return results;
}
"""

# ============================================================
# JS 脚本 — 发送消息（#im-input-box）
# ============================================================

FILL_INPUT_SCRIPT = """
(text) => {
    // 优先 textarea[data-qa-id]
    var input = document.querySelector('textarea[data-qa-id="qa-send-message-textarea"]');
    if (!input) input = document.querySelector('#im-input-box');
    if (!input) return 'no_input';

    var tag = input.tagName.toLowerCase();
    if (tag === 'textarea' || tag === 'input') {
        var nativeSetter = Object.getOwnPropertyDescriptor(
            window.HTMLTextAreaElement.prototype, 'value'
        ).set;
        nativeSetter.call(input, text);
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
        return 'textarea';
    }

    // div / contenteditable
    input.focus();
    input.textContent = text;
    input.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: text }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    input.dispatchEvent(new Event('blur', { bubbles: true }));
    return 'contenteditable';
}
"""

CLICK_SEND_SCRIPT = """
() => {
    // 1. 优先 textarea 回车发送（Enter 键）
    var textarea = document.querySelector('textarea[data-qa-id="qa-send-message-textarea"]');
    if (textarea) {
        textarea.dispatchEvent(new KeyboardEvent('keydown', {
            key: 'Enter', code: 'Enter', keyCode: 13, which: 13,
            bubbles: true, cancelable: true,
        }));
        return 'enter_keydown';
    }

    // 2. 按钮点击
    var allBtns = document.querySelectorAll('button, [role="button"], input[type="submit"], [class*="send"]');
    for (var i = 0; i < allBtns.length; i++) {
        var btn = allBtns[i];
        if (btn.offsetParent === null) continue;
        var text = (btn.textContent || '').trim();
        if (text === '\u53d1\u9001' || text.indexOf('\u53d1\u9001') !== -1 || text === 'Send') {
            btn.click(); return 'clicked';
        }
        var cls = btn.className || '';
        if (cls.indexOf('send-btn') !== -1 || cls.indexOf('sendBtn') !== -1 || cls.indexOf('chatd-send') !== -1 || cls.indexOf('SendButton') !== -1) {
            btn.click(); return 'clicked';
        }
    }

    // 3. 兜底: 回车
    var input = document.querySelector('#im-input-box');
    if (input) {
        input.dispatchEvent(new KeyboardEvent('keydown', {
            key: 'Enter', code: 'Enter', keyCode: 13, which: 13,
            bubbles: true, cancelable: true,
        }));
        return 'enter_key';
    }
    return 'no_button';
}
"""

CLICK_TRANSFER_SCRIPT = """
() => {
    var btn = document.querySelector('[data-qa-id="qa-transfer-conversation"]');
    if (!btn) return false;
    btn.click();
    return true;
}
"""

SELECT_TRANSFER_TARGET_SCRIPT = """
(agentNames) => {
    var items = document.querySelectorAll('[data-qa-id="qa-transfer-customer"]');
    var best = null;
    var bestCount = 9999;
    var bestFallback = null;
    var bestFallbackCount = 9999;
    for (var i = 0; i < items.length; i++) {
        var nameEl = items[i].querySelector('.userName-yhkhmJ, .name-FYR8Pd');
        if (!nameEl) continue;
        var name = (nameEl.textContent || '').trim();
        if (!name) continue;
        var numEl = items[i].querySelector('.num-aqoHIz');
        var current = 9999;
        if (numEl) {
            var m = (numEl.textContent || '').match(/(\d+)\//);
            if (m) current = parseInt(m[1], 10);
        }
        // fallback：记录所有客服中最空闲的
        if (current < bestFallbackCount) {
            bestFallbackCount = current;
            bestFallback = { el: items[i], name: name };
        }
        // 匹配名单
        for (var j = 0; j < agentNames.length; j++) {
            if (name.indexOf(agentNames[j]) !== -1 || agentNames[j].indexOf(name) !== -1) {
                if (current < bestCount) {
                    bestCount = current;
                    best = { el: items[i], name: name };
                }
                break;
            }
        }
    }
    var target = best || bestFallback;
    if (target) {
        target.el.click();
        return target.name;
    }
    return false;
}
"""

GET_SELECTED_CONTACT_SCRIPT = """
() => {
    const selected = document.querySelector(
        '[class*="contactCard"][class*="selected"], ' +
        '[class*="contactCard"][class*="active"], ' +
        '[class*="selected"], [class*="active"], [aria-selected="true"]'
    );
    if (selected) {
        const nameEl = selected.querySelector('[class*="uname"], [class*="name"], [class*="title"]');
        const img = selected.querySelector('img');
        return {
            name: nameEl ? (nameEl.textContent || '').trim() : '',
            avatar: img ? (img.getAttribute('src') || '') : '',
        };
    }
    return null;
}
"""

class FeigeMonitorAgent:
    """飞鸽消息自动化监控与 AI 回复（API 拦截驱动，不依赖 DOM 轮询）"""

    name = "feige_monitor"
    display_name = "飞鸽自动监控回复"

    def __init__(self, store: str = "", headless: bool = False,
                 dry_run: bool = False, max_replies_per_round: int = 10,
                 endpoint: str = "", fixed_reply: str = "",
                 min_delay: int = 2, max_delay: int = 6,
                 reply_cooldown: int = 60, tool_loop: bool = False):
        self.store = store
        self.headless = headless
        self.dry_run = dry_run
        self.max_replies_per_round = max_replies_per_round
        self.endpoint = endpoint
        self.fixed_reply = fixed_reply
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.reply_cooldown = reply_cooldown
        self.tool_loop = tool_loop

        self.browser = None
        self.context = None
        self.page = None
        self._pw = None
        self.running = False

        base_dir = os.path.join(DATA_DIR, store or "pigeon")
        self.storage_file = os.path.join(base_dir, "storage_state.json")
        self.cookie_file = os.path.join(base_dir, "cookies.json")
        self.jinritemai_storage = os.path.join(DATA_DIR, "jinritemai", "storage_state.json")
        self.jinritemai_cookie = os.path.join(DATA_DIR, "jinritemai", "cookies.json")

        # ---- API 拦截相关 ----
        self._message_queue: list[dict] = []
        self._queue_lock = threading.Lock()
        self._capturing = False             # --analyze 模式开启时置 True
        self._network_log: list[dict] = []   # 仅用于 --analyze
        self._captured_responses: list[dict] = []  # 仅用于 --analyze

        self._seen_msg_ids: set[str] = set()
        self._click_cooldown: dict[str, float] = {}
        self._last_msg_count: dict[str, int] = {}
        self._session_replied: set[str] = set()
        self._handoff_contacts: set[str] = set()

        self._crm_api_base = "http://localhost:7120/api/v1"
        self._service_key = ""
        try:
            sk_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "service.key")
            if os.path.exists(sk_path):
                self._service_key = open(sk_path, "r").read().strip()
        except Exception:
            pass

        self._forbidden_words: list[str] = []
        fw_path = os.path.join(DATA_DIR, "kf", "forbidden_words.json")
        if os.path.exists(fw_path):
            try:
                with open(fw_path, "r", encoding="utf-8") as f:
                    self._forbidden_words = json.load(f)
                logger.info(f"已加载 {len(self._forbidden_words)} 条禁用词")
            except Exception as e:
                logger.warning(f"加载禁用词文件失败: {e}")

        self._human_agents: list[str] = []
        ha_path = os.path.join(DATA_DIR, "kf", "human_agents.json")
        if os.path.exists(ha_path):
            try:
                with open(ha_path, "r", encoding="utf-8") as f:
                    self._human_agents = json.load(f)
                logger.info(f"已加载 {len(self._human_agents)} 个人工客服: {self._human_agents}")
            except Exception as e:
                logger.warning(f"加载人工名单失败: {e}")

    # -----------------------------------------------------------
    # DOM 消息提取（面板）
    # -----------------------------------------------------------

    # -----------------------------------------------------------
    # 浏览器生命周期
    # -----------------------------------------------------------

    def _resolve_storage(self) -> Optional[str]:
        if os.path.exists(self.storage_file):
            logger.info(f"使用 storage state: {self.storage_file}")
            return self.storage_file
        if not self.store and os.path.exists(self.jinritemai_storage):
            logger.info(f"使用 jinritemai storage fallback: {self.jinritemai_storage}")
            return self.jinritemai_storage
        logger.warning("未找到 storage state 文件")
        return None

    def _load_cookies_direct(self) -> bool:
        candidates = []
        if os.path.exists(self.cookie_file):
            candidates.append((self.cookie_file, self.store or "pigeon"))
        if not self.store and os.path.exists(self.jinritemai_cookie):
            candidates.append((self.jinritemai_cookie, "jinritemai"))
        for fpath, label in candidates:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    cookies = json.load(f)
                self.context.add_cookies(cookies)
                logger.info(f"已加载 cookie ({label}): {fpath} ({len(cookies)} 条)")
                return True
            except Exception as e:
                logger.warning(f"加载 cookie ({label}) 失败: {e}")
        return False

    def _save_storage_state(self):
        base_dir = os.path.dirname(self.storage_file)
        os.makedirs(base_dir, exist_ok=True)
        try:
            self.context.storage_state(path=self.storage_file)
            cookies = self.context.cookies()
            with open(self.cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            logger.info(f"storage state 已保存 ({len(cookies)} 条 cookie)")
        except Exception as e:
            logger.warning(f"保存 storage state 失败: {e}")

    def start(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        state_file = self._resolve_storage()
        self.browser = self._pw.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        self.context = self.browser.new_context(
            storage_state=state_file,
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        self.page = self.context.new_page()

        # ★ 立即注册网络监听，不放过从导航开始的所有网络活动
        self._network_log = []
        self._captured_responses = []
        self._capturing = False  # 仅 --analyze 模式开启
        self.context.on("request", self._on_network_event)
        self.context.on("response", self._on_network_event)
        self.context.on("websocket", self._on_websocket)

        if not state_file:
            self._load_cookies_direct()

    def _event_info(self, event) -> dict:
        """从 request / response / websocket 对象中安全提取信息"""
        import urllib.parse as up
        info = {"url": "", "method": "", "status": 0, "type": ""}
        try:
            if hasattr(event, "url"):
                info["url"] = str(event.url)[:300]
            if hasattr(event, "method"):
                info["method"] = str(event.method)
            elif hasattr(event, "request") and hasattr(event.request, "method"):
                info["method"] = str(event.request.method)
            if hasattr(event, "status"):
                info["status"] = event.status
            if hasattr(event, "request"):
                req = event.request
                if hasattr(req, "resource_type"):
                    info["type"] = str(req.resource_type)
                elif hasattr(req, "url") and req.url:
                    parsed = up.urlparse(req.url)
                    if parsed.scheme in ("ws", "wss"):
                        info["type"] = "websocket"
            return info
        except Exception as e:
            logger.debug(f"_event_info 异常: {e}")
            return info

    def close(self):
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self._pw:
                self._pw.stop()
        except Exception as e:
            logger.warning(f"关闭浏览器异常: {e}")
    def screenshot(self, name: str = "debug"):
        try:
            out_dir = os.path.join(DATA_DIR, "feige_monitor", "screenshots")
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"{name}_{datetime.now().strftime('%H%M%S')}.png")
            self.page.screenshot(path=path)
            logger.info(f"截图: {path}")
        except Exception as e:
            logger.warning(f"截图失败: {e}")

    def _wait_stable(self, timeout: int = 5):
        try:
            self.page.wait_for_load_state("networkidle", timeout=timeout * 1000)
        except Exception:
            pass
        time.sleep(2)

    # -----------------------------------------------------------
    # 登录
    # -----------------------------------------------------------

    def login(self) -> bool:
        logger.info(f"导航到卖家工作台: {PIGEON_IM_URL}")
        try:
            self.page.goto(PIGEON_IM_URL, wait_until="load", timeout=120000)
        except Exception as e:
            logger.warning(f"导航超时: {e}")
        time.sleep(8)
        current_url = self.page.url
        logger.info(f"当前 URL: {current_url}")

        if "login" in current_url or "passport" in current_url:
            logger.error("登录态已过期，请先运行:")
            logger.error(f"  python -m scraper.generic_login --name {self.store or 'pigeon'} --visible")
            self.screenshot("login_redirect")
            return False

        if "im.jinritemai.com" in current_url or "workspace" in current_url:
            text_len = self.page.evaluate("document.body.innerText.length")
            logger.info(f"卖家工作台已加载，文本长度: {text_len}")
            if text_len > 50:
                logger.info("登录成功")
                self._save_storage_state()
                return True
            logger.info("页面内容较少，继续等待...")
            for _ in range(10):
                time.sleep(3)
                text_len = self.page.evaluate("document.body.innerText.length")
                if text_len > 100:
                    logger.info(f"页面加载完成: {text_len} 字符")
                    self._save_storage_state()
                    return True
            logger.warning("页面加载超时")

        if "buyin.jinritemai.com" in current_url:
            logger.info("在巨量百应页面，尝试通过[消息]按钮进入工作台...")
            ok = self._click_chat_button()
            if ok:
                try:
                    self.page.wait_for_load_state("load", timeout=30000)
                except Exception:
                    pass
                time.sleep(5)
                logger.info(f"当前页面: {self.page.url}")
                for p in self.context.pages:
                    if p != self.page:
                        try: p.close()
                        except Exception: pass
                self._save_storage_state()
                return True

        logger.warning("无法进入卖家工作台，当前 URL: {current_url}")
        self.screenshot("login_failed")
        return False

    def _click_chat_button(self) -> bool:
        from playwright.sync_api import TimeoutError
        strategies = [
            ("#notice-tips-im-anchor-point", "#notice-tips-im-anchor-point"),
            ("[class*='tool_btn']:has-text('消息')", None),
            (".btn-item:has-text('消息')", None),
        ]
        for sel, explicit_sel in strategies:
            target = explicit_sel or sel
            logger.debug(f"尝试点击: {target}")
            try:
                with self.context.expect_event("page", timeout=15000) as page_info:
                    self.page.click(target, timeout=5000)
                self.page = page_info.value
                logger.info(f"点击成功: {target}")
                return True
            except (TimeoutError, Exception):
                continue
        logger.debug("通过 JS dispatchEvent 触发")
        try:
            with self.context.expect_event("page", timeout=15000) as page_info:
                self.page.evaluate("""() => {
                    const el = document.querySelector('#notice-tips-im-anchor-point');
                    if (!el) return;
                    const wrapper = el.closest('[class*="tool_btn"]') || el.parentElement;
                    if (wrapper) wrapper.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    else el.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                }""")
            self.page = page_info.value
            logger.info("JS dispatchEvent 打开新标签页成功")
            return True
        except Exception:
            pass
        pages = self.context.pages
        if len(pages) > 1:
            for p in pages:
                if "pigeon" in p.url.lower() or "im" in p.url.lower():
                    self.page = p
                    logger.info(f"检测到 Pigeon 页面: {p.url}")
                    return True
        return False

    # -----------------------------------------------------------
    # DOM 交互（点击会话 + 发送消息，API 拦截不需要这些做轮询）
    # -----------------------------------------------------------

    def click_contact(self, name: str) -> bool:
        logger.info(f"点击会话: {name}")
        clicked = self.page.evaluate(CLICK_CONTACT_SCRIPT, name)
        if clicked:
            time.sleep(0.5)
            return True
        logger.info(f"滚动查找会话: {name}")
        try:
            grid = self.page.locator('[class*="ReactVirtualized__Grid"], [class*="virtual-grid"]').first
            if grid.count() > 0:
                for _ in range(100):
                    before = grid.evaluate("el => el.scrollTop")
                    max_pos = grid.evaluate("el => el.scrollHeight - el.clientHeight")
                    if before >= max_pos - 1: break
                    grid.evaluate(f"el => el.scrollTop = Math.min(el.scrollTop + el.clientHeight, max_pos)")
                    time.sleep(0.3)
                    clicked = self.page.evaluate(CLICK_CONTACT_SCRIPT, name)
                    if clicked:
                        time.sleep(0.5)
                        return True
                    if before == grid.evaluate("el => el.scrollTop"): break
        except Exception:
            pass
        logger.warning(f"未找到会话: {name}")
        return False

    def extract_messages(self, max_messages: int = 3) -> list[dict]:
        """提取当前选中会话的最新消息（滚动到底部取最新）"""
        try:
            self.page.evaluate("""() => {
                const containers = document.querySelectorAll(
                    '[class*="message-list"], [class*="chat-content"], [class*="chat-body"], ' +
                    '[class*="im-body"], [class*="scroll"]'
                );
                for (const c of containers) {
                    c.scrollTop = c.scrollHeight;
                }
            }""")
            time.sleep(1)
        except Exception:
            pass
        try:
            # 二次滚动确保底部加载完成
            self.page.evaluate("""() => {
                const containers = document.querySelectorAll(
                    '[class*="message-list"], [class*="chat-content"], [class*="chat-body"], ' +
                    '[class*="im-body"], [class*="scroll"]'
                );
                for (const c of containers) {
                    c.scrollTop = c.scrollHeight;
                }
            }""")
            time.sleep(0.5)
        except Exception:
            pass
        try:
            messages = self.page.evaluate(EXTRACT_MESSAGES_SCRIPT) or []
        except Exception as e:
            logger.warning(f"提取消息失败: {e}")
            return []
        messages = messages[-max_messages:]
        logger.debug(f"消息: {len(messages)} 条")
        for m in messages:
            preview = m.get("content", "")[:60] or f"[{m.get('msg_type','?')}]"
            logger.debug(f"  [{m['sender']}] {preview}")
        return messages

    def send_message(self, text: str) -> bool:
        if not text:
            logger.warning("发送内容为空")
            return False
        # textarea[data-qa-id="qa-send-message-textarea"]
        fill_result = self.page.evaluate(FILL_INPUT_SCRIPT, text)
        if fill_result == "no_input":
            # fallback: 找 textarea
            try:
                self.page.fill('textarea[data-qa-id="qa-send-message-textarea"]', text)
                logger.debug("输入框填充 (page.fill on textarea)")
            except Exception as e:
                logger.warning(f"未找到输入框: {e}")
                self.screenshot("no_input_box")
                return False
        else:
            logger.debug(f"输入框填充: {fill_result}")
        time.sleep(0.5)
        send_result = self.page.evaluate(CLICK_SEND_SCRIPT)
        logger.info(f"发送结果: {send_result}")
        if send_result == "no_button":
            self.screenshot("no_send_button")
            return False
        time.sleep(1.5)
        return True

    def transfer_to_human(self) -> bool:
        logger.info(f"转接人工客服...")
        # 注释：以下为实际转接逻辑，需要时取消注释
        # try:
        #     ok = self.page.evaluate(CLICK_TRANSFER_SCRIPT)
        #     if not ok:
        #         logger.warning("未找到转人工按钮")
        #         return False
        #     time.sleep(1)
        #     selected = self.page.evaluate(SELECT_TRANSFER_TARGET_SCRIPT, self._human_agents)
        #     if selected:
        #         logger.info(f"转接人工成功 -> {selected}")
        #         time.sleep(1.5)
        #         return True
        #     logger.warning("弹窗中未找到可转接的客服")
        #     return False
        # except Exception as e:
        #     logger.warning(f"转接人工失败: {e}")
        #     return False
        return True

    def _sanitize_reply(self, text: str) -> str:
        if not self._forbidden_words:
            return text
        for word in self._forbidden_words:
            if word in text:
                text = text.replace(word, "**")
        return text

    def _api_chat(self, contact_name: str, message_content: str) -> dict:
        try:
            import requests
            logger.debug(f"[API_CHAT] begin contact={contact_name}")
            resp = requests.post(
                f"{self._crm_api_base}/chat",
                json={
                    "message": message_content,
                    "user_id": self.store or "feige",
                    "buyer_name": contact_name,
                    "session_key": contact_name,
                    "tool_loop": self.tool_loop,
                },
                timeout=(10, 120),
            )
            logger.debug(f"[API_CHAT] done status={resp.status_code}")
            if resp.ok:
                body = resp.json()
                if body.get("state", {}).get("code") == 0:
                    data = body.get("data", {})
                    return {
                        "answer": data.get("answer", ""),
                        "needs_handoff": data.get("needs_handoff", False),
                        "route": data.get("route", ""),
                    }
                logger.warning(f"[API_CHAT] state.code != 0: {body.get('state')}")
            else:
                logger.warning(f"[API_CHAT] HTTP {resp.status_code}")
        except requests.exceptions.ConnectTimeout:
            logger.warning(f"[API_CHAT] CRM 连接超时 (CRM 未启动?): {self._crm_api_base}")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"[API_CHAT] CRM 连接失败: {e}")
        except requests.exceptions.Timeout:
            logger.warning(f"[API_CHAT] CRM 响应超时")
        except Exception as e:
            logger.warning(f"[API_CHAT] 异常: {e}")
        return {"answer": "", "needs_handoff": False, "route": ""}

    # ============================================================
    # API 拦截核心逻辑
    # ============================================================

    def _find_json_message_structures(self, data: Any, depth: int = 0,
                                       max_depth: int = 12) -> list[dict]:
        """递归在 JSON 树中寻找消息结构

        通用规则:
          - dict 同时含 content/text + sender/from/role 等字段
        飞鸽特定规则:
          - dict 含 messageBody 子字段，且 messageBody 有 content
          - dict 含 msgList 列表（只取列表元素，不递归进 messageBody）
        """
        if depth > max_depth:
            return []
        results: list[dict] = []

        if isinstance(data, dict):
            sender_keys = {"sender", "from", "from_user", "user", "role", "speaker", "send_user"}
            content_keys = {"content", "text", "message", "msg", "body"}

            has_sender = any(k in data for k in sender_keys)
            has_content = any(k in data for k in content_keys)
            has_time = any(k in data for k in {"time", "timestamp", "send_time", "create_time", "created_at"})

            if has_content and isinstance(data.get("content") or data.get("text") or data.get("message"), str):
                if has_sender or has_time:
                    results.append(data)

            # 飞鸽特定: messageBody + msgList 已在此层处理，跳过子递归避免重复
            skip_keys = set()

            mb = data.get("messageBody")
            if isinstance(mb, dict) and mb.get("content"):
                results.append(data)
                skip_keys.add("messageBody")

            ml = data.get("msgList")
            if isinstance(ml, list):
                for item in ml:
                    if isinstance(item, dict):
                        results.append(item)
                skip_keys.add("msgList")

            for k, v in data.items():
                if k in skip_keys:
                    continue
                if isinstance(v, (dict, list)):
                    results.extend(self._find_json_message_structures(v, depth + 1, max_depth))

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    results.extend(self._find_json_message_structures(item, depth + 1, max_depth))

        return results

    def _extract_contact_hint(self, data: Any, depth: int = 0, max_depth: int = 8) -> str:
        """从 JSON 响应中提取联系人名称（支持嵌套 ext 结构）"""
        if depth > max_depth:
            return ""
        if isinstance(data, dict):
            # 优先从 ext 子对象中提取
            ext = data.get("ext")
            if isinstance(ext, dict):
                for k in ("nickname", "uname", "user_name", "name"):
                    v = ext.get(k)
                    if v and isinstance(v, str) and len(v) > 1:
                        return v
            for k in ("nickname", "uname", "contact_name", "user_name", "from_user", "name", "friend_name"):
                if k in data and isinstance(data[k], str) and len(data[k]) > 1:
                    return data[k]
            for v in data.values():
                if isinstance(v, (dict, list)):
                    name = self._extract_contact_hint(v, depth + 1, max_depth)
                    if name:
                        return name
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    name = self._extract_contact_hint(item, depth + 1, max_depth)
                    if name:
                        return name
        return ""

    def _normalize_message(self, raw: dict) -> Optional[dict]:
        """将原始 JSON 消息对象标准化为统一格式

        支持通用结构和飞鸽 Pigeon IM 嵌套结构:
          - 通用: {content, sender, timestamp, ...}
          - 飞鸽: {messageBody: {content, ext: {nickname, sender_role}}, serverMessageId}
        返回: {sender, content, contact_name, timestamp, msg_id} 或 None
        """
        # -------------------------------------------------------
        # 第一步: 尝试从 messageBody 提取（飞鸽 Pigeon IM 嵌套结构）
        # -------------------------------------------------------
        mb = raw.get("messageBody")
        if isinstance(mb, dict):
            ext = mb.get("ext") or {}
            content = mb.get("content", "") or ""
            sender_role = str(ext.get("sender_role", ""))
            nickname = ext.get("nickname") or ext.get("uname", "")
            biz_role = ext.get("s:sender_biz_role", "")
            msg_id = str(raw.get("serverMessageId", "") or mb.get("serverMessageId", ""))
            timestamp = str(mb.get("createTime", "") or mb.get("createTimestamp", ""))
            direction = ext.get("flow_extra", "")

            # 发送方判断: sender_role="2"=我方, "1"=达人; 或 biz_role 判断
            is_staff = (sender_role == "2") or ("CurrentServer" in str(biz_role))
            is_buyer = (sender_role == "1") or ("Buyer" in str(biz_role))

            if content and (is_staff or is_buyer or sender_role):
                return {
                    "sender": "me" if is_staff else "other",
                    "content": content.strip(),
                    "contact_name": (nickname or "").strip(),
                    "timestamp": timestamp,
                    "msg_id": msg_id or f"msg:{content[:40]}:{timestamp}",
                }

        # -------------------------------------------------------
        # 第二步: 通用提取（平铺结构）
        # -------------------------------------------------------
        sender = ""
        for k in ("sender", "from", "from_user", "user", "role", "speaker", "send_user"):
            v = raw.get(k)
            if v and isinstance(v, str) and v.strip():
                sender = v
                break

        content = ""
        for k in ("content", "text", "message", "msg", "body"):
            v = raw.get(k)
            if v and isinstance(v, str):
                content = v
                break

        if not content:
            return None

        timestamp = ""
        for k in ("time", "timestamp", "send_time", "create_time", "created_at"):
            v = raw.get(k)
            if v and isinstance(v, str):
                timestamp = v
                break

        contact_name = ""
        for k in ("contact_name", "nickname", "user_name", "from_user", "name", "friend_name", "contact"):
            v = raw.get(k)
            if v and isinstance(v, str) and len(v) > 1:
                contact_name = v
                break

        msg_id = ""
        for k in ("msg_id", "message_id", "id", "uuid", "mid"):
            v = raw.get(k)
            if v and isinstance(v, (str, int)):
                msg_id = str(v)
                break
        if not msg_id:
            msg_id = f"{sender}:{content[:50]}:{timestamp}"

        is_me = any(s.lower() in ("me", "self", "mine", "我") for s in (sender, raw.get("role", "")))

        return {
            "sender": "me" if is_me else "other",
            "content": content.strip() if content else "",
            "contact_name": contact_name,
            "timestamp": timestamp,
            "msg_id": msg_id,
        }

    def _parse_messages_from_api(self, data: Any) -> list[dict]:
        """自适应解析 API 响应中的消息列表

        1. 在 JSON 树中搜索消息结构
        2. 标准化为统一格式
        3. 去重（基于 msg_id）
        4. 返回 [{sender, content, contact_name, timestamp, msg_id}, ...]
        """
        raw_messages = self._find_json_message_structures(data)
        logger.debug(f"API 响应中找到 {len(raw_messages)} 个候选消息结构")

        if not raw_messages:
            return []

        # 如果没有 contact_name，尝试从响应中提取
        contact_name_hint = ""
        if raw_messages and not raw_messages[0].get("contact_name"):
            contact_name_hint = self._extract_contact_hint(data)

        normalized = []
        for raw in raw_messages:
            msg = self._normalize_message(raw)
            if msg is None:
                continue
            if not msg["contact_name"] and contact_name_hint:
                msg["contact_name"] = contact_name_hint
            if not msg["content"]:
                continue
            normalized.append(msg)

        # 去重
        seen = set()
        unique = []
        for msg in normalized:
            if msg["msg_id"] not in seen:
                seen.add(msg["msg_id"])
                unique.append(msg)

        return unique

    def _on_network_event(self, event):
        """统一网络入口 — 接收 context.on('request') 和 context.on('response')"""
        info = self._event_info(event)
        url = info["url"]
        method = info["method"]
        status = info["status"]
        rtype = info["type"]
        is_response = hasattr(event, "status")

        # 调试日志（仅 --analyze 模式，避免刷屏）
        if self._capturing and status and rtype in ("xhr", "fetch"):
            logger.debug(f"[NET] {method} {status} {rtype} {url[:120]}")

        # -------------------------------------------------------
        # --analyze 模式：记录全部
        # -------------------------------------------------------
        if self._capturing:
            self._network_log.append({
                "url": url, "method": method, "status": status,
                "type": rtype, "time": datetime.now().isoformat(),
            })
            if is_response and status == 200 and rtype in ("xhr", "fetch", ""):
                try:
                    body = event.json()
                    self._captured_responses.append({
                        "url": url, "status": status,
                        "type": rtype, "body": body,
                        "time": datetime.now().isoformat(),
                    })
                except Exception:
                    pass
            return

        # -------------------------------------------------------
        # 监控模式：仅处理匹配 endpoint 的 HTTP 响应
        # -------------------------------------------------------
        if not self.running or not self.endpoint:
            return
        if not is_response:
            return
        if status != 200:
            return
        if not re.search(self.endpoint, url):
            return

        try:
            data = event.json()
        except Exception:
            return

        messages = self._parse_messages_from_api(data)
        if messages:
            logger.info(f"拦截到 {len(messages)} 条消息: {url}")
            with self._queue_lock:
                self._message_queue.extend(messages)

    def _on_websocket(self, ws):
        """WebSocket 连接事件 — 记录连接 + 注册帧监听"""
        url = ws.url
        logger.debug(f"WebSocket: {url[:100]}")

        # 记录到日志（analyze 模式也需要）
        if self._capturing:
            self._network_log.append({
                "url": url, "method": "WEBSOCKET", "status": 101,
                "type": "websocket", "time": datetime.now().isoformat(),
            })

        # 始终注册帧监听（analyze 模式用 self._capturing 控制记录，监控模式用 self.running 控制）
        ws.on("framereceived", lambda f: self._on_ws_frame(url, f))
        ws.on("framesend", lambda f: self._on_ws_frame(url, f))

    def _on_ws_frame(self, ws_url: str, frame):
        """WebSocket 帧回调 — 记录 + 尝试解析 JSON 并入队"""
        text = frame.text if hasattr(frame, "text") else None
        if not text:
            return

        # analyze 模式：记录原始帧文本到日志（仅记录 JSON 格式）
        if self._capturing:
            try:
                json.loads(text)
                self._network_log.append({
                    "url": f"{ws_url}#frame",
                    "method": "WS_FRAME",
                    "status": 0,
                    "type": "websocket_frame",
                    "body_preview": text[:500],
                    "time": datetime.now().isoformat(),
                })
            except (json.JSONDecodeError, TypeError):
                pass

        # 监控模式：解析 JSON 并入队
        if self.running and self.endpoint and re.search(self.endpoint, ws_url):
            try:
                data = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return
            messages = self._parse_messages_from_api(data)
            if messages:
                logger.info(f"WebSocket 拦截到 {len(messages)} 条消息")
                with self._queue_lock:
                    self._message_queue.extend(messages)

    def _handle_incoming_message(self, msg: dict):
        """处理一条来自 API 的消息"""
        contact_name = msg.get("contact_name", "")
        content = msg.get("content", "").strip()
        sender = msg.get("sender", "other")
        msg_id = msg.get("msg_id", "")

        if not content or not contact_name:
            return

        # 跳过自己的消息
        if sender == "me":
            return

        # 内存去重
        if msg_id and msg_id in self._seen_msg_ids:
            return
        if msg_id:
            self._seen_msg_ids.add(msg_id)

        # 过滤
        if self._should_filter(content):
            logger.info(f"跳过过滤消息: {contact_name} - {content[:40]}")
            return

        logger.info(f"新消息: [{contact_name}] {content[:80]}")

        # 点击会话切换到该联系人
        if not self.click_contact(contact_name):
            logger.warning(f"无法点击会话 {contact_name}，尝试用联系人名称搜索")
            # 如果找不到，记录但不阻塞
            return

        if self.dry_run:
            logger.info(f"[DRY RUN] 将回复 {contact_name}: {content[:60]}")
            return

        # AI 回复
        result = self._api_chat(contact_name, content)
        reply = result.get("answer", "")
        if not reply:
            logger.warning(f"AI 回复生成失败 (CRM 返回空): {contact_name}")
            return
        reply = self._sanitize_reply(reply)

        logger.info(f"AI 回复 [{contact_name}]: {reply[:100]}")

        # 发送
        sent = self.send_message(reply)
        if sent:
            logger.info(f"回复成功: {contact_name}")
            if result.get("needs_handoff"):
                self.transfer_to_human()
        else:
            logger.error(f"发送失败: {contact_name}")

    def _save_history_turn(self, contact: str, incoming: str, outgoing: str):
        """将转人工期间的一轮对话写入 CRM 历史库。"""
        if not contact or (not incoming and not outgoing):
            return
        try:
            import requests
            resp = requests.post(
                f"{self._crm_api_base}/history/write",
                json={
                    "contact_username": contact,
                    "self_username": self.store or "feige",
                    "incoming_message": incoming,
                    "outgoing_message": outgoing,
                },
                timeout=5,
            )
            if resp.ok:
                logger.debug(f"历史写入成功: [{contact}] {incoming[:30]} / {outgoing[:30]}")
            else:
                logger.warning(f"历史写入失败: {resp.status_code}")
        except Exception as e:
            logger.warning(f"历史写入异常: {e}")

    def _sync_handoff_history(self, contact: str, messages: list[dict]):
        """检测转接通知，将人工客服期间的对话同步到 CRM 历史库。"""
        if not contact or not messages:
            return

        transfer_idx = [
            i for i, m in enumerate(messages)
            if m.get("msg_type") == "transfer"
        ]

        if len(transfer_idx) >= 2:
            start = transfer_idx[-2]
            end = transfer_idx[-1]
            handoff_msgs = messages[start+1:end]

            i = 0
            saved = 0
            while i < len(handoff_msgs):
                msg = handoff_msgs[i]
                if msg["sender"] == "other":
                    incoming = msg["content"]
                    outgoing = ""
                    j = i + 1
                    while j < len(handoff_msgs):
                        if handoff_msgs[j]["sender"] == "me":
                            outgoing = handoff_msgs[j]["content"]
                            break
                        if handoff_msgs[j]["sender"] == "other":
                            break
                        j += 1
                    self._save_history_turn(contact, incoming, outgoing)
                    saved += 1
                i += 1

            logger.info(f"转人工历史已同步: [{contact}] {saved} 轮对话")

        if len(transfer_idx) % 2 == 1:
            self._handoff_contacts.add(contact)
            logger.info(f"当前在人工客服期: {contact}")
        else:
            if contact in self._handoff_contacts:
                logger.info(f"已从人工切回 AI: {contact}")
            self._handoff_contacts.discard(contact)

    # -----------------------------------------------------------
    # 监控生命周期
    # -----------------------------------------------------------

    def start_monitor(self):
        """MutationObserver 监测左侧新会话 → click → extract → reply"""
        self.running = True
        logger.info("=" * 60)
        logger.info("飞鸽监控启动 (Observer 模式)")
        logger.info(f"  店铺: {self.store or 'pigeon'}")
        logger.info(f"  无头模式: {self.headless}")
        logger.info(f"  Dry Run: {self.dry_run}")
        logger.info(f"  单轮最大回复: {self.max_replies_per_round}")
        logger.info("=" * 60)

        # 注入侧边栏 MutationObserver
        self.page.evaluate(INJECT_SIDEBAR_OBSERVER_SCRIPT)
        logger.info("MutationObserver 已注入")

        last_stats_time = time.time()

        try:
            while self.running:
                new_names = self.page.evaluate("""() => {
                    var arr = window.__pendingConversations || [];
                    window.__pendingConversations = [];
                    return arr;
                }""") or []

                for name in new_names:
                    if not self.running:
                        break

                    logger.info(f"新会话: {name}")

                    if re.match(r'^\d+$', name):
                        logger.debug(f"跳过占位符会话: {name}")
                        continue

                    now = time.time()
                    if now - self._click_cooldown.get(name, 0) < 3:
                        continue
                    self._click_cooldown[name] = now

                    if not self.click_contact(name):
                        logger.warning(f"无法点击会话: {name}")
                        continue
                    time.sleep(1.5)

                    messages = self.extract_messages(max_messages=50)

                    # 检测转接通知，同步人工客服期对话到历史库
                    self._sync_handoff_history(name, messages)

                    # 取最后一条"我"回复之后的所有"对方"消息
                    other_msgs = []
                    for msg in reversed(messages):
                        if msg["sender"] == "other":
                            other_msgs.append(msg)
                        elif msg["sender"] == "me" and other_msgs:
                            break
                    other_msgs.reverse()

                    # 过滤掉本次运行已回复过的消息（有 msg_id 才对同 id 去重，无 id 不过滤）
                    new_msgs = []
                    for m in other_msgs:
                        uid = m.get("msg_id", "")
                        key = f"{name}|id|{uid}" if uid else None
                        if key is None or key not in self._session_replied:
                            new_msgs.append(m)
                            if key is not None:
                                self._session_replied.add(key)

                    if not new_msgs:
                        continue

                    logger.info(f"{name} 有 {len(new_msgs)} 条新消息: {[m['content'][:60] for m in new_msgs]}")

                    for msg in new_msgs:
                        try:
                            msg_content = msg.get("content", "").strip()
                            if not msg_content:
                                continue

                            if self.dry_run:
                                logger.info(f"[DRY RUN] 将回复 {name}: {msg_content[:60]}")
                                continue

                            is_media = False
                            _needs_handoff = False
                            if '图片' in msg_content:
                                reply = "图片收到，我看看哈~"
                                is_media = True
                            elif '视频' in msg_content:
                                reply = "视频收到，我先看看~"
                                is_media = True
                            else:
                                if self.fixed_reply:
                                    logger.debug(f"[REPLY] use fixed_reply for {name}")
                                    reply = self.fixed_reply
                                else:
                                    logger.debug(f"[REPLY] calling _api_chat for {name}")
                                    _sent_placeholder = False
                                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                                        future = executor.submit(self._api_chat, name, msg_content)
                                        try:
                                            _api_result = future.result(timeout=20)
                                            reply = _api_result.get("answer", "")
                                            _needs_handoff = _api_result.get("needs_handoff", False)
                                        except concurrent.futures.TimeoutError:
                                            logger.warning(f"[REPLY] _api_chat 慢 (20s)，先发兜底: {name}")
                                            fallback_reply = self._sanitize_reply("嗯嗯，我在呢~，我帮你确认下哈")
                                            self.send_message(fallback_reply)
                                            logger.info(f"兜底 [{name}]: {fallback_reply}")
                                            _sent_placeholder = True
                                            try:
                                                _api_result = future.result(timeout=60)
                                            except concurrent.futures.TimeoutError:
                                                logger.warning(f"[REPLY] _api_chat 完全超时 (80s): {name}")
                                                _api_result = {"answer": "", "needs_handoff": False, "route": ""}
                                            reply = _api_result.get("answer", "")
                                            _needs_handoff = _api_result.get("needs_handoff", False)
                                    logger.debug(f"[REPLY] _api_chat returned for {name}: {reply[:60] if reply else 'empty'} handoff={_needs_handoff}")
                            if not reply and not _sent_placeholder:
                                logger.warning(f"[REPLY] _api_chat 返回空，使用 fallback: {name}")
                                reply = "嗯嗯，我在呢~"
                            reply = self._sanitize_reply(reply)

                            logger.info(f"回复 [{name}]: {reply[:80]}")

                            delay = random.uniform(self.min_delay, self.max_delay)
                            logger.debug(f"随机延迟 {delay:.1f}s...")
                            time.sleep(delay)
                            sent = self.send_message(reply)
                            if sent:
                                logger.info(f"回复成功: {name}")
                                if is_media or (reply and _needs_handoff):
                                    self.transfer_to_human()
                            else:
                                logger.error(f"发送失败: {name}")
                        except Exception as e:
                            logger.error(f"处理消息异常 {name}: {e}", exc_info=True)
                            continue


                # 定期统计
                if time.time() - last_stats_time > 60:
                    try:
                        stats = self.reply_history.get_stats()
                        logger.info(f"统计: 共回复 {stats['total_replies']} 条 | "
                                   f"今日 {stats['today_replies']} 条 | "
                                   f"{stats['unique_contacts']} 个达人")
                    except Exception:
                        pass
                    last_stats_time = time.time()

                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("用户中断")
        finally:
            logger.info("监控已停止")

    def stop(self):
        self.running = False

    # ============================================================
    # 网络分析模式（--analyze）
    # ============================================================

    def analyze_network(self, duration: int = 120):
        """分析卖家工作台的网络请求，自动推荐消息 API endpoint（监听已在 start() 中注册）"""
        total_so_far = len(self._network_log)
        json_so_far = len(self._captured_responses)

        logger.info("=" * 60)
        logger.info(f"网络分析模式 — 已自动记录 {total_so_far} 个请求 / {json_so_far} 个 JSON 响应")
        logger.info(f"将在 {duration} 秒后自动分析并推荐最优 endpoint")
        logger.info("请在浏览器中操作：点击会话、刷新页面、或等待消息到达")
        logger.info("=" * 60)

        # 提示用户是否刷新页面以触发更多请求
        logger.info("已开启捕获，当前日志保留，持续记录中...")
        logger.info(f"（按 Enter 提前分析，或等待 {duration} 秒自动分析）")

        start_time = time.time()
        import msvcrt
        logger.info("按 Enter 提前结束 capture")
        while time.time() - start_time < duration:
            if msvcrt.kbhit():
                key = msvcrt.getch()
                if key in (b'\r', b'\n'):
                    logger.info("用户提前结束 capture")
                    break
            time.sleep(0.5)
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 30 == 0:
                logger.info(f"  已记录 {len(self._network_log)} 个请求, {len(self._captured_responses)} 个 JSON 响应 ({elapsed}s)")

        logger.info(f"记录完成: {len(self._network_log)} 请求 / {len(self._captured_responses)} JSON 响应")
        self._capturing = False

        # 评分
        scored = self._score_all_endpoints()
        self._print_scored_endpoints(scored)

        # 保存日志
        out_dir = os.path.join(DATA_DIR, "feige_monitor")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "network_analysis.json")
        output = {
            "analysis_time": datetime.now().isoformat(),
            "store": self.store,
            "total_requests": len(self._network_log),
            "total_json_responses": len(self._captured_responses),
            "scored_endpoints": scored[:20],
            "top_recommendation": scored[0] if scored else None,
            "network_log": self._network_log,
            "captured_responses": self._captured_responses,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logger.info(f"完整分析报告已保存: {out_path}")

        if scored:
            best = scored[0]
            logger.info("")
            logger.info("=" * 60)
            logger.info("推荐使用以下命令启动监控:")
            logger.info(f"  python -m scraper.feige_monitor_agent --store {self.store or 'pigeon'} --endpoint \"{best['url_pattern']}\"")
            logger.info("=" * 60)

        return scored

    def _score_api_endpoint(self, url: str, json_data: Any) -> float:
        """对单个 API endpoint 进行消息相关度评分"""
        score = 0.0

        # URL 关键字加分
        for kw, points in API_URL_KEYWORDS.items():
            if kw in url.lower():
                score += points

        # JSON 结构分析
        if isinstance(json_data, dict):
            data_str = json.dumps(json_data, ensure_ascii=False)
            # 检查 JSON key 名称
            for kw, points in API_JSON_KEYS.items():
                if kw in data_str.lower():
                    score += points

            # 查找消息结构
            msgs = self._find_json_message_structures(json_data)
            if msgs:
                score += len(msgs) * 5  # 每个消息结构 +5 分
                # 检查消息中是否有中文对话内容
                for m in msgs:
                    for k in ("content", "text", "message"):
                        v = m.get(k)
                        if v and isinstance(v, str) and len(v) > 5:
                            if any('\u4e00' <= c <= '\u9fff' for c in v):
                                score += 8  # 包含中文对话内容，高相关性

        return score

    def _score_all_endpoints(self) -> list[dict]:
        """对所有捕获的响应进行评分排序"""
        url_scores: dict[str, dict] = {}

        for entry in self._captured_responses:
            url = entry["url"]
            body = entry.get("body", {})
            if not body:
                continue

            # 聚合相同 URL 的评分
            if url not in url_scores:
                url_scores[url] = {
                    "url": url,
                    "score": 0.0,
                    "count": 0,
                    "sample_body": None,
                    "status": entry.get("status", 0),
                }

            score = self._score_api_endpoint(url, body)
            url_scores[url]["score"] += score
            url_scores[url]["count"] += 1
            # 保留第一条含消息结构的 body 作为示例
            if url_scores[url]["sample_body"] is None and score > 0:
                url_scores[url]["sample_body"] = body

        # 按总分排序
        scored = sorted(url_scores.values(), key=lambda x: x["score"], reverse=True)

        # 生成 URL 模式（通用化）
        for item in scored:
            url = item["url"]
            # 取路径部分作为 pattern
            from urllib.parse import urlparse
            try:
                parsed = urlparse(url)
                path = parsed.path
                if parsed.query:
                    path += "?" + parsed.query.split("&")[0]  # 只保留第一个参数
                item["url_pattern"] = re.escape(path).replace(r"\?", "?").replace(r"\&", "&")
            except Exception:
                item["url_pattern"] = url

        return scored

    def _print_scored_endpoints(self, scored: list[dict]):
        if not scored:
            logger.info("未捕获到任何可评分的 API 响应")
            return

        logger.info("")
        logger.info("=" * 60)
        logger.info("API Endpoint 评分排行 (TOP 10):")
        logger.info("=" * 60)
        for i, item in enumerate(scored[:10], 1):
            score = item["score"]
            url = item["url"]
            count = item["count"]
            has_msg = "✓" if item.get("sample_body") else " "
            logger.info(f"  {i}. [{score:4.0f}分] [{has_msg}] {url[:120]}")
            logger.info(f"      请求次数: {count}  |  推荐 pattern: {item.get('url_pattern', url)[:80]}")

        # 显示最佳候选的消息示例
        best = scored[0]
        if best.get("sample_body"):
            logger.info("")
            logger.info("最佳候选响应示例 (截取前 1000 字符):")
            sample = json.dumps(best["sample_body"], ensure_ascii=False, indent=2)[:1000]
            for line in sample.split("\n"):
                logger.info(f"  {line}")
        logger.info("=" * 60)


# ============================================================
# CLI
# ============================================================


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="飞鸽消息自动化监控与 AI 回复（轮询 conversation_list API）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用流程:
  第1步 — 分析 API（可选，查看网络结构）:
    python -m scraper.feige_monitor_agent --store sulida --analyze

  启动监控:
    python -m scraper.feige_monitor_agent --store sulida

  固定回复:
    python -m scraper.feige_monitor_agent --store sulida --fixed-reply "你好，有什么可以帮您的？"

  预览不回复:
    python -m scraper.feige_monitor_agent --store sulida --dry-run
        """,
    )
    parser.add_argument("--store", type=str, default="",
                        help="店铺目录名，对应 data/{store}/ 下的 storage_state (默认 data/pigeon/)")
    parser.add_argument("--headless", action="store_true",
                        help="无头模式（默认 False）")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅检测预览，不实际回复")
    parser.add_argument("--max-replies", type=int, default=10,
                        help="单轮最大回复数（默认 10）")
    parser.add_argument("--endpoint", type=str, default="",
                        help="消息 API URL 模式（正则），如 '/api/im/message/list'")
    parser.add_argument("--fixed-reply", type=str, default="",
                        help="固定回复文本，设置后不调用 AI 回复，直接用此文本回复")
    parser.add_argument("--min-delay", type=int, default=2,
                        help="发送前最小随机延迟秒数（默认 2）")
    parser.add_argument("--max-delay", type=int, default=6,
                        help="发送前最大随机延迟秒数（默认 6）")
    parser.add_argument("--reply-cooldown", type=int, default=60,
                        help="同一条消息的重复回复冷却秒数（默认 60）")
    parser.add_argument("--analyze", action="store_true",
                        help="网络分析模式：捕获 API 请求，自动推荐消息 endpoint")
    parser.add_argument("--duration", type=int, default=120,
                        help="--analyze 模式持续时间（秒，默认 120）")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="详细日志")
    parser.add_argument("--tool-loop", action="store_true",
                        help="启用灵活工具调用模式（LLM 自主选择工具）")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    agent = FeigeMonitorAgent(
        store=args.store,
        headless=args.headless,
        dry_run=args.dry_run,
        max_replies_per_round=args.max_replies,
        endpoint=args.endpoint,
        fixed_reply=args.fixed_reply,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        reply_cooldown=args.reply_cooldown,
        tool_loop=args.tool_loop,
    )

    try:
        logger.info("启动浏览器...")
        agent.start()

        ok = agent.login()
        if not ok:
            logger.error("登录失败")
            return

        agent._wait_stable()

        if args.analyze:
            agent.analyze_network(duration=args.duration)
        else:
            agent.start_monitor()

    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"运行失败: {e}", exc_info=True)
    finally:
        agent.close()


if __name__ == "__main__":
    main()
