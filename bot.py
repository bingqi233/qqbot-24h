#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QQ 官方机器人 + 免费大模型 · 7x24 常驻版
=========================================
- 连接：腾讯 QQ 开放平台 WebSocket 网关（出站长连接，不需要公网 IP / 不需要开 QQ 客户端）
- 对话：任意 OpenAI 兼容接口（默认智谱 GLM-4.7-Flash，官方免费模型）
- 记忆：SQLite 保存每个会话最近若干轮
- 保活：自带心跳、断线自动重连、token 自动刷新
- 探活：内置 HTTP 服务，供云平台健康检查
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from collections import OrderedDict

import httpx
from aiohttp import ClientSession, WSMsgType, web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("qqbot")

# ---------------- 配置 ----------------
APP_ID = os.getenv("QQ_APP_ID", "").strip()
APP_SECRET = os.getenv("QQ_APP_SECRET", "").strip()

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://open.bigmodel.cn/api/paas/v4").rstrip("/")
LLM_API_KEY = os.getenv("LLM_API_KEY", "").strip()
LLM_MODEL = os.getenv("LLM_MODEL", "glm-4.7-flash")

SYSTEM_PROMPT = os.getenv(
    "SYSTEM_PROMPT",
    "你是 QQ 上的聊天助手。回答用中文，口语化、简短自然，一般不超过 150 字；"
    "除非用户明确要求，不要输出 Markdown 标题、表格或大段代码。",
).strip()

HISTORY_TURNS = int(os.getenv("HISTORY_TURNS", "8"))
MAX_REPLY_CHARS = int(os.getenv("MAX_REPLY_CHARS", "1500"))
REPLY_CHUNK = 900
DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "data/bot.db")
ALLOW_GROUPS = {x.strip() for x in os.getenv("ALLOW_GROUPS", "").split(",") if x.strip()}

API_BASE = "https://api.sgroup.qq.com"
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
INTENTS = 1 << 25  # GROUP_AND_C2C_EVENT：群 @ 消息 + 单聊消息

MENTION_RE = re.compile(r"<@!?\d+>|<@![^>]+>|^\s*/?@\S+\s*")


# ---------------- token 管理 ----------------
class TokenManager:
    def __init__(self) -> None:
        self.token = None
        self.expires_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self, force: bool = False) -> str:
        async with self._lock:
            if not force and self.token and time.time() < self.expires_at - 60:
                return self.token
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(
                    TOKEN_URL, json={"appId": APP_ID, "clientSecret": APP_SECRET}
                )
                r.raise_for_status()
                data = r.json()
            token = data.get("access_token")
            if not token:
                raise RuntimeError(f"获取 access_token 失败: {r.text[:200]}")
            self.token = token
            self.expires_at = time.time() + int(data.get("expires_in", 7200))
            LOG.info("access_token 已刷新，有效期 %ss", data.get("expires_in", 7200))
            return token


# ---------------- 开放平台 REST ----------------
class QQApi:
    def __init__(self, tokens: TokenManager) -> None:
        self.tokens = tokens
        self._http = httpx.AsyncClient(timeout=20)

    async def _headers(self) -> dict:
        return {
            "Authorization": f"QQBot {await self.tokens.get()}",
            "Content-Type": "application/json",
        }

    async def gateway(self) -> str:
        r = await self._http.get(f"{API_BASE}/gateway", headers=await self._headers())
        r.raise_for_status()
        return r.json()["url"]

    async def _post_message(self, path: str, payload: dict) -> dict:
        r = await self._http.post(
            f"{API_BASE}{path}", headers=await self._headers(), json=payload
        )
        if r.status_code >= 400:
            LOG.warning("发送失败 %s %s -> %s %s", path, payload, r.status_code, r.text[:200])
        return r.json() if r.text else {}

    async def reply_c2c(self, openid: str, msg_id: str, content: str, seq: int) -> None:
        await self._post_message(
            f"/v2/users/{openid}/messages",
            {"content": content, "msg_type": 0, "msg_id": msg_id, "msg_seq": seq},
        )

    async def reply_group(self, group_openid: str, msg_id: str, content: str, seq: int) -> None:
        await self._post_message(
            f"/v2/groups/{group_openid}/messages",
            {"content": content, "msg_type": 0, "msg_id": msg_id, "msg_seq": seq},
        )


# ---------------- 记忆 ----------------
class Memory:
    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS history ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, skey TEXT, role TEXT, content TEXT, ts REAL)"
        )
        self.conn.commit()
        self._lock = asyncio.Lock()

    def _load(self, skey: str, turns: int):
        cur = self.conn.execute(
            "SELECT role, content FROM history WHERE skey=? ORDER BY id DESC LIMIT ?",
            (skey, turns * 2),
        )
        rows = cur.fetchall()[::-1]
        return [{"role": r, "content": c} for r, c in rows]

    def load(self, skey: str, turns: int):
        return self._load(skey, turns)

    def append(self, skey: str, role: str, content: str) -> None:
        self.conn.execute(
            "INSERT INTO history (skey, role, content, ts) VALUES (?,?,?,?)",
            (skey, role, content, time.time()),
        )
        self.conn.execute(
            "DELETE FROM history WHERE skey=? AND id NOT IN "
            "(SELECT id FROM history WHERE skey=? ORDER BY id DESC LIMIT ?)",
            (skey, skey, HISTORY_TURNS * 2 + 10),
        )
        self.conn.commit()

    def reset(self, skey: str) -> None:
        self.conn.execute("DELETE FROM history WHERE skey=?", (skey,))
        self.conn.commit()


# ---------------- 对话 ----------------
class Brain:
    def __init__(self, api: QQApi, mem: Memory) -> None:
        self.api = api
        self.mem = mem
        self.seen: OrderedDict[str, float] = OrderedDict()
        self.queue_locks: dict[str, asyncio.Lock] = {}

    def _seen(self, mid: str) -> bool:
        if not mid:
            return False
        if mid in self.seen:
            return True
        self.seen[mid] = time.time()
        if len(self.seen) > 500:
            self.seen.popitem(last=False)
        return False

    def _lock(self, skey: str) -> asyncio.Lock:
        if skey not in self.queue_locks:
            self.queue_locks[skey] = asyncio.Lock()
        return self.queue_locks[skey]

    async def chat(self, skey: str, user_text: str) -> str:
        if not LLM_API_KEY:
            return "（机器人还没配置模型密钥，请管理员补上 LLM_API_KEY）"
        history = self.mem.load(skey, HISTORY_TURNS)
        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history
        messages.append({"role": "user", "content": user_text})
        payload = {
            "model": LLM_MODEL,
            "messages": messages,
            "temperature": 0.7,
            "stream": False,
            "max_tokens": 1024,
        }
        # GLM-4.7-Flash 默认是「混合思考」模型，思考链会把正文挤掉、也更容易被限流
        if os.getenv("DISABLE_THINKING", "1") != "0":
            payload["thinking"] = {"type": "disabled"}

        headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
        data = None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=60) as c:
                    r = await c.post(f"{LLM_BASE_URL}/chat/completions", headers=headers, json=payload)
                if r.status_code == 429 or r.status_code >= 500:
                    LOG.warning("模型限流/异常 %s（第 %s 次），重试中", r.status_code, attempt + 1)
                    await asyncio.sleep(2 + attempt * 3)
                    continue
                if r.status_code >= 400:
                    LOG.warning("模型返回 %s: %s", r.status_code, r.text[:300])
                    return "模型那边出了点问题，稍后再试试～"
                data = r.json()
                break
            except Exception as e:  # noqa: BLE001
                LOG.warning("调用模型失败（第 %s 次）: %r", attempt + 1, e)
                await asyncio.sleep(2 + attempt * 3)

        if not data:
            return "我这边有点挤，等一下再问我吧～"

        msg = data["choices"][0]["message"]
        answer = (msg.get("content") or "").strip()
        if not answer:
            answer = (msg.get("reasoning_content") or "").strip()
        if not answer:
            return "嗯…我没想好怎么回，你再说一次？"
        self.mem.append(skey, "user", user_text)
        self.mem.append(skey, "assistant", answer)
        return answer

    @staticmethod
    def _chunks(text: str):
        text = text.strip()
        if len(text) > MAX_REPLY_CHARS:
            text = text[:MAX_REPLY_CHARS] + "…"
        out, buf = [], ""
        for line in text.splitlines(keepends=True):
            if len(buf) + len(line) > REPLY_CHUNK and buf:
                out.append(buf)
                buf = ""
            buf += line
        if buf:
            out.append(buf)
        return [c.strip() for c in out if c.strip()][:5]

    async def handle_event(self, etype: str, d: dict) -> None:
        mmid = d.get("id", "")
        if self._seen(mmid):
            return
        text = (d.get("content") or "").strip()
        text = MENTION_RE.sub("", text, count=1).strip()
        if not text:
            return

        if etype == "C2C_MESSAGE_CREATE":
            skey = "c2c:" + d.get("user", {}).get("openid", "")
            target = ("c2c", d.get("user", {}).get("openid", ""))
        else:
            gid = d.get("group_openid", "")
            if ALLOW_GROUPS and gid not in ALLOW_GROUPS:
                LOG.info("群 %s 不在白名单，忽略", gid)
                return
            skey = "group:" + gid
            target = ("group", gid)

        LOG.info("[%s] %s: %s", etype, skey, text[:80])

        if text in ("/reset", "重置对话", "清空记忆"):
            self.mem.reset(skey)
            if not DRY_RUN:
                await self._send(target, mmid, "好，之前的聊天我忘掉了。", 1)
            return

        if DRY_RUN:
            LOG.info("[DRY-RUN] 不回复，仅验证链路")
            return

        async with self._lock(skey):
            answer = await self.chat(skey, text)
            for i, part in enumerate(self._chunks(answer), start=1):
                await self._send(target, mmid, part, i)
                await asyncio.sleep(0.3)

    async def _send(self, target, msg_id: str, content: str, seq: int) -> None:
        kind, tid = target
        if kind == "c2c":
            await self.api.reply_c2c(tid, msg_id, content, seq)
        else:
            await self.api.reply_group(tid, msg_id, content, seq)


# ---------------- 网关 ----------------
async def heartbeat_loop(ws, interval: float, get_seq) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send_json({"op": 1, "d": get_seq()})
    except Exception:  # noqa: BLE001
        return


async def gateway_session(api: QQApi, brain: Brain) -> None:
    url = await api.gateway()
    LOG.info("网关地址: %s", url)
    async with ClientSession() as sess:
        ws = await sess.ws_connect(url, heartbeat=None)
        hb_interval = 45.0
        seq = [0]
        hb_task = None
        try:
            while True:
                msg = await ws.receive(timeout=hb_interval * 2 + 15)
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    LOG.warning("WebSocket 关闭: %s", msg.type)
                    return
                else:
                    continue

                op = data.get("op")
                if op == 10:  # Hello
                    hb_interval = data["d"]["heartbeat_interval"] / 1000
                    token = await api.tokens.get(force=True)
                    await ws.send_json(
                        {
                            "op": 2,
                            "d": {
                                "token": f"QQBot {token}",
                                "intents": INTENTS,
                                "shard": [0, 1],
                                "properties": {},
                            },
                        }
                    )
                    if hb_task:
                        hb_task.cancel()
                    hb_task = asyncio.create_task(heartbeat_loop(ws, hb_interval, lambda: seq[0]))
                elif op == 0:  # Dispatch
                    seq[0] = data.get("s", seq[0])
                    etype = data.get("t")
                    if etype == "READY":
                        sess_ = data.get("d", {}).get("session_id")
                        LOG.info("✅ 已上线，session_id=%s", sess_)
                    elif etype in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"):
                        asyncio.create_task(brain.handle_event(etype, data.get("d") or {}))
                elif op == 11:  # 心跳 ACK
                    continue
                elif op in (7, 9):
                    LOG.warning("要求重连 (op=%s)", op)
                    return
        finally:
            if hb_task:
                hb_task.cancel()
            await ws.close()


async def gateway_forever(api: QQApi, brain: Brain) -> None:
    while True:
        try:
            await gateway_session(api, brain)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            LOG.warning("网关异常: %r", e)
        LOG.info("5 秒后重连…")
        await asyncio.sleep(5)


# ---------------- 探活 HTTP ----------------
async def health(request):
    return web.Response(
        text=json.dumps({"ok": True, "app": APP_ID, "model": LLM_MODEL}),
        content_type="application/json",
    )


async def main() -> None:
    if not APP_ID or not APP_SECRET:
        LOG.error("缺少 QQ_APP_ID / QQ_APP_SECRET，请检查 .env")
        return

    tokens = TokenManager()
    api = QQApi(tokens)
    mem = Memory(DB_PATH)
    brain = Brain(api, mem)

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    LOG.info("探活服务已启动 :%s", PORT)

    LOG.info("启动机器人 appId=%s model=%s dry_run=%s", APP_ID, LLM_MODEL, DRY_RUN)
    await gateway_forever(api, brain)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("手动退出")
