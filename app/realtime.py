"""Hub thoi gian thuc cua MOI server: giu WebSocket cuc bo + noi Redis pub/sub de cac server noi chuyen voi nhau.
Moi su kien di qua dung 1 duong: publish len Redis -> MOI server (ke ca server gui) nhan -> day xuong WS cuc bo."""
import asyncio
import json
import logging
import redis.asyncio as aioredis
from . import keys
from .config import settings

log = logging.getLogger("livechat.hub")


class Hub:
    def __init__(self, server_id: str):
        self.server_id = server_id
        self.redis = None
        self.rooms: dict = {}      # conversation_id(str) -> {conn_id: websocket}
        self.feed: dict = {}       # conn_id -> websocket cua dashboard nhan vien
        self._pubsub = None
        self._task = None

    # ---------- vong doi ----------
    async def start(self):
        self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        try:
            await self.redis.ping()
        except Exception:
            log.error("Khong ket noi duoc Redis tai %s - hay chay: docker compose up -d", settings.redis_url)
            raise
        await self._subscribe()
        self._task = asyncio.create_task(self._listen())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        for obj in (self._pubsub, self.redis):
            if obj is not None:
                try:
                    await obj.aclose()
                except Exception:
                    pass

    async def _subscribe(self):
        self._pubsub = self.redis.pubsub()
        await self._pubsub.psubscribe(keys.CHAT_EVENTS_PATTERN)
        await self._pubsub.subscribe(keys.AGENT_EVENTS_CHANNEL)

    async def _listen(self):
        delay = 1
        while True:
            try:
                if self._pubsub is None:
                    await self._subscribe()
                    log.info("Da noi lai Redis pub/sub")
                async for raw in self._pubsub.listen():
                    delay = 1
                    if raw["type"] in ("message", "pmessage"):
                        await self._dispatch(raw["channel"], raw["data"])
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("Mat ket noi pub/sub (%s), thu lai sau %ss", type(e).__name__, delay)
                self._pubsub = None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15)

    # ---- ket noi WebSocket cuc bo ----
    def join(self, cid, conn_id, ws):
        self.rooms.setdefault(str(cid), {})[conn_id] = ws

    def leave(self, cid, conn_id):
        room = self.rooms.get(str(cid))
        if room:
            room.pop(conn_id, None)
            if not room:
                del self.rooms[str(cid)]

    def stats(self) -> dict:
        return {"server_id": self.server_id,
                "chat_connections": sum(len(r) for r in self.rooms.values()),
                "feed_connections": len(self.feed)}

    # ---------- PHAT (publish) ----------
    async def publish_chat(self, cid, payload: dict, exclude=None, close_room=False):
        env = {"origin": self.server_id, "payload": payload, "exclude": exclude, "close_room": close_room}
        try:
            await self.redis.publish(keys.channel(cid), json.dumps(env))
        except Exception as e:
            log.warning("Publish loi (%s) - chi phat cho ket noi tren server nay", type(e).__name__)
            await self._deliver_room(str(cid), env)

    async def publish_feed(self, payload: dict):
        env = {"origin": self.server_id, "payload": payload}
        try:
            await self.redis.publish(keys.AGENT_EVENTS_CHANNEL, json.dumps(env))
        except Exception:
            log.exception("Publish feed loi")
            await self._deliver_feed(env)

    # ---------- NHAN (dispatch) ----------
    async def _dispatch(self, channel: str, data: str):
        try:
            env = json.loads(data)
        except (TypeError, ValueError):
            return
        if channel == keys.AGENT_EVENTS_CHANNEL:
            await self._deliver_feed(env)
        else:                                   # chat:{cid}:events
            await self._deliver_room(channel.split(":")[1], env)

    async def _deliver_room(self, cid: str, env: dict):
        room = self.rooms.get(cid)
        if not room:
            return
        exclude = env.get("exclude")
        for conn_id, ws in list(room.items()):
            if conn_id == exclude:
                continue
            try:
                await ws.send_json(env["payload"])
            except Exception:
                room.pop(conn_id, None)
        if env.get("close_room"):
            for conn_id, ws in list(self.rooms.get(cid, {}).items()):
                try:
                    await ws.close(code=1000, reason="conversation closed")
                except Exception:
                    pass
            self.rooms.pop(cid, None)

    async def _deliver_feed(self, env: dict):
        for conn_id, ws in list(self.feed.items()):
            try:
                await ws.send_json(env["payload"])
            except Exception:
                self.feed.pop(conn_id, None)