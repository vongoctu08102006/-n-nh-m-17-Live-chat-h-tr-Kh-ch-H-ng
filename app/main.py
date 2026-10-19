import asyncio
import hmac
import json
import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import redis_store as store
from . import repository as repo
from . import service
from .cassandra_client import shutdown
from .config import settings
from .realtime import Hub
from .redis_store import safe

log = logging.getLogger("livechat")
MAX_LEN = 2000
router = APIRouter()


def _key_ok(provided) -> bool:
    """AGENT_KEY de trong -> mo (che do dev). Da dat -> phai khop (so sanh hang thoi gian)."""
    return (not settings.agent_key) or hmac.compare_digest(str(provided or "").encode(), settings.agent_key.encode())


def require_agent(x_agent_key: Optional[str] = Header(default=None)):
    if not _key_ok(x_agent_key):
        raise HTTPException(status_code=401, detail="Thieu hoac sai ma nhan vien (header X-Agent-Key)")


AGENT_ONLY = [Depends(require_agent)]

def _hub(obj) -> Hub:
    return obj.app.state.hub


def _not_found():
    return HTTPException(status_code=404, detail="Khong tim thay cuoc chat")


# ---------------- REST ----------------
class NewConversation(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)


@router.get("/api/health")
async def health(request: Request):
    hub = _hub(request)
    return {"ok": True, **hub.stats()}


@router.post("/api/conversations", status_code=201)
async def create_conversation(body: NewConversation, request: Request):
    hub = _hub(request)
    conv = await asyncio.to_thread(repo.create_conversation, body.customer_id.strip())
    # Hang cho + thong bao cho dashboard la phan Redis: loi thi chi ghi log, cuoc chat van tao thanh cong.
    await safe(store.queue_add(hub.redis, conv))
    await safe(hub.publish_feed({"type": "queue_add", "conversation": conv}))
    return conv


@router.get("/api/conversations/{conversation_id}")
async def get_conversation(conversation_id: uuid.UUID):
    try:
        return await asyncio.to_thread(repo.get_conversation, conversation_id)
    except repo.NotFound:
        raise _not_found()


@router.post("/api/conversations/{conversation_id}/close",dependencies=AGENT_ONLY)
async def close_conversation(conversation_id: uuid.UUID, request: Request):
    hub = _hub(request)
    try:
        conv = await asyncio.to_thread(repo.close_conversation, conversation_id)
    except repo.NotFound:
        raise _not_found()
    if await safe(store.queue_remove(hub.redis, conversation_id), False):
        await safe(hub.publish_feed({"type": "queue_remove", "conversation_id": str(conversation_id),
                                     "reason": "closed"}))
    # Phat qua Redis: ket noi nam o BAT KY server nao cung nhan duoc va bi dong.
    await hub.publish_chat(conversation_id, {"type": "closed", "conversation": conv}, close_room=True)
    return conv


@router.get("/api/conversations/{conversation_id}/messages")
async def get_messages(conversation_id: uuid.UUID, request: Request, limit: int = Query(50, ge=1, le=100),
                       before: Optional[uuid.UUID] = None):
    try:
        return await service.get_history(_hub(request), conversation_id, limit, before)
    except repo.NotFound:
        raise _not_found()


@router.get("/api/customers/{customer_id}/conversations")
async def customer_conversations(customer_id: str, limit: int = Query(20, ge=1, le=100)):
    return await asyncio.to_thread(repo.list_by_customer, customer_id, limit)


@router.get("/api/agents/{agent_id}/conversations",dependencies=AGENT_ONLY)
async def agent_conversations(agent_id: str, limit: int = Query(20, ge=1, le=100)):
    return await asyncio.to_thread(repo.list_by_agent, agent_id, limit)



@router.get("/api/conversations/{conversation_id}/queue-position")
async def queue_position(conversation_id: uuid.UUID, request: Request):
    """Cho widget cua KHACH: chi tra vi tri cua chinh cuoc chat nay, khong lo danh sach khach khac."""
    q = await safe(store.queue_list(_hub(request).redis), [])
    ids = [x["id"] for x in q]
    cid = str(conversation_id)
    return {"position": ids.index(cid) + 1 if cid in ids else None, "total": len(ids)}


@router.get("/api/queue", dependencies=AGENT_ONLY)
async def get_queue(request: Request):
    return await safe(store.queue_list(_hub(request).redis), [])

@router.get("/api/presence/{role}/{user_id}")
async def get_presence(role: str, user_id: str, request: Request):
    return {"role": role, "user_id": user_id, "online": await safe(store.is_online(_hub(request).redis, role, user_id), False)}


@router.get("/api/agents/online/list",dependencies=AGENT_ONLY)
async def online_agents(request: Request):
    return await safe(store.online_users(_hub(request).redis, "agent"), [])


@router.get("/api/metrics",dependencies=AGENT_ONLY)
async def metrics(request: Request):
    hub = _hub(request)
    m = await safe(store.get_metrics(hub.redis), {"cache_hits": None, "cache_misses": None, "hit_ratio": None})
    return {**m, **hub.stats()}


@router.post("/api/metrics/reset",dependencies=AGENT_ONLY)
async def reset_metrics(request: Request):
    await safe(store.reset_metrics(_hub(request).redis))
    return {"ok": True}


# ---------------- WebSocket: cuoc chat ----------------
@router.websocket("/ws/{conversation_id}")
async def chat_ws(ws: WebSocket, conversation_id: uuid.UUID, role: str, user_id: str, key: str = ""):
    """role = customer | agent. Ma dong: 4400 tham so sai, 4403 sai khach, 4404 khong ton tai,
    4409 da co nhan vien khac, 4410 cuoc chat da dong. Client nen gui {"type":"ping"} moi ~25s."""
    hub = _hub(ws)
    await ws.accept()
    user_id = user_id.strip()
    if role not in ("customer", "agent") or not user_id:
        await ws.close(code=4400, reason="bad role or user_id")
        return
    if role == "agent" and not _key_ok(key):
        await ws.close(code=4401, reason="unauthorized")
        return
    claimed = False
    try:
        conv = await asyncio.to_thread(repo.get_conversation, conversation_id)
        if conv["status"] == "closed":
            raise repo.Closed()
        if role == "customer":
            if conv["customer_id"] != user_id:
                raise PermissionError()
        else:
            was_waiting = conv["status"] == "waiting"
            conv = await asyncio.to_thread(repo.assign_agent, conversation_id, user_id)
            claimed = was_waiting
    except repo.NotFound:
        # cuoc chat khong con trong Cassandra -> don khoi hang cho Redis, de khong con "khach ma"
        if await safe(store.queue_remove(hub.redis, conversation_id), False):
            await safe(hub.publish_feed({"type": "queue_remove", "conversation_id": str(conversation_id),
                                         "reason": "gone"}))
        await ws.close(code=4404, reason="not found"); return
    except repo.Closed:
        await ws.close(code=4410, reason="conversation closed"); return
    except PermissionError:
        await ws.close(code=4403, reason="not your conversation"); return
    except repo.Conflict:
        await ws.close(code=4409, reason="already has another agent"); return

    conn_id = uuid.uuid4().hex
    hub.join(conversation_id, conn_id, ws)
    await safe(store.presence_connect(hub.redis, role, user_id))
    try:
        if claimed:
            await safe(store.queue_remove(hub.redis, conversation_id))
            await safe(hub.publish_feed({"type": "queue_remove", "conversation_id": str(conversation_id),
                                         "reason": "claimed", "agent_id": user_id}))
        await ws.send_json({"type": "joined", "conversation": conv, "server_id": hub.server_id})
        if role == "customer" and conv["status"] == "waiting":
            if await safe(store.queue_ensure(hub.redis, conv), False):
                await safe(hub.publish_feed({"type": "queue_add", "conversation": conv}))
        await hub.publish_chat(conversation_id,
                               {"type": "presence", "event": "join", "role": role, "user_id": user_id},
                               exclude=conn_id)
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send_json({"type": "error", "detail": "JSON khong hop le"})
                continue
            kind = data.get("type")
            if kind == "ping":
                await safe(store.presence_ping(hub.redis, role, user_id))
                await ws.send_json({"type": "pong"})
            elif kind == "message":
                content = str(data.get("content") or "").strip()
                if not content or len(content) > MAX_LEN:
                    await ws.send_json({"type": "error", "detail": f"Tin nhan rong hoac dai qua {MAX_LEN} ky tu"})
                    continue
                try:
                    # D6: (1) Cassandra TRUOC - loi thi dung han, KHONG cache, KHONG phat.
                    msg = await asyncio.to_thread(repo.save_message, conversation_id, role, user_id, content)
                except Exception:
                    log.exception("Khong luu duoc tin nhan")
                    await ws.send_json({"type": "error", "detail": "Khong luu duoc tin nhan, thu lai"})
                    continue
                await safe(store.cache_message(hub.redis, conversation_id, msg))      # (2) cache
                await hub.publish_chat(conversation_id, {"type": "message", "message": msg})  # (3) phat
            else:
                await ws.send_json({"type": "error", "detail": "type khong ho tro"})
    except WebSocketDisconnect:
        pass
    finally:
        async def _cleanup():
            hub.leave(conversation_id, conn_id)
            await safe(store.presence_disconnect(hub.redis, role, user_id))
            await hub.publish_chat(conversation_id,
                                   {"type": "presence", "event": "leave", "role": role, "user_id": user_id})
        # shield: du task bi huy giua chung, viec don dep (giam presence, bao 'leave') van chay den cung
        await asyncio.shield(_cleanup())


# ---------------- WebSocket: dashboard nhan vien (hang cho realtime) ----------------
@router.websocket("/ws-agents")
async def agent_feed(ws: WebSocket, agent_id: str, key: str = ""):
    hub = _hub(ws)
    await ws.accept()
    if not _key_ok(key):
        await ws.close(code=4401, reason="unauthorized")
        return
    agent_id = agent_id.strip()
    if not agent_id:
        await ws.close(code=4400, reason="bad agent_id")
        return
    conn_id = uuid.uuid4().hex
    hub.feed[conn_id] = ws          # dang ky TRUOC khi gui snapshot de khong lo su kien
    await safe(store.presence_connect(hub.redis, "agent", agent_id))
    try:
        snapshot = await safe(store.queue_list(hub.redis), [])
        await ws.send_json({"type": "queue_snapshot", "queue": snapshot, "server_id": hub.server_id})
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if data.get("type") == "ping":
                await safe(store.presence_ping(hub.redis, "agent", agent_id))
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        async def _cleanup():
            hub.feed.pop(conn_id, None)
            await safe(store.presence_disconnect(hub.redis, "agent", agent_id))
        await asyncio.shield(_cleanup())


# ---------------- Khoi tao ung dung ----------------
def create_app(server_id: Optional[str] = None) -> FastAPI:
    sid = server_id or settings.server_id

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(repo.init)
        hub = Hub(sid)
        await hub.start()
        app.state.hub = hub
        log.info("Server %s san sang", sid)
        yield
        await hub.stop()
        await asyncio.to_thread(shutdown)

    app = FastAPI(title=f"Livechat [{sid}]", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    app.include_router(router)

    # Giao dien web (shop + widget + dashboard) phuc vu tu chinh server nay -> cung origin, khong dinh CORS
    web_dir = Path(__file__).resolve().parents[1] / "web"
    web_dir.mkdir(exist_ok=True)
    app.mount("/web", StaticFiles(directory=web_dir, html=True), name="web")

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse("/web/")

    return app  


app = create_app()