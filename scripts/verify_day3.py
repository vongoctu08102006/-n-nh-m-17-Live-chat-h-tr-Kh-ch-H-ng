"""Checkpoint Ngay 3: Redis that + Cassandra that. Chay HAI server (A va B) cung luc de chung minh
pub/sub xuyen server, cache lich su, hang cho, presence."""
import asyncio, os, sys, threading, uuid
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import redis as syncredis
import redis.asyncio as aioredis
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from app import keys
from app import redis_store as store
from app.config import settings
from app.main import create_app

results = []
RUN = uuid.uuid4().hex[:8]
CUST, AG = f"cust_{RUN}", f"agent_{RUN}"
created, extra_customers = [], []
SR = syncredis.Redis.from_url(settings.redis_url, decode_responses=True)


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS" if ok else "FAIL"), "-", name, detail)


def recv_until(ws, pred, tries=20):
    for _ in range(tries):
        m = ws.receive_json()
        if pred(m):
            return m
    return None


def new_conv(client, customer):
    c = client.post("/api/conversations", json={"customer_id": customer}).json()
    created.append(c["id"]); extra_customers.append(customer)
    return c


def url(cid, role, user):
    return f"/ws/{cid}?role={role}&user_id={user}"


def history(client, cid, extra=""):
    return client.get(f"/api/conversations/{cid}/messages?limit=50{extra}").json()


def send(ws, text):
    ws.send_json({"type": "message", "content": text})
    return recv_until(ws, lambda m: m["type"] == "message" and m["message"]["content"] == text)


# ---------------------------------------------------------------- A. CACHE
def section_cache(ca):
    ca.post("/api/metrics/reset")
    cid = new_conv(ca, CUST)["id"]
    with ca.websocket_connect(url(cid, "customer", CUST)) as c:
        c.receive_json()
        for t in ("tin 1", "tin 2", "tin 3"):
            send(c, t)
        h1 = history(ca, cid)
        check("Doc lan dau (chua co cache) -> Cassandra", h1["source"] == "cassandra" and len(h1["messages"]) == 3)
        h2 = history(ca, cid)
        check("Doc lan hai -> Redis cache", h2["source"] == "cache")
        check("Cache dung noi dung va thu tu (moi nhat truoc)", [m["content"] for m in h2["messages"]] == ["tin 3", "tin 2", "tin 1"])
        check("Cache co TTL", 0 < SR.ttl(keys.recent_key(cid)) <= store.CACHE_TTL)
        send(c, "tin 4")
        h3 = history(ca, cid)
        check("Tin moi duoc day vao cache ngay (source=cache)", h3["source"] == "cache" and h3["messages"][0]["content"] == "tin 4" and len(h3["messages"]) == 4)
        SR.delete(keys.recent_key(cid))
        h4 = history(ca, cid)
        check("Cache bi xoa -> tu phuc hoi tu Cassandra, du 4 tin", h4["source"] == "cassandra" and len(h4["messages"]) == 4)
        check("Lan sau lai la cache", history(ca, cid)["source"] == "cache")
    m = ca.get("/api/metrics").json()
    check("So lieu: 3 hit / 2 miss", m["cache_hits"] == 3 and m["cache_misses"] == 2, f"(hit={m['cache_hits']} miss={m['cache_misses']} ratio={m['hit_ratio']})")


def section_bounded(ca):
    cid = new_conv(ca, CUST + "_b")["id"]
    with ca.websocket_connect(url(cid, "customer", CUST + "_b")) as c:
        c.receive_json()
        for i in range(55):
            send(c, f"m{i}")
        h = history(ca, cid)
        check("Trang dau: 50 tin moi nhat tu Cassandra", h["source"] == "cassandra" and len(h["messages"]) == 50 and h["messages"][0]["content"] == "m54" and h["messages"][-1]["content"] == "m5")
        send(c, "m55")
        key = keys.recent_key(cid)
        check("Cache bi cat dung 50 tin (LTRIM)", SR.llen(key) == 50)
        items = SR.lrange(key, 0, -1)
        import json
        check("Dau cache = m55, cuoi cache = m6", json.loads(items[0])["content"] == "m55" and json.loads(items[-1])["content"] == "m6")
        h2 = history(ca, cid)
        check("Trang dau phuc vu tu cache", h2["source"] == "cache" and h2["messages"][0]["content"] == "m55")
        h3 = history(ca, cid, f"&before={h2['next_before']}")
        check("Trang 2 (cu hon) lay tu Cassandra: m5..m0", h3["source"] == "cassandra" and [m["content"] for m in h3["messages"]] == ["m5", "m4", "m3", "m2", "m1", "m0"])
        ids = [m["id"] for m in h2["messages"] + h3["messages"]]
        check("Khong trung / khong sot (56 tin)", len(ids) == len(set(ids)) == 56)


def section_race():
    async def run():
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        cid = uuid.uuid4()
        seen = await store.read_version(r, cid)
        await store.cache_message(r, cid, {"id": "x", "content": "tin moi chen ngang"})
        absent = not await r.exists(keys.recent_key(cid))
        ok_stale = await store.fill_recent(r, cid, [{"id": "cu"}], seen)
        still_absent = not await r.exists(keys.recent_key(cid))
        ok_fresh = await store.fill_recent(r, cid, [{"id": "moi"}], await store.read_version(r, cid))
        await r.delete(keys.recent_key(cid), keys.version_key(cid))
        await r.aclose()
        return absent, ok_stale, still_absent, ok_fresh
    absent, ok_stale, still_absent, ok_fresh = asyncio.run(run())
    check("Ghi tin khi chua co cache khong tao list thieu (LPUSHX)", absent)
    check("Do cache voi du lieu cu (co tin chen ngang) bi tu choi", ok_stale is False and still_absent)
    check("Do cache voi du lieu moi nhat thanh cong", ok_fresh is True)


# ------------------------------------------------------- B. NHIEU SERVER
def section_cross_server(ca, cb):
    cid = new_conv(ca, CUST + "_x")["id"]
    cu = CUST + "_x"
    with ca.websocket_connect(url(cid, "customer", cu)) as cws:
        j = cws.receive_json()
        check("Khach noi vao server A", j["type"] == "joined" and j["server_id"] == "A")
        with cb.websocket_connect(url(cid, "agent", AG + "_x")) as aws:
            aj = aws.receive_json()
            check("Nhan vien noi vao server B, status active", aj["server_id"] == "B" and aj["conversation"]["status"] == "active")
            pres = recv_until(cws, lambda m: m["type"] == "presence" and m["event"] == "join")
            check("Khach (A) thay nhan vien (B) vao - xuyen server", pres is not None and pres["role"] == "agent")
            cws.send_json({"type": "message", "content": "chao tu A"})
            got = recv_until(aws, lambda m: m["type"] == "message")
            check("Tin tu server A toi nhan vien o server B", got is not None and got["message"]["content"] == "chao tu A")
            aws.send_json({"type": "message", "content": "tra loi tu B"})
            got = recv_until(cws, lambda m: m["type"] == "message" and m["message"]["sender_type"] == "agent")
            check("Tin tu server B toi khach o server A", got is not None and got["message"]["content"] == "tra loi tu B")
            r = cb.post(f"/api/conversations/{cid}/close")
            check("Dong chat qua server B", r.status_code == 200 and r.json()["status"] == "closed")
            ev = recv_until(cws, lambda m: m["type"] == "closed")
            check("Khach o server A nhan su kien 'closed'", ev is not None)
            ev = recv_until(aws, lambda m: m["type"] == "closed")
            check("Nhan vien o server B cung nhan 'closed'", ev is not None)
            try:
                cws.receive_json()
                dropped = False
            except WebSocketDisconnect:
                dropped = True
            check("Ket noi cua khach o server A bi dong", dropped)
    h = ca.get(f"/api/conversations/{cid}/messages").json()
    check("Lich su doc tu server A thay du 2 tin cua ca hai server", len(h["messages"]) == 2)


# ------------------------------------------------- C. HANG CHO + PRESENCE
def section_queue_presence(ca, cb):
    feed_agent = AG + "_feed"
    pk = keys.presence_key("agent", feed_agent)
    with ca.websocket_connect(f"/ws-agents?agent_id={feed_agent}") as feed:
        snap = feed.receive_json()
        check("Dashboard nhan queue_snapshot khi ket noi", snap["type"] == "queue_snapshot")
        check("Nhan vien online (hoi tu server B)", cb.get(f"/api/presence/agent/{feed_agent}").json()["online"] is True)
        check("Presence co TTL", 0 < SR.ttl(pk) <= store.PRESENCE_TTL)
        check("Co trong danh sach agent online", feed_agent in cb.get("/api/agents/online/list").json())

        conv = new_conv(cb, CUST + "_q")
        ev = recv_until(feed, lambda m: m["type"] == "queue_add" and m["conversation"]["id"] == conv["id"])
        check("Khach moi tao o server B -> dashboard o server A thay ngay", ev is not None)
        check("GET /api/queue co khach do", any(x["id"] == conv["id"] for x in ca.get("/api/queue").json()))

        with cb.websocket_connect(url(conv["id"], "agent", feed_agent)) as aws:
            aws.receive_json()
            ev = recv_until(feed, lambda m: m["type"] == "queue_remove" and m["conversation_id"] == conv["id"])
            check("Nhan khach -> queue_remove (claimed) toi dashboard", ev is not None and ev["reason"] == "claimed")
            check("Khach roi khoi hang cho", not any(x["id"] == conv["id"] for x in ca.get("/api/queue").json()))
        check("Dong 1 trong 2 ket noi, nhan vien VAN online (dem ket noi)", cb.get(f"/api/presence/agent/{feed_agent}").json()["online"] is True)

        conv2 = new_conv(cb, CUST + "_q2")
        recv_until(feed, lambda m: m["type"] == "queue_add" and m["conversation"]["id"] == conv2["id"])
        cb.post(f"/api/conversations/{conv2['id']}/close")
        ev = recv_until(feed, lambda m: m["type"] == "queue_remove" and m["conversation_id"] == conv2["id"])
        check("Khach dong chat khi dang cho -> queue_remove (closed)", ev is not None and ev["reason"] == "closed")
    check("Ngat het ket noi -> nhan vien offline", cb.get(f"/api/presence/agent/{feed_agent}").json()["online"] is False)
    check("Khong con khoa presence trong Redis", SR.exists(pk) == 0)


def cleanup():
    from app.cassandra_client import get_session
    s = get_session()
    today = keys.today_utc()
    for cid in created:
        cu = uuid.UUID(cid)
        s.execute("DELETE FROM conversations WHERE conversation_id = %s", (cu,))
        for back in range(0, 3):
            s.execute("DELETE FROM messages WHERE conversation_id = %s AND day = %s", (cu, today - timedelta(days=back)))
        SR.delete(keys.recent_key(cid), keys.version_key(cid))
        SR.zrem(keys.WAITING_QUEUE, cid); SR.hdel(keys.QUEUE_META, cid)
    for c in set(extra_customers):
        s.execute("DELETE FROM conversations_by_customer WHERE customer_id = %s", (c,))
    for a in (AG + "_x", AG + "_feed"):
        s.execute("DELETE FROM conversations_by_agent WHERE agent_id = %s", (a,))


def main():
    def boom():
        print("\nTIMEOUT: bai test treo qua 120 giay - co the 1 su kien khong toi noi. Gui toan bo output cho tao.")
        os._exit(2)
    t = threading.Timer(120, boom); t.daemon = True; t.start()
    with TestClient(create_app("A")) as ca, TestClient(create_app("B")) as cb:
        try:
            section_cache(ca)
            section_bounded(ca)
            section_race()
            section_cross_server(ca, cb)
            section_queue_presence(ca, cb)
        finally:
            cleanup()
    print(f"\n{sum(results)}/{len(results)} kiem tra dat")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()