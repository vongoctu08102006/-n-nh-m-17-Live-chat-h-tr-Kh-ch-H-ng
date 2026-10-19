"""Checkpoint bao mat: khi dat AGENT_KEY thi moi thu rieng cua nhan vien phai bi khoa, con phia KHACH van chay binh thuong.
Chay:  python scripts/verify_security.py     (tu dat AGENT_KEY tam thoi, khong can cau hinh gi them)"""
import os, sys, uuid
from datetime import timedelta
from pathlib import Path
KEY = os.environ.get("AGENT_KEY") or ("khoa-bi-mat-" + uuid.uuid4().hex[:8])   # dung ma dang dat, neu chua co thi tu tao
os.environ["AGENT_KEY"] = KEY                      # PHAI dat truoc khi import app
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import redis as syncredis
from app import keys
from app.config import settings
from app.main import create_app

results, created = [], []
RUN = uuid.uuid4().hex[:6]
CUST, AG = f"cust_sec_{RUN}", f"agent_sec_{RUN}"
H = {"X-Agent-Key": KEY}
SR = syncredis.Redis.from_url(settings.redis_url, decode_responses=True)


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS" if ok else "FAIL"), "-", name, detail)


def close_code(client, url):
    try:
        with client.websocket_connect(url) as ws:
            ws.receive_json()
        return None
    except WebSocketDisconnect as e:
        return e.code


def run(client):
    check("Key duoc doc tu moi truong", settings.agent_key == KEY)
    c = client.post("/api/conversations", json={"customer_id": CUST})
    check("Khach tao cuoc chat KHONG can ma nhan vien", c.status_code == 201)
    cid = c.json()["id"]; created.append(cid)

    for method, path in (("get", "/api/queue"), ("get", f"/api/agents/{AG}/conversations"), ("get", "/api/agents/online/list"),
                         ("get", "/api/metrics"), ("post", "/api/metrics/reset"), ("post", f"/api/conversations/{cid}/close")):
        r = getattr(client, method)(path)
        check(f"Khong co ma -> 401: {method.upper()} {path.replace(cid, '{id}')}", r.status_code == 401)
    check("Sai ma -> 401", client.get("/api/queue", headers={"X-Agent-Key": "sai"}).status_code == 401)
    check("Dung ma -> /api/queue 200", client.get("/api/queue", headers=H).status_code == 200)
    check("Dung ma -> /api/metrics 200", client.get("/api/metrics", headers=H).status_code == 200)

    a_url = f"/ws/{cid}?role=agent&user_id={AG}"
    check("Nhan vien WebSocket khong ma -> dong 4401", close_code(client, a_url) == 4401)
    check("Nhan vien WebSocket sai ma -> dong 4401", close_code(client, a_url + "&key=sai") == 4401)
    check("Dashboard /ws-agents khong ma -> dong 4401", close_code(client, f"/ws-agents?agent_id={AG}") == 4401)
    q = client.get("/api/queue", headers=H).json()
    check("Cuoc chat van o hang cho (ket noi trai phep khong nhan duoc khach)", any(x["id"] == cid for x in q))

    pos = client.get(f"/api/conversations/{cid}/queue-position").json()
    check("Khach xem duoc vi tri cua CHINH MINH, khong can ma", isinstance(pos["position"], int) and pos["total"] >= 1, str(pos))
    check("Vi tri khong lo danh sach khach khac", set(pos) == {"position", "total"})

    with client.websocket_connect(f"/ws/{cid}?role=customer&user_id={CUST}") as cws:
        check("Khach WebSocket khong can ma", cws.receive_json()["type"] == "joined")
        with client.websocket_connect(a_url + f"&key={KEY}") as aws:
            check("Nhan vien dung ma vao duoc", aws.receive_json()["type"] == "joined")
            for _ in range(5):
                m = cws.receive_json()
                if m["type"] == "presence":
                    break
            check("Khach thay nhan vien vao", m["type"] == "presence" and m["role"] == "agent")
            cws.send_json({"type": "message", "content": "xin chao"})
            aws.send_json({"type": "message", "content": "chao ban"})
            got = [cws.receive_json() for _ in range(2)]
            check("Chat van chay binh thuong", {g["message"]["content"] for g in got if g["type"] == "message"} == {"xin chao", "chao ban"})
        with client.websocket_connect(f"/ws-agents?agent_id={AG}&key={KEY}") as feed:
            check("Dashboard dung ma nhan duoc queue_snapshot", feed.receive_json()["type"] == "queue_snapshot")
    pos = client.get(f"/api/conversations/{cid}/queue-position").json()
    check("Sau khi duoc nhan, khach khong con vi tri cho", pos["position"] is None)
    check("Dung ma dong chat duoc", client.post(f"/api/conversations/{cid}/close", headers=H).status_code == 200)
    check("Lich su van doc duoc cong khai bang id (phia khach)", len(client.get(f"/api/conversations/{cid}/messages").json()["messages"]) == 2)


def cleanup():
    from app.cassandra_client import get_session
    s = get_session()
    today = keys.today_utc()
    for cid in created:
        cu = uuid.UUID(cid)
        s.execute("DELETE FROM conversations WHERE conversation_id = %s", (cu,))
        for back in range(0, 2):
            s.execute("DELETE FROM messages WHERE conversation_id = %s AND day = %s", (cu, today - timedelta(days=back)))
        SR.delete(keys.recent_key(cid), keys.version_key(cid)); SR.zrem(keys.WAITING_QUEUE, cid); SR.hdel(keys.QUEUE_META, cid)
    s.execute("DELETE FROM conversations_by_customer WHERE customer_id = %s", (CUST,))
    s.execute("DELETE FROM conversations_by_agent WHERE agent_id = %s", (AG,))


def main():
    with TestClient(create_app("SEC")) as client:
        try:
            run(client)
        finally:
            cleanup()
    print(f"\n{sum(results)}/{len(results)} kiem tra dat")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()