"""Checkpoint Ngay 2: chay server that (TestClient) voi Cassandra that, kiem tra REST + WebSocket + phan trang."""
import sys, time, uuid
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from app import keys
from app import repository as repo
from app.cassandra_client import get_session
from app.main import app

results = []
RUN = uuid.uuid4().hex[:8]
CUST, A1, A2 = f"cust_{RUN}", f"agent1_{RUN}", f"agent2_{RUN}"
created = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS" if ok else "FAIL"), "-", name, detail)


def close_code(client, url):
    """Ket noi roi tra ve ma dong ma server gui (hoac None neu khong bi dong)."""
    try:
        with client.websocket_connect(url) as ws:
            ws.receive_json()
        return None
    except WebSocketDisconnect as e:
        return e.code


def section_rest_and_ws(client):
    r = client.post("/api/conversations", json={"customer_id": CUST})
    check("POST tao cuoc chat -> 201", r.status_code == 201)
    conv = r.json(); cid = conv["id"]; created.append(cid)
    check("Trang thai ban dau = waiting", conv["status"] == "waiting")

    cust_url = f"/ws/{cid}?role=customer&user_id={CUST}"
    a1_url = f"/ws/{cid}?role=agent&user_id={A1}"
    a2_url = f"/ws/{cid}?role=agent&user_id={A2}"
    check("Sai customer_id bi tu choi (4403)", close_code(client, f"/ws/{cid}?role=customer&user_id=ke_la") == 4403)
    check("Cuoc chat khong ton tai (4404)", close_code(client, f"/ws/{uuid.uuid4()}?role=customer&user_id={CUST}") == 4404)
    check("role sai (4400)", close_code(client, f"/ws/{cid}?role=hacker&user_id=x") == 4400)

    with client.websocket_connect(cust_url) as cws:
        joined = cws.receive_json()
        check("Khach nhan 'joined' voi status waiting", joined["type"] == "joined" and joined["conversation"]["status"] == "waiting")
        cws.send_json({"type": "message", "content": "Xin chao, minh can ho tro"})
        m1 = cws.receive_json()
        check("Khach nhan lai tin da luu", m1["type"] == "message" and m1["message"]["content"].startswith("Xin chao"))
        cws.send_json({"type": "message", "content": "   "})
        check("Tin rong bi tu choi", cws.receive_json()["type"] == "error")

        with client.websocket_connect(a1_url) as aws:
            aj = aws.receive_json()
            check("Nhan vien vao -> status active", aj["conversation"]["status"] == "active" and aj["conversation"]["agent_id"] == A1)
            pres = cws.receive_json()
            check("Khach thay nhan vien vao", pres["type"] == "presence" and pres["event"] == "join" and pres["role"] == "agent")
            aws.send_json({"type": "message", "content": "Chao ban, toi giup duoc gi?"})
            a_echo = aws.receive_json()
            c_got = cws.receive_json()
            check("Tin nhan vien toi khach", c_got["type"] == "message" and c_got["message"]["sender_type"] == "agent")
            check("Hai ben cung nhan 1 tin (cung id)", a_echo["message"]["id"] == c_got["message"]["id"])
        leave = cws.receive_json()
        check("Khach thay nhan vien roi di", leave["type"] == "presence" and leave["event"] == "leave")

    check("Nhan vien thu 2 bi tu choi (4409)", close_code(client, a2_url) == 4409)
    with client.websocket_connect(a1_url) as aws:
        check("Nhan vien cu vao lai duoc", aws.receive_json()["type"] == "joined")

    msgs = client.get(f"/api/conversations/{cid}/messages?limit=10").json()["messages"]
    check("Lich su (Cassandra) co dung 2 tin", len(msgs) == 2)
    check("Moi nhat dung truoc", len(msgs) == 2 and msgs[0]["sender_type"] == "agent" and msgs[1]["sender_type"] == "customer")

    lst = client.get(f"/api/customers/{CUST}/conversations").json()
    check("Q3 by_customer: status da dong bo = active", any(x["id"] == cid and x["status"] == "active" for x in lst))
    lst = client.get(f"/api/agents/{A1}/conversations").json()
    check("Q4 by_agent co cuoc chat", any(x["id"] == cid and x["customer_id"] == CUST for x in lst))

    with client.websocket_connect(cust_url) as cws2:
        cws2.receive_json()
        r = client.post(f"/api/conversations/{cid}/close")
        check("POST close -> status closed", r.status_code == 200 and r.json()["status"] == "closed")
        check("Khach nhan su kien 'closed'", cws2.receive_json()["type"] == "closed")
    check("Vao lai cuoc chat da dong bi tu choi (4410)", close_code(client, cust_url) == 4410)
    lst = client.get(f"/api/customers/{CUST}/conversations").json()
    check("Q3 by_customer: status da dong bo = closed", any(x["id"] == cid and x["status"] == "closed" for x in lst))
    check("GET conversation 404 khi sai id", client.get(f"/api/conversations/{uuid.uuid4()}").status_code == 404)


def section_paging(client):
    """Chung minh phan trang vuot qua ranh gioi partition (conversation_id, day)."""
    cid = client.post("/api/conversations", json={"customer_id": CUST + "_p"}).json()["id"]
    created.append(cid); cu = uuid.UUID(cid)
    now = time.time()
    t2, t1, t0 = keys.timeuuid_at(now - 2 * 86400), keys.timeuuid_at(now - 86400), keys.new_timeuuid()
    get_session().execute("UPDATE conversations SET created_day = %s WHERE conversation_id = %s", (keys.day_of(t2), cu))
    repo.save_message(cu, "customer", CUST, "tin 2 ngay truoc", tid=t2)
    repo.save_message(cu, "agent", A1, "tin hom qua", tid=t1)
    repo.save_message(cu, "customer", CUST, "tin hom nay", tid=t0)
    p1 = client.get(f"/api/conversations/{cid}/messages?limit=2").json()
    check("Trang 1: 2 tin moi nhat", [m["content"] for m in p1["messages"]] == ["tin hom nay", "tin hom qua"])
    check("Trang 1 co next_before", p1["next_before"] is not None)
    p2 = client.get(f"/api/conversations/{cid}/messages?limit=2&before={p1['next_before']}").json()
    check("Trang 2 vuot ranh gioi ngay -> tin 2 ngay truoc", [m["content"] for m in p2["messages"]] == ["tin 2 ngay truoc"])
    check("Trang 2 het du lieu (next_before = None)", p2["next_before"] is None)
    ids = [m["id"] for m in p1["messages"] + p2["messages"]]
    check("Khong trung / khong sot tin giua cac trang", len(ids) == len(set(ids)) == 3)


def cleanup():
    s = get_session()
    today = keys.today_utc()
    for cid in created:
        cu = uuid.UUID(cid)
        s.execute("DELETE FROM conversations WHERE conversation_id = %s", (cu,))
        for back in range(0, 4):
            s.execute("DELETE FROM messages WHERE conversation_id = %s AND day = %s", (cu, today - timedelta(days=back)))
    for c in (CUST, CUST + "_p"):
        s.execute("DELETE FROM conversations_by_customer WHERE customer_id = %s", (c,))
    for a in (A1, A2):
        s.execute("DELETE FROM conversations_by_agent WHERE agent_id = %s", (a,))


def main():
    with TestClient(app) as client:
        try:
            section_rest_and_ws(client)
            section_paging(client)
        finally:
            cleanup()
    print(f"\n{sum(results)}/{len(results)} kiem tra dat")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()