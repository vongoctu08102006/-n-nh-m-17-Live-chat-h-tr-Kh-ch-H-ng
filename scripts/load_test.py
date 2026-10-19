"""Thu tai he thong: N cap (khach o server A <-> nhan vien o server B) nhan tin qua lai dong thoi.
Kiem tra: moi tin toi dung 1 lan, dung thu tu, duoc luu du vao Cassandra; do do tre va thong luong.
Can 2 server dang chay:   (cua so 1) SERVER_ID=A uvicorn app.main:app --port 8001
                          (cua so 2) SERVER_ID=B uvicorn app.main:app --port 8002
Chay:  python scripts/load_test.py --pairs 20 --messages 25"""
import argparse, asyncio, json, statistics, sys, time, uuid
import httpx
import websockets


def pct(values, p):
    s = sorted(values)
    return s[max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))] if s else float("nan")


async def sender(ws, m, gap_ms):
    for seq in range(m):
        await ws.send(json.dumps({"type": "message", "content": f"{seq}|{time.perf_counter()!r}"}))
        await asyncio.sleep(gap_ms / 1000 if gap_ms else 0)


async def receiver(ws, peer_type, m):
    """Doi du m tin tu phia kia + m ban sao tin cua chinh minh."""
    peer, lat, own = [], [], 0
    while len(peer) < m or own < m:
        d = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
        if d.get("type") != "message":
            continue
        msg = d["message"]
        if msg["sender_type"] == peer_type:
            seq, t0 = msg["content"].split("|")
            peer.append(int(seq)); lat.append((time.perf_counter() - float(t0)) * 1000)
        else:
            own += 1
    return peer, lat


async def one_pair(i, run, A, B, m, gap_ms):
    out = {"ok": False, "lat": [], "err": ""}
    cust, agent = f"lt_{run}_{i}", f"ltag_{run}_{i}"
    wa, wb = A.replace("http", "ws", 1), B.replace("http", "ws", 1)
    try:
        async with httpx.AsyncClient(timeout=30) as http:
            cid = (await http.post(f"{A}/api/conversations", json={"customer_id": cust})).json()["id"]
            async with websockets.connect(f"{wa}/ws/{cid}?role=customer&user_id={cust}") as cws, \
                       websockets.connect(f"{wb}/ws/{cid}?role=agent&user_id={agent}") as aws:
                await cws.recv(); await aws.recv()                      # 'joined' cua moi ben
                res = await asyncio.gather(sender(cws, m, gap_ms), sender(aws, m, gap_ms),
                                           receiver(cws, "agent", m), receiver(aws, "customer", m))
            (c_peer, c_lat), (a_peer, a_lat) = res[2], res[3]
            out["lat"] = c_lat + a_lat
            ordered = c_peer == list(range(m)) and a_peer == list(range(m))
            hist = (await http.get(f"{B}/api/conversations/{cid}/messages?limit=100")).json()["messages"]
            saved = len(hist) == 2 * m
            await http.post(f"{B}/api/conversations/{cid}/close")
            out["ok"] = ordered and saved
            if not ordered: out["err"] = "sai thu tu / thieu / trung tin"
            elif not saved: out["err"] = f"Cassandra chi luu {len(hist)}/{2 * m} tin"
    except Exception as e:
        out["err"] = f"{type(e).__name__}: {e}"
    return out


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", default="http://127.0.0.1:8001")
    ap.add_argument("--b", default="http://127.0.0.1:8002")
    ap.add_argument("--pairs", type=int, default=20)
    ap.add_argument("--messages", type=int, default=25, help="so tin MOI BEN gui (toi da 50)")
    ap.add_argument("--gap-ms", type=float, default=0, help="nghi giua 2 tin cua cung 1 ben")
    a = ap.parse_args()
    m = min(a.messages, 50)
    run = uuid.uuid4().hex[:6]
    for u in (a.a, a.b):
        try:
            print(u, "->", httpx.get(u + "/api/health", timeout=5).json())
        except Exception as e:
            sys.exit(f"Khong ket noi duoc {u} ({e}). Hay bat 2 server truoc.")
    print(f"\nChay {a.pairs} cap x {m} tin moi chieu = {a.pairs * 2 * m} tin, khach o A <-> nhan vien o B ...")
    t0 = time.perf_counter()
    results = await asyncio.gather(*(one_pair(i, run, a.a, a.b, m, a.gap_ms) for i in range(a.pairs)))
    dt = time.perf_counter() - t0
    lat = [x for r in results for x in r["lat"]]
    ok = sum(r["ok"] for r in results)
    print(f"\n  Cap dat yeu cau          : {ok}/{a.pairs}")
    print(f"  Tong tin da gui          : {a.pairs * 2 * m}")
    print(f"  Thoi gian                : {dt:.2f} s  ->  {a.pairs * 2 * m / dt:.0f} tin/giay")
    if lat:
        print(f"  Do tre khach<->nhan vien : p50 {pct(lat, 50):.1f} ms | p95 {pct(lat, 95):.1f} ms | max {max(lat):.1f} ms | TB {statistics.mean(lat):.1f} ms")
    for r in results:
        if r["err"]:
            print("  LOI:", r["err"])
    print("\nKET QUA:", "DAT - moi tin toi dung 1 lan, dung thu tu, da luu du" if ok == a.pairs else "KHONG DAT")
    sys.exit(0 if ok == a.pairs else 1)


if __name__ == "__main__":
    asyncio.run(main())