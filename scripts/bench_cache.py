"""Do toc do doc lich su chat: Redis cache (HIT) so voi Cassandra (MISS).
Chay:  python scripts/bench_cache.py            (mac dinh 50 tin, 200 vong)
       python scripts/bench_cache.py --messages 50 --rounds 500
Hai muc do: (1) thuan kho du lieu - LRANGE Redis vs truy van Cassandra
            (2) toan API - GET /messages di qua FastAPI (khong tinh mang)"""
import argparse, statistics, sys, time, uuid
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import redis as syncredis
from fastapi.testclient import TestClient
from app import keys
from app import repository as repo
from app.config import settings
from app.main import create_app


def pct(values, p):
    s = sorted(values)
    return s[max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))]


def timed(fn, rounds, warmup=10):
    for _ in range(warmup):
        fn()
    out = []
    for _ in range(rounds):
        t = time.perf_counter()
        fn()
        out.append((time.perf_counter() - t) * 1000)
    return out


def row(name, ms):
    print(f"  {name:<36}{min(ms):>8.2f}{pct(ms, 50):>9.2f}{pct(ms, 95):>9.2f}{statistics.mean(ms):>9.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--messages", type=int, default=50)
    ap.add_argument("--rounds", type=int, default=200)
    a = ap.parse_args()
    n = min(a.messages, settings.recent_cache_size)
    SR = syncredis.Redis.from_url(settings.redis_url, decode_responses=True)

    with TestClient(create_app("BENCH")) as client:
        cid = client.post("/api/conversations", json={"customer_id": f"bench_{uuid.uuid4().hex[:6]}"}).json()["id"]
        cu = uuid.UUID(cid)
        try:
            for i in range(n):
                repo.save_message(cu, "customer" if i % 2 == 0 else "agent", "bench", f"tin so {i}")
            key = keys.recent_key(cid)
            url = f"/api/conversations/{cid}/messages?limit={n}"

            warm = client.get(url).json()
            assert warm["source"] == "cassandra" and len(warm["messages"]) == n, "lan doc dau phai la MISS"
            assert client.get(url).json()["source"] == "cache", "lan doc thu hai phai la HIT"

            print(f"\nDo {a.rounds} vong, moi vong doc {n} tin gan nhat (don vi: ms)\n")
            print(f"  {'':<36}{'min':>8}{'p50':>9}{'p95':>9}{'mean':>9}")
            print("  --- Muc 1: thuan kho du lieu ---")
            redis_ms = timed(lambda: SR.lrange(key, 0, n - 1), a.rounds)
            cass_ms = timed(lambda: repo.get_messages(cu, n), a.rounds)
            row("Redis  LRANGE", redis_ms)
            row("Cassandra  truy van partition", cass_ms)
            print("  --- Muc 2: toan API (GET /messages) ---")
            hit_ms = timed(lambda: client.get(url), a.rounds)

            def miss():
                SR.delete(key)
                r = client.get(url).json()
                assert r["source"] == "cassandra"
            miss_ms = timed(miss, a.rounds)
            row("API  cache HIT", hit_ms)
            row("API  cache MISS (doc Cassandra + do cache)", miss_ms)
            client.get(url)
            assert client.get(url).json()["source"] == "cache"

            r1 = pct(cass_ms, 50) / max(pct(redis_ms, 50), 1e-9)
            r2 = pct(miss_ms, 50) / max(pct(hit_ms, 50), 1e-9)
            print(f"\n  Ti le thoi gian (p50): Cassandra / Redis = {r1:.1f}x   |   API MISS / API HIT = {r2:.1f}x")
            print("  Luu y: Cassandra o day la 1 node tren cung may, du lieu nho nen da nam trong bo nho dem;")
            print("  muc API con gom chi phi co dinh cua FastAPI, nen chenh lech nho hon muc kho du lieu.\n")
        finally:
            from app.cassandra_client import get_session
            s = get_session()
            s.execute("DELETE FROM conversations WHERE conversation_id = %s", (cu,))
            for back in range(0, 2):
                s.execute("DELETE FROM messages WHERE conversation_id = %s AND day = %s", (cu, keys.today_utc() - timedelta(days=back)))
            SR.delete(keys.recent_key(cid), keys.version_key(cid))
            SR.zrem(keys.WAITING_QUEUE, cid); SR.hdel(keys.QUEUE_META, cid)


if __name__ == "__main__":
    main()