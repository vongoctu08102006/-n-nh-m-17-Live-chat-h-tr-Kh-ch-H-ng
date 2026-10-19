"""Checkpoint Ngay 1: kiem tra ha tang + schema + vong ghi/doc that, roi don du lieu test."""
import asyncio, json, sys, time, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cassandra.util import uuid_from_time
from app import keys
from app.config import settings
from app.cassandra_client import get_session, shutdown
from app.redis_client import get_redis

results = []
def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS" if ok else "FAIL"), "-", name, detail)


async def main():
    r = get_redis()
    check("Redis ping", await r.ping())

    s = get_session()
    s.set_keyspace(settings.keyspace)
    tables = {row.table_name for row in s.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", (settings.keyspace,))}
    expected = {"messages", "conversations", "conversations_by_customer", "conversations_by_agent"}
    missing = expected - tables
    check("Du 4 bang", not missing, f"(thieu: {missing})" if missing else "")

    # Ghi tin vao 2 ngay khac nhau -> phai nam o 2 partition khac nhau
    conv = uuid.uuid4()
    t_old = uuid_from_time(time.time() - 86400)
    t_new = uuid_from_time(time.time())
    d_old, d_new = keys.day_of(t_old), keys.day_of(t_new)
    ins = s.prepare("INSERT INTO messages (conversation_id, day, sent_at, sender_type, sender_id, content) VALUES (?,?,?,?,?,?)")
    s.execute(ins, (conv, d_old, t_old, "customer", "c1", "tin hom qua"))
    s.execute(ins, (conv, d_new, t_new, "agent", "a1", "tin hom nay"))
    sel = s.prepare("SELECT content FROM messages WHERE conversation_id=? AND day=?")
    check("Day bucket khac nhau giua 2 ngay", d_old != d_new)
    check("Partition ngay cu chi co 1 tin", [x.content for x in s.execute(sel, (conv, d_old))] == ["tin hom qua"])
    check("Partition ngay moi chi co 1 tin", [x.content for x in s.execute(sel, (conv, d_new))] == ["tin hom nay"])

    # Redis: list cache giu dung N tin, moi nhat dung dau
    k = keys.recent_key(conv)
    n = settings.recent_cache_size
    for i in range(n + 5):
        await r.lpush(k, json.dumps({"i": i}))
        await r.ltrim(k, 0, n - 1)
    items = await r.lrange(k, 0, -1)
    check("Cache cat dung N tin", len(items) == n)
    check("Tin moi nhat o dau list", json.loads(items[0])["i"] == n + 4)

    # Pub/Sub
    pub = r.pubsub()
    await pub.subscribe(keys.channel(conv))
    await pub.get_message(timeout=1)
    await r.publish(keys.channel(conv), "ping")
    msg = await pub.get_message(ignore_subscribe_messages=True, timeout=2)
    check("Pub/Sub nhan duoc tin", bool(msg) and msg["data"] == "ping")
    await pub.aclose()

    # Don du lieu test
    for d in (d_old, d_new):
        s.execute("DELETE FROM messages WHERE conversation_id=%s AND day=%s", (conv, d))
    await r.delete(k)
    shutdown()
    await r.aclose()

asyncio.run(main())
print(f"\n{sum(results)}/{len(results)} kiem tra dat")
sys.exit(0 if all(results) else 1)