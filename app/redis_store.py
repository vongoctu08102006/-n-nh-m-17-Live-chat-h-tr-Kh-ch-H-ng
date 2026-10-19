"""Moi thao tac Redis cua he thong: cache lich su, hang cho, presence, so lieu.
Ten key lay HET tu keys.py (D5). Ham o day co the nem loi khi Redis chet -> noi goi boc bang safe()."""
import json
import logging
import time
from redis.exceptions import WatchError
from . import keys
from .config import settings

log = logging.getLogger("livechat.redis")

CACHE_TTL = 3600       # cache tin: 1 gio
VERSION_TTL = 86400
PRESENCE_TTL = 90      # client phai ping it nhat moi ~25-30s


async def safe(coro, default=None):
    """Redis la lop TOI UU, khong phai nguon su that: chet thi ghi log va chay tiep bang Cassandra."""
    try:
        return await coro
    except Exception as e:
        log.warning("Redis loi (%s: %s) - bo qua, he thong tiep tuc chay bang Cassandra", type(e).__name__, e)
        return default


# ================= CACHE LICH SU (D13) =================
# Bat bien: list `recent` HOAC khong ton tai, HOAC la toan bo lich su (neu < N tin), HOAC la dung N tin moi nhat.
# - Ghi tin: Cassandra truoc (D6), roi LPUSHX (chi day vao neu list da ton tai -> khong bao gio tao list thieu).
# - Doc miss: doc Cassandra roi do cache, CHI neu `ver` khong doi (khong co tin moi chen ngang).
async def cache_message(r, conv_id, msg: dict):
    key, ver = keys.recent_key(conv_id), keys.version_key(conv_id)
    pipe = r.pipeline(transaction=True)
    pipe.incr(ver)
    pipe.expire(ver, VERSION_TTL)
    pipe.lpushx(key, json.dumps(msg))
    pipe.ltrim(key, 0, settings.recent_cache_size - 1)
    pipe.expire(key, CACHE_TTL)
    await pipe.execute()


async def get_recent(r, conv_id, limit: int):
    """Tra ve list tin (moi nhat truoc) neu cache HIT, None neu MISS."""
    items = await r.lrange(keys.recent_key(conv_id), 0, limit - 1)
    return [json.loads(x) for x in items] if items else None


async def read_version(r, conv_id) -> str:
    return await r.get(keys.version_key(conv_id)) or "0"


async def fill_recent(r, conv_id, messages: list, seen_ver: str) -> bool:
    """Do cache tu ket qua Cassandra. Bo qua neu: list rong, da co nguoi do roi, hoac co tin moi chen ngang."""
    if not messages:
        return False
    key, ver = keys.recent_key(conv_id), keys.version_key(conv_id)
    async with r.pipeline(transaction=True) as pipe:
        try:
            await pipe.watch(ver, key)
            if await pipe.exists(key):
                return False
            if (await pipe.get(ver) or "0") != seen_ver:
                return False
            pipe.multi()
            pipe.rpush(key, *[json.dumps(m) for m in messages])
            pipe.expire(key, CACHE_TTL)
            await pipe.execute()
            return True
        except WatchError:
            return False


async def record_cache(r, hit: bool):
    await r.incr(keys.METRIC_HIT if hit else keys.METRIC_MISS)


async def get_metrics(r) -> dict:
    hits, misses = await r.mget(keys.METRIC_HIT, keys.METRIC_MISS)
    hits, misses = int(hits or 0), int(misses or 0)
    total = hits + misses
    return {"cache_hits": hits, "cache_misses": misses, "hit_ratio": round(hits / total, 4) if total else None}


async def reset_metrics(r):
    await r.delete(keys.METRIC_HIT, keys.METRIC_MISS)


# ================= HANG CHO KHACH =================
# Hang cho chi song trong Redis (D14): ZSET xep theo thoi gian vao hang, HASH giu thong tin hien thi.
async def queue_add(r, conv: dict):
    meta = {"id": conv["id"], "customer_id": conv["customer_id"], "created_at": conv["created_at"]}
    pipe = r.pipeline(transaction=True)
    pipe.zadd(keys.WAITING_QUEUE, {conv["id"]: time.time()})
    pipe.hset(keys.QUEUE_META, conv["id"], json.dumps(meta))
    await pipe.execute()

async def queue_ensure(r, conv: dict) -> bool:
    """Tu chua lanh: khach dang 'waiting' nhung khong co trong hang cho (Redis bi xoa/mat) -> dua lai vao,
    giu nguyen thu tu cho theo thoi diem tao cuoc chat. Tra ve True neu vua them lai."""
    from datetime import datetime
    score = datetime.fromisoformat(conv["created_at"]).timestamp()
    meta = {"id": conv["id"], "customer_id": conv["customer_id"], "created_at": conv["created_at"]}
    pipe = r.pipeline(transaction=True)
    pipe.zadd(keys.WAITING_QUEUE, {conv["id"]: score}, nx=True)
    pipe.hsetnx(keys.QUEUE_META, conv["id"], json.dumps(meta))
    added, _ = await pipe.execute()
    return bool(added)

async def queue_remove(r, conv_id) -> bool:
    pipe = r.pipeline(transaction=True)
    pipe.zrem(keys.WAITING_QUEUE, str(conv_id))
    pipe.hdel(keys.QUEUE_META, str(conv_id))
    removed, _ = await pipe.execute()
    return bool(removed)


async def queue_list(r, limit: int = 100) -> list:
    ids = await r.zrange(keys.WAITING_QUEUE, 0, limit - 1)      # cu nhat (cho lau nhat) truoc
    if not ids:
        return []
    metas = await r.hmget(keys.QUEUE_META, ids)
    return [json.loads(m) for m in metas if m]


# ================= PRESENCE (dem so ket noi, co TTL) =================
_DECR_LUA = """
local n = redis.call('DECR', KEYS[1])
if n <= 0 then redis.call('DEL', KEYS[1]) end
return n
"""


async def presence_connect(r, role, user_id):
    k = keys.presence_key(role, user_id)
    pipe = r.pipeline(transaction=True)
    pipe.incr(k)
    pipe.expire(k, PRESENCE_TTL)
    await pipe.execute()


async def presence_ping(r, role, user_id):
    await r.expire(keys.presence_key(role, user_id), PRESENCE_TTL)


async def presence_disconnect(r, role, user_id):
    await r.eval(_DECR_LUA, 1, keys.presence_key(role, user_id))


async def is_online(r, role, user_id) -> bool:
    return int(await r.get(keys.presence_key(role, user_id)) or 0) > 0


async def online_users(r, role) -> list:
    prefix = keys.presence_key(role, "")
    out = []
    async for k in r.scan_iter(match=f"{prefix}*"):
        if int(await r.get(k) or 0) > 0:
            out.append(k[len(prefix):])
    return sorted(out)