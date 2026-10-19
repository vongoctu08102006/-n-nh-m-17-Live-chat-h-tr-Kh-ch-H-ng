"""Doc lich su: Redis cache -> Cassandra. (Ghi tin nam o main.py de giu dung thu tu D6.)"""
import asyncio
from . import redis_store as store
from . import repository as repo
from .config import settings
from .redis_store import safe


async def get_history(hub, conv_id, limit: int, before=None) -> dict:
    r, n = hub.redis, settings.recent_cache_size
    # Chi trang DAU (khong co `before`) va khong vuot kich thuoc cache moi duoc phuc vu tu Redis.
    if before is None and limit <= n:
        cached = await safe(store.get_recent(r, conv_id, limit))
        if cached:
            await safe(store.record_cache(r, True))
            return {"messages": cached, "next_before": cached[-1]["id"] if len(cached) == limit else None,
                    "source": "cache"}
        await safe(store.record_cache(r, False))
        seen = await safe(store.read_version(r, conv_id))
        data = await asyncio.to_thread(repo.get_messages, conv_id, n)     # NotFound se bay len cho main xu ly
        if seen is not None:
            await safe(store.fill_recent(r, conv_id, data["messages"], seen))
        out = data["messages"][:limit]
        return {"messages": out, "next_before": out[-1]["id"] if len(out) == limit else None, "source": "cassandra"}
    data = await asyncio.to_thread(repo.get_messages, conv_id, limit, before)
    data["source"] = "cassandra"
    return data