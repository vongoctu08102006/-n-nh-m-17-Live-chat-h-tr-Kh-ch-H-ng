"""Noi DUY NHAT dinh nghia: ten key/channel Redis, cach sinh timeuuid, cach tinh day bucket.
Moi module khac bat buoc import tu day de khong lech nhau."""
import random
import threading
import time
import uuid
from datetime import date, datetime, timezone
from cassandra.util import datetime_from_uuid1

# --- Sinh timeuuid TANG DON DIEU ---
# Dong ho Windows tho -> 2 tin gui sat nhau co the trung timestamp va bi dao thu tu.
# O day moi timeuuid luon lon hon cai truoc it nhat 1 tick (100ns), trong cung 1 tien trinh.
# Nhieu server: moi tien trinh co NODE ngau nhien rieng nen khong trung nhau.
_GREGORIAN_OFFSET = 0x01B21DD213814000   # so tick 100ns tu 1582-10-15 den 1970-01-01
_NODE = random.getrandbits(48) | 0x010000000000
_CLOCK_SEQ = random.getrandbits(14)
_lock = threading.Lock()
_last_ticks = 0


def _build(ticks: int) -> uuid.UUID:
    return uuid.UUID(fields=(
        ticks & 0xFFFFFFFF,                      # time_low
        (ticks >> 32) & 0xFFFF,                  # time_mid
        ((ticks >> 48) & 0x0FFF) | 0x1000,       # time_hi + version 1
        0x80 | ((_CLOCK_SEQ >> 8) & 0x3F),       # clock_seq_hi + variant
        _CLOCK_SEQ & 0xFF,                       # clock_seq_low
        _NODE,
    ))


def new_timeuuid() -> uuid.UUID:
    global _last_ticks
    with _lock:
        ticks = time.time_ns() // 100 + _GREGORIAN_OFFSET
        if ticks <= _last_ticks:
            ticks = _last_ticks + 1
        _last_ticks = ticks
    return _build(ticks)


def timeuuid_at(epoch_seconds: float) -> uuid.UUID:
    """Timeuuid tai 1 thoi diem bat ky (dung cho test/nhap lieu cu, khong dam bao don dieu)."""
    return _build(int(epoch_seconds * 10_000_000) + _GREGORIAN_OFFSET)


def day_of(timeuuid: uuid.UUID) -> date:
    """Day bucket (UTC) suy ra tu timeuuid -> partition key luon khop voi sent_at."""
    return datetime_from_uuid1(timeuuid).replace(tzinfo=timezone.utc).date()


def iso_of(timeuuid: uuid.UUID) -> str:
    return datetime_from_uuid1(timeuuid).replace(tzinfo=timezone.utc).isoformat()


def today_utc() -> date:
    return datetime.now(timezone.utc).date()


# --- Redis keys ---
def recent_key(conv_id) -> str:        # LIST: N tin gan nhat (JSON), moi nhat o dau
    return f"chat:{conv_id}:recent"

def channel(conv_id) -> str:           # PUB/SUB: tin realtime cua 1 cuoc chat
    return f"chat:{conv_id}:events"

AGENT_EVENTS_CHANNEL = "agents:events" # PUB/SUB: thong bao cho dashboard (khach moi, ...)
WAITING_QUEUE = "queue:waiting"        # ZSET: conv_id -> timestamp vao hang cho

def presence_key(role: str, user_id: str) -> str:   # STRING co TTL: online/offline
    return f"presence:{role}:{user_id}"


# --- Bo sung Ngay 3 ---
CHAT_EVENTS_PATTERN = "chat:*:events"   # moi server psubscribe 1 pattern cho moi cuoc chat
QUEUE_META = "queue:waiting:meta"       # HASH: conv_id -> JSON thong tin hien thi o hang cho
METRIC_HIT = "metrics:cache:hit"        # STRING counter dung chung moi server
METRIC_MISS = "metrics:cache:miss"

def version_key(conv_id) -> str:        # STRING counter: tang moi khi co tin moi (chong ghi cache cu)
    return f"chat:{conv_id}:ver"