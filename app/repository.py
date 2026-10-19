"""Tang truy cap Cassandra. Moi ham la DONG BO -> goi qua asyncio.to_thread() trong code async.
Moi bang ung voi 1 truy van (Q1-Q4 trong db/schema.cql)."""
import uuid
from datetime import datetime, timedelta, timezone
from cassandra.query import BatchStatement
from . import keys
from .cassandra_client import get_session


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


class Closed(Exception):
    pass


_CQL = {
    "ins_conv": "INSERT INTO conversations (conversation_id, customer_id, status, created_tid, created_at, created_day) "
                "VALUES (?, ?, 'waiting', ?, ?, ?)",
    "ins_by_cust": "INSERT INTO conversations_by_customer (customer_id, created_at, conversation_id, status) "
                   "VALUES (?, ?, ?, 'waiting')",
    "get_conv": "SELECT conversation_id, customer_id, agent_id, status, created_tid, created_at, created_day, closed_at "
                "FROM conversations WHERE conversation_id = ?",
    "claim": "UPDATE conversations SET agent_id = ?, status = 'active' WHERE conversation_id = ? IF status = 'waiting'",
    "upd_cust_status": "UPDATE conversations_by_customer SET status = ? WHERE customer_id = ? AND created_at = ?",
    "ins_by_agent": "INSERT INTO conversations_by_agent (agent_id, created_at, conversation_id, customer_id) VALUES (?, ?, ?, ?)",
    "close": "UPDATE conversations SET status = 'closed', closed_at = ? WHERE conversation_id = ?",
    "ins_msg": "INSERT INTO messages (conversation_id, day, sent_at, sender_type, sender_id, content) VALUES (?, ?, ?, ?, ?, ?)",
    "msgs": "SELECT sent_at, sender_type, sender_id, content FROM messages WHERE conversation_id = ? AND day = ? LIMIT ?",
    "msgs_before": "SELECT sent_at, sender_type, sender_id, content FROM messages "
                   "WHERE conversation_id = ? AND day = ? AND sent_at < ? LIMIT ?",
    "by_cust": "SELECT created_at, conversation_id, status FROM conversations_by_customer WHERE customer_id = ? LIMIT ?",
    "by_agent": "SELECT created_at, conversation_id, customer_id FROM conversations_by_agent WHERE agent_id = ? LIMIT ?",
}
_stmts = {}


def init():
    """Goi 1 lan luc server khoi dong: tao session + prepare tat ca cau lenh."""
    s = get_session()
    for name, cql in _CQL.items():
        _stmts[name] = s.prepare(cql)


def _load(conv_id):
    row = get_session().execute(_stmts["get_conv"], (conv_id,)).one()
    if row is None:
        raise NotFound(str(conv_id))
    return row


def _utc_iso(dt):
    return dt.replace(tzinfo=timezone.utc).isoformat() if dt else None


def _conv_dict(row) -> dict:
    return {
        "id": str(row.conversation_id),
        "customer_id": row.customer_id,
        "agent_id": row.agent_id,
        "status": row.status,
        "created_at": _utc_iso(row.created_at),
        "closed_at": _utc_iso(row.closed_at),
    }


def _msg_dict(conv_id, tid, sender_type, sender_id, content) -> dict:
    return {
        "id": str(tid),
        "conversation_id": str(conv_id),
        "sent_at": keys.iso_of(tid),
        "sender_type": sender_type,
        "sender_id": sender_id,
        "content": content,
    }


# ---------- Conversations ----------
def create_conversation(customer_id: str) -> dict:
    conv_id = uuid.uuid4()
    tid = keys.new_timeuuid()
    created_at = datetime.utcnow().replace(microsecond=0)
    batch = BatchStatement()  # LOGGED: 2 bang denormalize luon cung ghi hoac cung khong
    batch.add(_stmts["ins_conv"], (conv_id, customer_id, tid, created_at, keys.day_of(tid)))
    batch.add(_stmts["ins_by_cust"], (customer_id, tid, conv_id))
    get_session().execute(batch)
    return _conv_dict(_load(conv_id))


def get_conversation(conv_id: uuid.UUID) -> dict:
    return _conv_dict(_load(conv_id))


def assign_agent(conv_id: uuid.UUID, agent_id: str) -> dict:
    row = _load(conv_id)
    if row.status == "closed":
        raise Closed(str(conv_id))
    if row.status == "active":
        if row.agent_id == agent_id:
            return _conv_dict(row)          # nhan vien vao lai cuoc chat cua chinh minh
        raise Conflict("da co nhan vien khac phu trach")
    s = get_session()
    # LWT: chi 1 nhan vien thang khi nhieu nguoi cung bam nhan khach
    if not s.execute(_stmts["claim"], (agent_id, conv_id)).was_applied:
        row = _load(conv_id)
        if row.status == "active" and row.agent_id == agent_id:
            return _conv_dict(row)
        if row.status == "closed":
            raise Closed(str(conv_id))
        raise Conflict("da co nhan vien khac phu trach")
    batch = BatchStatement()
    batch.add(_stmts["upd_cust_status"], ("active", row.customer_id, row.created_tid))
    batch.add(_stmts["ins_by_agent"], (agent_id, row.created_tid, conv_id, row.customer_id))
    s.execute(batch)
    return _conv_dict(_load(conv_id))


def close_conversation(conv_id: uuid.UUID) -> dict:
    row = _load(conv_id)
    if row.status == "closed":
        return _conv_dict(row)
    batch = BatchStatement()
    batch.add(_stmts["close"], (datetime.utcnow().replace(microsecond=0), conv_id))
    batch.add(_stmts["upd_cust_status"], ("closed", row.customer_id, row.created_tid))
    get_session().execute(batch)
    return _conv_dict(_load(conv_id))


def list_by_customer(customer_id: str, limit: int = 20) -> list:
    rows = get_session().execute(_stmts["by_cust"], (customer_id, limit))
    return [{"id": str(r.conversation_id), "created_at": keys.iso_of(r.created_at), "status": r.status} for r in rows]


def list_by_agent(agent_id: str, limit: int = 20) -> list:
    rows = get_session().execute(_stmts["by_agent"], (agent_id, limit))
    return [{"id": str(r.conversation_id), "created_at": keys.iso_of(r.created_at), "customer_id": r.customer_id} for r in rows]


# ---------- Messages ----------
def save_message(conv_id: uuid.UUID, sender_type: str, sender_id: str, content: str, tid=None) -> dict:
    tid = tid or keys.new_timeuuid()
    get_session().execute(_stmts["ins_msg"], (conv_id, keys.day_of(tid), tid, sender_type, sender_id, content))
    return _msg_dict(conv_id, tid, sender_type, sender_id, content)


def get_messages(conv_id: uuid.UUID, limit: int = 50, before=None) -> dict:
    """Tin moi nhat truoc. Duyet partition (conv, day) tu ngay moi ve ngay cu cho den khi du `limit`.
    `before` = id (timeuuid) cua tin cuoi trang truoc -> lay trang ke tiep."""
    floor = _load(conv_id).created_day.date()
    s = get_session()
    before_day = keys.day_of(before) if before else None
    d = before_day or keys.today_utc()
    out = []
    while d >= floor and len(out) < limit:
        need = limit - len(out)
        if before is not None and d == before_day:
            rs = s.execute(_stmts["msgs_before"], (conv_id, d, before, need))
        else:
            rs = s.execute(_stmts["msgs"], (conv_id, d, need))
        out.extend(_msg_dict(conv_id, r.sent_at, r.sender_type, r.sender_id, r.content) for r in rs)
        d -= timedelta(days=1)
    return {"messages": out, "next_before": out[-1]["id"] if len(out) == limit else None}