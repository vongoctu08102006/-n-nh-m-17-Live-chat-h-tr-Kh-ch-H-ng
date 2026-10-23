"""Ham dung chung cho: bulk_seed.py (nap du lieu lon), stream_writer.py (ghi luong), monitor_stream.py, scale_report.py.
Moi thu o day la TAT DINH (cung tham so -> cung ket qua) de co the nap xong roi XOA SACH dung nhung gi da nap."""
import random
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import keys

NS = uuid.UUID("c0de0000-1d2a-4f10-8000-000000000001")
INSERT_MSG = ("INSERT INTO messages (conversation_id, day, sent_at, sender_type, sender_id, content) "
              "VALUES (?, ?, ?, ?, ?, ?)")
INS_CONV = ("INSERT INTO conversations (conversation_id, customer_id, agent_id, status, created_tid, created_at, "
            "created_day, closed_at) VALUES (?, ?, ?, 'closed', ?, ?, ?, ?)")
INS_BY_CUST = ("INSERT INTO conversations_by_customer (customer_id, created_at, conversation_id, status) "
               "VALUES (?, ?, ?, 'closed')")
INS_BY_AGENT = ("INSERT INTO conversations_by_agent (agent_id, created_at, conversation_id, customer_id) "
                "VALUES (?, ?, ?, ?)")

PHRASES = [
    "Xin chào, mình muốn hỏi về đơn hàng vừa đặt", "Bạn cho mình xin mã đơn hàng nhé", "Đơn của mình chưa thấy giao",
    "Mình kiểm tra giúp bạn ngay", "Sản phẩm này còn hàng không ạ", "Dạ còn hàng, bạn muốn lấy màu nào",
    "Phí vận chuyển về Hà Nội là bao nhiêu", "Đơn trên 300.000đ được miễn phí giao hàng ạ",
    "Mình muốn đổi sang hạt Robusta", "Được ạ, bạn chọn gói 500g hay 1kg", "Cho mình xin địa chỉ nhận hàng",
    "Mình thanh toán chuyển khoản được không", "Được ạ, bạn chuyển theo thông tin này nhé", "Mình đã chuyển khoản rồi",
    "Cảm ơn bạn, đơn sẽ giao trong 1 đến 3 ngày", "Phin nhôm này dùng cho ly cỡ nào", "Dạ dùng cho ly khoảng 200ml",
    "Hạt rang xay sẵn hay nguyên hạt ạ", "Bên mình có cả hai, bạn chọn loại nào", "Mình muốn xay vừa pha phin",
    "Ok bạn, mình ghi chú vào đơn", "Sản phẩm bị lỗi, mình muốn đổi trả", "Bạn gửi giúp mình ảnh sản phẩm nhé",
    "Mình đã gửi ảnh rồi đó", "Bên mình sẽ đổi mới cho bạn trong 7 ngày", "Có mã giảm giá nào không ạ",
    "Hiện có mã GIAM10 cho đơn đầu tiên", "Cảm ơn bạn đã hỗ trợ", "Chúc bạn một ngày tốt lành", "Mình cần xuất hóa đơn công ty",
]

_GREG = 0x01B21DD213814000      # so tick 100ns tu 1582-10-15 den 1970-01-01


def node_clock(tag: str):
    """NODE va CLOCK_SEQ co dinh theo `tag` -> timeuuid sinh lai y het (de xoa sach)."""
    h = uuid.uuid5(NS, "node:" + tag).int
    return (h & 0xFFFFFFFFFF) | 0x010000000000, (h >> 40) & 0x3FFF


def tuuid(ts: float, node: int, clock: int) -> uuid.UUID:
    whole = int(ts)
    ticks = whole * 10_000_000 + int(round((ts - whole) * 10_000_000)) + _GREG
    return uuid.UUID(fields=(ticks & 0xFFFFFFFF, (ticks >> 32) & 0xFFFF, ((ticks >> 48) & 0x0FFF) | 0x1000,
                             0x80 | ((clock >> 8) & 0x3F), clock & 0xFF, node))


def plan_sizes(tag: str, rows: int, convs: int) -> list:
    """Chia CHINH XAC `rows` tin cho `convs` cuoc chat (kich thuoc khong deu, moi cuoc >= 1 tin)."""
    if rows < convs:
        raise ValueError("rows phai >= conversations")
    rng = random.Random(f"{tag}:sizes")
    extra = rows - convs
    bounds = [0] + sorted(rng.randint(0, extra) for _ in range(convs - 1)) + [extra]
    return [1 + bounds[k + 1] - bounds[k] for k in range(convs)]


def conv_timestamps(tag: str, i: int, n: int, end_ts: float, days: int) -> list:
    """n moc thoi gian tang NGHIEM NGAT (cach nhau >= 2ms) trai tren 0,5..72 gio, nam trong `days` ngay gan nhat."""
    rng = random.Random(f"{tag}:conv:{i}")
    span = min(rng.uniform(0.5, 72.0), days * 24 - 1) * 3600.0
    back = rng.uniform(span + n * 0.002 + 1.0, days * 86400.0)
    start = end_ts - back
    offs = sorted(rng.uniform(0, span) for _ in range(n))
    return [start + o + j * 0.002 for j, o in enumerate(offs)]


def identity(tag: str, i: int, convs: int, demo_agent):
    cid = uuid.uuid5(NS, f"{tag}:conv:{i}")
    customer = f"sim_{tag}_c{i % max(1, convs // 3)}"
    agent = demo_agent if (demo_agent and i % 40 == 0) else f"simag_{tag}_{i % 20}"
    return cid, customer, agent


def build_conv(cfg: dict, i: int, n: int, node: int, clock: int):
    """Tra ve (cid, customer, agent, rows, first_ts, last_ts); rows da sap theo thoi gian."""
    tag = cfg["tag"]
    cid, customer, agent = identity(tag, i, cfg["convs"], cfg.get("demo_agent"))
    ts = conv_timestamps(tag, i, n, cfg["end_ts"], cfg["days"])
    rng = random.Random(f"{tag}:content:{i}")
    rows = []
    for j, t in enumerate(ts):
        tid = tuuid(t, node, clock)
        mine = (j % 2 == 0)
        rows.append((cid, keys.day_of(tid), tid, "customer" if mine else "agent", customer if mine else agent,
                     f"{rng.choice(PHRASES)} (#{j + 1})"))
    return cid, customer, agent, rows, ts[0], ts[-1]


def stream_ids(n: int) -> list:
    """Dinh danh co dinh cua cac 'luong' ghi lien tuc (de monitor_stream biet doc o dau)."""
    return [uuid.uuid5(NS, f"stream:{k}") for k in range(n)]