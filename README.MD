# Live Chat hỗ trợ khách hàng

Hệ thống chat hỗ trợ khách hàng thời gian thực: khách chat với nhân viên qua widget nhúng trong trang web, nhân viên xử lý nhiều khách cùng lúc trên dashboard. Đồ án môn cơ sở dữ liệu NoSQL, dùng **Cassandra** (lưu lịch sử, phân vùng) và **Redis** (cache, pub/sub, hàng chờ, trạng thái online).

## Kiến trúc

```
 Trang shop + widget ──WebSocket──┐                       ┌── Dashboard nhân viên
 (khách)                          ▼                       ▼   (WebSocket)
                           ┌─────────────┐  pub/sub  ┌─────────────┐
                           │  Server A   │◄─────────►│  Server B   │   (FastAPI, nhiều server cùng chạy)
                           └──────┬──────┘   Redis   └──────┬──────┘
                                  │      ┌───────────┐      │
                                  ├─────►│   Redis   │◄─────┤  cache 50 tin gần nhất, hàng chờ,
                                  │      └───────────┘      │  presence, pub/sub, số liệu
                                  │      ┌───────────┐      │
                                  └─────►│ Cassandra │◄─────┘  lịch sử đầy đủ, phân vùng (conversation_id, day)
                                         └───────────┘
```

**Đường đi của một tin nhắn** (thứ tự quan trọng): (1) ghi Cassandra, (2) đẩy vào cache Redis, (3) publish lên Redis, mọi server nhận rồi đẩy xuống WebSocket của người trong cuộc chat. Ghi Cassandra lỗi thì dừng, không cache, không phát.

## Cài đặt và chạy

Yêu cầu: Docker Desktop, Python 3.12 (không dùng 3.13).
```Chạy vào thư mục và cài venvs
cd "C:\Users\admin\OneDrive\Documents\code\Đồ án Live Chat Khách hàng\livechat"
C:\venvs\livechat\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
```Chay server
$env:AGENT_KEY = "nhanvientuvan"
powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1 -NoBrower

```TẠO LINK TRANG KHÁCH
cloudflared tunnel --url http://127.0.0.1:8001


```nap 1 trieu dong powershell
python scripts/bulk_seed.py --demo-agent nv1

```do do tre
python scripts/scale_report.py

```Chay demo ngam
-Bat 3 trang PowerShell da cai venz
python scripts/stream_writer.py
python scripts/monitor_stream.py
docker exec -it livechat-cassandra cqlsh


**Cách thủ công:**
```powershell
docker compose up -d                          # đợi cassandra "healthy" (1-2 phút)
python scripts/init_db.py                     # tạo bảng
$env:SERVER_ID = "A"; uvicorn app.main:app --port 8001      # cửa sổ 1
$env:SERVER_ID = "B"; uvicorn app.main:app --port 8002      # cửa sổ 2
```

| Địa chỉ | Dùng để |
|---|---|
| `http://127.0.0.1:8001/` | Trang shop + widget chat (thêm `?customer=ten` để giả lập nhiều khách) |
| `http://127.0.0.1:8002/web/agent.html?agent=nv1` | Dashboard nhân viên |
| `http://127.0.0.1:8001/docs` | Tài liệu API (Swagger) |
| `http://127.0.0.1:8001/api/metrics` | Số liệu cache hit/miss |

## Cấu trúc thư mục

```
app/        main.py (API + WebSocket) · repository.py (Cassandra) · redis_store.py (cache, hàng chờ, presence)
            realtime.py (pub/sub liên server) · service.py (đọc lịch sử) · keys.py (tên key, timeuuid)
db/         schema.cql
web/        index.html + chat-widget.js (khách) · agent.html + agent.js (nhân viên)
scripts/    init_db · verify_day1/2/3 · run_all_checks · bench_cache · demo_partitioning · load_test · start_demo.ps1
docs/       PROGRESS.md (nhật ký + quyết định thiết kế) · DEMO.md (kịch bản demo)
```

## Thiết kế dữ liệu

**Cassandra** (thiết kế theo truy vấn):

| Bảng | Khóa | Phục vụ |
|---|---|---|
| `messages` | `((conversation_id, day), sent_at DESC)` | Đọc tin của cuộc chat, phân trang, mỗi ngày một partition |
| `conversations` | `conversation_id` | Thông tin và trạng thái cuộc chat |
| `conversations_by_customer` | `(customer_id), created_at DESC` | Lịch sử chat của một khách |
| `conversations_by_agent` | `(agent_id), created_at DESC` | Các cuộc chat nhân viên đã xử lý |

**Redis:**

| Key | Kiểu | Vai trò |
|---|---|---|
| `chat:{id}:recent` | LIST | Cache 50 tin gần nhất (TTL 1 giờ) |
| `chat:{id}:ver` | STRING | Số phiên bản, chống ghi cache cũ |
| `chat:{id}:events`, `agents:events` | PUB/SUB | Phát sự kiện giữa các server |
| `queue:waiting`, `queue:waiting:meta` | ZSET, HASH | Hàng chờ khách |
| `presence:{role}:{user}` | STRING (đếm, TTL 90s) | Online/offline |
| `metrics:cache:hit`, `metrics:cache:miss` | STRING | Số liệu cache |

## WebSocket

- Cuộc chat: `ws://host/ws/{conversation_id}?role=customer|agent&user_id=...`
- Dashboard (hàng chờ): `ws://host/ws-agents?agent_id=...`
- Client gửi: `{"type":"message","content":"..."}` và `{"type":"ping"}` (mỗi ~25s để giữ trạng thái online).
- Server gửi: `joined`, `message`, `presence`, `closed`, `error`, `pong`; dashboard thêm `queue_snapshot`, `queue_add`, `queue_remove`.
- Mã đóng: `4400` tham số sai, `4403` sai khách, `4404` không tồn tại, `4409` đã có nhân viên khác, `4410` đã kết thúc.

## Kiểm thử

```powershell
python scripts/run_all_checks.py --repeat 3       # 8 + 29 + 39 kiểm tra, lặp 3 lần
python scripts/bench_cache.py                      # tốc độ cache hit/miss
python scripts/demo_partitioning.py                # minh họa phân vùng
python scripts/load_test.py --pairs 20 --messages 25 --gap-ms 20   # cần 2 server đang chạy
```

## Hạn chế và hướng phát triển

- Chưa có xác thực: khách và nhân viên chỉ khai tên. Hướng phát triển: tài khoản và token.
- Mỗi server nhận mọi sự kiện chat rồi tự lọc (psubscribe); hệ thống lớn nên subscribe theo từng cuộc chat.
- Hàng chờ chỉ ở Redis (tự chữa lành khi khách kết nối lại, nhưng không phục hồi từ Cassandra).
- `conversations_by_agent` không lưu trạng thái nên dashboard gọi thêm API khi tải trang.
- Chưa có gửi ảnh/tệp, chỉ báo "đang gõ", đánh giá cuối chat.