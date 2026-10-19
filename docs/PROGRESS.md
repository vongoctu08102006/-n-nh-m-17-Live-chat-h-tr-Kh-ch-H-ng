# NHAT KY TIEN DO - Do an Live Chat ho tro khach hang
Deadline: Thu Bay 17/10/2026

## Quyet dinh thiet ke da chot (KHONG doi neu khong ghi ly do)
- D1: Python + FastAPI + WebSocket goc
- D2: Cassandra = nguon su that cho lich su chat
- D3: Redis = cache + pub/sub + hang cho + presence
- D4: Partition (conversation_id, day), day tinh tu timeuuid, UTC
- D5: Ten key Redis chi dinh nghia trong app/keys.py
- D6: Ghi Cassandra TRUOC, roi moi cache Redis + publish
- D7: Cassandra driver dong bo, goi qua asyncio.to_thread
- D8: Bang theo truy van Q1-Q4 (xem db/schema.cql)

## Ngay 1 - 10/10: Ha tang + schema
- [x] Tao day du 12 file
- [ ] docker compose ps: ca hai healthy
- [ ] init_db.py: 5 dong OK
- [ ] verify_day1.py: 8/8 PASS
## Quyet dinh bo sung (Ngay 3)
- D13: Cache lich su = Redis list; ghi bang LPUSHX; do cache chi khi `ver` khong doi (WATCH) -> khong bao gio thieu tin
- D14: Hang cho khach chi song trong Redis (ZSET + HASH); Cassandra khong co bang "tat ca cuoc chat dang cho"
- D15: Moi su kien realtime di qua Redis pub/sub, ke ca khi cung server (mot duong duy nhat, de kiem chung)
- D16: Redis la lop toi uu: loi thi safe() ghi log va chay tiep bang Cassandra

## Ngay 3 - 11/10: Redis (cache, pub/sub 2 server, hang cho, presence)  [HOAN THANH]
- [x] redis_store.py, realtime.py, service.py, main.py moi, verify_day3.py
- [x] verify_day3 39/39, verify_day2 29/29, verify_day1 8/8
- Client phai ping moi ~25s de giu presence (lam o Ngay 4, 5)


## Ngay 4 - 11/10: Trang shop + widget chat khach  [HOAN THANH]
- [x] web/index.html, web/chat-widget.js, main.py phuc vu /web/
## Quyet dinh bo sung (Ngay 4)
- D17: Server phuc vu luon giao dien (/web/) -> cung origin, khong can CORS/file://
- D18: Khach = sessionStorage (moi tab 1 khach, ?customer=ten de gia lap); vao lai cuoc chat dang mo qua API Q3
- D19: Widget chong XSS (textContent), chong trung tin theo id, giu tin den trong luc tai lich su, tu noi lai co gian cach
- Han che: chua co xac thuc that (chi dua vao customer_id) -> huong phat trien: token
- Ngay 5: web/agent.html - dashboard nhan vien (hang cho realtime /ws-agents, nhan khach, chat nhieu khach, xem lich su)
## Quyet dinh bo sung (Ngay 5)
- D20: "Nhan khach" = nhan vien noi WebSocket vao cuoc chat; server quyet dinh nguoi thang bang LWT (khong them API rieng)
- D21: Dashboard giu 1 WebSocket / cuoc chat dang phu trach; tai lai trang tu noi lai qua API Q4
- D22: Lich su dung REST (khong WebSocket), doc theo trang qua `before`; chat cu cua khach lay tu Q3
- Han che: chua xac thuc nhan vien; by_agent khong co status nen N+1 request khi tai trang

## Ngay 5 - 12/10: Dashboard nhan vien  [HOAN THANH]
- [x] web/agent.html, web/agent.js (khong sua server)
- Ngay 6: kiem thu tong the, so lieu demo (phan vung + cache), kich ban demo, README