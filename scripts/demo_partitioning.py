"""Minh hoa PHAN VUNG (partition) cua Cassandra tren bang messages: PRIMARY KEY ((conversation_id, day), sent_at).
Chay:  python scripts/demo_partitioning.py
Script tu tao du lieu thu (~3000 dong, 300 partition) roi XOA het khi xong."""
import sys, time, uuid
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cassandra import InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from app import keys
from app.cassandra_client import get_session, shutdown

NOISE_CONVS = 300
MSGS_PER_NOISE = 10


def title(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def avg_ms(fn, rounds):
    fn()
    t = time.perf_counter()
    for _ in range(rounds):
        fn()
    return (time.perf_counter() - t) * 1000 / rounds


def first_line(e):
    return str(e).strip().splitlines()[0][:150]


def main():
    s = get_session()
    ins = s.prepare("INSERT INTO messages (conversation_id, day, sent_at, sender_type, sender_id, content) VALUES (?,?,?,?,?,?)")
    target = uuid.uuid4()
    now = time.time()
    noise = [uuid.uuid4() for _ in range(NOISE_CONVS)]
    deleted_pairs = []

    try:
        params = []
        for back in range(3):
            for k in range(5):
                tid = keys.timeuuid_at(now - back * 86400 + k)
                params.append((target, keys.day_of(tid), tid, "customer" if k % 2 == 0 else "agent", "demo_agent", f"tin {k} cua ngay -{back}"))
        for c in noise:
            for k in range(MSGS_PER_NOISE):
                tid = keys.timeuuid_at(now - 60 + k)
                params.append((c, keys.day_of(tid), tid, "customer", f"khach_{k}", "du lieu nhieu"))
        deleted_pairs = sorted({(p[0], p[1]) for p in params}, key=str)          # de don dung cac partition da tao
        days = sorted({p[1] for p in params if p[0] == target}, reverse=True)    # cac ngay that cua cuoc chat muc tieu
        for ok, err in execute_concurrent_with_args(s, ins, params, concurrency=50, raise_on_first_error=False):
            if not ok:
                sys.exit(f"Loi khi nap du lieu thu: {err}")
        total = len(params)
        print(f"Da nap {total} dong: 1 cuoc chat muc tieu ({len(days)} ngay x 5 tin) + {NOISE_CONVS} cuoc chat nhieu ({MSGS_PER_NOISE} tin moi cuoc).")

        title("1. PARTITION KEY = (conversation_id, day): moi cuoc chat moi ngay la 1 partition rieng")
        print(f"  {'ngay':<12}{'so tin':>8}   token (vi tri tren vong bam cua Cassandra)")
        sel_tok = s.prepare("SELECT token(conversation_id, day) AS tk FROM messages WHERE conversation_id = ? AND day = ? LIMIT 1")
        sel_cnt = s.prepare("SELECT count(*) AS n FROM messages WHERE conversation_id = ? AND day = ?")
        for d in days:
            tk = s.execute(sel_tok, (target, d)).one().tk
            n = s.execute(sel_cnt, (target, d)).one().n
            print(f"  {str(d):<12}{n:>8}   {tk}")
        print(f"  -> {len(days)} ngay cua CUNG 1 cuoc chat co {len(days)} token KHAC NHAU = {len(days)} partition, co the nam tren cac node khac nhau.")
        print("     (Chi 1 node thi chua thay tac dung phan tan, nhung cau truc du lieu da san sang mo rong.)")

        title("2. Truy van DUNG partition key  ->  Cassandra di thang toi 1 partition")
        q_ok = s.prepare("SELECT sent_at, sender_type, content FROM messages WHERE conversation_id = ? AND day = ?")
        t_ok = avg_ms(lambda: list(s.execute(q_ok, (target, days[0]))), 100)
        print(f"  CQL: SELECT ... FROM messages WHERE conversation_id = ? AND day = ?")
        print(f"  Thoi gian trung binh (100 lan): {t_ok:.2f} ms")
        rows = list(s.execute(q_ok, (target, days[0])))
        print(f"  Thu tu tra ve (clustering DESC = tin MOI nhat truoc): {[r.content for r in rows][:3]} ...")

        title("3. Truy van THIEU 1 PHAN partition key  ->  Cassandra tu choi")
        for cql in ("SELECT * FROM messages WHERE conversation_id = %s",
                    "SELECT * FROM messages WHERE conversation_id = %s ALLOW FILTERING"):
            try:
                n = len(list(s.execute(cql, (target,))))
                print(f"  [{'cho phep' if 'ALLOW' in cql else 'chay duoc'}] {cql}  -> {n} dong")
            except InvalidRequest as e:
                print(f"  [BI TU CHOI] {cql}\n       {first_line(e)}")

        title("4. Loc theo cot thuong (sender_id) -> phai QUET CA BANG")
        cql = "SELECT * FROM messages WHERE sender_id = 'demo_agent'"
        try:
            list(s.execute(cql))
            print("  Cassandra chap nhan khong can ALLOW FILTERING (khong mong doi).")
        except InvalidRequest as e:
            print(f"  [BI TU CHOI] {cql}\n       {first_line(e)}")
        cql_f = cql + " ALLOW FILTERING"
        t_scan = avg_ms(lambda: list(s.execute(cql_f)), 5)
        found = len(list(s.execute(cql_f)))
        print(f"  Voi ALLOW FILTERING: tim thay {found} dong, mat {t_scan:.2f} ms (phai doc moi partition cua bang).")
        print(f"  So voi truy van dung partition key ({t_ok:.2f} ms): cham hon ~{t_scan / max(t_ok, 1e-9):.0f} lan tren chi {total} dong;")
        print("  chi phi nay tang TUYEN TINH theo kich thuoc bang, con truy van theo partition key thi gan nhu khong doi.")

        title("5. Clustering key sent_at: tim kiem khoang TRONG 1 partition (dung cho phan trang)")
        newest = rows[0].sent_at
        q_rng = s.prepare("SELECT content FROM messages WHERE conversation_id = ? AND day = ? AND sent_at < ?")
        older = [r.content for r in s.execute(q_rng, (target, days[0], newest))]
        print(f"  Tin moi nhat: {rows[0].content!r}")
        print(f"  Cac tin CU HON (sent_at < tin moi nhat): {older}")
        print("  -> day la co che 'before=' cua API phan trang lich su.")

        title("6. VI SAO chia partition theo NGAY (khong chi theo cuoc chat)")
        per_day, nday = 500, 90
        print(f"  Gia su 1 cuoc chat hoat dong lien tuc, {per_day} tin/ngay, {nday} ngay:")
        print(f"    Partition theo (conversation_id)       -> {per_day * nday:>7} dong trong 1 partition (phinh vo han, doc cham, kho can bang tai)")
        print(f"    Partition theo (conversation_id, day)  -> {per_day:>7} dong/partition, gioi han tren theo thoi gian")
        print("  Doi lai: doc lich su dai ngay phai truy van nhieu partition (code lam viec nay o repository.get_messages).")

        title("7. (Tuy chon) Xem thong ke that cua bang bang nodetool")
        print("  docker exec livechat-cassandra nodetool flush livechat messages")
        print("  docker exec livechat-cassandra nodetool tablestats livechat.messages")
        print("  -> xem dong 'Number of partitions (estimate)' va 'Compacted partition maximum bytes'.")
    finally:
        print("\nDang don du lieu thu ...")
        dele = s.prepare("DELETE FROM messages WHERE conversation_id = ? AND day = ?")
        execute_concurrent_with_args(s, dele, deleted_pairs, concurrency=50, raise_on_first_error=False)
        shutdown()
        print("Xong.")


if __name__ == "__main__":
    main()