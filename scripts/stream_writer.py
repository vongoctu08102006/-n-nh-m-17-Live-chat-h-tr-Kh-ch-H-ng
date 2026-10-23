"""GHI LIEN TUC vao Cassandra de mo phong LUONG DU LIEU THOI GIAN THUC (real-time streaming) luc demo.
Moi 'luong' giong 1 cuoc chat / 1 xe dang day du lieu: dong moi co dau thoi gian (timeuuid) nam trong partition (luong, ngay hom nay).

Chay (de CHAY NGAM, giu cua so nay mo trong luc demo):
  python scripts/stream_writer.py                         # 50 dong/giay, 5 luong, chay den khi Ctrl+C
  python scripts/stream_writer.py --rate 500 --streams 10 # 500 dong/giay (nang hon)
  python scripts/stream_writer.py --duration 120          # tu dung sau 120 giay
  python scripts/stream_writer.py --purge                 # xoa du lieu cua cac luong (3 ngay gan nhat)
Cua so thu hai xem so lieu tang: python scripts/monitor_stream.py"""
import argparse
import random
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

from seedlib import INSERT_MSG, PHRASES, stream_ids

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cassandra.concurrent import execute_concurrent_with_args
from app import keys
from app.cassandra_client import get_session, shutdown


def pct(v, p):
    if not v:
        return float("nan")
    s = sorted(v)
    return s[max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))]


def cmd_purge(a):
    s = get_session()
    s.default_timeout = 60
    dele = s.prepare("DELETE FROM messages WHERE conversation_id = ? AND day = ?")
    today = keys.today_utc()
    args = [(cid, today - timedelta(days=b)) for cid in stream_ids(a.streams) for b in range(0, 4)]
    execute_concurrent_with_args(s, dele, args, concurrency=64, raise_on_first_error=False)
    shutdown()
    print(f"Da xoa du lieu {a.streams} luong (4 ngay gan nhat).")


def main():
    ap = argparse.ArgumentParser(description="Ghi lien tuc vao Cassandra (mo phong real-time streaming)")
    ap.add_argument("--rate", type=float, default=50.0, help="so dong moi giay (mac dinh 50)")
    ap.add_argument("--streams", type=int, default=5, help="so luong du lieu (partition nong) ghi song song")
    ap.add_argument("--duration", type=float, default=0, help="chay bao nhieu giay (0 = den khi Ctrl+C)")
    ap.add_argument("--max-inflight", type=int, default=2000, help="so lenh ghi toi da dang cho Cassandra tra loi")
    ap.add_argument("--purge", action="store_true", help="xoa du lieu cua cac luong roi thoat")
    a = ap.parse_args()
    if a.purge:
        return cmd_purge(a)

    s = get_session()
    s.default_timeout = 30
    ins = s.prepare(INSERT_MSG)
    ids = stream_ids(a.streams)
    rng = random.Random()
    lock = threading.Lock()
    st = {"acked": 0, "errors": 0, "lat": [], "last_err": ""}

    def on_ok(_res, t0):
        with lock:
            st["acked"] += 1
            st["lat"].append((time.perf_counter() - t0) * 1000)

    def on_err(exc):
        with lock:
            st["errors"] += 1
            st["last_err"] = f"{type(exc).__name__}: {exc}"[:120]

    today = keys.today_utc()
    print(f"Ghi {a.rate:g} dong/giay vao {a.streams} luong ({a.rate / a.streams:g} dong/giay/luong). Ctrl+C de dung.\n")
    print("Mo them cua so cqlsh de XEM DU LIEU TANG:  docker exec -it livechat-cassandra cqlsh")
    print(f"  SELECT count(*) FROM livechat.messages WHERE conversation_id = {ids[0]} AND day = '{today}';")
    print(f"  SELECT toTimestamp(sent_at), content FROM livechat.messages WHERE conversation_id = {ids[0]} AND day = '{today}' LIMIT 5;")
    print("Hoac chay: python scripts/monitor_stream.py\n")
    seq = [0] * a.streams
    sent = 0
    t_start = time.perf_counter()
    last_t, last_sent, last_ack = t_start, 0, 0
    try:
        while True:
            now = time.perf_counter()
            if a.duration and now - t_start >= a.duration:
                break
            due = int((now - t_start) * a.rate) - sent            # so dong DEN HAN gui (dieu tiet toc do)
            due = min(due, int(a.rate) + 1)                       # khong don cuc: toi da 1 giay du lieu moi lan
            for _ in range(max(0, due)):
                with lock:
                    inflight = sent - st["acked"] - st["errors"]
                if inflight >= a.max_inflight:
                    break
                k = sent % a.streams
                seq[k] += 1
                tid = keys.new_timeuuid()
                typ = "customer" if seq[k] % 2 else "agent"
                params = (ids[k], keys.day_of(tid), tid, typ, f"stream{k}", f"[luong {k}] #{seq[k]} {rng.choice(PHRASES)}")
                fut = s.execute_async(ins, params)
                t0 = time.perf_counter()
                fut.add_callbacks(callback=on_ok, callback_args=(t0,), errback=on_err)
                sent += 1
            if now - last_t >= 1.0:
                with lock:
                    acked, errs, lat = st["acked"], st["errors"], st["lat"]
                    st["lat"] = []
                el = now - last_t
                print(f"  t={now - t_start:5.0f}s | gui {(sent - last_sent) / el:6.0f}/s | ack {(acked - last_ack) / el:6.0f}/s | "
                      f"tong {acked:>9,} | loi {errs} | tre ghi p50 {pct(lat, 50):5.1f} ms  p95 {pct(lat, 95):5.1f} ms | dang cho {sent - acked - errs}"
                      + (f" | {st['last_err']}" if errs else ""), flush=True)
                last_t, last_sent, last_ack = now, sent, acked
            time.sleep(0.005)
    except KeyboardInterrupt:
        print("\nDang dung ...")
    deadline = time.time() + 10
    while time.time() < deadline:                              # cho cac lenh dang bay hoan tat
        with lock:
            if sent - st["acked"] - st["errors"] <= 0:
                break
        time.sleep(0.1)
    el = max(time.perf_counter() - t_start, 1e-9)
    print(f"\nTONG KET: da ghi {st['acked']:,} dong trong {el:.0f}s (trung binh {st['acked'] / el:.0f} dong/giay), loi {st['errors']}.")
    print("Xoa du lieu luong sau khi demo:  python scripts/stream_writer.py --purge")
    shutdown()


if __name__ == "__main__":
    main()