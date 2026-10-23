"""XEM TRUC TIEP du lieu dang chay vao (doc song song luc ghi): moi giay dem lai cac partition cua stream_writer.
Chay (cua so rieng, trong luc stream_writer.py dang chay):  python scripts/monitor_stream.py
Thoat: Ctrl+C.   Dung de chung minh Cassandra DOC va GHI dong thoi van on dinh."""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from seedlib import stream_ids

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import keys
from app.cassandra_client import get_session, shutdown


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--streams", type=int, default=5, help="phai bang --streams cua stream_writer")
    ap.add_argument("--interval", type=float, default=1.0)
    a = ap.parse_args()
    s = get_session()
    s.default_timeout = 30
    cnt = s.prepare("SELECT count(*) AS n FROM messages WHERE conversation_id = ? AND day = ?")
    last = s.prepare("SELECT toTimestamp(sent_at) AS ts, content FROM messages WHERE conversation_id = ? AND day = ? LIMIT 1")
    ids = stream_ids(a.streams)
    prev_total, prev_t = None, time.time()
    print("Dang theo doi cac luong ghi (Ctrl+C de thoat) ...\n")
    try:
        while True:
            day = keys.today_utc()
            t0 = time.perf_counter()
            counts = [s.execute(cnt, (cid, day)).one().n for cid in ids]
            newest = s.execute(last, (ids[0], day)).one()
            read_ms = (time.perf_counter() - t0) * 1000
            total, now = sum(counts), time.time()
            rate = 0.0 if prev_total is None else (total - prev_total) / max(now - prev_t, 1e-9)
            prev_total, prev_t = total, now
            print(f"{datetime.now():%H:%M:%S} | tong {total:>9,} dong (+{rate:6.0f}/s) | "
                  f"moi luong: {' '.join(f'{c:,}' for c in counts)} | doc {a.streams + 1} truy van mat {read_ms:5.1f} ms"
                  + (f" | moi nhat: {newest.content[:48]}" if newest else ""), flush=True)
            time.sleep(max(0.0, a.interval - (time.perf_counter() - t0)))
    except KeyboardInterrupt:
        pass
    shutdown()


if __name__ == "__main__":
    main()