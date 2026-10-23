"""BAO CAO O QUY MO LON sau khi nap du lieu:
  1) DEM CHINH XAC so dong trong bang messages bang TOKEN RANGE (chia vong bam thanh nhieu lat cat nho, dem tung lat song song).
     Vi sao khong dung SELECT count(*) thang? Vi no quet ca bang tren 1 lan va thuong bi ReadTimeout khi bang hang trieu dong.
  2) DO DO TRE DOC: doc 1 partition (cuoc chat, ngay) va doc 50 tin moi nhat cua 1 cuoc chat, tren bang da hang trieu dong.
  3) (tuy chon --scan-demo) chi phi quet ca bang bang ALLOW FILTERING, de doi chung.
Chay:  python scripts/scale_report.py            (sau bulk_seed.py)
       python scripts/scale_report.py --scan-demo"""
import argparse
import json
import random
import statistics
import sys
import time
from pathlib import Path

from seedlib import conv_timestamps, identity, node_clock, plan_sizes, tuuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cassandra.concurrent import execute_concurrent_with_args
from app import keys
from app import repository as repo
from app.cassandra_client import get_session, shutdown

DOCS = Path(__file__).resolve().parents[1] / "docs"


def token_slices(n):
    """Chia vong bam Murmur3 [-2^63, 2^63-1] thanh n lat cat lien tiep, khong ho, khong chong. Lat cuoi khong co can tren."""
    lo, step = -(2 ** 63), (2 ** 64) // n
    edges = [lo + k * step for k in range(n)]
    return [(edges[k], edges[k + 1] if k + 1 < n else None) for k in range(n)]


def count_rows(s, slices=256, concurrency=32):
    mid = s.prepare("SELECT count(*) AS n FROM messages WHERE token(conversation_id, day) >= ? AND token(conversation_id, day) < ?")
    last = s.prepare("SELECT count(*) AS n FROM messages WHERE token(conversation_id, day) >= ?")
    total, bad = 0, 0
    parts = [(mid, [(lo, hi) for lo, hi in token_slices(slices) if hi is not None]),
             (last, [(lo,) for lo, hi in token_slices(slices) if hi is None])]
    for prep, args in parts:
        for ok, res in execute_concurrent_with_args(s, prep, args, concurrency=concurrency, raise_on_first_error=False):
            if ok:
                total += res.one().n
            else:
                bad += 1
    return total, bad


def pct(v, p):
    s = sorted(v)
    return s[max(0, min(len(s) - 1, int(round(p / 100 * (len(s) - 1)))))]


def stat_line(name, ms):
    return f"  {name:<46}{min(ms):>8.2f}{pct(ms, 50):>9.2f}{pct(ms, 95):>9.2f}{pct(ms, 99):>9.2f}{max(ms):>9.2f}"


def sample_partitions(m, k, rng):
    sizes = plan_sizes(m["tag"], m["rows_requested"], m["conversations"])
    node, clock = node_clock(m["tag"])
    out = []
    for i in rng.sample(range(m["conversations"]), min(k, m["conversations"])):
        cid, _, _ = identity(m["tag"], i, m["conversations"], m.get("demo_agent"))
        ts = conv_timestamps(m["tag"], i, sizes[i], m["end_ts"], m["days"])
        out.append((cid, keys.day_of(tuuid(rng.choice(ts), node, clock))))
    return out


def timed(fn, items):
    out = []
    for it in items:
        t = time.perf_counter()
        fn(it)
        out.append((time.perf_counter() - t) * 1000)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slices", type=int, default=256, help="so lat cat token khi dem")
    ap.add_argument("--samples", type=int, default=300, help="so partition lay mau de do do tre doc")
    ap.add_argument("--scan-demo", action="store_true", help="thu quet ca bang bang ALLOW FILTERING (co the bi timeout)")
    a = ap.parse_args()
    s = get_session()
    s.default_timeout = 60
    repo.init()
    files = sorted(DOCS.glob("seed_manifest_*.json"))
    manifests = [json.loads(f.read_text(encoding="utf-8")) for f in files]

    print("\n=== 1. DEM SO DONG (token range, %d lat cat) ===" % a.slices)
    t = time.perf_counter()
    total, bad = count_rows(s, a.slices)
    dt = time.perf_counter() - t
    print(f"  Bang messages hien co: {total:,} dong  (dem trong {dt:.1f} giay{', ' + str(bad) + ' lat cat loi - so lieu thieu' if bad else ''})")
    if manifests:
        w = sum(m["rows_written"] for m in manifests)
        print(f"  Cac dot nap theo manifest: {len(manifests)} dot, tong {w:,} dong"
              + ("  -> khop (phan chenh la du lieu khac trong bang)" if total >= w else "  -> THIEU, kiem tra loi luc nap"))
        for m in manifests:
            print(f"    - tag {m['tag']}: {m['rows_written']:,} dong / {m['seconds']}s = {m['rows_per_sec']:,} dong/giay ({m['workers']} tien trinh)")

    if manifests:
        m = manifests[-1]
        rng = random.Random(12345)
        print(f"\n=== 2. DO TRE DOC tren bang {total:,} dong (don vi ms, {a.samples} mau ngau nhien) ===")
        print(f"  {'':<46}{'min':>8}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}")
        parts = sample_partitions(m, a.samples, rng)
        q_all = s.prepare("SELECT sent_at, sender_type, sender_id, content FROM messages WHERE conversation_id = ? AND day = ?")
        q_top = s.prepare("SELECT sent_at, content FROM messages WHERE conversation_id = ? AND day = ? LIMIT 20")
        print(stat_line("doc CA 1 partition (cuoc chat + ngay)", timed(lambda p: list(s.execute(q_all, p)), parts)))
        print(stat_line("doc 20 tin MOI NHAT cua 1 partition", timed(lambda p: list(s.execute(q_top, p)), parts)))
        if m.get("with_conversations"):
            cids = [p[0] for p in parts[:200]]
            try:
                print(stat_line("repo.get_messages: 50 tin moi nhat (nhieu ngay)", timed(lambda c: repo.get_messages(c, 50), cids)))
            except Exception as e:
                print(f"  (bo qua repo.get_messages: {type(e).__name__})")
        print("\n  Doc theo partition key: thoi gian gan nhu KHONG doi khi bang lon len, vi Cassandra di thang toi dung partition.")
    else:
        print("\n(Khong tim thay docs/seed_manifest_*.json -> bo qua phan do do tre. Chay bulk_seed.py truoc.)")

    if a.scan_demo:
        print("\n=== 3. DOI CHUNG: quet ca bang bang ALLOW FILTERING ===")
        t = time.perf_counter()
        try:
            r = s.execute("SELECT count(*) FROM messages WHERE sender_id = 'khong_ton_tai' ALLOW FILTERING").one()
            print(f"  Quet xong sau {time.perf_counter() - t:.1f} giay (tim thay {r[0]} dong) - phai doc TOAN BO {total:,} dong.")
        except Exception as e:
            print(f"  Sau {time.perf_counter() - t:.1f} giay: {type(e).__name__} - Cassandra bo cuoc vi quet ca bang qua lau. Day la ly do phai thiet ke theo khoa phan vung.")

    print("\n=== 4. THONG KE DUNG LUONG / PARTITION (chay trong PowerShell) ===")
    print("  docker exec livechat-cassandra nodetool flush livechat messages")
    print("  docker exec livechat-cassandra nodetool tablestats livechat.messages")
    print("  -> xem: Number of partitions (estimate), SSTable count, Space used (live), Compacted partition maximum bytes")
    shutdown()


if __name__ == "__main__":
    main()