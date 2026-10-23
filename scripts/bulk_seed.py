"""NAP DU LIEU LON vao Cassandra (bang log tin nhan `messages`) de minh hoa HEAVY WRITE / TIME-SERIES.
Moi dong = 1 tin nhan co dau thoi gian (timeuuid), nam trong partition (cuoc chat, ngay).

Chay (tu thu muc livechat, Docker dang chay, da init_db):
  python scripts/bulk_seed.py --rows 20000          # THU NHO truoc (vai giay) de chac moi thu on
  python scripts/bulk_seed.py                       # 1.000.000 dong (mac dinh)
  python scripts/bulk_seed.py --rows 2000000 --workers 4
  python scripts/bulk_seed.py --demo-agent nv1      # gan ~2% cuoc chat cho nhan vien nv1 de xem tren dashboard
  python scripts/bulk_seed.py --purge               # XOA SACH du lieu da nap (theo file docs/seed_manifest_*.json)

Ky thuat: nhom tin theo (cuoc chat, ngay) = dung 1 partition roi ghi bang UNLOGGED BATCH (toi da 20 dong/batch),
nhieu batch chay dong thoi (execute_concurrent), nhieu tien trinh (--workers)."""
import argparse
import json
import multiprocessing as mp
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from seedlib import (INS_BY_AGENT, INS_BY_CUST, INS_CONV, INSERT_MSG, build_conv, conv_timestamps, identity,
                     node_clock, plan_sizes, tuuid)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.query import BatchStatement, BatchType
from app import keys
from app.cassandra_client import get_session, shutdown

DOCS = Path(__file__).resolve().parents[1] / "docs"
COUNTER = None      # so dong da ghi (dung chung giua cac tien trinh)
FAILED = None


def _init(counter, failed):
    global COUNTER, FAILED
    COUNTER, FAILED = counter, failed


def batches(prepared, rows, size):
    """Cat `rows` (da sap theo thoi gian) thanh cac BATCH, moi batch chi chua 1 partition va <= size dong."""
    cur_day, b = None, None
    for r in rows:
        if b is None or r[1] != cur_day or len(b) >= size:
            if b is not None:
                yield b
            b = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=ConsistencyLevel.LOCAL_ONE)
            cur_day = r[1]
        b.add(prepared, r)
    if b is not None:
        yield b


def run_batches(session, stmts, concurrency, retries=3):
    """Chay song song; batch loi (qua tai, timeout) thi thu lai toi da `retries` lan. Tra ve (dong_ok, dong_loi)."""
    pending, ok_rows = stmts, 0
    for attempt in range(retries + 1):
        res = execute_concurrent(session, pending, concurrency=concurrency, raise_on_first_error=False)
        nxt = []
        for (stmt, _), (ok, _r) in zip(pending, res):
            if ok:
                ok_rows += len(stmt)
            else:
                nxt.append((stmt, None))
        if not nxt:
            return ok_rows, 0
        pending = nxt
        time.sleep(1.0 + attempt)
    return ok_rows, sum(len(s) for s, _ in pending)


def worker_main(task):
    cfg, lo, sizes = task["cfg"], task["lo"], task["sizes"]
    s = get_session()
    s.default_timeout = 60
    ins = s.prepare(INSERT_MSG)
    p_conv, p_bc, p_ba = s.prepare(INS_CONV), s.prepare(INS_BY_CUST), s.prepare(INS_BY_AGENT)
    node, clock = node_clock(cfg["tag"])
    step = 100                                           # so cuoc chat xu ly moi dot
    for a in range(0, len(sizes), step):
        stmts, metas = [], []
        for off in range(a, min(a + step, len(sizes))):
            i = lo + off
            cid, customer, agent, rows, t_first, t_last = build_conv(cfg, i, sizes[off], node, clock)
            for b in batches(ins, rows, cfg["batch_rows"]):
                stmts.append((b, None))
            metas.append((cid, customer, agent, t_first, t_last))
        ok, bad = run_batches(s, stmts, cfg["concurrency"])
        if cfg["with_conv"]:
            c_args, bc_args, ba_args = [], [], []
            for cid, customer, agent, t_first, t_last in metas:
                tid = tuuid(t_first, node, clock)
                created = datetime.fromtimestamp(t_first, tz=timezone.utc).replace(tzinfo=None)
                closed = datetime.fromtimestamp(t_last, tz=timezone.utc).replace(tzinfo=None)
                c_args.append((cid, customer, agent, tid, created, keys.day_of(tid), closed))
                bc_args.append((customer, tid, cid))
                ba_args.append((agent, tid, cid, customer))
            for prep, args in ((p_conv, c_args), (p_bc, bc_args), (p_ba, ba_args)):
                execute_concurrent_with_args(s, prep, args, concurrency=cfg["concurrency"], raise_on_first_error=False)
        with COUNTER.get_lock():
            COUNTER.value += ok
        with FAILED.get_lock():
            FAILED.value += bad
    return True


def make_tasks(sizes, parts):
    n = len(sizes)
    per = max(1, -(-n // parts))
    return [(lo, sizes[lo:lo + per]) for lo in range(0, n, per)]


def fmt_eta(sec):
    return f"{int(sec // 60)}p{int(sec % 60):02d}s"


def cmd_load(a):
    s = get_session()
    have = {r.table_name for r in s.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s", (s.keyspace,))}
    shutdown()
    if "messages" not in have:
        sys.exit("Chua co bang messages. Chay truoc: python scripts/init_db.py")
    tag = a.tag or datetime.now().strftime("%m%d%H%M%S")
    end_ts = time.time()
    sizes = plan_sizes(tag, a.rows, a.conversations)
    cfg = {"tag": tag, "rows": a.rows, "convs": a.conversations, "days": a.days, "end_ts": end_ts,
           "demo_agent": a.demo_agent, "with_conv": not a.no_conversations, "batch_rows": a.batch_rows,
           "concurrency": a.concurrency}
    tasks = [{"cfg": cfg, "lo": lo, "sizes": sz} for lo, sz in make_tasks(sizes, max(1, a.workers) * 4)]
    counter, failed = mp.Value("q", 0), mp.Value("q", 0)
    print(f"Nap {a.rows:,} dong / {a.conversations:,} cuoc chat / {a.days} ngay | tag={tag} | {a.workers} tien trinh x {a.concurrency} batch dong thoi")
    t0 = time.time()
    if a.workers <= 1:
        _init(counter, failed)
        for t in tasks:
            worker_main(t)
            _progress(counter, failed, a.rows, t0)
    else:
        with mp.Pool(a.workers, initializer=_init, initargs=(counter, failed)) as pool:
            res = pool.map_async(worker_main, tasks)
            while not res.ready():
                _progress(counter, failed, a.rows, t0)
                time.sleep(1.0)
            res.get()
    dt = time.time() - t0
    _progress(counter, failed, a.rows, t0, final=True)
    manifest = {"tag": tag, "rows_requested": a.rows, "rows_written": counter.value, "rows_failed": failed.value,
                "conversations": a.conversations, "days": a.days, "end_ts": end_ts, "demo_agent": a.demo_agent,
                "with_conversations": cfg["with_conv"], "workers": a.workers, "concurrency": a.concurrency,
                "batch_rows": a.batch_rows, "seconds": round(dt, 2), "rows_per_sec": round(counter.value / dt) if dt else 0,
                "finished_utc": datetime.now(timezone.utc).isoformat()}
    DOCS.mkdir(exist_ok=True)
    (DOCS / f"seed_manifest_{tag}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nXONG: ghi {counter.value:,}/{a.rows:,} dong trong {dt:.1f} giay = {manifest['rows_per_sec']:,} dong/giay"
          f"{'  | LOI: ' + format(failed.value, ',') + ' dong (xem log Cassandra)' if failed.value else ''}")
    print(f"Da luu {DOCS / ('seed_manifest_' + tag + '.json')}  (dung de xoa sach: python scripts/bulk_seed.py --purge)")
    print("Buoc tiep: python scripts/scale_report.py   (dem dong bang token range, do do tre doc o quy mo lon)")
    return 0 if not failed.value else 1


def _progress(counter, failed, total, t0, final=False):
    done = counter.value
    el = max(time.time() - t0, 1e-9)
    rate = done / el
    eta = (total - done) / rate if rate > 0 else 0
    print(f"\r  {done:>10,}/{total:,} ({100 * done / total:5.1f}%)  {rate:>9,.0f} dong/giay  con ~{fmt_eta(eta)}  loi={failed.value}   ",
          end="\n" if final else "", flush=True)


def purge_one(s, m):
    tag, convs, rows, days, end_ts = m["tag"], m["conversations"], m["rows_requested"], m["days"], m["end_ts"]
    sizes = plan_sizes(tag, rows, convs)
    node, clock = node_clock(tag)
    parts, conv_ids, customers, agents, demo_rows = set(), [], set(), set(), []
    for i in range(convs):
        cid, customer, agent = identity(tag, i, convs, m.get("demo_agent"))
        ts = conv_timestamps(tag, i, sizes[i], end_ts, days)
        for t in ts:
            parts.add((cid, keys.day_of(tuuid(t, node, clock))))
        conv_ids.append(cid)
        customers.add(customer)
        if m.get("demo_agent") and agent == m["demo_agent"]:
            demo_rows.append((agent, tuuid(ts[0], node, clock)))
        else:
            agents.add(agent)
    print(f"  tag={tag}: xoa {len(parts):,} partition tin nhan, {len(conv_ids):,} cuoc chat ...")
    ops = [("DELETE FROM messages WHERE conversation_id = ? AND day = ?", sorted(parts, key=str)),
           ("DELETE FROM conversations WHERE conversation_id = ?", [(c,) for c in conv_ids]),
           ("DELETE FROM conversations_by_customer WHERE customer_id = ?", [(c,) for c in sorted(customers)]),
           ("DELETE FROM conversations_by_agent WHERE agent_id = ?", [(x,) for x in sorted(agents)]),
           ("DELETE FROM conversations_by_agent WHERE agent_id = ? AND created_at = ?", demo_rows)]
    for cql, args in ops:
        if args:
            execute_concurrent_with_args(s, s.prepare(cql), args, concurrency=128, raise_on_first_error=False)


def cmd_purge(a):
    files = sorted(DOCS.glob("seed_manifest_*.json"))
    if a.tag:
        files = [f for f in files if f.stem == f"seed_manifest_{a.tag}"]
    if not files:
        sys.exit("Khong co file docs/seed_manifest_*.json nao de xoa.")
    s = get_session()
    s.default_timeout = 60
    for f in files:
        purge_one(s, json.loads(f.read_text(encoding="utf-8")))
        f.unlink()
    shutdown()
    print("Da xoa sach du lieu da nap. (Cassandra giai phong o dia sau khi compaction; de thay ngay: docker exec livechat-cassandra nodetool compact livechat messages)")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Nap du lieu lon vao bang messages cua Cassandra")
    ap.add_argument("--rows", type=int, default=1_000_000, help="tong so dong tin nhan (mac dinh 1.000.000)")
    ap.add_argument("--conversations", type=int, default=20_000, help="so cuoc chat (moi cuoc chat nhieu partition theo ngay)")
    ap.add_argument("--days", type=int, default=30, help="tin nhan trai tren bao nhieu ngay gan day (>= 3)")
    ap.add_argument("--workers", type=int, default=2, help="so tien trinh ghi song song (1 = chay trong tien trinh nay)")
    ap.add_argument("--concurrency", type=int, default=128, help="so batch dang bay dong thoi moi tien trinh")
    ap.add_argument("--batch-rows", type=int, default=20, help="so dong toi da moi batch (giu < 5KB/batch)")
    ap.add_argument("--demo-agent", default=None, help="vd nv1: gan ~2%% cuoc chat cho nhan vien nay de xem tren dashboard")
    ap.add_argument("--no-conversations", action="store_true", help="chi nap bang messages, khong tao cuoc chat/ho so")
    ap.add_argument("--tag", default=None, help="nhan cua dot nap (mac dinh theo gio)")
    ap.add_argument("--purge", action="store_true", help="XOA du lieu da nap")
    a = ap.parse_args()
    if a.purge:
        sys.exit(cmd_purge(a))
    if a.days < 3:
        sys.exit("--days phai >= 3")
    if a.rows < a.conversations:
        sys.exit("--rows phai >= --conversations")
    sys.exit(cmd_load(a))


if __name__ == "__main__":
    mp.freeze_support()
    main()