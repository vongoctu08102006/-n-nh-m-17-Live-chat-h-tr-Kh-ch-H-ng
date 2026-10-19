"""Ap schema.cql vao Cassandra (idempotent: chay lai nhieu lan khong sao)."""
import re, sys, time
from pathlib import Path
from cassandra.cluster import Cluster
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.config import settings

SCHEMA = Path(__file__).resolve().parents[1] / "db" / "schema.cql"


def statements():
    text = "\n".join(
        l for l in SCHEMA.read_text(encoding="utf-8").splitlines()
        if not l.strip().startswith("--")
    )
    return [s.strip() for s in text.split(";") if s.strip()]


def main():
    for attempt in range(30):
        try:
            cluster = Cluster(list(settings.cassandra_hosts), port=settings.cassandra_port)
            session = cluster.connect()
            break
        except Exception as e:
            print(f"[{attempt+1}/30] Cassandra chua san sang: {e.__class__.__name__}. Doi 5s...")
            time.sleep(5)
    else:
        sys.exit("Khong ket noi duoc Cassandra. Kiem tra: docker compose ps")
    for stmt in statements():
        session.execute(stmt)
        print("OK:", re.sub(r"\s+", " ", stmt)[:80])
    cluster.shutdown()
    print("Schema da ap dung xong.")


if __name__ == "__main__":
    main()