from cassandra.cluster import Cluster
from .config import settings

# Dung reactor MAC DINH (giong scripts/init_db.py) de moi noi hanh xu nhu nhau.
# Python 3.12 can thu vien pyasyncore (da co trong requirements.txt).
_cluster = None
_session = None


def get_session():
    """Session dung chung (thread-safe). Goi trong code async bang asyncio.to_thread()."""
    global _cluster, _session
    if _session is None:
        _cluster = Cluster(list(settings.cassandra_hosts), port=settings.cassandra_port)
        _session = _cluster.connect()
        _session.set_keyspace(settings.keyspace)
    return _session


def shutdown():
    global _cluster, _session
    if _cluster:
        _cluster.shutdown()
    _cluster = _session = None