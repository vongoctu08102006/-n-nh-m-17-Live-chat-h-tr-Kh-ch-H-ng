import os
import uuid
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    cassandra_hosts: tuple = tuple(os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(","))
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "livechat")
    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    recent_cache_size: int = int(os.getenv("RECENT_CACHE_SIZE", "50"))
    # Ten server: dat SERVER_ID=A / B khi chay nhieu server de phan biet trong demo
    server_id: str = os.getenv("SERVER_ID") or uuid.uuid4().hex[:6]
    # Ma nhan vien: de TRONG = che do dev (mo het). Dat gia tri = bat buoc dung cho moi thu danh rieng cho nhan vien.
    agent_key: str = os.getenv("AGENT_KEY", "")

settings = Settings()