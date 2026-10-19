-- Livechat schema. Nguyen tac Cassandra: thiet ke bang theo TRUY VAN, khong theo thuc the.
-- Single node (do an) nen RF=1. Production: NetworkTopologyStrategy, RF=3.
CREATE KEYSPACE IF NOT EXISTS livechat
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1: Lay tin nhan cua 1 cuoc chat, moi nhat truoc, phan trang.
-- Partition = (conversation_id, day): moi cuoc chat moi ngay la 1 partition,
-- tranh partition phinh vo han voi cuoc chat dai ngay.
CREATE TABLE IF NOT EXISTS livechat.messages (
  conversation_id uuid,
  day             date,
  sent_at         timeuuid,
  sender_type     text,
  sender_id       text,
  content         text,
  PRIMARY KEY ((conversation_id, day), sent_at)
) WITH CLUSTERING ORDER BY (sent_at DESC);

-- Q2: Lay thong tin 1 cuoc chat theo id (trang thai, nhan vien phu trach, ngay bat dau).
-- created_day dung de biet phai duyet cac partition ngay nao cua bang messages.
CREATE TABLE IF NOT EXISTS livechat.conversations (
  conversation_id uuid PRIMARY KEY,
  customer_id     text,
  agent_id        text,
  status          text,
  created_tid     timeuuid,
  created_at      timestamp,
  created_day     date,
  closed_at       timestamp
);

-- Q3: Lich su cac cuoc chat cua 1 khach, moi nhat truoc.
CREATE TABLE IF NOT EXISTS livechat.conversations_by_customer (
  customer_id     text,
  created_at      timeuuid,
  conversation_id uuid,
  status          text,
  PRIMARY KEY ((customer_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- Q4: Lich su cac cuoc chat 1 nhan vien da xu ly, moi nhat truoc.
CREATE TABLE IF NOT EXISTS livechat.conversations_by_agent (
  agent_id        text,
  created_at      timeuuid,
  conversation_id uuid,
  customer_id     text,
  PRIMARY KEY ((agent_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);