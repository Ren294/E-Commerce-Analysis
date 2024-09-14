CREATE KEYSPACE IF NOT EXISTS log_data WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE log_data;
CREATE TABLE IF NOT EXISTS logs (
  ip_address TEXT,
  timestamp TEXT,
  request_path TEXT,
  status_code TEXT,
  bytes_sent TEXT,
  PRIMARY KEY (ip_address, timestamp, request_path)
);

CREATE TABLE IF NOT EXISTS clicks (
    timestamp TEXT,
    log_level TEXT,
    user_id TEXT,
    session_id TEXT,
    event_type TEXT,
    product_id TEXT,
    amount TEXT,
    outcome TEXT,
    PRIMARY KEY (timestamp, user_id, session_id)
);
