CREATE TABLE IF NOT EXISTS sessions(
  session_id varchar primary key,
  device_type varchar,
  user_agent varchar,
  ip varchar,
  user_id varchar
);

CREATE TABLE IF NOT EXISTS events(
  event_id UUID primary key,
  event_type varchar,
  created_at UInt64,
  received_at UInt64,
  session_id varchar,
  url varchar,
  referrer varchar,
  event_title varchar,
  element_id varchar,
  x int,
  y int
);

CREATE TABLE IF NOT EXISTS ctr_mart(
  time UInt64,
  ctr float
);

CREATE TABLE IF NOT EXISTS ctr_element_mart(
  time UInt64,
  element_id varchar,
  ctr float
);

CREATE TABLE IF NOT EXISTS user_activity_mart(
  time UInt64,
  user_id varchar
);

CREATE TABLE IF NOT EXISTS session_duration_mart(
  time UInt64,
  session_id varchar,
  duration float
);
