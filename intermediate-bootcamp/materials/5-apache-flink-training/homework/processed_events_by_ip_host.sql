-- Create processed_events_by_ip_host table
CREATE TABLE processed_events_by_ip_host (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_hits BIGINT
);
