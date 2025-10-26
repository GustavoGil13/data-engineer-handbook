-- Create processed_events_aggregated table
CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);
