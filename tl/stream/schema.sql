CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS report;
CREATE TABLE IF NOT EXISTS stream.activity (
    activity_id VARCHAR PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    customer VARCHAR,
    anonymous_customer_id VARCHAR,
    activity VARCHAR NOT NULL,
    feature_json JSON NOT NULL,
    revenue_impact DECIMAL(18,2),
    link VARCHAR,
    _recorded_at TIMESTAMPTZ NOT NULL CHECK (_recorded_at >= ts),
    _stream_position BIGINT NOT NULL UNIQUE,
    _source VARCHAR NOT NULL,
    _actor VARCHAR NOT NULL,
    _lane VARCHAR NOT NULL CHECK (_lane IN ('sim', 'dev')),
    _schema_hash VARCHAR NOT NULL,
    CHECK (customer IS NOT NULL OR anonymous_customer_id IS NOT NULL)
);
