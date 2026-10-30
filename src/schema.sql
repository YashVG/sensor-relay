CREATE TABLE IF NOT EXISTS streams (
    device_id VARCHAR NOT NULL,
    stream_id VARCHAR NOT NULL,
    next_seq BIGINT NOT NULL,
    PRIMARY KEY (device_id, stream_id)
);
CREATE TABLE IF NOT EXISTS telemetry (
    device_id VARCHAR NOT NULL,
    stream_id VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    observed_ms BIGINT NOT NULL,
    temperature_mc BIGINT NOT NULL,
    battery_bps INTEGER NOT NULL,
    PRIMARY KEY (device_id, stream_id, seq)
);
CREATE TABLE IF NOT EXISTS receipts (
    device_id VARCHAR NOT NULL,
    stream_id VARCHAR NOT NULL,
    first_seq BIGINT NOT NULL,
    digest VARCHAR NOT NULL,
    sample_count BIGINT NOT NULL,
    PRIMARY KEY (device_id, stream_id, first_seq)
);
