CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS fx_rates(
    time TIMESTAMPTZ NOT NULL,
    event_id UUID NOT NULL,
    pair TEXT NOT NULL,
    rate NUMERIC NOT NULL,
    source TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (time, event_id)
);

SELECT create_hypertable('fx_rates', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_time ON fx_rates(pair, time DESC);

CREATE TABLE IF NOT EXISTS macro_history(
    time TIMESTAMPTZ NOT NULL,
    event_id UUID NOT NULL,
    series_id TEXT NOT NULL,
    value NUMERIC NOT NULL,
    source TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    UNIQUE (time, event_id)
);

SELECT create_hypertable('macro_history', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_macro_history_series_time ON macro_history(series_id, time DESC);

CREATE TABLE IF NOT EXISTS fx_anomalies(
    time TIMESTAMPTZ NOT NULL,
    event_id UUID NOT NULL,
    pair TEXT NOT NULL,
    rate NUMERIC NOT NULL,
    z_score NUMERIC NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    UNIQUE (time, event_id)
);

SELECT create_hypertable('fx_anomalies', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_fx_anomalies_pair_time ON fx_anomalies(pair, time DESC);