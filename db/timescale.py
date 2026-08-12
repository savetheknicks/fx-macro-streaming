import psycopg

from db.config import TIMESCALE_DSN

INSERT_FX_RATE = """
    INSERT INTO fx_rates (time, event_id, pair, rate, source, ingested_at)
    VALUES (%(observed_at)s, %(event_id)s, %(pair)s, %(rate)s, %(source)s, %(ingested_at)s)
    ON CONFLICT (time, event_id) DO NOTHING
"""

INSERT_MACRO_OBSERVATION = """
    INSERT INTO macro_history (time, event_id, series_id, value, source, ingested_at)
    VALUES (%(period)s, %(event_id)s, %(series_id)s, %(value)s, %(source)s, %(ingested_at)s)
    ON CONFLICT (time, event_id) DO NOTHING
"""

INSERT_FX_ANOMALY = """
    INSERT INTO fx_anomalies (time, event_id, pair, rate, z_score, window_start, window_end)
    VALUES (%(window_end)s, %(event_id)s, %(pair)s, %(rate)s, %(z_score)s, %(window_start)s, %(window_end)s)
    ON CONFLICT (time, event_id) DO NOTHING
"""

def make_connection() -> psycopg.Connection:
    conn = psycopg.connect(TIMESCALE_DSN, autocommit=False)
    conn.execute("SET TIME ZONE 'UTC'")
    return conn

def insert_fx_rate(conn: psycopg.Connection, event: dict) -> bool:
    cur = conn.execute(INSERT_FX_RATE, event)
    return cur.rowcount > 0


def insert_macro_observation(conn: psycopg.Connection, event: dict) -> bool:
    cur = conn.execute(INSERT_MACRO_OBSERVATION, event)
    return cur.rowcount > 0

def insert_fx_anomaly(conn: psycopg.Connection, event: dict) -> bool:
    cur = conn.execute(INSERT_FX_ANOMALY, event)
    return cur.rowcount > 0