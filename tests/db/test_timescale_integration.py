import uuid
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
import pytest

from db.timescale import insert_fx_anomaly, insert_fx_rate, insert_macro_observation, make_connection

pytestmark = pytest.mark.integration

@pytest.fixture
def conn():
    try:
        connection = make_connection()
    except psycopg.OperationalError as exc:
        pytest.skip(f"TimescaleDB not reachable at configured TIMESCALE_DSN: {exc}")
    yield connection
    connection.rollback()
    connection.close()
    
def _fx_rate_event(**overrides) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "pair": "EURUSD",
        "rate": "1.0912",
        "source": "test",
        "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 1, 1, tzinfo=timezone.utc)
    }
    event.update(overrides)
    return event

def _macro_observation_event(**overrides):
    event = {
        "event_id": str(uuid.uuid4()),
        "series_id": "CPIAUCSL",
        "value": "3.1",
        "source": "test",
        "period": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    event.update(overrides)
    return event

def _fx_anomaly_event(**overrides) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "pair": "EURUSD",
        "rate": "1.1500",
        "z_score": "3.4",
        "window_start": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "window_end": datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc),
    }
    event.update(overrides)
    return event

class TestInsertFXRate:
    def test_inserts_a_new_row(self, conn):
        event = _fx_rate_event()
        
        inserted = insert_fx_rate(conn, event)
        
        assert inserted is True
        row = conn.execute(
            "SELECT pair, source FROM fx_rates WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert row == ("EURUSD", "test")
    
    def test_duplicate_event_id_is_ignored(self, conn):
        event = _fx_rate_event()
        insert_fx_rate(conn, event)

        inserted_again = insert_fx_rate(conn, event)

        assert inserted_again is False
        rows = conn.execute(
            "SELECT count(*) FROM fx_rates WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert rows == (1,)
        
class TestInsertMacroObservation:
    
    def test_inserts_a_new_row(self, conn):
        event = _macro_observation_event()

        inserted = insert_macro_observation(conn, event)

        assert inserted is True
        row = conn.execute(
            "SELECT series_id, source FROM macro_history WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert row == ("CPIAUCSL", "test")
        
    def test_duplicate_event_id_is_ignored(self, conn):
        event = _macro_observation_event()
        insert_macro_observation(conn, event)

        inserted_again = insert_macro_observation(conn, event)

        assert inserted_again is False
        rows = conn.execute(
            "SELECT count(*) FROM macro_history WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert rows == (1,)
        
class TestInsertFXAnomaly:
    def test_inserts_a_new_row(self, conn):
        event = _fx_anomaly_event()

        inserted = insert_fx_anomaly(conn, event)

        assert inserted is True
        row = conn.execute(
            "SELECT pair, z_score FROM fx_anomalies WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert row == ("EURUSD", Decimal("3.4"))

    def test_duplicate_event_id_is_ignored(self, conn):
        event = _fx_anomaly_event()
        insert_fx_anomaly(conn, event)

        inserted_again = insert_fx_anomaly(conn, event)

        assert inserted_again is False
        rows = conn.execute(
            "SELECT count(*) FROM fx_anomalies WHERE event_id = %s",
            [event["event_id"]],
        ).fetchone()
        assert rows == (1,)