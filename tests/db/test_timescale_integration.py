import uuid
from datetime import datetime, timezone

import psycopg
import pytest

from db.timescale import insert_fx_rate, insert_macro_observation, make_connection

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
        "replayed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
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