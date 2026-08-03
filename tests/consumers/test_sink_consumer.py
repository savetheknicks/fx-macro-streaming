import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from consumers import sink_consumer
from consumers.sink_consumer import handle_message, run

def _make_message(topic, value):
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.value.return_value = json.dumps(value).encode("utf-8")
    return msg

class TestHandleMessage:
    def test_dispatches_to_handler_for_topic(self, monkeypatch):
        conn = MagicMock()
        fake_handler = MagicMock(return_value=True)
        monkeypatch.setitem(sink_consumer.TOPIC_HANDLERS, "fx.rates", fake_handler)
        event = {"event_id": "abc", "pair": "EURUSD"}
        msg = _make_message("fx.rates", event)
        
        handle_message(conn, msg)
        
        fake_handler.assert_called_once_with(conn, event)
        
    def test_commits_after_successful_handling(self, monkeypatch):
        conn = MagicMock()
        monkeypatch.setitem(sink_consumer.TOPIC_HANDLERS, "fx.rates", MagicMock(return_value=True))
        msg = _make_message("fx.rates", {"event_id": "abc"})

        handle_message(conn, msg)

        conn.commit.assert_called_once()
        
    def test_logs_wrote_when_inserted(self, monkeypatch, caplog):
        conn = MagicMock()
        monkeypatch.setitem(sink_consumer.TOPIC_HANDLERS, "fx.rates", MagicMock(return_value=True))
        msg = _make_message("fx.rates", {"event_id": "abc"})

        with caplog.at_level(logging.INFO, logger="consumers.sink_consumer"):
            handle_message(conn, msg)

        assert "wrote" in caplog.text
        assert "abc" in caplog.text
        
    def test_logs_skipped_when_duplicate(self, monkeypatch, caplog):
        conn = MagicMock()
        monkeypatch.setitem(sink_consumer.TOPIC_HANDLERS, "fx.rates", MagicMock(return_value=False))
        msg = _make_message("fx.rates", {"event_id": "abc"})

        with caplog.at_level(logging.INFO, logger="consumers.sink_consumer"):
            handle_message(conn, msg)

        assert "skipped duplicate" in caplog.text
        assert "abc" in caplog.text
        
    def test_raises_for_unmapped_topic(self):
        conn = MagicMock()
        msg = _make_message("unknown.topic", {"event_id": "abc"})

        with pytest.raises(KeyError):
            handle_message(conn, msg)