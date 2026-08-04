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
            
    class TestRun:
        @patch("consumers.sink_consumer.make_connection")
        @patch("consumers.sink_consumer.make_consumer")
        @patch("consumers.sink_consumer.handle_message")
        def test_commits_offset_after_successful_processing(
            self, mock_handle, mock_make_consumer, mock_make_connection
        ):
            consumer = MagicMock()
            msg = MagicMock()
            msg.error.return_value = None
            consumer.poll.side_effect = [msg, KeyboardInterrupt()]
            mock_make_consumer.return_value = consumer

            run()

            mock_handle.assert_called_once_with(mock_make_connection.return_value, msg)
            consumer.commit.assert_called_once_with(msg)
            
        @patch("consumers.sink_consumer.make_connection")
        @patch("consumers.sink_consumer.make_consumer")
        @patch("consumers.sink_consumer.handle_message")
        def test_skips_when_poll_returns_none(
            self, mock_handle, mock_make_consumer, mock_make_connection
        ):
            consumer = MagicMock()
            consumer.poll.side_effect = [None, KeyboardInterrupt()]
            mock_make_consumer.return_value = consumer

            run()

            mock_handle.assert_not_called()
            consumer.commit.assert_not_called()
            
        @patch("consumers.sink_consumer.make_connection")
        @patch("consumers.sink_consumer.make_consumer")
        @patch("consumers.sink_consumer.handle_message")
        def test_logs_and_continues_on_kafka_error(
            self, mock_handle, mock_make_consumer, mock_make_connection, caplog
        ):
            consumer = MagicMock()
            error_msg = MagicMock()
            error_msg.error.return_value = "some kafka error"
            consumer.poll.side_effect = [error_msg, KeyboardInterrupt()]
            mock_make_consumer.return_value = consumer

            with caplog.at_level(logging.ERROR, logger="consumers.sink_consumer"):
                run()

            mock_handle.assert_not_called()
            assert "kafka error" in caplog.text
            
        @patch("consumers.sink_consumer.make_connection")
        @patch("consumers.sink_consumer.make_consumer")
        @patch("consumers.sink_consumer.handle_message")
        def test_rolls_back_and_does_not_commit_on_handler_failure(
            self, mock_handle, mock_make_consumer, mock_make_connection, caplog
        ):
            consumer = MagicMock()
            msg = MagicMock()
            msg.error.return_value = None
            msg.topic.return_value = "fx.rates"
            msg.partition.return_value = 0
            msg.offset.return_value = 42
            consumer.poll.side_effect = [msg, KeyboardInterrupt()]
            mock_make_consumer.return_value = consumer
            mock_handle.side_effect = Exception("boom")
            conn = mock_make_connection.return_value

            with caplog.at_level(logging.ERROR, logger="consumers.sink_consumer"):
                run()

            conn.rollback.assert_called_once()
            consumer.commit.assert_not_called()
            assert "failed to process" in caplog.text
            
        @patch("consumers.sink_consumer.make_connection")
        @patch("consumers.sink_consumer.make_consumer")
        def test_closes_consumer_and_connection_on_shutdown(
            self, mock_make_consumer, mock_make_connection
        ):
            consumer = MagicMock()
            consumer.poll.side_effect = KeyboardInterrupt()
            mock_make_consumer.return_value = consumer
            conn = mock_make_connection.return_value

            run()

            consumer.close.assert_called_once()
            conn.close.assert_called_once()