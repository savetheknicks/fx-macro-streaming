import json
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

from stream_processor import processor
from stream_processor.processor import handle_message, run
from stream_processor.windowing import WindowSnapshot

def _make_message(value):
    msg = MagicMock()
    msg.value.return_value = json.dumps(value).encode("utf-8")
    return msg

def _snapshot(**overrides):
    defaults = dict(
        rolling_average=1.0, sample_count=4,
        window_start=datetime(2026, 1, 1, 12, 0, 0),
        window_end=datetime(2026, 1, 1, 12, 5, 0),
        delta=0.01, z_score=0.4, is_anomaly=False,
    )
    return WindowSnapshot(**{**defaults, **overrides})

class TestHandleMessage:
    def test_updates_the_window_for_the_events_pair(self):
        windows = {}
        producer = MagicMock()
        event = {"event_id": "abc", "pair": "USD/EUR", "rate": 0.91, "observed_at": "2026-01-01T12:00:00"}

        handle_message(windows, producer, _make_message(event))

        assert "USD/EUR" in windows
        
    def test_publishes_to_the_anomaly_topic_when_flagged(self, monkeypatch):
        windows = {}
        producer = MagicMock()
        fake_window = MagicMock()
        fake_window.add.return_value = _snapshot(delta=0.5, z_score=4.2, is_anomaly=True)
        monkeypatch.setattr(processor, "PairWindow", lambda *a, **k: fake_window)
        event = {"event_id": "abc", "pair": "USD/EUR", "rate": 1.5, "observed_at": "2026-01-01T12:05:00"}

        with patch("stream_processor.processor.producer_event") as mock_produce:
            handle_message(windows, producer, _make_message(event))

        mock_produce.assert_called_once()
        args = mock_produce.call_args.args
        assert args[1] == "fx.anomalies"
        assert args[2] == "USD/EUR"
        
    def test_does_not_publish_when_not_anomalous(self, monkeypatch):
        windows = {}
        producer = MagicMock()
        fake_window = MagicMock()
        fake_window.add.return_value = _snapshot()
        monkeypatch.setattr(processor, "PairWindow", lambda *a, **k: fake_window)
        event = {"event_id": "abc", "pair": "USD/EUR", "rate": 1.01, "observed_at": "2026-01-01T12:05:00"}

        with patch("stream_processor.processor.producer_event") as mock_produce:
            handle_message(windows, producer, _make_message(event))

        mock_produce.assert_not_called()
        
class TestRun:
    @patch("stream_processor.processor.make_producer")
    @patch("stream_processor.processor.make_consumer")
    @patch("stream_processor.processor.handle_message")
    def test_commits_offset_after_successful_processing(self, mock_handle, mock_make_consumer, mock_make_producer):
        consumer = MagicMock()
        msg = MagicMock()
        msg.error.return_value = None
        consumer.poll.side_effect = [msg, KeyboardInterrupt()]
        mock_make_consumer.return_value = consumer

        run()

        consumer.commit.assert_called_once_with(msg)
    
    @patch("stream_processor.processor.make_producer")
    @patch("stream_processor.processor.make_consumer")
    @patch("stream_processor.processor.handle_message")
    def test_does_not_commit_on_handler_failure(self, mock_handle, mock_make_consumer, mock_make_producer, caplog):
        consumer = MagicMock()
        msg = MagicMock()
        msg.error.return_value = None
        msg.topic.return_value = "fx.rates"
        msg.partition.return_value = 0
        msg.offset.return_value = 7
        consumer.poll.side_effect = [msg, KeyboardInterrupt()]
        mock_make_consumer.return_value = consumer
        mock_handle.side_effect = Exception("boom")

        with caplog.at_level(logging.ERROR, logger="stream_processor.processor"):
            run()

        consumer.commit.assert_not_called()
        assert "failed to process" in caplog.text
        
    @patch("stream_processor.processor.make_producer")
    @patch("stream_processor.processor.make_consumer")
    def test_closes_consumer_and_flushes_producer_on_shutdown(self, mock_make_consumer, mock_make_producer):
        consumer = MagicMock()
        consumer.poll.side_effect = KeyboardInterrupt()
        mock_make_consumer.return_value = consumer
        producer = mock_make_producer.return_value

        run()

        consumer.close.assert_called_once()
        producer.flush.assert_called_once_with(10)