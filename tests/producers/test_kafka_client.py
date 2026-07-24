import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from producers.kafka_client import make_producer, delivery_report, producer_event

class TestMakeProducer:
    @patch("producers.kafka_client.Producer")
    @patch("producers.kafka_client.KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    def test_uses_configured_bootstrap_servers(self, mock_producer_cls):
        make_producer()
        mock_producer_cls.assert_called_once_with({"bootstrap.servers": "localhost:19092"})
        
class TestDeliveryReport:
    def test_logs_error_on_failed_delivery(self, caplog):
        msg = MagicMock()
        msg.topic.return_value = "fx-rates"
        err = MagicMock()
        
        with caplog.at_level(logging.ERROR):
            delivery_report(err, msg)
        
        assert "delivery failed" in caplog.text
        assert "fx-rates" in caplog.text
        
    def test_logs_debug_on_successful_delivery(self, caplog):
        msg = MagicMock()
        msg.topic.return_value = "fx-rates"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        
        with caplog.at_level(logging.DEBUG, logger="producers.kafka_client"):
            delivery_report(None, msg)
            
        assert "delivered to" in caplog.text
        assert "fx-rates" in caplog.text
        
class TestProducerEvent:
    def test_produces_json_encoded_event_with_callback(self):
        mock_producer = MagicMock()
        event = {"symbol": "EURUSD", "rate": 1.09}
        
        producer_event(mock_producer, "fx-rates", event)
        
        mock_producer.produce.assert_called_once()
        _, kwargs = mock_producer.produce.call_args
        
        assert mock_producer.produce.call_args[0][0] == "fx-rates"
        assert json.loads(kwargs["value"].decode("utf-8")) == event
        assert kwargs["callback"] is delivery_report
    
    def test_polls_after_produce(self):
        mock_producer = MagicMock()
        
        producer_event(mock_producer, "fx-rates", {"a":1})
        mock_producer.poll.assert_called_once_with(0)
        
        