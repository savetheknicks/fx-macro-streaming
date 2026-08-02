from unittest.mock import patch, MagicMock

import pytest

from consumers.kafka_client import make_consumer

class TestMakeConsumer:
    @patch("consumers.kafka_client.Consumer")
    @patch("consumers.kafka_client.KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    @patch("consumers.kafka_client.SINK_CONSUMER_GROUP_ID", "test-group")
    def test_uses_configured_bootstrap_servers_and_group_id(self, mock_consumer_cls):
        make_consumer()
        
        mock_consumer_cls.assert_called_once_with({
            "bootstrap.servers": "localhost:19092",
            "group.id": "test-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
        
    @patch("consumers.kafka_client.Consumer")
    def test_disables_auto_commit(self, mock_consumer_cls):
        make_consumer()
        
        _, kwargs = mock_consumer_cls.call_args
        config = mock_consumer_cls.call_args[0][0]
        assert config["enable.auto.commit"] is False
        
    @patch("consumers.kafka_client.Consumer")
    def test_sets_auto_offset_reset_to_earliest(self, mock_consumer_cls):
        make_consumer()
        
        config = mock_consumer_cls.call_args[0][0]
        assert config["auto.offset.reset"] == "earliest"
        
    @patch("consumers.kafka_client.Consumer")
    @patch("consumers.kafka_client.SINK_TOPICS", ["fx.rates", "macro.history"])
    def test_subscribes_to_sink_topics(self, mock_consumer_cls):
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        
        result = make_consumer()
        
        mock_consumer.subscribe.assert_called_once_with(["fx.rates", "macro.history"])
        assert result is mock_consumer