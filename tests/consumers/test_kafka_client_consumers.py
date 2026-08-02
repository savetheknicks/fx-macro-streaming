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