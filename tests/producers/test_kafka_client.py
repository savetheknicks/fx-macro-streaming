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