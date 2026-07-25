import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from producers.fx_poller import RateLimited, fetch_rate, run

def make_response(json_data, raise_error=None):
    mock_response = MagicMock()
    mock_response.json.return_value = json_data
    mock_response.raise_for_status.side_effect = raise_error
    return mock_response

def alpha_vantage_response(rate: str, refreshed: str = "2026-07-25 12:00:01"):
    return make_response({
        "Realtime Currency Exchange Rate": {
            "5. Exchange Rate": rate,
            "6. Last Refreshed": refreshed,
        }
    })
    
def stop_after_one_cycle(mock_producer):
    state = {"calls": 0}
    
    def _flush_side_effect(timeout):
        state["calls"] += 1
        if state["calls"] == 1:
            raise KeyboardInterrupt
        
    mock_producer.flush.side_effect = _flush_side_effect
    
    
class TestFetchRate:
    @patch("producers.fx_poller.requests.get")

    def test_maps_a_valid_quote_to_the_event_schema(self, mock_get):
        mock_get.return_value = alpha_vantage_response("0.9123")
        
        event = fetch_rate("USD", "EUR")
        
        assert event["pair"] == "USD/EUR"
        assert event["rate"] == 0.9123
        assert event["source"] == "alpha_vantage"
        assert event["observed_at"] == "2026-07-25 12:00:01"
        assert uuid.UUID(event["event_id"])
        assert datetime.fromisoformat(event["ingested_at"]).tzinfo is not None
        
    @patch("time.sleep", return_value=None)
    @patch("producers.fx_poller.requests.get")
    def test_raises_rate_limited_when_note_field_present(self, mock_get, _sleep):
        mock_get.return_value = make_response({"Note": "Thank you for use Alpha Vantage!"})

        with pytest.raises(RateLimited):
            fetch_rate("USD", "EUR")

    @patch("time.sleep", return_value=None)
    @patch("producers.fx_poller.requests.get")
    def test_raises_rate_limited_when_information_field_present(self, mock_get, _sleep):
        mock_get.return_value = make_response({"Information": "rate limit reached"})

        with pytest.raises(RateLimited):
            fetch_rate("USD", "EUR")
            
    @patch("time.sleep", return_value=None)
    @patch("producers.fx_poller.requests.get")
    def test_rejects_a_payload_missing_the_expected_quote(self, mock_get, _sleep):
        mock_get.return_value = make_response({"unexpected": "shape"})

        with pytest.raises(ValueError, match="unexpected response shape"):
            fetch_rate("USD", "EUR")
            
    @patch("time.sleep", return_value=None)
    @patch("producers.fx_poller.requests.get")
    def test_recovers_from_a_transient_failure_within_retry_budget(self, mock_get, _sleep):
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection error"),
            alpha_vantage_response("0.9123"),
        ]
        
        event = fetch_rate("USD", "EUR")
        
        assert event["pair"] == "USD/EUR"
        assert event["rate"] == 0.9123
    