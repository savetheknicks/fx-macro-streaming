import uuid
from datetime import datetime
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
import requests

from producers.fred_replay import fetch_series, load_or_fetch_history, run

def make_response(json_data, raise_error=None):
    mock_response = MagicMock()
    mock_response.json.return_value = json_data
    mock_response.raise_for_status.side_effect = raise_error
    return mock_response

def fred_response(observations):
    return make_response({"observations": observations})

def stop_after_one_cycle(mock_producer):
    state = {"calls": 0}
    
    def _flush_side_effect(timeout):
        state["calls"] += 1
        if state["calls"] == 1:
            raise KeyboardInterrupt
        
    mock_producer.flush.side_effect = _flush_side_effect
    
class TestFetchSeries:
    @patch("producers.fred_replay.requests.get")
    def test_maps_valid_observations_to_expected_schema(self, mock_get):
        mock_get.return_value = fred_response([
            {"date": "2026-01-01", "value": "4.25"},
            {"date": "2026-02-01", "value": "4.30"},
        ])
        
        result = fetch_series("DGS10")
        
        assert result == [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
            {"series_id": "DGS10", "period": "2026-02-01", "value": "4.30"},
        ]
        
    @patch("producers.fred_replay.requests.get")
    def test_filters_out_missing_value_observations(self, mock_get):
        mock_get.return_value = fred_response([
            {"date": "2026-01-01", "value": "4.25"},
            {"date": "2026-02-01", "value": "."},
        ])
        
        result = fetch_series("DGS10")
        
        assert [obs["period"] for obs in result] == ["2026-01-01"]
        
    @patch("producers.fred_replay.requests.get")
    def test_all_missing_values_returns_empty_list(self, mock_get):
        mock_get.return_value = fred_response([
            {"date": "2026-01-01", "value": "."},
        ])

        assert fetch_series("DGS10") == []
        
    @patch("producers.fred_replay.requests.get")
    def test_sends_correct_request_params(self, mock_get):
        mock_get.return_value = fred_response([])
        
        fetch_series("DGS10")
        
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["series_id"] == "DGS10"
        assert kwargs["params"]["file_type"] == "json"
        assert kwargs["timeout"] == 10
        
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.requests.get")
    def test_recovers_from_transient_failure_within_retry_budget(self, mock_get, _sleep):
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection error"),
            fred_response([{"date": "2026-01-01", "value": "4.25"}]),
        ]
        
        result = fetch_series("DGS10")
        
        assert result == [{"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"}]
        
        
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.requests.get")
    def test_gives_up_once_retry_budget_exhausted(self, mock_get, _sleep):
        mock_get.side_effect = requests.exceptions.ConnectionError("connection error")
        
        with pytest.raises(requests.exceptions.ConnectionError):
            fetch_series("DGS10")
            
        assert mock_get.call_count == 5
        
    @patch("producers.fred_replay.requests.get")
    def test_raises_when_observations_field_missing(self, mock_get):
        mock_get.return_value = make_response({"error_code": 400, "error_message": "Bad Request"})
        
        with pytest.raises(KeyError):
            fetch_series("DGS10")
        