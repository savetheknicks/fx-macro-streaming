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