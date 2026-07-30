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

class TestLoadOrFetchHistory:
    @patch("producers.fred_replay.json")
    @patch("producers.fred_replay.open", new_callable=mock_open)
    @patch("producers.fred_replay.os.path.exists", return_value=False)
    @patch("producers.fred_replay.fetch_series")
    @patch("producers.fred_replay.FRED_SERIES", ["DGS10", "DEXUSEU"])
    def test_fetches_and_merges_all_configured_series_when_no_cache(
        self, mock_fetch_series, _exists, _open, mock_json
    ):
        mock_fetch_series.side_effect = lambda series_id: {
            "DGS10": [{"series_id": "DGS10", "period": "2026-02-01", "value": "4.25"}],
            "DEXUSEU": [{"series_id": "DEXUSEU", "period": "2026-01-01", "value": "1.08"}],
        }[series_id]
        
        history = load_or_fetch_history()
        
        assert mock_fetch_series.call_args_list == [call("DGS10"), call("DEXUSEU")]
        assert [obs["period"] for obs in history] == ["2026-01-01", "2026-02-01"]
        
    @patch("producers.fred_replay.json")
    @patch("producers.fred_replay.open", new_callable=mock_open)
    @patch("producers.fred_replay.os.path.exists", return_value=False)
    @patch("producers.fred_replay.fetch_series")
    @patch("producers.fred_replay.FRED_SERIES", ["DGS10", "DEXUSEU"])
    def test_sorts_merged_history_by_period_across_series(
        self, mock_fetch_series, _exists, _open, mock_json
    ):
        mock_fetch_series.side_effect = lambda series_id: {
            "DGS10": [
                {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
                {"series_id": "DGS10", "period": "2026-03-01", "value": "4.40"},
            ],
            "DEXUSEU": [
                {"series_id": "DEXUSEU", "period": "2026-02-01", "value": "1.08"},
            ],
        }[series_id]

        history = load_or_fetch_history()

        assert [obs["period"] for obs in history] == [
            "2026-01-01", "2026-02-01", "2026-03-01",
        ]
    
    @patch("producers.fred_replay.json")
    @patch("producers.fred_replay.open", new_callable=mock_open)
    @patch("producers.fred_replay.os.path.exists", return_value=False)
    @patch("producers.fred_replay.fetch_series", return_value=[{"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"}])
    @patch("producers.fred_replay.FRED_SERIES", ["DGS10"])
    def test_writes_fetched_history_to_cache_file(
        self, _fetch_series, _exists, mock_open_call, mock_json
    ):
        load_or_fetch_history()

        mock_json.dump.assert_called_once()
        written_history, file_handle = mock_json.dump.call_args.args
        assert written_history == [{"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"}]
        
        
    @patch("producers.fred_replay.json")
    @patch("producers.fred_replay.open", new_callable=mock_open)
    @patch("producers.fred_replay.os.path.exists", return_value=True)
    @patch("producers.fred_replay.fetch_series")
    def test_loads_from_cache_when_file_exists(
        self, mock_fetch_series, _exists, _open, mock_json
    ):
        cached_history = [{"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"}]
        mock_json.load.return_value = cached_history

        history = load_or_fetch_history()

        assert history == cached_history
        mock_fetch_series.assert_not_called()
        
    @patch("producers.fred_replay.json")
    @patch("producers.fred_replay.open", new_callable=mock_open)
    @patch("producers.fred_replay.os.path.exists", return_value=False)
    @patch("producers.fred_replay.fetch_series")
    @patch("producers.fred_replay.FRED_SERIES", ["DGS10", "DEXUSEU"])
    def test_propagates_error_and_skips_cache_write_when_a_series_fetch_fails(
        self, mock_fetch_series, _exists, _open, mock_json
    ):
        mock_fetch_series.side_effect = [
            [{"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"}],
            requests.exceptions.ConnectionError("connection error"),
        ]

        with pytest.raises(requests.exceptions.ConnectionError):
            load_or_fetch_history()

        mock_json.dump.assert_not_called()
        
class TestRun:
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_publishes_each_observation_to_macro_history_topic(
        self, mock_load_history, mock_make_producer, mock_producer_event, _sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
            {"series_id": "DEXUSEU", "period": "2026-01-01", "value": "1.08"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer
        stop_after_one_cycle(mock_producer)

        run()

        topics = [c.args[1] for c in mock_producer_event.call_args_list]
        assert topics == ["macro.history", "macro.history"]
    
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_event_payload_matches_expected_schema(
        self, mock_load_history, mock_make_producer, mock_producer_event, _sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer
        stop_after_one_cycle(mock_producer)

        run()

        event = mock_producer_event.call_args_list[0].args[3]
        assert event["series_id"] == "DGS10"
        assert event["value"] == 4.25
        assert event["period"] == "2026-01-01"
        assert event["source"] == "fred"
        assert uuid.UUID(event["event_id"])
        assert datetime.fromisoformat(event["replayed_at"]).tzinfo is not None

    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_keys_each_event_by_series_id(
        self, mock_load_history, mock_make_producer, mock_producer_event, _sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
            {"series_id": "DEXUSEU", "period": "2026-01-01", "value": "1.08"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer
        stop_after_one_cycle(mock_producer)

        run()

        keys = [c.args[2] for c in mock_producer_event.call_args_list]
        assert keys == ["DGS10", "DEXUSEU"]

    @patch("producers.fred_replay.FRED_REPLAY_INTERVAL_SECONDS", 5)
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_sleeps_between_each_event_by_configured_interval(
        self, mock_load_history, mock_make_producer, mock_producer_event, mock_sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
            {"series_id": "DGS10", "period": "2026-02-01", "value": "4.30"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer
        stop_after_one_cycle(mock_producer)

        run()

        assert mock_sleep.call_args_list == [call(5), call(5)]
        
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_flushes_and_loops_back_after_reaching_end_of_history(
        self, mock_load_history, mock_make_producer, mock_producer_event, _sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer

        state = {"calls": 0}

        def _flush_side_effect(timeout):
            state["calls"] += 1
            if state["calls"] == 2:
                raise KeyboardInterrupt

        mock_producer.flush.side_effect = _flush_side_effect

        run()

        # one flush(5) per completed pass, plus flush(10) in the finally block
        assert mock_producer.flush.call_args_list == [call(5), call(5), call(10)]
        assert mock_producer_event.call_count == 2
        
    @patch("time.sleep", return_value=None)
    @patch("producers.fred_replay.producer_event")
    @patch("producers.fred_replay.make_producer")
    @patch("producers.fred_replay.load_or_fetch_history")
    def test_shuts_down_gracefully_and_flushes_on_keyboard_interrupt(
        self, mock_load_history, mock_make_producer, mock_producer_event, _sleep
    ):
        mock_load_history.return_value = [
            {"series_id": "DGS10", "period": "2026-01-01", "value": "4.25"},
        ]
        mock_producer = MagicMock()
        mock_make_producer.return_value = mock_producer
        stop_after_one_cycle(mock_producer)

        run()  # should not raise

        mock_producer.flush.assert_called_with(10)