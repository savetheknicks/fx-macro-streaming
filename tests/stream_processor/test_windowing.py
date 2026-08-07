from datetime import datetime, timedelta

import pytest

from stream_processor.windowing import PairWindow

def dt(seconds: int) -> datetime:
    return datetime(2026, 1, 1, 12, 0, 0) + timedelta(seconds=seconds)

class TestPairWindow:
    def test_rolling_average_over_observations_in_window(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0)

        window.add(dt(0), 1.0)
        window.add(dt(60), 1.2)
        snapshot = window.add(dt(120), 1.4)

        assert snapshot.rolling_average == pytest.approx(1.2)
        assert snapshot.sample_count == 3
    
    def test_evicts_observations_older_than_the_window(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0)

        window.add(dt(0), 1.0)
        snapshot = window.add(dt(400), 2.0)

        assert snapshot.sample_count == 1
        assert snapshot.rolling_average == pytest.approx(2.0)
        
    def test_first_observation_has_no_delta_or_z_score(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0)

        snapshot = window.add(dt(0), 1.0)

        assert snapshot.delta is None
        assert snapshot.z_score is None
        assert snapshot.is_anomaly is False
        
    def test_no_anomaly_flagged_before_min_samples_reached(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0, min_samples=3)

        window.add(dt(0), 1.0)
        snapshot = window.add(dt(60), 1.5)

        assert snapshot.z_score is None
        assert snapshot.is_anomaly is False
        
    def test_flags_a_delta_far_outside_the_established_baseline(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0, min_samples=3)

        window.add(dt(0), 1.000)
        window.add(dt(60), 1.001)
        window.add(dt(120), 1.003)
        snapshot = window.add(dt(180), 1.500)

        assert snapshot.is_anomaly is True
        assert snapshot.z_score > 3.0
        
    def test_does_not_flag_a_delta_consistent_with_the_baseline(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0, min_samples=3)

        window.add(dt(0), 1.000)
        window.add(dt(60), 1.010)
        window.add(dt(120), 1.020)
        snapshot = window.add(dt(180), 1.030)

        assert snapshot.is_anomaly is False
        
    def test_zero_variance_baseline_does_not_divide_by_zero(self):
        window = PairWindow(window_seconds=300, z_threshold=3.0, min_samples=3)

        window.add(dt(0), 1.0)
        window.add(dt(60), 1.1)
        window.add(dt(120), 1.2)
        snapshot = window.add(dt(180), 5.0)

        assert snapshot.z_score is None
        assert snapshot.is_anomaly is False