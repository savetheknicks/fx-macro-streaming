from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean, pstdev

@dataclass
class WindowSnapshot:
    rolling_average: float
    sample_count: int
    window_start: datetime
    window_end: datetime
    delta: float | None
    z_score: float | None
    is_anomaly: bool

class PairWindow:
    # Below this, recent tick-to-tick moves are indistinguishable from a stale/unchanged
    # quote (e.g. USD/JPY polled faster than Alpha Vantage refreshes it). Scoring against
    # a baseline stdev that small blows up any real move into an absurd z-score, so treat
    # it as "not enough signal yet" instead.
    _MIN_BASELINE_STDEV = 1e-4

    def __init__(self, window_seconds: int, z_threshold: float, min_samples: int = 3) -> None:
        self._span = timedelta(seconds=window_seconds)
        self._z_threshold = z_threshold
        self._min_samples = min_samples
        self._observations: deque[tuple[datetime, float]] = deque()
        self._pct_deltas: deque[tuple[datetime, float]] = deque()
        self._last_rate: float | None = None

    def add(self, observed_at: datetime, rate: float) -> WindowSnapshot:
        self._observations.append((observed_at, rate))

        delta = None
        pct_delta = None

        if self._last_rate is not None:
            delta = rate - self._last_rate
            pct_delta = delta / self._last_rate
            self._pct_deltas.append((observed_at, pct_delta))
        self._last_rate = rate

        self._evict_stale(observed_at)

        z_score = self._z_score(pct_delta)
        return WindowSnapshot(
            rolling_average=mean(r for _, r in self._observations),
            sample_count=len(self._observations),
            window_start=self._observations[0][0],
            window_end=observed_at,
            delta=delta,
            z_score=z_score,
            is_anomaly=z_score is not None and abs(z_score) >= self._z_threshold
        )

    def _evict_stale(self, now: datetime) -> None:
        cutoff = now - self._span
        while self._observations and self._observations[0][0] < cutoff:
            self._observations.popleft()
        while self._pct_deltas and self._pct_deltas[0][0] < cutoff:
            self._pct_deltas.popleft()

    def _z_score(self, pct_delta: float | None) -> float | None:
        if pct_delta is None:
            return None
        baseline = [d for _, d in self._pct_deltas][:-1]
        if len(baseline) < self._min_samples - 1:
            return None
        baseline_mean = mean(baseline)
        baseline_stdev = pstdev(baseline)
        if baseline_stdev < self._MIN_BASELINE_STDEV:
            return None
        return (pct_delta - baseline_mean) / baseline_stdev