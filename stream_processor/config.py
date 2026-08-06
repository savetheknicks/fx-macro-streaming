import os

from producers.config import KAFKA_BOOTSTRAP_SERVERS

STREAM_PROCESSOR_GROUP_ID = os.getenv("STREAM_PROCESSOR_GROUP_ID", "stream-processor")
SOURCE_TOPIC = ["fx.rates"]
ANOMALY_TOPIC = "fx.anomalies"

WINDOW_SECONDS = int(os.getenv("ROLLING_WINDOW_SECONDS", "300"))
Z_SCORE_THRESHOLD = float(os.getenv("ANOMALY_Z_SCORE_THRESHOLD", "3.0"))
MIN_SAMPLES_FOR_ANOMALY_CHECK = int(os.getenv("ANOMALY_MIN_SAMPLES", "3"))