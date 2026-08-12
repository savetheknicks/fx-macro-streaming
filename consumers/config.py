import os

from producers.config import KAFKA_BOOTSTRAP_SERVERS

SINK_CONSUMER_GROUP_ID = os.getenv("SINK_CONSUMER_GROUP_ID", "sink-consumer")
SINK_TOPICS = ["fx.rates", "macro.history", "fx.anomalies"]