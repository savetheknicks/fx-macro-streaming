import os

from producers.config import KAFKA_BOOTSTRAP_SERVERS

SINK_CONSUMER_GROUP_ID = os.getenv("SINK_CONSUMER_GROUP_ID", "sink-consumer")
SINK_TOPICS = ["fx.rates", "macro.history"]
TIMESCALE_DSN = os.getenv("TIMESCALE_DSN", "postgresql://fxmacro:fxmacro@localhost:5432/fxmacro")