import json
import logging
import uuid
from datetime import datetime

from confluent_kafka import Message

from producers.kafka_client import make_producer, producer_event
from stream_processor.config import (
    ANOMALY_TOPIC,
    MIN_SAMPLES_FOR_ANOMALY_CHECK,
    WINDOW_SECONDS,
    Z_SCORE_THRESHOLD,
)
from stream_processor.kafka_client import make_consumer
from stream_processor.windowing import PairWindow, WindowSnapshot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_anomaly_event(event: dict, snapshot: WindowSnapshot) -> dict:
    
    return {
        "event_id": str(uuid.uuid4()),
        "pair": event["pair"],
        "rate": event["rate"],
        "z_score": round(snapshot.z_score, 4),
        "window_start": snapshot.window_start.isoformat(),
        "window_end": snapshot.window_end.isoformat(),
    }
    
def handle_message(windows: dict[str, PairWindow], producer, msg: Message) -> None:
    
    event = json.loads(msg.value().decode("utf-8"))
    pair = event["pair"]
    observed_at = datetime.fromisoformat(event["observed_at"])
    
    window = windows.setdefault(
        pair, PairWindow(WINDOW_SECONDS, Z_SCORE_THRESHOLD, MIN_SAMPLES_FOR_ANOMALY_CHECK)
    )
    
    snapshot = window.add(observed_at, event["rate"])
    
    logger.info(
        "%s rolling_average=%.5f (n=%d)%s",
        pair, snapshot.rolling_average, snapshot.sample_count,
        f" z={snapshot.z_score:.2f}" if snapshot.z_score is not None else ""
    )
    
    if snapshot.is_anomaly:
        anomaly = build_anomaly_event(event, snapshot)
        producer_event(producer, ANOMALY_TOPIC, pair, anomaly)
        logger.warning("anomaly flagged for %s: z=%.2f rate=%s", pair, snapshot.z_score, event["rate"])
        
def run() -> None:
    consumer = make_consumer()
    producer = make_producer()
    windows: dict[str, PairWindow] = {}
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("kafka error: %s", msg.error())
                continue
            
            try:
                handle_message(windows, producer, msg)
                consumer.commit(msg)
            except Exception:
                logger.exception(
                    "failed to process %s[%d]@%d, will retry on delivery",
                    msg.topic(), msg.partition(), msg.offset()
                )
    except KeyboardInterrupt:
        logger.info("shutting down stream processor")
    finally:
        producer.flush(10)
        consumer.close()
        
if __name__ == "__main__":
    run()