import json
import logging

from confluent_kafka import Producer, Message, KafkaError
from producers.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err: KafkaError, msg: Message) -> None:
    if err is not None:
        logger.error("delivery failed for %s: %s", msg.topic(), err)
    else:
        logger.debug("delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())
        
def producer_event(producer: Producer, topic: str, event: dict) -> None:
    producer.produce(topic, value=json.dumps(event).encode("utf-8"), callback=delivery_report)
    producer.poll(0)