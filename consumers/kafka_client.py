from confluent_kafka import Consumer

from consumers.config import SINK_CONSUMER_GROUP_ID, SINK_TOPICS, KAFKA_BOOTSTRAP_SERVERS

def make_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": SINK_CONSUMER_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(SINK_TOPICS)
    return consumer