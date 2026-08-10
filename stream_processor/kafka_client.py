from confluent_kafka import Consumer

from stream_processor.config import KAFKA_BOOTSTRAP_SERVERS, SOURCE_TOPIC, STREAM_PROCESSOR_GROUP_ID

def make_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": STREAM_PROCESSOR_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(SOURCE_TOPIC)
    return consumer