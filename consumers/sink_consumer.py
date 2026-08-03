import json
import logging

from confluent_kafka import KafkaError, Message

from db.timescale import insert_fx_rate, insert_macro_observation, make_connection
from consumers.kafka_client import make_consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC_HANDLERS = {
    "fx.rates": insert_fx_rate,
    "macro.history": insert_macro_observation,
}

def handle_message(conn, msg: Message):
    topic = msg.topic()
    handler = TOPIC_HANDLERS[topic]
    event = json.loads(msg.value().decode("utf-8"))
    
    inserted = handler(conn, event)
    conn.commit()
    
    if inserted:
        logger.info("wrote %s event %s", topic, event["event_id"])
    else:
        logger.info("skipped duplicate %s event %s", topic, event["event_id"])
        
def run() -> None:
    consumer = make_consumer()
    conn = make_connection()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("kafka error: %s", msg.error())
                continue
            
            try:
                handle_message(conn, msg)
                consumer.commit(msg)
            except Exception:
                conn.rollback()
                logger.exception(
                    "failed to process %s[%d]@%d, will retry on delivery",
                    msg.topic(), msg.partition(), msg.offset()
                )
    except KeyboardInterrupt:
        logger.info("shutting down consumer")
    
    finally:
        consumer.close()
        conn.close()
        
if __name__ == "__main__":
    run()