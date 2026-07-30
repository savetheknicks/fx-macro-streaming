import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from producers.config import FRED_API_KEY, FRED_CACHE_PATH, FRED_REPLAY_INTERVAL_SECONDS, FRED_SERIES
from producers.kafka_client import make_producer, producer_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
TOPIC = "macro.history"

@retry(retry=retry_if_exception_type(requests.exceptions.RequestException),stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=5, max=120), reraise=True)
def fetch_series(series_id: str) -> list[dict]:
    params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json"}
    response = requests.get(FRED_URL, params=params, timeout=10)
    response.raise_for_status()
    observations = response.json()["observations"]
    
    return [
        {"series_id": series_id, "period": obs["date"], "value": obs["value"]}
        for obs in observations
        if obs["value"] != "."
    ]
    
def load_or_fetch_history() -> list[dict]:
    if os.path.exists(FRED_CACHE_PATH):
        logger.info("loading cached FRED history from %s", FRED_CACHE_PATH)
        with open(FRED_CACHE_PATH) as f:
            return json.load(f)
        
    logger.info("fetching FRED history for %s", FRED_SERIES)
    history = []
    for series_id in FRED_SERIES:
        history.extend(fetch_series(series_id))
    history.sort(key=lambda obs: obs["period"])
    
    with open(FRED_CACHE_PATH, "w") as f:
        json.dump(history, f)
        
    return history

def run() -> None:
    history = load_or_fetch_history()
    producer = make_producer()
    
    try:
        while True:
            for obs in history:
                event = {
                    "event_id": str(uuid.uuid4()),
                    "series_id": obs["series_id"],
                    "value": float(obs["value"]),
                    "period": obs["period"],
                    "source": "fred",
                    "replayed_at": datetime.now(timezone.utc).isoformat(),
                }
                producer_event(producer, TOPIC, event["series_id"], event)
                time.sleep(FRED_REPLAY_INTERVAL_SECONDS)
            producer.flush(5)
            logger.info("replay reached end of history, looping back to start")
    except KeyboardInterrupt:
        logger.info("shutting down fred replay producer")
    finally:
        producer.flush(10)
        
if __name__ == "__main__":
    run()