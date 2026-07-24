import logging
import time
import uuid
from datetime import datetime, timezone

import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from producers.config import ALPHA_VANTAGE_API_KEY, FX_PAIRS, FX_POLL_INTERVAL_SECONDS
from producers.kafka_client import make_producer, producer_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
TOPIC = "fx.rates"

class RateLimited(Exception):
    pass

@retry(stop=stop_after_attempt(5), wait=wait_exponential_jitter(initial=5, max=20), reraise=True)
def fetch_rate(from_currency: str, to_currency: str) -> dict:
    params = {
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": from_currency,
        "to_currency": to_currency,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }
    
    response: Response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=10)
    response.raise_for_status()
    payload: dict = response.json()
    
    if "Note" in payload or "Information" in payload:
        logger.warning("alpha vantage rate limit hit: %s", payload.get("Note") or payload.get("Information"))
        raise RateLimited(payload)
    
    quote = payload.get("Realtime Currency Exchange")
    if quote is None:
        raise ValueError(f"unexpected response shape: {payload}")
    
    return {
        "event_id": str(uuid.uuid4()),
        "pair": f"{from_currency}/{to_currency}",
        "rate": float(quote["5. Exchange Rate"]),
        "source": "alpha_vantage",
        "observed_at": quote["6. Last Refreshed"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    
def run() -> None:
    producer = make_producer()
    try:
        while True:
            for from_currency, to_currency in FX_PAIRS:
                try:
                    event = fetch_rate(from_currency, to_currency)
                    producer_event(producer, TOPIC, event)
                    logger.info("published %s", event)
                except RateLimited:
                    logger.warning("giving up on %s/%s this cycle after retries", from_currency, to_currency)
                except Exception:
                    logger.exception("failed to fetch %s/%s", from_currency, to_currency)
            
            producer.flush(5)
            time.sleep(FX_POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("shutting down fx poller")
    finally:
        producer.flush(10)
        
if __name__ == "__main__":
    run()