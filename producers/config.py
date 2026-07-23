import os

from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY=os.environ["ALPHA_VANTAGE_API_KEY"]
FRED_API_KEY=os.environ["FRED_API_KEY"]
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

FX_PAIRS = [("USD", "EUR"), ("USD", "JPY")]
FX_POLL_INTERVALS_SECONDS = 90

FRED_SERIES = ["UNRATE", "FEDFUNDS", "CPIAUCSL"]
FRED_REPLAY_INTERVAL_SECONDS = 1
FRED_CACHE_PATH = "fred_cache.json"