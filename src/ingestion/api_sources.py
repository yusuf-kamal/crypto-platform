"""HTTP access to the two public REST APIs (source type 2).

Only fetching lives here: the Airflow task writes the untouched response body
to Bronze, exactly as the Binance path stores the untouched zip file. Parsing
happens later, in src/transform/api_daily.py, so that a schema change at the
API never costs us the raw payload.
"""

import logging
import time

import requests

log = logging.getLogger("airflow.task")

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"

# Both APIs are free and unauthenticated, and both rate-limit by IP
# (CoinGecko at roughly 10-30 calls/minute). One call per coin per day stays
# far below that, but a 429 is still retried with a growing pause.
RETRY_STATUS = (429, 500, 502, 503, 504)


def fetch_json(url: str, params: dict | None = None,
               retries: int = 3, backoff: float = 5.0) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code in RETRY_STATUS:
                raise requests.HTTPError(
                    f"{resp.status_code} from {url}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == retries:
                break
            pause = backoff * attempt
            log.warning("%s failed (attempt %d/%d): %s - retrying in %.0fs",
                        url, attempt, retries, exc, pause)
            time.sleep(pause)
    raise RuntimeError(f"giving up on {url}: {last_error}")


def fetch_market_chart(coin_id: str, days: int) -> dict:
    """Daily price / market cap / 24h volume for one coin.

    Returns the raw payload: {"prices": [[ms, value], ...], "market_caps":
    [...], "total_volumes": [...]}.
    """
    return fetch_json(
        f"{COINGECKO_BASE}/coins/{coin_id}/market_chart",
        {"vs_currency": "usd", "days": days},
    )


def fetch_fear_greed(limit: int) -> dict:
    """Crypto Fear & Greed index, one value per day.

    Returns {"data": [{"value": "20", "value_classification": "Extreme Fear",
    "timestamp": "1794528000"}, ...]}. limit=0 means the full history.
    """
    return fetch_json(FEAR_GREED_URL, {"limit": limit, "format": "json"})
