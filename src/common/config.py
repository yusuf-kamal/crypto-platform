# ---------------------------------------------------------------------------
# Source 1: Binance public archive (batch files, aggTrades)
# ---------------------------------------------------------------------------
# SYMBOLS = ["XRPUSDT"]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
# SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]

# MONTHS = ["2026-05"]
# MONTHS = ["2026-09", "2026-10"]
MONTHS = ["2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]

# ---------------------------------------------------------------------------
# Source 2: public REST APIs (daily grain, JSON)
# ---------------------------------------------------------------------------
# CoinGecko uses its own coin ids, not Binance trading pairs.
COINGECKO_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "BNBUSDT": "binancecoin",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
}

# CoinGecko's free tier returns hourly points for <= 90 days and daily points
# above that, so 365 keeps the grain daily without a paid `interval` parameter.
COINGECKO_DAYS = 365

# The Fear & Greed index is a single daily value; 0 means "the full history".
FEAR_GREED_LIMIT = 0
