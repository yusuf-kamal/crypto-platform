"""Binance WebSocket -> Kafka.

A long-running process, not an Airflow task: Airflow schedules work that
ends, and this never does. It is a compose service under the `streaming`
profile instead, and Airflow keeps owning the batch path.

The producer stays deliberately dumb. It does not parse, validate or
reshape -- it forwards the raw message to Kafka and lets the Spark job do
the typing, exactly as Bronze stores the untouched Binance zip. If the
schema changes, the data is already in the topic.
"""

import json
import logging
import os
import signal
import sys
import time

from src.common.config import SYMBOLS

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger("binance_ws_producer")

WS_BASE = "wss://stream.binance.com:9443/stream?streams="
TOPIC = os.environ.get("KAFKA_TOPIC", "binance.trades")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

_running = True


def _stop(signum, _frame):
    global _running
    log.info("signal %s received, shutting down", signum)
    _running = False


def stream_url(symbols: list[str]) -> str:
    """Binance combined stream: lowercase symbol + @trade, slash-separated."""
    return WS_BASE + "/".join(f"{s.lower()}@trade" for s in symbols)


def main() -> int:
    import websocket  # websocket-client
    from confluent_kafka import Producer

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        # Throughput over latency: a few ms of batching cuts the request rate
        # a lot, and a candle is only published once its minute closes anyway.
        "linger.ms": 50,
        "compression.type": "lz4",
    })

    url = stream_url(SYMBOLS)
    sent = 0
    backoff = 1

    while _running:
        try:
            log.info("connecting to %s", url)
            ws = websocket.create_connection(url, timeout=30)
            backoff = 1
            while _running:
                message = ws.recv()
                if not message:
                    continue
                # The symbol is the partition key, so all trades of one
                # symbol keep their order within a partition.
                try:
                    symbol = json.loads(message)["data"]["s"]
                except (ValueError, KeyError, TypeError):
                    symbol = None
                producer.produce(TOPIC, value=message,
                                 key=symbol.encode() if symbol else None)
                producer.poll(0)
                sent += 1
                if sent % 1000 == 0:
                    log.info("forwarded %d messages", sent)
            ws.close()
        except Exception as exc:                              # noqa: BLE001
            # A dropped WebSocket is normal operation, not a crash: Binance
            # cuts connections after 24h and on maintenance.
            log.warning("stream error: %s - reconnecting in %ds", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    producer.flush(10)
    log.info("stopped after %d messages", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
