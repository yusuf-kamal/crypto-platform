"""Spark Structured Streaming: Kafka -> Silver candles.

Writes 1-minute candles to their own Silver prefix rather than into the batch
one. The two paths answer different questions -- the batch layer is complete
and reproducible, the stream is fresh but only covers the time the consumer
was running -- and mixing them in one table would make neither trustworthy.

Append mode only emits a candle once its window is closed by the watermark,
so a published Parquet file is never rewritten.
"""

import os
import sys

from src.common.spark import get_spark
from src.streaming.trade_events import (
    parse_trade_events,
    stream_candles_1m,
    valid_trades,
)

TOPIC = os.environ.get("KAFKA_TOPIC", "binance.trades")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

SILVER_STREAM = "s3a://silver/stream_candles_1m"
CHECKPOINT = "s3a://silver/_checkpoints/stream_candles_1m"
WATERMARK = os.environ.get("STREAM_WATERMARK", "2 minutes")
TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER", "1 minute")


def main() -> int:
    spark = get_spark("trades_stream")
    spark.sparkContext.setLogLevel("WARN")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        # On a fresh checkpoint, start at the oldest retained message so a
        # restart does not silently skip the backlog.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    candles = stream_candles_1m(
        valid_trades(parse_trade_events(kafka)), WATERMARK)

    query = (
        candles.writeStream.format("parquet")
        .option("path", SILVER_STREAM)
        # The checkpoint is what makes a restart exactly-once for the sink:
        # it holds the Kafka offsets and the open window state.
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("symbol")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
