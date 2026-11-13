# Small image for the WebSocket -> Kafka producer only. The Spark consumer
# runs in the Airflow image, which already has Java, PySpark and the jars.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app

WORKDIR /app

COPY docker/streaming/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ /app/src/

CMD ["python", "-m", "src.streaming.binance_ws_producer"]
