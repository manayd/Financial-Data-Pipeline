"""Faust application and Kafka topic declarations for stream processing."""

import faust

from src.config import Settings

settings = Settings()

app = faust.App(
    "financial-processor",
    broker=f"kafka://{settings.kafka_bootstrap_servers}",
    store="memory://",
    topic_replication_factor=1,
    processing_guarantee="at_least_once",
)

raw_topic = app.topic("raw-financial-news", value_type=bytes, value_serializer="raw")
processed_topic = app.topic("processed-financial-news", value_type=bytes, value_serializer="raw")
aggregation_topic = app.topic("aggregation-results", value_type=bytes, value_serializer="raw")
