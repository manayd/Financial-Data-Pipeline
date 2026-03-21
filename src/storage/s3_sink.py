"""S3 Parquet sink consumer: batches processed events and writes Parquet to S3."""

import io
import signal
import time
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from confluent_kafka import Consumer, KafkaError

from src.config import Settings
from src.models import KafkaDeserializer
from src.models.events import ProcessedFinancialNewsEvent

logger = structlog.get_logger()

PROCESSED_TOPIC = "processed-financial-news"

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("source", pa.string()),
        pa.field("source_url", pa.string()),
        pa.field("title", pa.string()),
        pa.field("content", pa.string()),
        pa.field("tickers", pa.list_(pa.string())),
        pa.field("companies", pa.list_(pa.string())),
        pa.field("content_hash", pa.string()),
        pa.field("processed_at", pa.timestamp("us", tz="UTC")),
    ]
)


def events_to_parquet_bytes(events: list[ProcessedFinancialNewsEvent]) -> bytes:
    """Convert a list of events to Parquet bytes."""
    columns: dict[str, list] = {field.name: [] for field in PARQUET_SCHEMA}
    for e in events:
        columns["event_id"].append(str(e.event_id))
        columns["timestamp"].append(e.timestamp)
        columns["source"].append(e.source.value)
        columns["source_url"].append(e.source_url or "")
        columns["title"].append(e.title)
        columns["content"].append(e.content)
        columns["tickers"].append(e.tickers)
        columns["companies"].append(e.companies)
        columns["content_hash"].append(e.content_hash)
        columns["processed_at"].append(e.processed_at)

    table = pa.table(columns, schema=PARQUET_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def partition_key(event: ProcessedFinancialNewsEvent, ticker: str) -> tuple[str, str]:
    """Return (date_str, ticker) partition key."""
    date_str = event.timestamp.strftime("%Y-%m-%d")
    return date_str, ticker


class S3ParquetSink:
    """Consumes processed events, batches them, and writes Parquet to S3."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._running = True
        self._batch: list[ProcessedFinancialNewsEvent] = []
        self._last_flush = time.monotonic()
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "s3-sink-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
        self._bucket = settings.s3_bucket

    def _shutdown(self, signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self._running = False

    def _should_flush(self) -> bool:
        if len(self._batch) >= self.settings.s3_sink_batch_size:
            return True
        if time.monotonic() - self._last_flush >= self.settings.s3_sink_flush_interval:
            return len(self._batch) > 0
        return False

    def _flush(self) -> None:
        if not self._batch:
            return

        # Group events by (date, ticker)
        groups: dict[tuple[str, str], list[ProcessedFinancialNewsEvent]] = defaultdict(list)
        for event in self._batch:
            tickers = event.tickers or ["UNKNOWN"]
            for ticker in tickers:
                groups[partition_key(event, ticker)].append(event)

        for (date_str, ticker), events in groups.items():
            parquet_bytes = events_to_parquet_bytes(events)
            batch_ts = int(time.time() * 1000)
            key = f"processed/date={date_str}/ticker={ticker}/batch_{batch_ts}.parquet"
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=parquet_bytes)
            logger.debug("parquet_written", key=key, event_count=len(events))

        logger.info("s3_flush_complete", total_events=len(self._batch), partitions=len(groups))
        self._batch.clear()
        self._last_flush = time.monotonic()

    def consume(self) -> None:
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        self._consumer.subscribe([PROCESSED_TOPIC])
        logger.info("s3_sink_started", topic=PROCESSED_TOPIC)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is not None:
                    error = msg.error()
                    if error:
                        if error.code() != KafkaError._PARTITION_EOF:
                            logger.error("consumer_error", error=error)
                        continue

                    raw = msg.value()
                    if raw is not None:
                        try:
                            event = KafkaDeserializer.deserialize(raw, ProcessedFinancialNewsEvent)
                            self._batch.append(event)
                        except Exception:
                            logger.exception("deserialization_failed", offset=msg.offset())

                if self._should_flush():
                    try:
                        self._flush()
                        self._consumer.commit(asynchronous=False)
                    except Exception:
                        logger.exception("s3_flush_failed")

            # Flush remaining on shutdown
            if self._batch:
                try:
                    self._flush()
                    self._consumer.commit(asynchronous=False)
                except Exception:
                    logger.exception("s3_final_flush_failed")
        finally:
            self._consumer.close()
            logger.info("s3_sink_stopped")
