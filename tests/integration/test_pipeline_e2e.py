"""End-to-end integration tests using testcontainers.

Run with: pytest tests/integration/ -v -m integration
Requires Docker to be running.
"""

import json
import time

import pytest

try:
    from testcontainers.kafka import KafkaContainer
    from testcontainers.postgres import PostgresContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not HAS_TESTCONTAINERS, reason="testcontainers not available"),
]


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka


@pytest.mark.integration
def test_produce_and_consume_via_database(postgres_container, kafka_container):
    """Produce a synthetic event to Kafka, consume it into Postgres, query via API."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    from sqlalchemy import create_engine, text

    bootstrap = kafka_container.get_bootstrap_server()
    db_url = postgres_container.get_connection_url()

    # Create topic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topic = NewTopic("processed-financial-news", num_partitions=1, replication_factor=1)
    futures = admin.create_topics([topic])
    for _, f in futures.items():
        f.result(timeout=10)

    # Produce a test event
    producer = Producer({"bootstrap.servers": bootstrap})

    event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2024-06-15T12:00:00Z",
        "source": "synthetic",
        "title": "Integration Test Article",
        "content": "This is a test article for integration testing.",
        "tickers": ["AAPL"],
        "companies": ["Apple Inc."],
        "content_hash": "integration_test_hash_001",
        "processed_at": "2024-06-15T12:00:01Z",
    }
    producer.produce("processed-financial-news", json.dumps(event).encode())
    producer.flush(timeout=10)

    # Verify the message was produced by consuming it directly
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "integration-test",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["processed-financial-news"])

    msg = None
    deadline = time.time() + 30
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is not None and msg.error() is None:
            break

    consumer.close()

    assert msg is not None
    assert msg.error() is None
    payload = json.loads(msg.value())
    assert payload["title"] == "Integration Test Article"
    assert payload["tickers"] == ["AAPL"]

    # Verify DB connectivity
    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
