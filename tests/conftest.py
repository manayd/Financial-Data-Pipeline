import pytest

from src.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers="localhost:9092",
        database_url="postgresql://pipeline:localdev@localhost:5432/financial_data",
        producer_type="fake",
    )
