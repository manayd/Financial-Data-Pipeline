from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"

    # PostgreSQL
    database_url: str = "postgresql://pipeline:localdev@localhost:5432/financial_data"

    # S3 / MinIO
    s3_endpoint_url: str | None = None
    s3_bucket: str = "financial-data-lake"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "us-east-1"

    # Producer
    producer_type: str = "fake"
    fake_producer_interval: float = 3.0  # seconds between events

    # Alpha Vantage (Week 2)
    alpha_vantage_api_key: str = ""
    watchlist_tickers: str = "AAPL,MSFT,GOOGL,AMZN,TSLA"
    alpha_vantage_poll_interval: int = 300  # seconds

    # SEC EDGAR (Week 2)
    sec_edgar_poll_interval: int = 600  # seconds
    sec_edgar_user_agent: str = "FinancialDataPipeline/1.0 (contact@example.com)"

    # Consumer (Week 4)
    consumer_type: str = "console"
    s3_sink_batch_size: int = 100
    s3_sink_flush_interval: int = 60  # seconds

    # API (Week 4)
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Stream Processor (Week 3)
    processor_window_seconds: int = 3600  # aggregation window size
    processor_emit_interval: int = 300  # emit aggregation snapshots interval

    # LLM (Week 5)
    llm_provider: str = "openai"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    llm_model_id: str = "gpt-4o-mini"
    llm_requests_per_minute: int = 50
    llm_tokens_per_minute: int = 100_000
    llm_max_retries: int = 3
    llm_content_max_chars: int = 3000

    @property
    def watchlist_tickers_list(self) -> list[str]:
        return [t.strip() for t in self.watchlist_tickers.split(",") if t.strip()]
