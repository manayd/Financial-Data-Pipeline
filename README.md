# Financial Data Pipeline

[![CI](https://github.com/manaydivatia/Financial-Data-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/manaydivatia/Financial-Data-Pipeline/actions) ![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An AI-powered financial data pipeline that ingests real-time financial news and SEC filings via Apache Kafka, processes and stores them in a data lake (S3/Parquet), and runs LLM-based analysis (summarization, sentiment analysis, entity extraction) — all deployed on AWS with Infrastructure as Code.

## Architecture

```
Data Sources            Ingestion           Kafka              Processing          Sinks               Serving
┌──────────────┐      ┌──────────┐      ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│ Alpha Vantage │─────>│          │─────>│ raw-financial │───>│  Faust       │───>│ PostgreSQL   │<───│ FastAPI  │
│ SEC EDGAR     │─────>│ Producer │      │ -news (6p)   │    │  Processor   │    │ (articles,   │    │ REST API │
│ Synthetic     │─────>│          │      └──────┬───────┘    │  - dedup     │    │  aggregations│    │ :8000    │
└──────────────┘      └──────────┘             │            │  - enrich    │    │  llm_analyses│    └──────────┘
                                                │            │  - aggregate │    └──────────────┘
                                         ┌──────▼───────┐    └──────┬───────┘    ┌──────────────┐
                                         │ raw-financial │          │            │ S3 / MinIO   │
                                         │ -news-dlq(3p)│    ┌─────▼────────┐   │ (Parquet     │
                                         └──────────────┘    │ processed-   │   │  partitioned │
                                                             │ financial-   │   │  by date &   │
                                         ┌──────────────┐    │ news (6p)    │   │  ticker)     │
                                         │ aggregation- │    └──────┬───────┘   └──────────────┘
                                         │ results (3p) │<──────────┤
                                         └──────────────┘           │
                                                             ┌──────▼───────┐
                                         ┌──────────────┐    │ LLM Analyzer │
                                         │ llm-analysis │<───│ (Claude /    │
                                         │ -results (6p)│    │  OpenAI)     │
                                         └──────────────┘    └──────────────┘
```

## Features

- **3 data sources** — Alpha Vantage market news, SEC EDGAR filings, synthetic generator
- **Apache Kafka** streaming with 5 topics, dead-letter queue, and configurable partitioning
- **Faust stream processing** — deduplication (content hashing), enrichment (ticker extraction, sentiment), windowed aggregations
- **Dual storage** — PostgreSQL for serving, S3/Parquet for analytics (partitioned by date and ticker)
- **LLM analysis** — summarization, sentiment analysis, entity extraction via Claude or OpenAI with rate limiting
- **REST API** — FastAPI with async SQLAlchemy, pagination, and article/aggregation/analysis endpoints
- **Infrastructure as Code** — Full AWS deployment via boto3 (VPC, ECS Fargate, RDS, MSK, ALB, ECR, S3, Secrets Manager)
- **CI/CD** — GitHub Actions with linting, type checking, and unit tests

## Tech Stack

| Category | Tool | Purpose |
|---|---|---|
| Streaming | Apache Kafka (Confluent) | Message broker with 5 topics |
| Stream Processing | Faust | Dedup, enrichment, windowed aggregation |
| API | FastAPI + Uvicorn | Async REST API |
| Database | PostgreSQL 16 | Serving layer (articles, aggregations, analyses) |
| Object Storage | S3 / MinIO | Parquet data lake partitioned by date/ticker |
| LLM | Claude (Anthropic) / OpenAI | Summarization, sentiment, entity extraction |
| ORM | SQLAlchemy 2.0 | Async ORM with Alembic migrations |
| Serialization | Pydantic v2 | Event schemas, config, API validation |
| Data Format | Apache Parquet (PyArrow) | Columnar storage for analytics |
| Infrastructure | boto3 (IaC) | AWS provisioning (VPC, ECS, RDS, MSK, ALB) |
| Containers | Docker + ECS Fargate | Local dev and production deployment |
| CI/CD | GitHub Actions | Lint, type check, unit tests |
| Logging | structlog | Structured JSON logging |

## Prerequisites

- **Docker & Docker Compose** (for local development)
- **Python 3.12+** (for running tests and linting)
- **AWS account** (optional, only for production deployment)

## Quick Start

```bash
# Clone the repo
git clone <repo-url>
cd Financial-Data-Pipeline

# Copy environment config
cp .env.example .env

# Start all services
make up

# Verify the API is running
curl http://localhost:8000/api/v1/health
# {"status":"ok"}

# View logs
make logs

# Stop all services
make down
```

This starts Zookeeper, Kafka (with 5 topics), PostgreSQL, MinIO (S3-compatible), and all 5 application services: producer, processor, consumer, LLM analyzer, and API.

## Configuration

All configuration is managed via environment variables. Copy `.env.example` to `.env` and adjust as needed.

### Core Settings

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `DATABASE_URL` | `postgresql://pipeline:localdev@localhost:5432/financial_data` | PostgreSQL connection string |
| `S3_ENDPOINT_URL` | `http://localhost:9000` | S3/MinIO endpoint |
| `S3_BUCKET` | `financial-data-lake` | S3 bucket name |

### Producer Settings

| Variable | Default | Description |
|---|---|---|
| `PRODUCER_TYPE` | `fake` | Producer type: `fake`, `real`, or `all` |
| `FAKE_PRODUCER_INTERVAL` | `3` | Seconds between synthetic events |
| `ALPHA_VANTAGE_API_KEY` | *(empty)* | Alpha Vantage API key for market news |
| `ALPHA_VANTAGE_POLL_INTERVAL` | `300` | Seconds between Alpha Vantage polls |
| `WATCHLIST_TICKERS` | `AAPL,MSFT,GOOGL,AMZN,TSLA` | Comma-separated ticker symbols |
| `SEC_EDGAR_POLL_INTERVAL` | `600` | Seconds between SEC EDGAR polls |
| `SEC_EDGAR_USER_AGENT` | `FinancialDataPipeline/1.0 (...)` | SEC EDGAR required user agent |

### Consumer Settings

| Variable | Default | Description |
|---|---|---|
| `CONSUMER_TYPE` | `console` | Consumer type: `console`, `db_sink`, `s3_sink`, or `all` |
| `S3_SINK_BATCH_SIZE` | `100` | Records per Parquet file |
| `S3_SINK_FLUSH_INTERVAL` | `60` | Max seconds before flushing a batch |

### LLM Settings

| Variable | Default | Description |
|---|---|---|
| `LLM_PROVIDER` | `openai` | LLM provider: `openai` or `anthropic` |
| `OPENAI_API_KEY` | *(empty)* | OpenAI API key |
| `ANTHROPIC_API_KEY` | *(empty)* | Anthropic API key |
| `LLM_MODEL_ID` | `gpt-4o-mini` | Model identifier |
| `LLM_REQUESTS_PER_MINUTE` | `50` | Rate limit for LLM calls |
| `LLM_MAX_RETRIES` | `3` | Max retries on LLM failure |
| `LLM_CONTENT_MAX_CHARS` | `3000` | Max characters sent to LLM |

### Infrastructure Settings (`.env.infra`)

| Variable | Default | Description |
|---|---|---|
| `INFRA_PROJECT_NAME` | `financial-data-pipeline` | Resource naming prefix |
| `INFRA_ENVIRONMENT` | `dev` | Environment tag |
| `INFRA_AWS_REGION` | `us-east-1` | AWS region |
| `INFRA_DB_INSTANCE_CLASS` | `db.t3.micro` | RDS instance type |
| `INFRA_MSK_INSTANCE_TYPE` | `kafka.t3.small` | MSK broker instance type |

## API Reference

All endpoints are prefixed with `/api/v1`.

### Health Check

```bash
GET /api/v1/health

curl http://localhost:8000/api/v1/health
# {"status": "ok"}
```

### List Articles

```bash
GET /api/v1/articles?ticker=AAPL&limit=10&offset=0

curl "http://localhost:8000/api/v1/articles?ticker=AAPL&limit=5"
# {"items": [...], "count": 5}
```

| Param | Type | Default | Description |
|---|---|---|---|
| `ticker` | string | *(all)* | Filter by ticker symbol |
| `limit` | int | 10 | Results per page (1-100) |
| `offset` | int | 0 | Pagination offset |

### Get Article by Event ID

```bash
GET /api/v1/articles/{event_id}

curl http://localhost:8000/api/v1/articles/550e8400-e29b-41d4-a716-446655440000
```

Returns 404 if not found.

### Latest Articles for Ticker

```bash
GET /api/v1/tickers/{ticker}/latest?limit=5

curl "http://localhost:8000/api/v1/tickers/AAPL/latest?limit=5"
```

| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | int | 5 | Number of articles (1-50) |

### Get LLM Analysis for Article

```bash
GET /api/v1/articles/{event_id}/analysis

curl http://localhost:8000/api/v1/articles/550e8400-e29b-41d4-a716-446655440000/analysis
```

Returns the LLM-generated summary, sentiment, entities, and key topics. Returns 404 if no analysis exists.

### List Aggregations

```bash
GET /api/v1/aggregations?ticker=AAPL&limit=10

curl "http://localhost:8000/api/v1/aggregations?ticker=TSLA&limit=5"
```

| Param | Type | Default | Description |
|---|---|---|---|
| `ticker` | string | *(all)* | Filter by ticker symbol |
| `limit` | int | 10 | Results per page (1-100) |

## Kafka Topics

| Topic | Partitions | Producer | Consumer |
|---|---|---|---|
| `raw-financial-news` | 6 | Producer (all sources) | Faust Processor |
| `raw-financial-news-dlq` | 3 | Faust Processor (on error) | *(manual inspection)* |
| `processed-financial-news` | 6 | Faust Processor | Consumer sinks, LLM Analyzer |
| `llm-analysis-results` | 6 | LLM Analyzer | Consumer sinks |
| `aggregation-results` | 3 | Faust Processor | Consumer sinks |

## Database Schema

Three tables managed by Alembic migrations:

**`articles`** — Processed financial news events

| Column | Type | Notes |
|---|---|---|
| `event_id` | UUID | Unique, from Kafka event |
| `timestamp` | timestamptz | Event timestamp |
| `source` | varchar(50) | `alpha_vantage`, `sec_edgar`, `synthetic` |
| `title`, `content` | text | Article text |
| `tickers` | text[] | GIN-indexed array |
| `content_hash` | varchar(64) | SHA-256 for dedup |

**`ticker_aggregations`** — Windowed per-ticker statistics

| Column | Type | Notes |
|---|---|---|
| `ticker` | varchar(10) | Ticker symbol |
| `window_start`, `window_end` | timestamptz | Aggregation window |
| `article_count` | int | Articles in window |
| `avg_sentiment_score` | float | Average sentiment |
| `dominant_sentiment` | varchar(20) | Most common sentiment |

**`llm_analyses`** — LLM analysis results per article

| Column | Type | Notes |
|---|---|---|
| `article_event_id` | UUID | FK to `articles.event_id` |
| `summary` | text | LLM-generated summary |
| `sentiment` | varchar(20) | `positive`, `negative`, `neutral` |
| `sentiment_confidence` | float | Confidence score |
| `entities` | jsonb | Extracted entities |
| `key_topics` | text[] | Extracted topics |
| `model_id` | varchar(100) | Model used for analysis |

## Project Structure

```
src/
├── config.py                    # Pydantic Settings (env-based configuration)
├── models/
│   └── events.py                # Pydantic event schemas + Kafka serializers
├── producer/
│   ├── main.py                  # Producer entrypoint (fake/real/all)
│   ├── base.py                  # BaseProducer ABC
│   ├── fake_producer.py         # Synthetic data generator (Faker)
│   ├── alpha_vantage_producer.py # Alpha Vantage market news
│   └── sec_edgar_producer.py    # SEC EDGAR RSS feed
├── processor/
│   ├── main.py                  # Processor entrypoint
│   ├── app.py                   # Faust application
│   ├── agents.py                # Faust agents (dedup, route, aggregate)
│   └── enrichment.py            # Ticker extraction, sentiment scoring
├── consumer/
│   ├── main.py                  # Consumer factory (console/db/s3/all)
│   └── console_consumer.py      # Simple stdout consumer
├── llm/
│   ├── main.py                  # LLM analyzer entrypoint
│   ├── analyzer.py              # Kafka consume → LLM → produce loop
│   ├── provider.py              # OpenAI / Anthropic provider abstraction
│   ├── prompts.py               # Analysis prompt templates
│   └── rate_limiter.py          # Token bucket rate limiter
├── api/
│   ├── app.py                   # FastAPI application factory
│   ├── dependencies.py          # Async DB session dependency
│   ├── schemas.py               # Pydantic response models
│   └── routes/
│       ├── health.py            # GET /health
│       ├── articles.py          # GET /articles, /articles/{id}, /tickers/{t}/latest, /articles/{id}/analysis
│       └── aggregations.py      # GET /aggregations
└── storage/
    ├── database.py              # Async SQLAlchemy engine
    ├── models.py                # ORM models (Article, TickerAggregation, LLMAnalysis)
    ├── repository.py            # Async query functions
    ├── db_sink.py               # Kafka → PostgreSQL consumer sink
    └── s3_sink.py               # Kafka → S3/Parquet consumer sink

infra/                           # AWS Infrastructure as Code (boto3)
├── config.py                    # InfraSettings (pydantic-settings)
├── helpers.py                   # Shared utilities (tagging, polling, waiter)
├── networking.py                # VPC, subnets, IGW, NAT, route tables, security groups
├── ecr.py                       # ECR repositories (5 services)
├── storage.py                   # S3 bucket with lifecycle rules
├── secrets.py                   # Secrets Manager (DB URL, API keys)
├── database.py                  # RDS PostgreSQL 16
├── kafka.py                     # MSK cluster + topic creation
├── alb.py                       # Application Load Balancer
├── ecs.py                       # ECS Fargate cluster, task defs, services, IAM
├── deploy.py                    # 4-phase deployment orchestrator
└── destroy.py                   # Reverse-order teardown

docker/                          # Dockerfiles for each service
├── producer.Dockerfile
├── processor.Dockerfile
├── consumer.Dockerfile
├── llm-analyzer.Dockerfile
└── api.Dockerfile

alembic/                         # Database migrations
├── env.py
└── versions/
    ├── 0001_initial_tables.py   # articles + ticker_aggregations
    └── 0002_add_llm_analysis_table.py

tests/
├── unit/                        # ~70 unit tests
└── integration/                 # Integration tests (testcontainers)

.github/workflows/ci.yml        # CI: lint + unit tests + infra validation
```

## Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Run linting (ruff + mypy)
make lint

# Auto-fix lint issues
make fmt

# Run all tests
make test

# Run only unit tests
make test-unit

# Run database migrations
make migrate
```

### Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

Configured hooks: ruff check (with auto-fix) and ruff format.

## AWS Deployment

The `infra/` directory contains a complete boto3-based Infrastructure as Code solution that provisions:

- **VPC** with 2 public + 2 private subnets across 2 AZs
- **NAT Gateway** for private subnet internet access
- **RDS PostgreSQL 16** (db.t3.micro, encrypted, single-AZ)
- **MSK** (Kafka) cluster with 2 brokers (kafka.t3.small)
- **ECS Fargate** cluster with FARGATE_SPOT capacity, 5 services
- **ALB** (Application Load Balancer) routing to the API service
- **ECR** repositories for all 5 Docker images
- **S3** data lake bucket with lifecycle rules (Standard-IA at 30d, Glacier at 90d)
- **Secrets Manager** for database URL and API keys
- **IAM roles** with least-privilege policies

### Deploy

```bash
# Configure (optional — defaults work for dev)
cp .env.example .env.infra
# Edit .env.infra with your settings

# Deploy all infrastructure
make deploy
```

Deployment runs in 4 phases:
1. Networking, ECR, S3, Secrets Manager
2. RDS + MSK + ALB (parallel, ~20 min)
3. Database secret update, Kafka topics, IAM roles, log groups
4. ECS task definitions, cluster, services

### Destroy

```bash
make destroy
```

Tears down all resources in reverse dependency order. Requires `--confirm` flag.

## Makefile Targets

| Target | Description |
|---|---|
| `make up` | Start all services with Docker Compose |
| `make down` | Stop all services |
| `make logs` | Tail logs from all services |
| `make lint` | Run ruff check + ruff format check + mypy |
| `make fmt` | Auto-fix lint issues and format code |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests |
| `make migrate` | Run Alembic database migrations |
| `make deploy` | Deploy AWS infrastructure |
| `make destroy` | Tear down AWS infrastructure |
| `make infra-lint` | Lint infrastructure code |
| `make test-infra` | Run infrastructure unit tests |
| `make clean` | Remove caches and virtual environment |
| `make test-cov` | Run tests with HTML coverage report |
| `make docker-build` | Build all Docker images |
| `make check-health` | Check API health endpoint |

## License

[MIT](LICENSE)
