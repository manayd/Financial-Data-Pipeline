"""Tests for FastAPI endpoints using TestClient with mocked database sessions."""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.api.dependencies import get_session


def _make_article(**overrides):  # type: ignore[no-untyped-def]
    defaults = {
        "id": 1,
        "event_id": uuid.uuid4(),
        "timestamp": datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
        "source": "alpha_vantage",
        "source_url": "https://example.com/article",
        "title": "Apple Q4 Earnings",
        "content": "Apple reported strong earnings.",
        "tickers": ["AAPL"],
        "companies": ["Apple Inc."],
        "content_hash": "abc123",
        "raw_metadata": {"key": "value"},
        "processed_at": datetime(2024, 1, 15, 10, 31, tzinfo=UTC),
        "created_at": datetime(2024, 1, 15, 10, 31, tzinfo=UTC),
    }
    defaults.update(overrides)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def _make_analysis(**overrides):  # type: ignore[no-untyped-def]
    defaults = {
        "id": 1,
        "article_event_id": uuid.uuid4(),
        "summary": "Apple reported strong Q4 earnings.",
        "sentiment": "bullish",
        "sentiment_confidence": 0.92,
        "entities": [
            {"name": "Apple Inc.", "entity_type": "company", "relevance_score": 0.95},
        ],
        "key_topics": ["earnings", "revenue"],
        "analyzed_at": datetime(2024, 1, 15, 10, 35, tzinfo=UTC),
        "model_id": "gpt-4o-mini",
        "analysis_version": "1.0",
        "created_at": datetime(2024, 1, 15, 10, 35, tzinfo=UTC),
    }
    defaults.update(overrides)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def _make_aggregation(**overrides):  # type: ignore[no-untyped-def]
    defaults = {
        "id": 1,
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "window_start": datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
        "window_end": datetime(2024, 1, 15, 11, 0, tzinfo=UTC),
        "article_count": 15,
        "avg_sentiment_score": None,
        "dominant_sentiment": None,
        "created_at": datetime(2024, 1, 15, 11, 0, tzinfo=UTC),
    }
    defaults.update(overrides)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


def _mock_session_with_results(results: list) -> AsyncMock:  # type: ignore[type-arg]
    """Create a mock async session that returns given results from execute()."""
    session = AsyncMock()
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = results
    scalars_mock.first.return_value = results[0] if results else None
    execute_result = MagicMock()
    execute_result.scalars.return_value = scalars_mock
    session.execute.return_value = execute_result
    return session


@pytest.fixture
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


class TestHealthEndpoint:
    def test_returns_ok(self, client: TestClient) -> None:
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestArticlesEndpoint:
    def test_list_articles(self, client: TestClient) -> None:
        articles = [_make_article(), _make_article(id=2, event_id=uuid.uuid4())]
        mock_session = _mock_session_with_results(articles)
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/articles")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["items"]) == 2

        app.dependency_overrides.clear()

    def test_list_articles_with_ticker_filter(self, client: TestClient) -> None:
        articles = [_make_article()]
        mock_session = _mock_session_with_results(articles)
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/articles?ticker=AAPL")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1

        app.dependency_overrides.clear()

    def test_list_articles_limit_validation(self, client: TestClient) -> None:
        mock_session = _mock_session_with_results([])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/articles?limit=0")
        assert response.status_code == 422

        response = client.get("/api/v1/articles?limit=101")
        assert response.status_code == 422

        app.dependency_overrides.clear()

    def test_get_article_by_event_id(self, client: TestClient) -> None:
        article = _make_article()
        mock_session = _mock_session_with_results([article])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{article.event_id}")
        assert response.status_code == 200
        assert response.json()["title"] == "Apple Q4 Earnings"

        app.dependency_overrides.clear()

    def test_get_article_not_found(self, client: TestClient) -> None:
        mock_session = _mock_session_with_results([])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{uuid.uuid4()}")
        assert response.status_code == 404

        app.dependency_overrides.clear()

    def test_get_article_invalid_uuid(self, client: TestClient) -> None:
        mock_session = _mock_session_with_results([])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/articles/not-a-uuid")
        assert response.status_code == 422

        app.dependency_overrides.clear()

    def test_latest_for_ticker(self, client: TestClient) -> None:
        articles = [_make_article(), _make_article(id=2, event_id=uuid.uuid4())]
        mock_session = _mock_session_with_results(articles)
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/tickers/AAPL/latest")
        assert response.status_code == 200
        assert len(response.json()) == 2

        app.dependency_overrides.clear()

    def test_article_response_schema(self, client: TestClient) -> None:
        article = _make_article()
        mock_session = _mock_session_with_results([article])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{article.event_id}")
        data = response.json()
        expected_keys = {
            "event_id",
            "timestamp",
            "source",
            "source_url",
            "title",
            "content",
            "tickers",
            "companies",
            "processed_at",
        }
        assert expected_keys == set(data.keys())

        app.dependency_overrides.clear()


class TestAggregationsEndpoint:
    def test_list_aggregations(self, client: TestClient) -> None:
        aggs = [_make_aggregation(), _make_aggregation(id=2, ticker="MSFT")]
        mock_session = _mock_session_with_results(aggs)
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/aggregations")
        assert response.status_code == 200
        assert len(response.json()) == 2

        app.dependency_overrides.clear()

    def test_aggregations_filter_by_ticker(self, client: TestClient) -> None:
        aggs = [_make_aggregation()]
        mock_session = _mock_session_with_results(aggs)
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/aggregations?ticker=AAPL")
        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0]["ticker"] == "AAPL"

        app.dependency_overrides.clear()

    def test_aggregation_response_schema(self, client: TestClient) -> None:
        agg = _make_aggregation()
        mock_session = _mock_session_with_results([agg])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get("/api/v1/aggregations")
        data = response.json()[0]
        expected_keys = {
            "ticker",
            "company_name",
            "window_start",
            "window_end",
            "article_count",
            "avg_sentiment_score",
            "dominant_sentiment",
        }
        assert expected_keys == set(data.keys())

        app.dependency_overrides.clear()


class TestAnalysisEndpoint:
    def test_get_analysis_for_article(self, client: TestClient) -> None:
        analysis = _make_analysis()
        mock_session = _mock_session_with_results([analysis])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{analysis.article_event_id}/analysis")
        assert response.status_code == 200
        data = response.json()
        assert data["summary"] == "Apple reported strong Q4 earnings."
        assert data["sentiment"] == "bullish"

        app.dependency_overrides.clear()

    def test_get_analysis_not_found(self, client: TestClient) -> None:
        mock_session = _mock_session_with_results([])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{uuid.uuid4()}/analysis")
        assert response.status_code == 404

        app.dependency_overrides.clear()

    def test_analysis_response_schema(self, client: TestClient) -> None:
        analysis = _make_analysis()
        mock_session = _mock_session_with_results([analysis])
        app.dependency_overrides[get_session] = lambda: mock_session

        response = client.get(f"/api/v1/articles/{analysis.article_event_id}/analysis")
        data = response.json()
        expected_keys = {
            "article_event_id",
            "summary",
            "sentiment",
            "sentiment_confidence",
            "entities",
            "key_topics",
            "analyzed_at",
            "model_id",
            "analysis_version",
        }
        assert expected_keys == set(data.keys())

        app.dependency_overrides.clear()
