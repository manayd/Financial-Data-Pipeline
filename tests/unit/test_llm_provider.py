"""Tests for LLM provider abstraction and prompt formatting."""

import pytest

from src.llm.prompts import SYSTEM_PROMPT, format_user_prompt
from src.llm.provider import AnthropicProvider, BedrockProvider, OpenAIProvider, create_provider
from src.llm.rate_limiter import RateLimiter


class TestPrompts:
    def test_system_prompt_contains_required_fields(self) -> None:
        assert "summary" in SYSTEM_PROMPT
        assert "sentiment" in SYSTEM_PROMPT
        assert "entities" in SYSTEM_PROMPT
        assert "key_topics" in SYSTEM_PROMPT

    def test_system_prompt_specifies_sentiment_labels(self) -> None:
        assert "bullish" in SYSTEM_PROMPT
        assert "bearish" in SYSTEM_PROMPT
        assert "neutral" in SYSTEM_PROMPT
        assert "mixed" in SYSTEM_PROMPT

    def test_format_user_prompt_includes_title_and_content(self) -> None:
        prompt = format_user_prompt("Test Title", "Test content body")
        assert "Test Title" in prompt
        assert "Test content body" in prompt

    def test_format_user_prompt_truncates_content(self) -> None:
        long_content = "x" * 5000
        prompt = format_user_prompt("Title", long_content, max_chars=100)
        # Should be truncated to 100 chars of content
        assert len(prompt) < 5000
        assert "x" * 100 in prompt
        assert "x" * 101 not in prompt


class TestProviderFactory:
    def test_create_openai_provider(self) -> None:
        from unittest.mock import patch

        with patch("src.config.Settings") as mock_settings:
            settings = mock_settings()
            settings.llm_provider = "openai"
            settings.openai_api_key = "test-key"
            settings.llm_model_id = "gpt-4o-mini"
            provider = create_provider(settings)
            assert isinstance(provider, OpenAIProvider)

    def test_create_anthropic_provider(self) -> None:
        from unittest.mock import patch

        with patch("src.config.Settings") as mock_settings:
            settings = mock_settings()
            settings.llm_provider = "anthropic"
            settings.anthropic_api_key = "test-key"
            settings.llm_model_id = "claude-sonnet-4-20250514"
            provider = create_provider(settings)
            assert isinstance(provider, AnthropicProvider)

    def test_create_bedrock_provider(self) -> None:
        from unittest.mock import patch

        with (
            patch("src.config.Settings") as mock_settings,
            patch("src.llm.provider.boto3"),
        ):
            settings = mock_settings()
            settings.llm_provider = "bedrock"
            settings.llm_model_id = "anthropic.claude-sonnet-4-20250514-v1:0"
            settings.aws_region = "us-east-1"
            provider = create_provider(settings)
            assert isinstance(provider, BedrockProvider)

    def test_create_unknown_provider_raises(self) -> None:
        from unittest.mock import patch

        with patch("src.config.Settings") as mock_settings:
            settings = mock_settings()
            settings.llm_provider = "unknown"
            with pytest.raises(ValueError, match="Unknown LLM provider"):
                create_provider(settings)


class TestRateLimiter:
    def test_initial_acquire_no_wait(self) -> None:
        limiter = RateLimiter(requests_per_minute=60)
        # Interval should be 1 second
        assert limiter._interval == pytest.approx(1.0)

    def test_zero_rpm_handled(self) -> None:
        limiter = RateLimiter(requests_per_minute=0)
        # Should not crash; uses max(0, 1) = 1
        assert limiter._interval == 60.0

    @pytest.mark.asyncio
    async def test_acquire_updates_last_request(self) -> None:
        limiter = RateLimiter(requests_per_minute=6000)  # very high to avoid waiting
        assert limiter._last_request == 0.0
        await limiter.acquire()
        assert limiter._last_request > 0.0
