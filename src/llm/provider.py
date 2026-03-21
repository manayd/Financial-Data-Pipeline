"""LLM provider abstraction supporting OpenAI, Anthropic, and AWS Bedrock."""

import asyncio
import re
from abc import ABC, abstractmethod
from typing import Any

import anthropic
import boto3
import openai
import orjson
import structlog

from src.config import Settings
from src.llm.prompts import SYSTEM_PROMPT, format_user_prompt

logger = structlog.get_logger()


def _extract_json(text: str) -> str:
    """Strip markdown code fences if present, returning raw JSON."""
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        return match.group(1).strip()
    return text.strip()


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    async def analyze(self, title: str, content: str, max_chars: int = 3000) -> dict[str, Any]:
        """Send article to LLM, return parsed JSON with analysis results."""
        ...


class OpenAIProvider(LLMProvider):
    """OpenAI-based LLM provider."""

    def __init__(self, api_key: str, model_id: str = "gpt-4o-mini") -> None:
        self._client = openai.AsyncOpenAI(api_key=api_key)
        self._model = model_id

    async def analyze(self, title: str, content: str, max_chars: int = 3000) -> dict[str, Any]:
        response = await self._client.chat.completions.create(
            model=self._model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": format_user_prompt(title, content, max_chars)},
            ],
            temperature=0.1,
        )
        text = response.choices[0].message.content or "{}"
        result: dict[str, Any] = orjson.loads(text)
        return result


class AnthropicProvider(LLMProvider):
    """Anthropic-based LLM provider."""

    def __init__(self, api_key: str, model_id: str = "claude-sonnet-4-20250514") -> None:
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model = model_id

    async def analyze(self, title: str, content: str, max_chars: int = 3000) -> dict[str, Any]:
        response = await self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": format_user_prompt(title, content, max_chars)},
            ],
        )
        text = response.content[0].text  # type: ignore[union-attr]
        result: dict[str, Any] = orjson.loads(text)
        return result


class BedrockProvider(LLMProvider):
    """AWS Bedrock-based LLM provider using the Converse API."""

    def __init__(
        self,
        model_id: str = "anthropic.claude-sonnet-4-20250514-v1:0",
        region: str = "us-east-1",
    ) -> None:
        self._client = boto3.client("bedrock-runtime", region_name=region)
        self._model = model_id

    async def analyze(self, title: str, content: str, max_chars: int = 3000) -> dict[str, Any]:
        user_prompt = format_user_prompt(title, content, max_chars)

        response = await asyncio.to_thread(
            self._client.converse,
            modelId=self._model,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[
                {"role": "user", "content": [{"text": user_prompt}]},
            ],
            inferenceConfig={"maxTokens": 1024, "temperature": 0.1},
        )

        text = response["output"]["message"]["content"][0]["text"]
        result: dict[str, Any] = orjson.loads(_extract_json(text))
        return result


def create_provider(settings: Settings) -> LLMProvider:
    """Factory function to create the appropriate LLM provider."""
    provider = settings.llm_provider.lower()
    if provider == "openai":
        return OpenAIProvider(settings.openai_api_key, settings.llm_model_id)
    if provider == "anthropic":
        return AnthropicProvider(settings.anthropic_api_key, settings.llm_model_id)
    if provider == "bedrock":
        return BedrockProvider(settings.llm_model_id, settings.aws_region)
    raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")
