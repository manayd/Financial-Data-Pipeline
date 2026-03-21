"""Prompt templates for LLM financial news analysis."""

SYSTEM_PROMPT = (
    "You are a financial news analyst. Analyze the given article and return a JSON object with:\n"
    '- "summary": 2-3 sentence summary of the key points\n'
    '- "sentiment": one of "bullish", "bearish", "neutral", "mixed"\n'
    '- "sentiment_confidence": float 0.0-1.0\n'
    '- "entities": list of {"name": str, "entity_type": str, "relevance_score": float}\n'
    '  entity_type is one of: "company", "person", "sector", "index", "commodity", "currency"\n'
    '- "key_topics": list of 3-5 topic strings\n'
    "\n"
    "Return ONLY valid JSON, no markdown."
)


def format_user_prompt(title: str, content: str, max_chars: int = 3000) -> str:
    """Format the user prompt with article title and truncated content."""
    truncated = content[:max_chars]
    return f"Title: {title}\n\nContent: {truncated}"
