"""
DSR|RIECT — Cloud LLM Client
Claude / Gemini / ChatGPT via user-provided API keys (from settings)
"""

import logging
from typing import AsyncIterator

from settings.settings_store import get_llm_key

logger = logging.getLogger(__name__)


async def generate_claude(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> str:
    """Generate using Anthropic Claude (claude-sonnet-4-6)."""
    api_key = get_llm_key("claude")
    if not api_key:
        raise ValueError("Claude API key not configured in settings")

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)
        message = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return message.content[0].text
    except ImportError:
        raise RuntimeError("anthropic package not installed")
    except Exception as e:
        logger.error(f"Claude generate failed: {e}")
        raise


async def generate_claude_stream(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> AsyncIterator[str]:
    """Stream response from Claude."""
    api_key = get_llm_key("claude")
    if not api_key:
        raise ValueError("Claude API key not configured")

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)
        async with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        ) as stream:
            async for text in stream.text_stream:
                yield text
    except ImportError:
        raise RuntimeError("anthropic package not installed")
    except Exception as e:
        logger.error(f"Claude stream failed: {e}")
        raise


async def generate_gemini(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> str:
    """Generate using Google Gemini."""
    api_key = get_llm_key("gemini")
    if not api_key:
        raise ValueError("Gemini API key not configured in settings")

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            "gemini-1.5-pro",
            system_instruction=system_prompt,
        )
        response = model.generate_content(
            user_prompt,
            generation_config=genai.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            ),
        )
        return response.text
    except ImportError:
        raise RuntimeError("google-generativeai package not installed")
    except Exception as e:
        logger.error(f"Gemini generate failed: {e}")
        raise


async def generate_openai(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> str:
    """Generate using OpenAI ChatGPT."""
    api_key = get_llm_key("openai")
    if not api_key:
        raise ValueError("OpenAI API key not configured in settings")

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.choices[0].message.content
    except ImportError:
        raise RuntimeError("openai package not installed")
    except Exception as e:
        logger.error(f"OpenAI generate failed: {e}")
        raise


async def generate_openai_stream(
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> AsyncIterator[str]:
    """Stream response from OpenAI."""
    api_key = get_llm_key("openai")
    if not api_key:
        raise ValueError("OpenAI API key not configured")

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        stream = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )
        async for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta
    except ImportError:
        raise RuntimeError("openai package not installed")
    except Exception as e:
        logger.error(f"OpenAI stream failed: {e}")
        raise
