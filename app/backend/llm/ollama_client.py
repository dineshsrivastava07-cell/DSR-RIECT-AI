"""
DSR|RIECT — Ollama Client
Local LLM via Ollama HTTP API — auto-detects available models
"""

import asyncio
import json
import logging
from typing import AsyncIterator, Optional

import httpx

from config import OLLAMA_BASE_URL

logger = logging.getLogger(__name__)

# Preferred model order (first match wins)
# qwen3-coder:480b-cloud (cloud-backed, 3000 tok/s) is preferred over local 7B models
PREFERRED_MODEL_ORDER = [
    "qwen3", "qwen2.5", "llama3", "llama3.1", "llama3.2",
    "mistral", "gemma3", "gemma2", "phi3", "phi3.5",
    "deepseek", "codellama", "vicuna",
]

_available_models_cache: Optional[list] = None


async def get_available_models() -> list[str]:
    """Return list of available Ollama model names (full names with tags)."""
    global _available_models_cache
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            if resp.status_code == 200:
                data = resp.json()
                models = [m["name"] for m in data.get("models", [])]
                _available_models_cache = models
                return models
    except Exception:
        pass
    return _available_models_cache or []


async def get_best_model(preferred: str = None) -> Optional[str]:
    """
    Find the best available Ollama model.
    Tries: preferred → PREFERRED_MODEL_ORDER → first available.
    """
    available = await get_available_models()
    if not available:
        return None

    # Build base names (strip tags) for matching
    bases = {m.split(":")[0].lower(): m for m in available}

    # 1. Try exact preferred match
    if preferred:
        pref_lower = preferred.lower().split(":")[0]
        if pref_lower in bases:
            return bases[pref_lower]
        # Prefix match
        for base, full in bases.items():
            if base.startswith(pref_lower) or pref_lower.startswith(base.split("-")[0]):
                return full

    # 2. Try preferred order
    for pref in PREFERRED_MODEL_ORDER:
        for base, full in bases.items():
            if base.startswith(pref) or pref.startswith(base.split(".")[0].split("-")[0]):
                return full

    # 3. Return first available
    return available[0] if available else None


async def is_available(model: str = None) -> tuple[bool, str]:
    """
    Check if Ollama is running and resolve the model to use.
    Returns (available: bool, resolved_model: str)
    """
    resolved = await get_best_model(model)
    return (resolved is not None), (resolved or "")


async def generate(
    system_prompt: str,
    user_prompt: str,
    model: str = None,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> str:
    """Generate text using Ollama. Auto-resolves model. Returns full response."""
    use_model = await get_best_model(model)
    if not use_model:
        raise RuntimeError("No Ollama models available")

    logger.info(f"Ollama generate: model={use_model}")

    payload = {
        "model": use_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
        },
    }

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(f"{OLLAMA_BASE_URL}/api/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "")


async def generate_stream(
    system_prompt: str,
    user_prompt: str,
    model: str = None,
    max_tokens: int = 2000,
    temperature: float = 0.7,
) -> AsyncIterator[str]:
    """Stream response via Ollama. Auto-resolves model. Yields token chunks."""
    use_model = await get_best_model(model)
    if not use_model:
        raise RuntimeError("No Ollama models available")

    logger.info(f"Ollama stream: model={use_model}")

    payload = {
        "model": use_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": True,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
        },
    }

    async with httpx.AsyncClient(timeout=120) as client:
        async with client.stream("POST", f"{OLLAMA_BASE_URL}/api/chat", json=payload) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if line.strip():
                    try:
                        chunk = json.loads(line)
                        token = chunk.get("message", {}).get("content", "")
                        if token:
                            yield token
                        if chunk.get("done"):
                            break
                    except json.JSONDecodeError:
                        continue
