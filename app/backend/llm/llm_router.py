"""
DSR|RIECT — LLM Router
Routes LLM calls: Qwen (email-auth) → Ollama → Claude → Gemini → OpenAI
Qwen3.5-Plus is the default cloud model.
"""

import logging
from typing import AsyncIterator, Optional

from llm import ollama_client, cloud_client
from settings.settings_store import get_default_llm, get_llm_key

logger = logging.getLogger(__name__)

CLOUD_PROVIDERS = ["qwen", "claude", "gemini", "openai"]

# Qwen sub-model identifiers (user-selectable in UI)
QWEN_MODEL_IDS = {"qwen3.5-plus", "qwen3.5-flash", "qwen3-max", "qwen"}


class LLMRouter:
    def __init__(self, preferred_model: str = None):
        self.preferred_model = preferred_model or get_default_llm()

    async def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> str:
        """Generate response with fallback chain."""
        errors = []
        model = self.preferred_model

        # ── Qwen — always first if configured (default model) ──────────
        from llm import qwen_client
        _qwen_tried = False
        if qwen_client.is_configured() and model not in {"claude", "gemini", "openai"}:
            _qwen_tried = True
            try:
                qwen_model = model if model in qwen_client.QWEN_MODELS else qwen_client.get_model()
                result = await qwen_client.generate(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    model=qwen_model,
                )
                logger.info(f"LLM via Qwen/{qwen_model}")
                return result
            except Exception as e:
                errors.append(f"Qwen: {e}")
                logger.warning(f"Qwen failed: {e}")

        # ── Ollama — fallback when Qwen fails or model is not a cloud provider ──
        # NOTE: also tried when model is a Qwen ID but Qwen failed (_qwen_tried)
        if model not in CLOUD_PROVIDERS:
            try:
                available, resolved = await ollama_client.is_available(model)
                if available:
                    result = await ollama_client.generate(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        model=resolved,
                        max_tokens=max_tokens,
                        temperature=temperature,
                    )
                    logger.info(f"LLM via Ollama/{resolved}")
                    return result
                else:
                    errors.append("Ollama: no models available")
            except Exception as e:
                errors.append(f"Ollama: {e}")
                logger.warning(f"Ollama failed: {e}")

        # ── Claude ─────────────────────────────────────────────────────
        if model == "claude" or (model not in CLOUD_PROVIDERS and get_llm_key("claude")):
            try:
                result = await cloud_client.generate_claude(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                logger.info("LLM via Claude")
                return result
            except Exception as e:
                errors.append(f"Claude: {e}")

        # ── Gemini ─────────────────────────────────────────────────────
        if model == "gemini" or (model not in CLOUD_PROVIDERS and get_llm_key("gemini")):
            try:
                result = await cloud_client.generate_gemini(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                logger.info("LLM via Gemini")
                return result
            except Exception as e:
                errors.append(f"Gemini: {e}")

        # ── OpenAI ─────────────────────────────────────────────────────
        if model == "openai" or (model not in CLOUD_PROVIDERS and get_llm_key("openai")):
            try:
                result = await cloud_client.generate_openai(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                logger.info("LLM via OpenAI")
                return result
            except Exception as e:
                errors.append(f"OpenAI: {e}")

        raise RuntimeError(f"All LLM providers failed: {'; '.join(errors)}")

    async def stream(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.7,
    ) -> AsyncIterator[str]:
        """Stream response with fallback chain."""
        model = self.preferred_model
        errors = []

        # ── Qwen streaming — always first if configured (default model) ─
        from llm import qwen_client
        _qwen_tried = False
        if qwen_client.is_configured() and model not in {"claude", "gemini", "openai"}:
            _qwen_tried = True
            try:
                qwen_model = model if model in qwen_client.QWEN_MODELS else qwen_client.get_model()
                async for chunk in qwen_client.generate_stream(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    model=qwen_model,
                ):
                    yield chunk
                return
            except Exception as e:
                errors.append(f"Qwen: {e}")
                logger.warning(f"Qwen stream failed: {e}")

        # ── Ollama streaming — fallback when Qwen fails or model is local ─
        # NOTE: also tried when model is a Qwen ID but Qwen failed (_qwen_tried)
        if model not in CLOUD_PROVIDERS:
            try:
                available, resolved = await ollama_client.is_available(model)
                if available:
                    async for chunk in ollama_client.generate_stream(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        model=resolved,
                        max_tokens=max_tokens,
                        temperature=temperature,
                    ):
                        yield chunk
                    return
                else:
                    errors.append("Ollama: no models available")
            except Exception as e:
                errors.append(f"Ollama: {e}")

        # ── Claude streaming ───────────────────────────────────────────
        if model == "claude" or (model not in CLOUD_PROVIDERS and get_llm_key("claude")):
            try:
                async for chunk in cloud_client.generate_claude_stream(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                ):
                    yield chunk
                return
            except Exception as e:
                errors.append(f"Claude: {e}")

        # ── Gemini streaming ───────────────────────────────────────────
        if model == "gemini" or (model not in CLOUD_PROVIDERS and get_llm_key("gemini")):
            try:
                result = await cloud_client.generate_gemini(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                yield result
                return
            except Exception as e:
                errors.append(f"Gemini: {e}")

        # ── OpenAI streaming ───────────────────────────────────────────
        if model == "openai" or (model not in CLOUD_PROVIDERS and get_llm_key("openai")):
            try:
                async for chunk in cloud_client.generate_openai_stream(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                ):
                    yield chunk
                return
            except Exception as e:
                errors.append(f"OpenAI: {e}")

        raise RuntimeError(f"All LLM providers failed: {'; '.join(errors)}")


_router: Optional[LLMRouter] = None


def get_router(preferred_model: str = None) -> LLMRouter:
    global _router
    if preferred_model:
        return LLMRouter(preferred_model)
    if _router is None:
        _router = LLMRouter()
    return _router
