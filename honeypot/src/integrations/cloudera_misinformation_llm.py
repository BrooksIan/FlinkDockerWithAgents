"""
Use Cloudera AI Inference (OpenAI-compatible /v1/chat/completions) to build honeypot
misinformation payloads for defensive counter-attacks.

Only for authorized honeypot / deception contexts. Does not call Cowrie; callers apply
``fake_data`` via ``feed_attacker_disinformation`` (or the combined ReAct tool).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict


def _strip_json_fence(text: str) -> str:
    text = text.strip()
    m = re.match(r"^```(?:json)?\s*([\s\S]*?)\s*```$", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text


def generate_misinformation_fake_data(
    honeypot_context: str,
    *,
    max_tokens: int = 600,
    temperature: float = 0.7,
) -> Dict[str, Any]:
    """
    Ask the hosted LLM for a JSON object suitable as ``fake_data`` for honeypot disinformation.

    Expected JSON shape (model must comply):
      {"type": str, "files": [str, ...], "fake_command_outputs": {cmd: output}, "rationale": str}

    ``honeypot_context`` should summarize attacker-visible activity (commands, event types).
    """
    try:
        from openai import OpenAI
    except ImportError as e:
        return {
            "type": "fake_filesystem",
            "files": ["README.txt", "notes/decoy.log"],
            "llm_generated": False,
            "error": f"openai package missing: {e}",
        }

    try:
        from cloudera_llm_config import get_cloudera_config, validate_config
    except ImportError:
        base = (os.getenv("CLOUDERA_AI_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "").strip().rstrip("/")
        token = (
            os.getenv("CLOUDERA_JWT_TOKEN")
            or os.getenv("CLOUDERA_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or ""
        ).strip()
        model = (
            os.getenv("CLOUDERA_MODEL_ID")
            or os.getenv("CLOUDERA_MODEL_NAME")
            or os.getenv("OPENAI_MODEL")
            or "NousResearch/Hermes-3-Llama-3.1-8B"
        ).strip()
        cfg = {"base_url": base, "api_key": token, "model_id": model}
    else:
        cfg = get_cloudera_config()
        if not validate_config(cfg):
            return {
                "type": "fake_filesystem",
                "files": ["var/log/auth.log"],
                "llm_generated": False,
                "error": "Cloudera / OpenAI-compatible config incomplete (set CLOUDERA_AI_BASE_URL and CLOUDERA_JWT_TOKEN or use cloudera_llm_config.py).",
            }

    base_url = cfg["base_url"].strip().rstrip("/")
    api_key = cfg["api_key"].strip()
    model = cfg["model_id"].strip()

    system = (
        "You generate defensive honeypot misinformation only: plausible-but-fake filesystem "
        "listings and optional fake shell command outputs to slow or mislead unauthorized intruders. "
        "Do not include real secrets, real credentials, or instructions to harm real systems. "
        "Respond with a single JSON object only, no markdown, no prose. Keys: "
        '"type" (string, e.g. "fake_filesystem"), '
        '"files" (array of plausible decoy filenames), '
        '"fake_command_outputs" (object mapping command string to fake stdout string, optional), '
        '"rationale" (one short sentence why this decoy fits the context).'
    )
    user = (
        "Honeypot session context (summarized from logs):\n"
        f"{honeypot_context[:8000]}\n\n"
        "Return JSON only."
    )

    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        raw = (resp.choices[0].message.content or "").strip()
        if not raw:
            raise ValueError("empty completion")
        parsed = json.loads(_strip_json_fence(raw))
        if not isinstance(parsed, dict):
            raise ValueError("model did not return a JSON object")
        parsed.setdefault("type", "fake_filesystem")
        parsed.setdefault("files", ["decoy.txt"])
        parsed["llm_generated"] = True
        parsed["model"] = model
        return parsed
    except Exception as e:
        return {
            "type": "fake_filesystem",
            "files": ["lost+found/", "tmp/.X11-unix"],
            "llm_generated": False,
            "error": str(e)[:500],
        }
