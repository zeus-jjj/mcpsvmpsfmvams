"""Regression tests for the intensive funnel deeplink and FSM config."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
os.environ.setdefault("BOT_TOKEN", "123456:ABCDEF")

from apps.funnels.links import (  # noqa: E402
    DEEPLINK_PAYLOAD_LIMIT,
    INTENSIVE_DEFAULT_PAYLOAD,
    build_intensive_payload,
)
from modules import available_funnels, normalize_funnel_name  # noqa: E402

FUNNEL_FILE = ROOT_DIR / "apps" / "funnels" / "intensive.json"


def _load_funnel() -> dict:
    return json.loads(FUNNEL_FILE.read_text(encoding="utf-8"))


def test_intensive_funnel_is_available() -> None:
    assert "intensive" in available_funnels()


def test_short_alias_is_supported() -> None:
    assert normalize_funnel_name("int") == "intensive"


def test_default_payload_fits_limit() -> None:
    assert len(INTENSIVE_DEFAULT_PAYLOAD) <= DEEPLINK_PAYLOAD_LIMIT
    assert INTENSIVE_DEFAULT_PAYLOAD.startswith("fn=int")


def test_builder_raises_for_long_payload() -> None:
    with pytest.raises(ValueError):
        build_intensive_payload(source="x" * 20, campaign="y" * 20)


def test_username_prompt_collects_text() -> None:
    funnel = _load_funnel()
    prompt = funnel["callback"]["doge2_username_prompt"]
    action = prompt["actions"][0]
    collect = action["collect_data"][0]
    assert action["func"] == "start_fsm"
    assert collect["name"] == "intensive_username"
    assert collect["expected_data"] == "text"
    assert action["if_collected"] == "return_ok"


def test_username_done_marks_funnel_passed() -> None:
    funnel = _load_funnel()
    actions = funnel["callback"]["doge2_username_done"].get("actions", [])
    assert any(item.get("func") == "funnel_passed" for item in actions)
