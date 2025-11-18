"""Helper utilities for building deeplink payloads for funnels."""

from __future__ import annotations

DEEPLINK_PAYLOAD_LIMIT = 36
_DEFAULT_ALIAS = "int"
_DEFAULT_SOURCE = "tgads"
_DEFAULT_CAMPAIGN = "wbint"
_DEFAULT_MEDIUM = "bot"


def build_intensive_payload(
    *,
    source: str = _DEFAULT_SOURCE,
    campaign: str = _DEFAULT_CAMPAIGN,
    medium: str | None = _DEFAULT_MEDIUM,
    term: str | None = None,
    content: str | None = None,
) -> str:
    """Return a start payload for the intensive funnel within the TG Ads limit."""

    pairs: list[tuple[str, str]] = [("fn", _DEFAULT_ALIAS)]
    for key, value in (
        ("s", source),
        ("ca", campaign),
        ("m", medium),
        ("t", term),
        ("co", content),
    ):
        if value:
            pairs.append((key, value))

    payload = "-".join(f"{key}={value}" for key, value in pairs)
    if len(payload) > DEEPLINK_PAYLOAD_LIMIT:
        raise ValueError(
            f"Payload '{payload}' exceeds {DEEPLINK_PAYLOAD_LIMIT} characters"
        )
    return payload


INTENSIVE_DEFAULT_PAYLOAD = build_intensive_payload()
