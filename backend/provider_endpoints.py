from __future__ import annotations

from urllib.parse import urlsplit

# Vendor documentation (checked 2026-10-12) names these hosts. The previous
# polygon.io hosts are still accepted for provider-returned URLs only.
MASSIVE_REST_HOST = "api.massive.com"
LEGACY_REST_HOST = "api.polygon.io"
REST_HOSTS = frozenset({MASSIVE_REST_HOST, LEGACY_REST_HOST})
REST_BASE_URL = f"https://{MASSIVE_REST_HOST}"

STOCKS_REALTIME_WS_URL = "wss://socket.massive.com/stocks"
OPTIONS_REALTIME_WS_URL = "wss://socket.massive.com/options"
STOCKS_DELAYED_WS_URL = "wss://delayed.massive.com/stocks"
OPTIONS_DELAYED_WS_URL = "wss://delayed.massive.com/options"


def is_rest_url(url: str) -> bool:
    parsed = urlsplit(url)
    return (
        parsed.scheme == "https"
        and parsed.hostname is not None
        and parsed.hostname.lower() in REST_HOSTS
        and parsed.username is None
        and parsed.password is None
    )
