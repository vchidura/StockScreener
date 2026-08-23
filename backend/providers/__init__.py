"""Provider abstraction for market-data ingestion (Yahoo, Twelve Data, Polygon).

All providers implement the PriceProvider interface so run_scheduler.py and
the standalone update_*.py scripts can select a provider without branching
on provider name at every call site.
"""

from .base import PriceProvider
from .yahoo_provider import YahooProvider
from .twelvedata_provider import TwelveDataProvider
from .polygon_provider import PolygonProvider

_PROVIDERS = {
    "yahoo": YahooProvider,
    "twelvedata": TwelveDataProvider,
    "polygon": PolygonProvider,
}


def get_provider(name: str) -> PriceProvider:
    """Return a PriceProvider instance for the given provider name."""
    try:
        return _PROVIDERS[name]()
    except KeyError:
        raise ValueError(
            f"Unknown provider '{name}'. Choices: {', '.join(_PROVIDERS)}"
        )


__all__ = ["PriceProvider", "YahooProvider", "TwelveDataProvider", "PolygonProvider", "get_provider"]
