from __future__ import annotations

from options.config import DataEngine, OptionRuntimeConfiguration
from options.repositories.base import ConnectionFactory
from options.repositories.ingestion import OptionIngestionRepository
from options.repositories.trades import OptionTradeRepository

from .base import BaseDataEngine
from .polygon_developer import PolygonDeveloperEngine
from .polygon_http import PolygonHttpTransport


def build_data_engine(
    configuration: OptionRuntimeConfiguration,
    *,
    connection_factory: ConnectionFactory | None = None,
    transport: PolygonHttpTransport | None = None,
) -> BaseDataEngine:
    if configuration.settings.data_engine is DataEngine.POLYGON_DEVELOPER:
        return PolygonDeveloperEngine(
            configuration,
            OptionIngestionRepository(connection_factory),
            OptionTradeRepository(connection_factory),
            transport=transport,
        )
    raise RuntimeError("Polygon Advanced data engine is reserved but not implemented")