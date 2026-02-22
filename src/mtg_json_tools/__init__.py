"""mtg-json-tools — DuckDB-backed query client for Magic: The Gathering card data."""

from .async_client import AsyncMtgJsonTools
from .client import MtgJsonTools

__all__ = ["AsyncMtgJsonTools", "MtgJsonTools"]
__version__ = "0.1.0"
