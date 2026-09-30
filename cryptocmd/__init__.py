"""cryptocmd — resident cryptocurrency history collection.

Public surface (backwards compatible):

* :class:`CryptoData`          — per-symbol fetch/cache/persist entry point.
* :class:`CmcParser`           — default CoinMarketCap parser.
* :class:`SQLiteAdapter`       — default transactional SQLite storage.
* :class:`CollectionService`   — background refresh + /status endpoint.
* :class:`DataUnavailableError`— unified error for parse/network/cache misses.

Old callers keep using ``CryptoData("BTC").get_data()`` unchanged.
"""

from .__version__ import __version__
from .adapter import InMemoryAdapter, SQLiteAdapter, StorageAdapter
from .core import HEADERS, CryptoData
from .exceptions import CryptoCmdError, DataUnavailableError
from .parser import CmcParser, Parser
from .service import CollectionService

__all__ = [
    "__version__",
    "CryptoData",
    "CmcParser",
    "Parser",
    "SQLiteAdapter",
    "InMemoryAdapter",
    "StorageAdapter",
    "CollectionService",
    "DataUnavailableError",
    "CryptoCmdError",
    "HEADERS",
]
