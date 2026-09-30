"""Unified exception hierarchy for cryptocmd.

Parsing failures, network timeouts and cache misses all collapse to a single
:class:`DataUnavailableError` type so callers only have to catch one thing.
Every raise site uses ``raise DataUnavailableError(...) from original`` to keep
the root cause available via ``__cause__``.
"""


class CryptoCmdError(Exception):
    """Base class for every error raised by cryptocmd."""


class DataUnavailableError(CryptoCmdError):
    """Raised when data for a symbol cannot be obtained from any source.

    Covers three situations that callers treat identically:

    * parsing / malformed remote payloads,
    * network timeouts and exhausted retries,
    * cache misses when no fresh data can be fetched.

    ``reason`` is a short machine-readable label
    (``"parse"`` | ``"network"`` | ``"cache"`` | ``"storage"`` | ``"empty"``).
    """

    def __init__(self, symbol, message, reason="unknown", cause=None):
        self.symbol = symbol
        self.reason = reason
        if cause is not None and cause not in (message,):
            message = "{}: {}".format(message, cause)
        super().__init__(message)


__all__ = ["CryptoCmdError", "DataUnavailableError"]
