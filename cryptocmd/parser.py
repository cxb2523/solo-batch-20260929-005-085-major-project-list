"""Parser abstraction and the default CoinMarketCap parser.

A *parser* turns a symbol (and optional date range / fiat) into a list of
history records. Each record is a dict keyed by the public column names in
:data:`HEADERS`. The service is constructed with any object implementing the
:class:`Parser` protocol; the default :class:`CmcParser` preserves the
historical CoinMarketCap behaviour of the package.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import requests

from .exceptions import DataUnavailableError

_CMC_DATA_API = "https://api.coinmarketcap.com/data-api/v3"

with open(os.path.join(os.path.dirname(__file__), "fiat_ids.json")) as _f:
    _FIAT_IDS = json.load(_f)

_SUPPORTED_FIATS = ", ".join(sorted(_FIAT_IDS))

_HEADERS = {
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Market Cap",
    "Time Open",
    "Time High",
    "Time Low",
    "Time Close",
}

# Canonical column order, identical to the legacy package output.
HEADERS: List[str] = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Market Cap",
    "Time Open",
    "Time High",
    "Time Low",
    "Time Close",
]


@runtime_checkable
class Parser(Protocol):
    """Anything that can fetch history rows for a symbol."""

    def fetch(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Return newest-first history records for *symbol*.

        Raises :class:`DataUnavailableError` on network / parse failure.
        """
        ...


def _parse_rows(quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _row in quotes:
        quote = _row["quote"]
        date = _dt.datetime.strptime(
            _row["timeOpen"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).strftime("%d-%m-%Y")
        rows.append(
            {
                "Date": date,
                "Open": quote["open"],
                "High": quote["high"],
                "Low": quote["low"],
                "Close": quote["close"],
                "Volume": quote["volume"],
                "Market Cap": quote["marketCap"],
                "Time Open": _row["timeOpen"],
                "Time High": _row["timeHigh"],
                "Time Low": _row["timeLow"],
                "Time Close": _row["timeClose"],
            }
        )
    return rows


class CmcParser:
    """Fetch daily OHLCV history from coinmarketcap.com.

    :param timeout: per-request connect/read timeout in seconds.
    :param fiat: fiat currency code, e.g. ``"USD"`` or ``"EUR"``.
    :param coin_name: disambiguation when several coins share a code.
    :param id_number: explicit CMC numeric id (overrides symbol lookup).
    :param session: optional injected ``requests.Session`` (testing).
    """

    def __init__(
        self,
        timeout: float = 8.0,
        fiat: str = "USD",
        coin_name: Optional[str] = None,
        id_number: Optional[int] = None,
        session: Optional[requests.Session] = None,
    ):
        self.timeout = timeout
        self.fiat = fiat
        self.coin_name = coin_name
        self.id_number = id_number
        self._session = session or requests.Session()
        self._session.headers.setdefault("User-Agent", "Mozilla/5.0")

    def _get_json(self, url: str) -> Dict[str, Any]:
        try:
            response = self._session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            raise DataUnavailableError(
                self.symbol, "network failure while talking to CoinMarketCap",
                reason="network", cause=exc,
            ) from exc

    def _coin_id(self, symbol: str) -> int:
        url = (
            "{}/cryptocurrency/listing?start=1&limit=5000"
            "&sortBy=market_cap&sortType=desc&convert=USD"
            "&cryptoType=all&tagType=all&audited=false"
        ).format(_CMC_DATA_API)
        data = self._get_json(url)
        try:
            error_code = str(data["status"]["error_code"])
            if error_code != "0":
                raise KeyError(data["status"].get("error_message", "api error"))
            coins = data["data"]["cryptoCurrencyList"]
            matches = [
                coin
                for coin in coins
                if coin["symbol"].upper() == symbol.upper()
                and (
                    self.coin_name is None
                    or coin["name"].lower() == self.coin_name.lower()
                )
            ]
            if not matches:
                raise KeyError("coin code unavailable")
            return matches[0]["id"]
        except (KeyError, TypeError) as exc:
            raise DataUnavailableError(
                symbol, "could not resolve CoinMarketCap coin id",
                reason="parse", cause=exc,
            ) from exc

    def fetch(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        self.symbol = symbol

        if start_date is None:
            start_date = "28-4-2013"
        if end_date is None:
            yesterday = _dt.date.today() - _dt.timedelta(days=1)
            end_date = yesterday.strftime("%d-%m-%Y")

        coin_id = self.id_number if self.id_number else self._coin_id(symbol)
        convert_id = _FIAT_IDS.get(self.fiat.upper())
        if convert_id is None:
            raise DataUnavailableError(
                symbol,
                "unknown fiat '{}'. Supported: {}".format(
                    self.fiat, _SUPPORTED_FIATS
                ),
                reason="parse",
            )

        try:
            start_dt = _dt.datetime.strptime(start_date, "%d-%m-%Y").replace(
                tzinfo=_dt.timezone.utc
            )
            end_dt = _dt.datetime.strptime(end_date, "%d-%m-%Y").replace(
                tzinfo=_dt.timezone.utc
            )
        except ValueError as exc:
            raise DataUnavailableError(
                symbol, "invalid date range", reason="parse", cause=exc
            ) from exc

        all_quotes: List[Dict[str, Any]] = []
        result_json: Optional[Dict[str, Any]] = None
        chunk_start = start_dt - _dt.timedelta(days=1)

        try:
            while chunk_start < end_dt:
                chunk_end = min(
                    chunk_start + _dt.timedelta(days=365), end_dt
                )
                url = (
                    "{}/cryptocurrency/historical"
                    "?id={}&convertId={}&timeStart={}&timeEnd={}"
                ).format(
                    _CMC_DATA_API,
                    coin_id,
                    convert_id,
                    int(chunk_start.timestamp()),
                    int(chunk_end.timestamp()),
                )
                page = self._get_json(url)
                status = page.get("status", {})
                error_code = status.get("error_code")
                if error_code and str(error_code) != "0":
                    raise requests.RequestException(
                        status.get("error_message", "Unknown error")
                    )
                if result_json is None:
                    result_json = page
                all_quotes.extend(page["data"]["quotes"])
                chunk_start = chunk_end + _dt.timedelta(days=1)
        except (KeyError, TypeError) as exc:
            raise DataUnavailableError(
                symbol, "malformed CoinMarketCap payload",
                reason="parse", cause=exc,
            ) from exc

        try:
            rows = _parse_rows(all_quotes)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataUnavailableError(
                symbol, "failed to parse CoinMarketCap history",
                reason="parse", cause=exc,
            ) from exc

        # newest date first, matching the legacy row ordering.
        rows.sort(
            key=lambda item: _dt.datetime.strptime(
                item["Date"], "%d-%m-%Y"
            ),
            reverse=True,
        )
        return rows


__all__ = ["Parser", "CmcParser", "HEADERS"]
