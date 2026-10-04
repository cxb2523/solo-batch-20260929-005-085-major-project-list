"""cryptocmd: 常驻加密货币行情采集服务。

``CryptoData`` 是对外的稳定入口: 公开方法与返回结构保持不变,
老调用方无需改动。服务化能力见 :mod:`cryptocmd.service`。
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from datetime import datetime, timezone

from .errors import CryptoDataError

__all__ = ["CryptoData", "CryptoDataError"]
__version__ = "1.0.0"

_USER_AGENT = "cryptocmd/1.0"

# CoinGecko 使用 coin id 而非 symbol, 常见 symbol 的兜底映射。
_COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "USDT": "tether",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "TRX": "tron",
    "LTC": "litecoin",
}


class CryptoData:
    """按 symbol 抓取日线历史行情。

    公开方法:
      - ``fetch_raw()``   -> (source, payload)
      - ``get_history()`` -> list[dict], 取不到数据返回空列表而非 None

    解析失败 / 网络超时统一抛出 :class:`CryptoDataError`, 并用
    ``raise ... from`` 保留原始原因。
    """

    def __init__(self, symbol, fiat="USD", days=30, timeout=10.0, opener=None):
        self.symbol = str(symbol).upper()
        self.fiat = str(fiat).upper()
        self.days = int(days)
        self.timeout = float(timeout)
        self._opener = opener  # 可注入: callable(url, timeout) -> bytes

    def _endpoints(self):
        yield "binance", (
            "https://api.binance.com/api/v3/klines?symbol=%sUSDT&interval=1d&limit=%d"
            % (self.symbol, self.days)
        )
        coin_id = _COINGECKO_IDS.get(self.symbol)
        if coin_id:
            yield "coingecko", (
                "https://api.coingecko.com/api/v3/coins/%s/market_chart"
                "?vs_currency=%s&days=%d" % (coin_id, self.fiat.lower(), self.days)
            )

    def _http_get(self, url):
        try:
            if self._opener is not None:
                return self._opener(url, self.timeout)
            req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
            raise CryptoDataError("network error fetching %s: %s" % (url, exc)) from exc

    def fetch_raw(self):
        """依次尝试各数据源, 返回 (source, payload)。全部失败抛 CryptoDataError。"""
        errors = []
        for source, url in self._endpoints():
            try:
                body = self._http_get(url)
            except CryptoDataError as exc:
                errors.append(exc)
                continue
            try:
                return source, json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as exc:
                errors.append(CryptoDataError("invalid JSON from %s" % source))
                raise CryptoDataError("invalid JSON from %s" % source) from exc
        if errors:
            raise CryptoDataError(
                "all endpoints failed for %s" % self.symbol
            ) from errors[-1]
        raise CryptoDataError("no endpoint available for %s" % self.symbol)

    def get_history(self):
        """返回 list[dict] 历史行情; 取不到数据返回空列表而非 None。"""
        source, payload = self.fetch_raw()
        try:
            if source == "binance":
                rows = [self._row_from_kline(k) for k in payload]
            else:
                rows = self._rows_from_coingecko(payload)
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise CryptoDataError(
                "failed to parse %s payload for %s" % (source, self.symbol)
            ) from exc
        return rows or []

    @staticmethod
    def _row_from_kline(kline):
        return {
            "date": datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
            "volume": float(kline[5]),
        }

    @staticmethod
    def _rows_from_coingecko(payload):
        volumes = {int(v[0] // 86400000): float(v[1]) for v in payload.get("total_volumes", [])}
        rows = []
        for ts, price in payload["prices"]:
            day = int(ts // 86400000)
            rows.append({
                "date": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
                "open": float(price),
                "high": float(price),
                "low": float(price),
                "close": float(price),
                "volume": volumes.get(day, 0.0),
            })
        return rows
