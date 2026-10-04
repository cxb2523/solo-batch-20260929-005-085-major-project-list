"""Parser 抽象: 把 symbol 解析成历史行情行。

默认实现 :class:`CryptoDataParser` 沿用 ``cryptocmd/__init__.py`` 中
``CryptoData`` 的现有抓取与解析行为。
"""

from . import CryptoData
from .errors import CryptoDataError

__all__ = ["CryptoDataParser"]


class CryptoDataParser:
    """默认 parser: 委托给 CryptoData, 行为与返回结构完全一致。"""

    def __init__(self, fiat="USD", days=30, timeout=10.0, crypto_factory=None):
        if crypto_factory is not None:
            self._factory = crypto_factory
        else:
            self._factory = lambda symbol: CryptoData(
                symbol, fiat=fiat, days=days, timeout=timeout
            )

    def parse(self, symbol):
        """返回 list[dict]; 任何底层失败都收敛为 CryptoDataError。"""
        try:
            rows = self._factory(str(symbol).upper()).get_history()
        except CryptoDataError:
            raise
        except Exception as exc:
            raise CryptoDataError("parse failed for %s" % symbol) from exc
        return rows if rows is not None else []
