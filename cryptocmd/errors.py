"""Unified exception type for the cryptocmd collection pipeline."""


class CryptoDataError(Exception):
    """解析失败、网络超时、缓存未命中等错误的统一出口。

    所有底层原因都通过 ``raise ... from`` 保留在 ``__cause__`` 上。
    """
