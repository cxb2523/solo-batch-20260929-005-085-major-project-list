"""Adapter 抽象: 历史行情持久化。

默认实现 :class:`SQLiteAdapter` 使用事务写入: 任何一步失败都会回滚,
旧历史保持原样, 绝不写坏。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time

from .errors import CryptoDataError

__all__ = ["SQLiteAdapter"]

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS history ("
    "symbol TEXT PRIMARY KEY, "
    "payload TEXT NOT NULL, "
    "updated_at REAL NOT NULL)"
)


class SQLiteAdapter:
    """把每个 symbol 的历史行情以 JSON 形式落 SQLite, 事务保证原子性。"""

    def __init__(self, path="cryptocmd_history.db"):
        self._path = str(path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.isolation_level = None  # 手动控制事务边界
        self._conn.execute(_SCHEMA)

    def read(self, symbol):
        """读取已存历史; 缓存未命中统一抛 CryptoDataError。"""
        symbol = str(symbol).upper()
        row = self._conn.execute(
            "SELECT payload FROM history WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row is None:
            raise CryptoDataError("cache miss: no stored history for %s" % symbol)
        try:
            return json.loads(row[0])
        except json.JSONDecodeError as exc:
            raise CryptoDataError("stored history for %s is corrupt" % symbol) from exc

    def write(self, symbol, rows):
        """事务化整体替换; 失败回滚到旧历史并抛 CryptoDataError。"""
        symbol = str(symbol).upper()
        payload = json.dumps(rows)
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("DELETE FROM history WHERE symbol = ?", (symbol,))
                self._conn.execute(
                    "INSERT INTO history (symbol, payload, updated_at) VALUES (?, ?, ?)",
                    (symbol, payload, time.time()),
                )
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                self._conn.execute("ROLLBACK")
                raise CryptoDataError(
                    "failed to persist history for %s; rolled back" % symbol
                ) from exc

    def snapshot(self, symbol):
        """返回旧历史的副本, 没有则返回 None。"""
        try:
            return self.read(symbol)
        except CryptoDataError:
            return None

    def restore(self, symbol, rows):
        """把旧历史写回, 用于写库失败后的兜底恢复。"""
        self.write(symbol, rows)

    def close(self):
        self._conn.close()
