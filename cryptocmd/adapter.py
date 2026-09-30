"""Storage adapters: where fetched history is persisted.

A *storage adapter* is a small protocol with two methods:

``load(symbol) -> list[dict]``
    Return stored records (newest-first), or ``[]`` when nothing is stored.
``save(symbol, rows) -> None``
    Atomically replace the stored history for *symbol*. A failed write must
    roll back and leave the previous history intact -- never half-written.

The default :class:`SQLiteAdapter` satisfies that with a single
delete-then-insert inside one transaction; any error triggers ``ROLLBACK``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Protocol, runtime_checkable

from .exceptions import DataUnavailableError
from .parser import HEADERS

_DEFAULT_DB = os.path.join(
    os.path.expanduser("~"), ".cryptocmd", "cryptocmd.db"
)


@runtime_checkable
class StorageAdapter(Protocol):
    def load(self, symbol: str) -> List[Dict[str, Any]]: ...

    def save(self, symbol: str, rows: List[Dict[str, Any]]) -> None: ...


class SQLiteAdapter:
    """Persist history rows in a SQLite database.

    Each symbol keeps a JSON blob of rows inside a versioned table. ``save``
    wraps the replacement in a transaction, so a crash or error rolls back to
    the old blob.
    """

    def __init__(self, path: str = _DEFAULT_DB):
        self.path = path
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            os.makedirs(directory, exist_ok=True)
        # check_same_thread=False is safe because every call owns its
        # connection; the class-level lock additionally serialises writers.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_table()

    def _create_table(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS history (
                    symbol TEXT PRIMARY KEY,
                    rows_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    def load(self, symbol: str) -> List[Dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT rows_json FROM history WHERE symbol = ?",
                (symbol.upper(),),
            )
            record = cursor.fetchone()
        if record is None:
            return []
        try:
            rows = json.loads(record[0])
            if not isinstance(rows, list):
                raise ValueError("stored payload is not a list")
            return rows
        except (ValueError, TypeError) as exc:
            raise DataUnavailableError(
                symbol, "stored history is corrupt",
                reason="storage", cause=exc,
            ) from exc

    def save(self, symbol: str, rows: List[Dict[str, Any]]) -> None:
        symbol = symbol.upper()
        payload = json.dumps(rows, default=str)
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute(
                    "INSERT INTO history(symbol, rows_json, updated_at) "
                    "VALUES (?, ?, strftime('%s','now')) "
                    "ON CONFLICT(symbol) DO UPDATE SET "
                    "rows_json = excluded.rows_json, "
                    "updated_at = excluded.updated_at",
                    (symbol, payload),
                )
                self._conn.execute("COMMIT")
            except Exception as exc:
                # Explicit rollback: the old blob survives untouched.
                self._conn.execute("ROLLBACK")
                raise DataUnavailableError(
                    symbol, "history write failed; rolled back to old data",
                    reason="storage", cause=exc,
                ) from exc

    def symbols(self) -> List[str]:
        with self._lock:
            cursor = self._conn.execute("SELECT symbol FROM history")
            return [row[0] for row in cursor.fetchall()]

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class InMemoryAdapter:
    """A dict-backed adapter handy for tests and ephemeral runs."""

    def __init__(self):
        self._data: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()

    def load(self, symbol: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._data.get(symbol.upper(), [])]

    def save(self, symbol: str, rows: List[Dict[str, Any]]) -> None:
        with self._lock:
            self._data[symbol.upper()] = [dict(row) for row in rows]

    def symbols(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())

    def close(self) -> None:
        with self._lock:
            self._data.clear()


__all__ = ["StorageAdapter", "SQLiteAdapter", "InMemoryAdapter", "HEADERS"]
