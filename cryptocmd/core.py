"""Resident collection service core.

:class:`CryptoData` is the public, backwards-compatible entry point for one
symbol. Old callers keep using ``get_data`` / ``get_dataframe`` /
``export_csv`` exactly as before; the resident :class:`~cryptocmd.service.CollectionService`
adds background refreshing, single-flight de-duplication and status reporting
on top of the same object.

Three guarantees are enforced here:

1. **One fetch, one write.** Concurrent requests for the same symbol share a
   single in-flight ``_Flight`` (including its exponential-backoff retry
   window); only a successful fetch writes to storage, once.
2. **Rollback safety.** Storage writes are transactional; a write failure
   raises and the previous history stays in place.
3. **One error type.** Parse errors, network timeouts and cache misses all
   surface as :class:`DataUnavailableError`, chained with ``raise ... from``.
   Successful responses always return a list -- never ``None``.
"""

from __future__ import annotations

import csv
import os
import threading
import time
import warnings
from typing import Any, Dict, List, Optional

from .adapter import SQLiteAdapter, StorageAdapter
from .exceptions import DataUnavailableError
from .parser import CmcParser, HEADERS, Parser


class _Flight:
    """A single coalesced fetch attempt shared by all waiting callers."""

    __slots__ = (
        "done",
        "attempt",
        "max_attempts",
        "started_at",
        "finished_at",
        "ok",
        "rows",
        "error",
        "stored",
    )

    def __init__(self, max_attempts: int):
        self.done = threading.Event()
        self.attempt = 0
        self.max_attempts = max_attempts
        self.started_at = time.monotonic()
        self.finished_at: Optional[float] = None
        self.ok = False
        self.rows: List[Dict[str, Any]] = []
        self.error: Optional[BaseException] = None
        self.stored = False

    @property
    def duration(self) -> float:
        end = self.finished_at if self.finished_at else time.monotonic()
        return max(0.0, end - self.started_at)


class CryptoData:
    """Fetch, cache and persist history for one symbol.

    :param symbol: coin code, e.g. ``"BTC"`` (legacy name ``coin_code``).
    :param start_date: optional ``dd-mm-yyyy`` start.
    :param end_date: optional ``dd-mm-yyyy`` end.
    :param parser: injected :class:`~cryptocmd.parser.Parser`.
    :param storage: injected :class:`~cryptocmd.adapter.StorageAdapter`.
    :param max_retries: retries *after* the first attempt on timeout/error.
    :param base_backoff: initial backoff seconds; doubles per retry.
    :param max_backoff: cap on the exponential backoff.
    """

    HEADERS = HEADERS

    def __init__(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parser: Optional[Parser] = None,
        storage: Optional[StorageAdapter] = None,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
        **_legacy: Any,
    ):
        if symbol is None:
            symbol = _legacy.get("coin_code")
        if symbol is None:
            raise DataUnavailableError(
                symbol="?", message="a symbol/coin_code is required",
                reason="parse",
            )
        self.symbol = str(symbol).upper()
        self.start_date = start_date
        self.end_date = end_date
        self.parser = parser if parser is not None else CmcParser()
        self.storage = storage if storage is not None else SQLiteAdapter()
        self.max_retries = max(0, int(max_retries))
        self.base_backoff = float(base_backoff)
        self.max_backoff = float(max_backoff)

        self._cond = threading.Condition()
        self._flight: Optional[_Flight] = None
        self._abort = threading.Event()
        self._rows: Optional[List[Dict[str, Any]]] = None
        self._loaded_from_cache = False

        # observability metrics surfaced through /status.
        self.hit_source: Optional[str] = None
        self.from_cache: bool = False
        self.last_fetch_duration: Optional[float] = None
        self.consecutive_failures = 0
        self.last_success_at: Optional[float] = None
        self.last_attempt_at: Optional[float] = None
        self.last_error: Optional[str] = None

        self._prime_from_cache()

    def __repr__(self):
        return "<CryptoData symbol:{} rows:{} failures:{}>".format(
            self.symbol,
            0 if self._rows is None else len(self._rows),
            self.consecutive_failures,
        )

    # ------------------------------------------------------------------
    # cache priming
    # ------------------------------------------------------------------
    def _prime_from_cache(self) -> None:
        try:
            cached = self.storage.load(self.symbol)
        except DataUnavailableError:
            cached = []
        if cached:
            self._rows = cached
            self._loaded_from_cache = True
            self.hit_source = "cache"
            self.from_cache = True

    # ------------------------------------------------------------------
    # single-flight orchestration
    # ------------------------------------------------------------------
    def _start_flight_locked(self) -> _Flight:
        flight = self._flight
        if flight is None:
            flight = _Flight(max_attempts=self.max_retries + 1)
            self._flight = flight
            worker = threading.Thread(
                target=self._run_flight,
                args=(flight,),
                name="cryptocmd-fetch-{}".format(self.symbol),
                daemon=True,
            )
            worker.start()
        return flight

    def _await(self, flight: _Flight, timeout: Optional[float]) -> bool:
        if timeout is None:
            flight.done.wait()
            return True
        return flight.done.wait(timeout)

    def _run_flight(self, flight: _Flight) -> None:
        error: Optional[BaseException] = None
        try:
            attempt = 0
            while attempt <= self.max_retries:
                if self._abort.is_set():
                    raise DataUnavailableError(
                        self.symbol, "fetch aborted during shutdown",
                        reason="network",
                    )
                attempt += 1
                flight.attempt = attempt
                attempt_start = time.monotonic()
                with self._cond:
                    self.last_attempt_at = time.time()

                try:
                    rows = self.parser.fetch(
                        self.symbol, self.start_date, self.end_date
                    )
                    if not isinstance(rows, list):
                        raise DataUnavailableError(
                            self.symbol,
                            "parser returned {!r}, expected list".format(
                                type(rows).__name__
                            ),
                            reason="parse",
                        )
                except DataUnavailableError as exc:
                    error = exc
                except Exception as exc:  # defensive normalisation
                    error = DataUnavailableError(
                        self.symbol, "unexpected parser failure",
                        reason="parse", cause=exc,
                    )
                    error.__cause__ = exc
                else:
                    attempt_duration = time.monotonic() - attempt_start
                    # The commit (and its single transactional storage write)
                    # lives outside the retry loop's fetch guard: a write
                    # failure rolls back and fails the flight immediately,
                    # without wasting a second real fetch.
                    try:
                        self._commit_success(flight, rows, attempt_duration)
                    except DataUnavailableError as commit_error:
                        normalized = commit_error
                    except Exception as commit_error:
                        normalized = DataUnavailableError(
                            self.symbol,
                            "storage write failed; old history retained",
                            reason="storage",
                            cause=commit_error,
                        )
                    else:
                        return
                    self._finish_failure(
                        flight, normalized, attempt_duration
                    )
                    return

                with self._cond:
                    self.last_fetch_duration = (
                        time.monotonic() - attempt_start
                    )
                    self.last_error = "{}: {}".format(
                        getattr(error, "reason", "unknown"), error
                    )

                if attempt <= self.max_retries:
                    backoff = min(
                        self.max_backoff,
                        self.base_backoff * (2 ** (attempt - 1)),
                    )
                    if self._abort.wait(backoff):
                        break

            # all fetch attempts exhausted
            duration = time.monotonic() - attempt_start
            self._finish_failure(flight, error, duration)
        except DataUnavailableError as fatal:
            # abort / fatal path
            self._finish_failure(flight, fatal, flight.duration)
        finally:
            flight.done.set()

    def _finish_failure(
        self, flight: _Flight, error: BaseException, duration: float
    ) -> None:
        with self._cond:
            self.consecutive_failures += 1
            self.last_fetch_duration = duration
            self.last_error = "{}: {}".format(
                getattr(error, "reason", "unknown"), error
            )
            flight.ok = False
            flight.error = error
            flight.finished_at = time.monotonic()
            self._flight = None
            self._cond.notify_all()

    def _commit_success(
        self, flight: _Flight, rows: List[Dict[str, Any]], duration: float
    ) -> None:
        with self._cond:
            cached = self._rows or []

        if rows:
            # Exactly one storage write per successful fetch. The adapter
            # rolls this transaction back if anything goes wrong.
            self.storage.save(self.symbol, rows)
            flight.stored = True
            committed = rows
        elif cached:
            # An empty payload must never erase good history.
            committed = cached
        else:
            self.storage.save(self.symbol, [])
            flight.stored = True
            committed = []

        with self._cond:
            self._rows = committed
            self._loaded_from_cache = False
            self.hit_source = "fetch"
            self.from_cache = False
            self.last_fetch_duration = duration
            self.consecutive_failures = 0
            self.last_success_at = time.time()
            self.last_error = None
            flight.ok = True
            flight.rows = committed
            flight.finished_at = time.monotonic()
            self._flight = None
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # public read API
    # ------------------------------------------------------------------
    def get_history(
        self,
        force: bool = False,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Return history rows (newest-first); never ``None``.

        :param force: bypass the in-memory cache and start a fresh fetch.
        :param wait: block for the fetch; ``False`` returns cache immediately.
        :param timeout: max seconds to wait before falling back to cache.
        """
        with self._cond:
            if not force and self._rows:
                self.hit_source = "cache"
                self.from_cache = True
                return [dict(row) for row in self._rows]
            flight = self._start_flight_locked()

        if not wait:
            with self._cond:
                if self._rows:
                    self.hit_source = "cache"
                    self.from_cache = True
                    return [dict(row) for row in self._rows]
                self.hit_source = "fetch-pending"
                self.from_cache = False
                return []

        self._await(flight, timeout)

        with self._cond:
            completed = flight.done.is_set()
            if completed and flight.ok:
                self.hit_source = "fetch"
                self.from_cache = False
                return [dict(row) for row in flight.rows]
            cached = self._rows
            error = flight.error
            reason = getattr(error, "reason", None) or "cache"
            # Serve stale cache as a graceful fallback for a *non-forced*,
            # timed-out read. A forced read (or an explicitly failed flight)
            # must surface the unified error instead of masking the failure.
            if cached and not force and not (completed and not flight.ok):
                self.hit_source = "stale-cache"
                self.from_cache = True
                return [dict(row) for row in cached]

        if not completed:
            cause = TimeoutError(
                "fetch did not finish within {}s".format(timeout)
            )
        else:
            cause = error
        raise DataUnavailableError(
            self.symbol,
            "cache miss and no fresh data available",
            reason=reason,
            cause=cause,
        ) from cause

    def refresh(
        self,
        force: bool = False,
        wait: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Trigger a background refresh (used by the scheduler)."""
        with self._cond:
            if not force and self._flight is not None:
                flight = self._flight
            elif not force and self._rows and not self._due_for_refresh():
                return self._rows
            else:
                flight = self._start_flight_locked()
        if wait:
            self._await(flight, timeout)
            with self._cond:
                if flight.ok:
                    return list(flight.rows)
        return None

    def _due_for_refresh(self) -> bool:
        return True

    def shutdown(self) -> None:
        self._abort.set()
        with self._cond:
            flight = self._flight
        if flight is not None:
            flight.done.wait(timeout=5)

    # ------------------------------------------------------------------
    # backwards-compatible API (unchanged names / return shapes)
    # ------------------------------------------------------------------
    def get_data(
        self, format: str = "", verbose: bool = False, **kwargs: Any
    ):
        rows = self.get_history(
            force=bool(kwargs.get("forced")),
            timeout=kwargs.get("timeout"),
        )
        if verbose:
            print(",".join(HEADERS))
            for row in rows:
                print(",".join(str(row[key]) for key in HEADERS))
            return None
        if format:
            import json as _json

            if format == "json":
                return _json.dumps(rows, default=str)
            warnings.warn(
                "format {!r} unsupported in this build; returning rows".format(
                    format
                ),
                stacklevel=2,
            )
        return HEADERS, [[row[key] for key in HEADERS] for row in rows]

    def get_dataframe(self, date_as_index: bool = False, **kwargs: Any):
        try:
            import pandas as pd
        except ImportError as exc:
            raise NotImplementedError(
                "DataFrame format requires 'pandas'. Try: pip install pandas"
            ) from exc
        rows = self.get_history(timeout=kwargs.get("timeout"))
        frame = pd.DataFrame(rows, columns=HEADERS)
        frame["Date"] = pd.to_datetime(
            frame["Date"], format="%d-%m-%Y", dayfirst=True
        )
        if date_as_index:
            frame.set_index("Date", inplace=True)
        return frame

    def export_csv(
        self,
        csv_name: Optional[str] = None,
        csv_path: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        rows = self.get_history(timeout=kwargs.get("timeout"))
        if csv_path is None:
            csv_path = os.getcwd()
        if csv_name is None:
            csv_name = "{0}_{1}_{2}.csv".format(
                self.symbol,
                getattr(self.parser, "fiat", "USD"),
                self.start_date or "all",
                self.end_date or "all",
            )
        if not csv_name.endswith(".csv"):
            csv_name += ".csv"
        target = os.path.join(csv_path, csv_name)
        with open(target, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(HEADERS)
            for row in rows:
                writer.writerow([row[key] for key in HEADERS])
        return target

    # ------------------------------------------------------------------
    # status snapshot for the /status endpoint
    # ------------------------------------------------------------------
    def status(self) -> Dict[str, Any]:
        with self._cond:
            flight = self._flight
            return {
                "symbol": self.symbol,
                "state": "fetching" if flight is not None else (
                    "error"
                    if self.consecutive_failures and not self._rows
                    else "ready"
                ),
                "hit_source": self.hit_source,
                "from_cache": bool(self.from_cache),
                "last_fetch_duration_ms": (
                    round(self.last_fetch_duration * 1000, 1)
                    if self.last_fetch_duration is not None
                    else None
                ),
                "consecutive_failures": self.consecutive_failures,
                "rows_cached": 0 if self._rows is None else len(self._rows),
                "attempt": flight.attempt if flight else 0,
                "max_attempts": self.max_retries + 1,
                "last_success_at": self.last_success_at,
                "last_attempt_at": self.last_attempt_at,
                "last_error": self.last_error,
            }


__all__ = ["CryptoData", "HEADERS"]
