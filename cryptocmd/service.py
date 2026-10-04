"""常驻采集服务。

后台线程按 symbol 定时刷新; 构造时注入 parser 与 adapter, 默认实现沿用
``cryptocmd/__init__.py`` 中 ``CryptoData`` 的现有行为。

三个互相咬合的决策:
  1. 同一 symbol 的并发请求只触发一次真实抓取、只落一次库 (single-flight);
     超时按指数退避重试, 重试窗口内并发请求继续复用同一次抓取。
  2. 写库失败回滚到旧历史, 绝不写坏。
  3. 解析失败 / 网络超时 / 缓存未命中收敛到 CryptoDataError,
     用 ``raise ... from`` 保留原始原因; 取不到数据返回空列表而非 None。
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from .adapter import SQLiteAdapter
from .errors import CryptoDataError
from .parser import CryptoDataParser

__all__ = ["CollectorService"]


class _InFlight:
    """同一 symbol 的在途抓取, 并发调用方共享同一个结果。"""

    __slots__ = ("event", "result")

    def __init__(self):
        self.event = threading.Event()
        self.result = []


def _wrap_unexpected(symbol, exc):
    """把非预期异常收敛为 CryptoDataError, 并保留原始原因。"""
    try:
        raise CryptoDataError("unexpected failure for %s" % symbol) from exc
    except CryptoDataError as wrapped:
        return wrapped


class CollectorService:
    """按 symbol 常驻刷新历史行情, 对外提供查询与状态。"""

    def __init__(
        self,
        symbols,
        parser=None,
        adapter=None,
        interval=60.0,
        cache_ttl=30.0,
        max_retries=3,
        backoff_base=0.5,
        backoff_cap=8.0,
    ):
        self.symbols = [str(s).upper() for s in symbols]
        self.parser = parser if parser is not None else CryptoDataParser()
        self.adapter = adapter if adapter is not None else SQLiteAdapter()
        self.interval = float(interval)
        self.cache_ttl = float(cache_ttl)
        self.max_retries = int(max_retries)
        self.backoff_base = float(backoff_base)
        self.backoff_cap = float(backoff_cap)

        self._started_at = time.time()
        self._stop = threading.Event()
        self._threads = []
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._cache = {}
        self._stats_lock = threading.Lock()
        self._stats = {s: self._new_stats(s) for s in self.symbols}

    @staticmethod
    def _new_stats(symbol):
        return {
            "symbol": symbol,
            "last_source": "-",          # network | cache | store
            "cache_hit": False,
            "last_fetch_ms": 0.0,
            "consecutive_failures": 0,
            "total_fetches": 0,
            "total_failures": 0,
            "rows_cached": 0,
            "inflight": False,
            "last_success_at": None,
            "last_error": None,
        }

    # ------------------------------------------------------------------
    # 查询入口
    # ------------------------------------------------------------------
    def get_history(self, symbol, force=False):
        """返回 list[dict]; 取不到数据返回空列表而非 None。"""
        symbol = str(symbol).upper()
        if not force:
            try:
                rows = self._cache_get(symbol)
            except CryptoDataError:
                pass
            else:
                self._record(symbol, source="cache", cache_hit=True, rows=len(rows))
                return list(rows)
        return self._join_or_fetch(symbol)

    # ------------------------------------------------------------------
    # single-flight: 并发请求共享同一次抓取与落库
    # ------------------------------------------------------------------
    def _join_or_fetch(self, symbol):
        with self._inflight_lock:
            flight = self._inflight.get(symbol)
            if flight is None:
                flight = _InFlight()
                self._inflight[symbol] = flight
                leader = True
                self._mark_inflight(symbol, True)
            else:
                leader = False
        if not leader:
            # 重试窗口内继续等待同一次抓取, 不另起请求
            flight.event.wait()
            return list(flight.result)
        try:
            flight.result = self._collect(symbol)
            return list(flight.result)
        finally:
            flight.event.set()
            with self._inflight_lock:
                self._inflight.pop(symbol, None)
            self._mark_inflight(symbol, False)

    # ------------------------------------------------------------------
    # 抓取 + 指数退避重试 + 原子落库
    # ------------------------------------------------------------------
    def _collect(self, symbol):
        started = time.monotonic()
        last_error = None
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delay = min(self.backoff_base * (2 ** (attempt - 1)), self.backoff_cap)
                if self._stop.wait(delay):
                    break
            try:
                rows = self.parser.parse(symbol)
                if rows is None:
                    rows = []
                self._persist(symbol, rows)
            except CryptoDataError as exc:
                last_error = exc
                continue
            except Exception as exc:  # 兜底: 非预期异常同样收敛
                last_error = _wrap_unexpected(symbol, exc)
                continue
            duration_ms = (time.monotonic() - started) * 1000.0
            self._cache_put(symbol, rows)
            self._record(
                symbol,
                source="network",
                cache_hit=False,
                fetch_ms=duration_ms,
                rows=len(rows),
                success=True,
            )
            return list(rows)
        duration_ms = (time.monotonic() - started) * 1000.0
        self._record(
            symbol,
            source="network",
            cache_hit=False,
            fetch_ms=duration_ms,
            success=False,
            error=last_error,
        )
        # 抓取失败: 回退到已落库的旧历史, 有则用, 没有则空列表
        stale = self.adapter.snapshot(symbol)
        if stale:
            self._record(symbol, source="store", cache_hit=False, rows=len(stale))
            return list(stale)
        return []

    def _persist(self, symbol, rows):
        """写库失败回滚到旧历史, 绝不写坏。"""
        old = self.adapter.snapshot(symbol)
        try:
            self.adapter.write(symbol, rows)
        except CryptoDataError:
            if old is not None:
                try:
                    self.adapter.restore(symbol, old)
                except CryptoDataError:
                    pass
            raise

    # ------------------------------------------------------------------
    # TTL 缓存: 未命中统一抛 CryptoDataError
    # ------------------------------------------------------------------
    def _cache_get(self, symbol):
        entry = self._cache.get(symbol)
        if entry is None:
            raise CryptoDataError("cache miss for %s" % symbol)
        expires_at, rows = entry
        if expires_at <= time.monotonic():
            self._cache.pop(symbol, None)
            raise CryptoDataError("cache expired for %s" % symbol)
        return rows

    def _cache_put(self, symbol, rows):
        self._cache[symbol] = (time.monotonic() + self.cache_ttl, list(rows))

    # ------------------------------------------------------------------
    # 统计
    # ------------------------------------------------------------------
    def _record(self, symbol, source=None, cache_hit=None, fetch_ms=None,
                rows=None, success=None, error=None):
        with self._stats_lock:
            st = self._stats.setdefault(symbol, self._new_stats(symbol))
            if source is not None:
                st["last_source"] = source
            if cache_hit is not None:
                st["cache_hit"] = cache_hit
            if fetch_ms is not None:
                st["last_fetch_ms"] = round(fetch_ms, 2)
            if rows is not None:
                st["rows_cached"] = rows
            if success is True:
                st["total_fetches"] += 1
                st["consecutive_failures"] = 0
                st["last_error"] = None
                st["last_success_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            elif success is False:
                st["total_fetches"] += 1
                st["total_failures"] += 1
                st["consecutive_failures"] += 1
                st["last_error"] = str(error) if error is not None else "unknown"

    def _mark_inflight(self, symbol, value):
        with self._stats_lock:
            st = self._stats.setdefault(symbol, self._new_stats(symbol))
            st["inflight"] = value

    def status(self):
        """逐个 symbol 的命中来源 / 缓存 / 耗时 / 连续失败次数。"""
        with self._stats_lock:
            symbols = {name: dict(st) for name, st in sorted(self._stats.items())}
        return {
            "uptime_s": round(time.time() - self._started_at, 1),
            "interval_s": self.interval,
            "cache_ttl_s": self.cache_ttl,
            "symbols": symbols,
        }

    # ------------------------------------------------------------------
    # 后台常驻刷新
    # ------------------------------------------------------------------
    def start(self):
        """每个 symbol 一个后台线程, 按 interval 定时刷新。"""
        if self._threads:
            return
        self._stop.clear()
        for symbol in self.symbols:
            thread = threading.Thread(
                target=self._refresh_loop,
                args=(symbol,),
                name="cryptocmd-%s" % symbol,
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def _refresh_loop(self, symbol):
        while not self._stop.is_set():
            self.get_history(symbol, force=True)
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads = []

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc_info):
        self.stop()
        return False
