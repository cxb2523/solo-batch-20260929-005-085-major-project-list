"""Resident collection service.

:class:`CollectionService` owns one :class:`~cryptocmd.core.CryptoData` per
symbol and a background scheduler thread that refreshes them on an interval.
It also exposes a small embedded HTTP server:

``GET /status``   machine-readable JSON for every watched symbol.
``GET /``         single-page dashboard (auto-refreshing, good for recording).
``GET /healthz``  plain ``ok`` liveness probe.

Parser and storage are injected at construction; the defaults reproduce the
legacy CoinMarketCap behaviour of the package.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .adapter import SQLiteAdapter, StorageAdapter
from .core import CryptoData
from .parser import CmcParser, Parser

_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>cryptocmd · collection status</title>
<style>
  :root {
    --bg: #0d1117; --panel: #161b22; --line: #30363d;
    --text: #e6edf3; --muted: #8b949e; --ok: #3fb950;
    --warn: #d29922; --err: #f85149; --accent: #58a6ff;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 14px/1.45 -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    padding: 28px;
  }
  h1 { font-size: 20px; margin: 0 0 4px; }
  .sub { color: var(--muted); margin-bottom: 22px; font-size: 13px; }
  .cards { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 22px; }
  .card {
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 10px; padding: 12px 16px; min-width: 130px;
  }
  .card .k { color: var(--muted); font-size: 12px; }
  .card .v { font-size: 22px; font-weight: 600; margin-top: 2px; }
  table { width: 100%; border-collapse: collapse;
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 10px; overflow: hidden; }
  th, td { padding: 10px 14px; text-align: left;
    border-bottom: 1px solid var(--line); font-size: 13px; }
  th { color: var(--muted); font-weight: 600; background: #1c2230; }
  tr:last-child td { border-bottom: none; }
  .sym { font-weight: 700; color: var(--accent); }
  .badge { display: inline-block; padding: 2px 9px; border-radius: 999px;
    font-size: 12px; font-weight: 600; }
  .ready { background: rgba(63,185,80,.15); color: var(--ok); }
  .fetching { background: rgba(88,166,255,.15); color: var(--accent); }
  .error { background: rgba(248,81,73,.15); color: var(--err); }
  .src-cache, .src-stale-cache { color: var(--warn); }
  .src-fetch { color: var(--ok); }
  .src-fetch-pending { color: var(--accent); }
  .src-none { color: var(--muted); }
  .dot { display:inline-block; width:8px; height:8px; border-radius:50%;
    margin-right:6px; vertical-align:middle; }
  .pulse { animation: pulse 1s infinite; }
  @keyframes pulse { 0%{opacity:1} 50%{opacity:.25} 100%{opacity:1} }
  .err-text { color: var(--err); font-size: 12px;
    max-width: 320px; overflow: hidden; text-overflow: ellipsis;
    white-space: nowrap; }
  code { color: var(--muted); }
</style>
</head>
<body>
  <h1>cryptocmd resident collection</h1>
  <div class="sub">Per-symbol cache, single-flight fetches and exponential
    backoff · polling <code>/status</code> every 3s ·
    <span id="clock"></span></div>
  <div class="cards" id="cards"></div>
  <table>
    <thead><tr>
      <th>Symbol</th><th>State</th><th>Hit source</th><th>From cache</th>
      <th>Last fetch (ms)</th><th>Attempt</th><th>Fails</th>
      <th>Rows</th><th>Last error</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
<script>
async function tick() {
  try {
    const res = await fetch('/status', {cache: 'no-store'});
    const data = await res.json();
    render(data);
  } catch (e) { /* keep last render */ }
}
function cls(state) {
  return state === 'ready' ? 'ready' : state === 'fetching' ? 'fetching' : 'error';
}
function srcCell(s) {
  const src = s.hit_source || 'none';
  const dot = src === 'fetching' || src === 'fetch-pending'
    ? '<span class="dot pulse" style="background:#58a6ff"></span>'
    : '<span class="dot" style="background:#8b949e"></span>';
  return '<span class="src-' + src + '">' + dot + src + '</span>';
}
function render(data) {
  const syms = data.symbols;
  const totalRows = syms.reduce((a, s) => a + s.rows_cached, 0);
  const fetching = syms.filter(s => s.state === 'fetching').length;
  const errors = syms.filter(s => s.state === 'error').length;
  const cards = [
    ['Symbols', syms.length],
    ['Cached rows', totalRows],
    ['Fetching', fetching],
    ['In error', errors],
    ['Uptime (s)', Math.round(data.uptime_seconds)],
  ];
  document.getElementById('cards').innerHTML = cards.map(([k, v]) =>
    '<div class="card"><div class="k">' + k + '</div><div class="v">' +
    v + '</div></div>').join('');
  document.getElementById('rows').innerHTML = syms.map(s =>
    '<tr>' +
      '<td class="sym">' + s.symbol + '</td>' +
      '<td><span class="badge ' + cls(s.state) + '">' + s.state + '</span></td>' +
      '<td>' + srcCell(s) + '</td>' +
      '<td>' + (s.from_cache ? 'yes' : 'no') + '</td>' +
      '<td>' + (s.last_fetch_duration_ms ?? '–') + '</td>' +
      '<td>' + s.attempt + '/' + s.max_attempts + '</td>' +
      '<td>' + s.consecutive_failures + '</td>' +
      '<td>' + s.rows_cached + '</td>' +
      '<td><div class="err-text" title="' +
        (s.last_error || '') + '">' + (s.last_error || '') + '</div></td>' +
    '</tr>').join('');
  document.getElementById('clock').textContent =
      new Date().toLocaleTimeString();
}
tick();
setInterval(tick, 3000);
</script>
</body>
</html>
"""


class _StatusHandler(BaseHTTPRequestHandler):
    server_version = "cryptocmd/1.0"

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        path = self.path.split("?", 1)[0]
        service = self.server.service  # type: ignore[attr-defined]
        if path == "/status":
            payload = json.dumps(service.status_snapshot()).encode("utf-8")
            self._send(200, payload, "application/json; charset=utf-8")
        elif path in ("/", "/index.html", "/dashboard"):
            self._send(
                200, _DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8"
            )
        elif path == "/healthz":
            self._send(200, b"ok", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, *args: Any) -> None:
        # Silence default stderr access logging.
        return None


class CollectionService:
    """Background per-symbol refresher plus /status HTTP endpoint.

    :param symbols: iterable of coin codes to watch.
    :param interval: scheduled refresh interval per symbol (seconds).
    :param parser: shared :class:`~cryptocmd.parser.Parser` instance.
    :param storage: shared :class:`~cryptocmd.adapter.StorageAdapter`.
    :param status_port: HTTP port for /status. ``0`` picks an ephemeral port.
    :param status_host: bind interface.
    :param max_retries / base_backoff / max_backoff: retry policy passed to
        each :class:`~cryptocmd.core.CryptoData`.
    """

    def __init__(
        self,
        symbols: Optional[Iterable[str]] = None,
        interval: float = 300.0,
        parser: Optional[Parser] = None,
        storage: Optional[StorageAdapter] = None,
        status_port: int = 8642,
        status_host: str = "127.0.0.1",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
        autostart: bool = False,
    ):
        self.interval = float(interval)
        self.parser = parser if parser is not None else CmcParser()
        self.storage = storage if storage is not None else SQLiteAdapter()
        self.status_host = status_host
        self.start_date = start_date
        self.end_date = end_date
        self._retry_kwargs = dict(
            max_retries=max_retries,
            base_backoff=base_backoff,
            max_backoff=max_backoff,
        )

        self._lock = threading.RLock()
        self._data: Dict[str, CryptoData] = {}
        self._next_due: Dict[str, float] = {}

        self._running = threading.Event()
        self._scheduler: Optional[threading.Thread] = None
        self._started_at: Optional[float] = None

        self._httpd = ThreadingHTTPServer(
            (status_host, status_port), _StatusHandler
        )
        self._httpd.service = self  # type: ignore[attr-defined]
        self.status_port = self._httpd.server_address[1]
        self._http_thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="cryptocmd-status-http",
            daemon=True,
        )

        for symbol in symbols or ():
            self.add_symbol(symbol)

        if autostart:
            self.start()

    # ------------------------------------------------------------------
    # symbol management
    # ------------------------------------------------------------------
    def add_symbol(self, symbol: str) -> CryptoData:
        symbol = str(symbol).upper()
        with self._lock:
            existing = self._data.get(symbol)
            if existing is not None:
                return existing
            data = CryptoData(
                symbol=symbol,
                start_date=self.start_date,
                end_date=self.end_date,
                parser=self.parser,
                storage=self.storage,
                **self._retry_kwargs,
            )
            self._data[symbol] = data
            # First scheduled tick is immediate.
            self._next_due[symbol] = 0.0
            return data

    def remove_symbol(self, symbol: str) -> None:
        symbol = str(symbol).upper()
        with self._lock:
            data = self._data.pop(symbol, None)
            self._next_due.pop(symbol, None)
        if data is not None:
            data.shutdown()

    def get(self, symbol: str) -> CryptoData:
        symbol = str(symbol).upper()
        with self._lock:
            return self._data[symbol]

    def symbols(self) -> List[str]:
        with self._lock:
            return sorted(self._data)

    def request(self, symbol: str, **kwargs: Any) -> List[Dict[str, Any]]:
        """Convenience read through the service (single-flight aware)."""
        return self.add_symbol(symbol).get_history(**kwargs)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> "CollectionService":
        if self._running.is_set():
            return self
        self._running.set()
        self._started_at = time.time()
        self._http_thread.start()
        self._scheduler = threading.Thread(
            target=self._schedule_loop,
            name="cryptocmd-scheduler",
            daemon=True,
        )
        self._scheduler.start()
        return self

    def stop(self) -> None:
        if not self._running.is_set():
            return
        self._running.clear()
        with self._lock:
            entries = list(self._data.values())
        for data in entries:
            data.shutdown()
        try:
            self._httpd.shutdown()
        except Exception:
            pass
        self._httpd.server_close()
        if self._scheduler is not None:
            self._scheduler.join(timeout=5)
        self._http_thread.join(timeout=5)

    def __enter__(self) -> "CollectionService":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # scheduler
    # ------------------------------------------------------------------
    def _schedule_loop(self) -> None:
        tick = 0.2
        while self._running.is_set():
            now = time.time()
            with self._lock:
                due = [
                    symbol
                    for symbol, due_at in self._next_due.items()
                    if now >= due_at
                ]
            for symbol in due:
                with self._lock:
                    data = self._data.get(symbol)
                if data is None:
                    continue
                # refresh() coalesces onto any in-flight fetch.
                data.refresh(force=False, wait=False)
                with self._lock:
                    self._next_due[symbol] = time.time() + self.interval
            self._running.wait(tick)

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------
    def status_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            entries = [self._data[s] for s in sorted(self._data)]
        return {
            "status": "running" if self._running.is_set() else "stopped",
            "uptime_seconds": (
                round(time.time() - self._started_at, 1)
                if self._started_at
                else 0.0
            ),
            "interval_seconds": self.interval,
            "status_port": self.status_port,
            "symbols": [data.status() for data in entries],
        }


__all__ = ["CollectionService"]
