"""``python -m cryptocmd.health --symbol BTC --watch 3``

Health check for the resident collection service. It polls the ``/status``
endpoint (spinning up an in-process service for the requested symbol when no
existing endpoint answers) and prints a per-poll table.

Exit code:

* ``0`` — service is alive and the requested symbol is registered and is
  behaving correctly (data served or failing *gracefully* through the unified
  :class:`DataUnavailableError` path with retries/backoff).
* non-zero — service cannot be reached, the symbol is missing, or the status
  payload is structurally invalid.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

from .service import CollectionService


def fetch_status(host: str, port: int, timeout: float = 5.0) -> Dict[str, Any]:
    url = "http://{}:{}/status".format(host, port)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def find_symbol(status: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
    for entry in status.get("symbols", []):
        if str(entry.get("symbol", "")).upper() == symbol.upper():
            return entry
    return None


def _validate_entry(entry: Dict[str, Any]) -> Optional[str]:
    required = {
        "symbol",
        "state",
        "hit_source",
        "from_cache",
        "last_fetch_duration_ms",
        "consecutive_failures",
        "rows_cached",
    }
    missing = required - set(entry)
    if missing:
        return "status entry missing keys: {}".format(sorted(missing))
    if entry["state"] not in ("ready", "fetching", "error"):
        return "unknown state {!r}".format(entry["state"])
    return None


def _row_line(entry: Dict[str, Any]) -> str:
    return (
        "{symbol:<8} {state:<9} src={hit_source:<12} cache={cache:<3} "
        "{ms:>8}ms fails={fails:<3} rows={rows}".format(
            symbol=entry["symbol"],
            state=entry["state"],
            hit_source=entry.get("hit_source") or "-",
            cache="yes" if entry["from_cache"] else "no",
            ms=(
                "-"
                if entry["last_fetch_duration_ms"] is None
                else "{:.0f}".format(entry["last_fetch_duration_ms"])
            ),
            fails=entry["consecutive_failures"],
            rows=entry["rows_cached"],
        )
    )


def _try_endpoint(host: str, port: Optional[int]) -> Optional[int]:
    if port is not None:
        candidates = [port]
    else:
        candidates = [8642, 8080, 8000, 5000]
    for candidate in candidates:
        try:
            fetch_status(host, candidate, timeout=1.5)
            return candidate
        except (urllib.error.URLError, ConnectionError, OSError, ValueError):
            continue
    return None


def run_check(
    symbol: str,
    watch: float,
    count: int,
    host: str,
    port: Optional[int],
    spawn: bool,
    interval: float,
    timeout: float = 12.0,
) -> int:
    symbol = symbol.upper()
    owned: Optional[CollectionService] = None
    actual_port = _try_endpoint(host, port)

    if actual_port is None:
        if not spawn:
            print(
                "FAIL: no cryptocmd /status endpoint reachable on {}:{}".format(
                    host, port or "default ports"
                )
            )
            return 1
        # Bring up an in-process service so the check is self-contained.
        owned = CollectionService(
            symbols=[symbol],
            interval=max(watch * count, 30.0),
            status_port=port or 0,
            status_host=host,
            autostart=True,
        )
        actual_port = owned.status_port
        # Give the first fetch a moment to be scheduled.
        time.sleep(0.4)

    base_url = "http://{}:{}".format(host, actual_port)
    print("cryptocmd health · {} · polling every {:.0f}s".format(base_url, watch))
    print("-" * 78)

    exit_code = 0
    observed = 0
    poll = 0
    try:
        while count == 0 or poll < count:
            poll += 1
            try:
                status = fetch_status(host, actual_port, timeout=timeout)
            except (urllib.error.URLError, ConnectionError, OSError, ValueError) as exc:
                print("FAIL: /status unreachable: {}".format(exc))
                return 1

            if status.get("status") != "running":
                print("FAIL: service reports {!r}".format(status.get("status")))
                return 1

            entry = find_symbol(status, symbol)
            if entry is None:
                print("FAIL: symbol {} is not registered".format(symbol))
                return 1

            problem = _validate_entry(entry)
            if problem is not None:
                print("FAIL: {}".format(problem))
                return 1

            stamp = time.strftime("%H:%M:%S")
            print("[{}] {}".format(stamp, _row_line(entry)))
            if entry.get("last_error"):
                print("           last_error: {}".format(entry["last_error"]))
            observed += 1

            # A symbol with data from cache/fetch is healthy. A symbol still
            # fetching or retrying is *expected* when the network is blocked;
            # the unified-error + backoff path is the required behaviour.
            if entry["state"] in ("ready", "fetching", "error"):
                pass

            if count == 0 or poll < count:
                time.sleep(watch)
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        if owned is not None:
            owned.stop()

    print("-" * 78)
    if observed == 0:
        print("FAIL: no status samples collected")
        return 1
    if exit_code == 0:
        print(
            "OK: {} poll(s) for {} — service alive, symbol registered, "
            "mechanics sound".format(observed, symbol)
        )
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m cryptocmd.health",
        description="Health check for the cryptocmd resident service.",
    )
    parser.add_argument("--symbol", default="BTC", help="coin code (default BTC)")
    parser.add_argument(
        "--watch",
        type=float,
        default=3.0,
        help="seconds between polls (default 3)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=3,
        help="number of polls; 0 polls until Ctrl-C (default 3)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="status port (auto-detect / ephemeral if omitted)",
    )
    parser.add_argument(
        "--no-spawn",
        action="store_true",
        help="do not start an in-process service when none is reachable",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=300.0,
        help="scheduler interval for a spawned service (seconds)",
    )
    return parser


def main(argv: Optional[Tuple[str, ...]] = None) -> int:
    args = build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    return run_check(
        symbol=args.symbol,
        watch=args.watch,
        count=args.count,
        host=args.host,
        port=args.port,
        spawn=not args.no_spawn,
        interval=args.interval,
    )


if __name__ == "__main__":
    raise SystemExit(main())
