"""健康检查: python -m cryptocmd.health --symbol BTC [--watch 3]

退出码: 0 = 健康, 非零 = 不合格。
--watch N: 持续监控 N 秒(每秒检查一次)后退出, 期间任何一次不合格则退出码非零。
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from .adapter import SQLiteAdapter
from .service import CollectorService


def run_check(service, symbol):
    rows = service.get_history(symbol, force=True)
    stats = service.status()["symbols"].get(symbol.upper(), {})
    healthy = bool(rows) and stats.get("consecutive_failures", 1) == 0
    return healthy, len(rows), stats


def format_line(symbol, healthy, rows, stats):
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return (
        "[%s] %s rows=%d source=%s cache_hit=%s fetch_ms=%s failures=%d -> %s"
        % (
            stamp,
            symbol.upper(),
            rows,
            stats.get("last_source", "-"),
            stats.get("cache_hit", False),
            stats.get("last_fetch_ms", 0.0),
            stats.get("consecutive_failures", 0),
            "OK" if healthy else "UNHEALTHY",
        )
    )


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m cryptocmd.health")
    parser.add_argument("--symbol", required=True, help="要检查的 symbol, 如 BTC")
    parser.add_argument(
        "--watch",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="持续监控 N 秒(每秒检查一次)后退出; 0 表示只检查一次",
    )
    parser.add_argument("--db", default="cryptocmd_history.db")
    parser.add_argument("--timeout", type=float, default=10.0, help="单次抓取超时(秒)")
    args = parser.parse_args(argv)

    from .parser import CryptoDataParser

    service = CollectorService(
        [args.symbol],
        parser=CryptoDataParser(timeout=args.timeout),
        adapter=SQLiteAdapter(args.db),
        max_retries=2,
        backoff_base=0.5,
    )

    failed = False
    if args.watch and args.watch > 0:
        deadline = time.monotonic() + args.watch
        while True:
            healthy, rows, stats = run_check(service, args.symbol)
            print(format_line(args.symbol, healthy, rows, stats), flush=True)
            failed = failed or not healthy
            if time.monotonic() >= deadline:
                break
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    else:
        healthy, rows, stats = run_check(service, args.symbol)
        print(format_line(args.symbol, healthy, rows, stats), flush=True)
        failed = not healthy

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
