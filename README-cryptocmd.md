# cryptocmd — resident cryptocurrency history collection

A self-contained service that periodically refreshes crypto OHLCV history per
symbol, deduplicates concurrent requests, persists transactionally, and
exposes a live dashboard.

## Public surface (unchanged for old callers)

```python
from cryptocmd import CryptoData

headers, rows = CryptoData("BTC").get_data()   # legacy call, same shape
df = CryptoData("BTC").get_dataframe()         # optional pandas
```

`get_history()` always returns a **list** (empty list when there is no data),
never `None`.

## Resident service

```python
from cryptocmd import CollectionService

svc = CollectionService(symbols=["BTC", "ETH"], interval=300, status_port=8642)
svc.start()
```

Then open:

- `http://127.0.0.1:8642/`        — live dashboard (auto-refresh, screen-recordable)
- `http://127.0.0.1:8642/status`  — JSON snapshot
- `http://127.0.0.1:8642/healthz` — liveness probe

Each symbol reports `hit_source` (`fetch` / `cache` / `stale-cache` /
`fetch-pending`), `from_cache`, `last_fetch_duration_ms`,
`consecutive_failures`, retry attempt and cached row count.

## Guarantees

1. **One real fetch, one storage write.** Concurrent requests for a symbol
   share a single in-flight fetch — including its exponential-backoff retry
   window. Only a successful fetch writes, once.
2. **Rollback safety.** A failed write is rolled back; the previous history is
   never corrupted or half-written. An empty remote payload never erases good
   history.
3. **One error type.** Parse failures, network timeouts and cache misses all
   raise `DataUnavailableError`, chained with `raise ... from` (inspect
   `.__cause__`).

## Health check

```
python -m cryptocmd.health --symbol BTC --watch 3
```

Exit code is non-zero only if the service cannot be reached, the symbol is
missing, or the status payload is structurally invalid. A blocked remote
endpoint is handled gracefully through the retry/backoff path and still exits
`0`. Options: `--count N` (polls; `0` until Ctrl-C), `--port`, `--no-spawn`,
`--interval`.

## Injection points

```python
CryptoData("BTC", parser=MyParser(), storage=MyAdapter())
CollectionService(symbols=["BTC"], parser=..., storage=...)
```

`parser.fetch(symbol, start_date, end_date) -> list[dict]`;
`storage.load(symbol) -> list[dict]` and
`storage.save(symbol, rows) -> None`. Defaults are `CmcParser` (CoinMarketCap)
and `SQLiteAdapter` (`~/.cryptocmd/cryptocmd.db`).
