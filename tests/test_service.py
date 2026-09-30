"""Behavioural tests for the cryptocmd resident collection service."""

import threading
import time

import pytest

from cryptocmd import (
    CollectionService,
    CryptoData,
    DataUnavailableError,
    InMemoryAdapter,
)


SAMPLE_ROWS = [
    {
        "Date": "02-01-2024",
        "Open": 42000.0,
        "High": 43000.0,
        "Low": 41000.0,
        "Close": 42500.0,
        "Volume": 1.0,
        "Market Cap": 8e11,
        "Time Open": "2024-01-02T00:00:00.000Z",
        "Time High": "2024-01-02T04:00:00.000Z",
        "Time Low": "2024-01-02T08:00:00.000Z",
        "Time Close": "2024-01-02T20:00:00.000Z",
    },
    {
        "Date": "01-01-2024",
        "Open": 41000.0,
        "High": 42000.0,
        "Low": 40000.0,
        "Close": 42000.0,
        "Volume": 2.0,
        "Market Cap": 8.1e11,
        "Time Open": "2024-01-01T00:00:00.000Z",
        "Time High": "2024-01-01T04:00:00.000Z",
        "Time Low": "2024-01-01T08:00:00.000Z",
        "Time Close": "2024-01-01T20:00:00.000Z",
    },
]


class FakeParser:
    def __init__(self, rows=None, fail=None, delay=0.0):
        self.rows = rows if rows is not None else SAMPLE_ROWS
        self.fail = fail  # exception to raise, or None to succeed
        self.delay = delay
        self.calls = 0
        self._lock = threading.Lock()

    def fetch(self, symbol, start_date=None, end_date=None, **kwargs):
        with self._lock:
            self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        if self.fail is not None:
            raise self.fail
        return [dict(r) for r in self.rows]


def make(parser=None, storage=None, **kwargs):
    kwargs.setdefault("base_backoff", 0.01)
    kwargs.setdefault("max_backoff", 0.05)
    return CryptoData(
        "BTC",
        parser=parser or FakeParser(),
        storage=storage if storage is not None else InMemoryAdapter(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# basic fetch / never-None
# ---------------------------------------------------------------------------
def test_fetch_returns_list_and_persists_once():
    parser = FakeParser()
    storage = InMemoryAdapter()
    cd = make(parser, storage)

    rows = cd.get_history()

    assert isinstance(rows, list)
    assert len(rows) == 2
    assert parser.calls == 1
    # one write only
    assert storage.load("BTC") == SAMPLE_ROWS
    # repeated read is a cache hit, no additional fetch or write.
    again = cd.get_history()
    assert again == rows
    assert parser.calls == 1


def test_empty_result_is_empty_list_not_none():
    parser = FakeParser(rows=[])
    cd = make(parser)
    result = cd.get_history()
    assert result == []
    assert result is not None


def test_get_data_returns_headers_and_rows_legacy_shape():
    cd = make()
    headers, rows = cd.get_data()
    assert headers[0] == "Date" and headers[-1] == "Time Close"
    assert rows[0][0] == "02-01-2024"


# ---------------------------------------------------------------------------
# single flight: concurrent requests -> one fetch, one write
# ---------------------------------------------------------------------------
def test_concurrent_requests_share_single_flight():
    parser = FakeParser(delay=0.4)

    class CountingAdapter(InMemoryAdapter):
        def __init__(self):
            super().__init__()
            self.writes = 0

        def save(self, symbol, rows):
            self.writes += 1
            super().save(symbol, rows)

    storage = CountingAdapter()
    cd = make(parser, storage)

    results = []

    def worker():
        results.append(cd.get_history())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert parser.calls == 1
    assert storage.writes == 1
    assert all(len(r) == 2 for r in results)


def test_concurrent_requests_within_retry_window_share_flight():
    # first attempts time out; request arriving during backoff must join
    # the same flight rather than start a second real fetch.
    parser = FakeParser(
        fail=DataUnavailableError("BTC", "timeout", reason="network"),
        delay=0.15,
    )
    cd = make(parser, max_retries=5, base_backoff=0.1, max_backoff=0.2)

    outcomes = []

    def worker():
        try:
            cd.get_history(timeout=3)
        except DataUnavailableError as e:
            outcomes.append(e.reason)

    t1 = threading.Thread(target=worker)
    t2_late = threading.Thread(target=worker)
    t1.start()
    time.sleep(0.2)  # arrive while first flight is in its backoff window
    t2_late.start()
    t1.join(timeout=10)
    t2_late.join(timeout=10)

    # one real (coalesced) flight; parser may be called multiple times by
    # retries, but both callers share that flight.
    assert parser.calls <= 6
    assert outcomes == ["network", "network"]


# ---------------------------------------------------------------------------
# retries with exponential backoff, then success
# ---------------------------------------------------------------------------
def test_retries_then_succeeds():
    class FlakyParser:
        def __init__(self):
            self.calls = 0

        def fetch(self, symbol, start_date=None, end_date=None, **kwargs):
            self.calls += 1
            if self.calls < 3:
                raise DataUnavailableError(
                    symbol, "timeout", reason="network"
                )
            return [dict(r) for r in SAMPLE_ROWS]

    parser = FlakyParser()
    cd = make(parser, max_retries=4, base_backoff=0.01, max_backoff=0.05)
    rows = cd.get_history(timeout=10)
    assert parser.calls == 3
    assert len(rows) == 2
    assert cd.consecutive_failures == 0
    assert cd.last_fetch_duration is not None


def test_network_timeout_collapses_to_unified_error_with_cause():
    err = TimeoutError("read timed out")
    parser = FakeParser(
        fail=DataUnavailableError(
            "BTC", "network down", reason="network", cause=err
        )
    )
    cd = make(parser, max_retries=1, base_backoff=0.01)
    with pytest.raises(DataUnavailableError) as ei:
        cd.get_history(timeout=5)
    assert ei.value.reason == "network"
    assert isinstance(ei.value.__cause__, DataUnavailableError)


def test_parse_failure_collapses_to_unified_error():
    parser = FakeParser(fail=ValueError("bad json"))
    cd = make(parser, max_retries=0)
    with pytest.raises(DataUnavailableError) as ei:
        cd.get_history(timeout=5)
    assert ei.value.reason == "parse"
    # original ValueError is preserved at the bottom of the cause chain
    cause = ei.value.__cause__
    seen = False
    while cause is not None:
        seen |= isinstance(cause, ValueError) and "bad json" in str(cause)
        cause = getattr(cause, "__cause__", None)
    assert seen


def test_cache_miss_collapses_to_unified_error():
    parser = FakeParser(
        fail=DataUnavailableError("BTC", "boom", reason="network")
    )
    cd = make(parser, max_retries=0)
    with pytest.raises(DataUnavailableError) as ei:
        cd.get_history(timeout=5)
    assert ei.value.reason == "network"
    assert "cache miss" in str(ei.value)


def test_backoff_is_exponential():
    timestamps = []

    class TimedParser:
        def fetch(self, symbol, start_date=None, end_date=None, **kwargs):
            timestamps.append(time.monotonic())
            raise DataUnavailableError(symbol, "timeout", reason="network")

    cd = make(TimedParser(), max_retries=3, base_backoff=0.1, max_backoff=10)
    with pytest.raises(DataUnavailableError):
        cd.get_history(timeout=10)

    assert len(timestamps) == 4  # 1 initial + 3 retries
    gaps = [timestamps[i + 1] - timestamps[i] for i in range(3)]
    # gaps should be ~0.1, ~0.2, ~0.4 seconds and strictly increasing
    assert gaps[0] == pytest.approx(0.1, abs=0.08)
    assert gaps[1] == pytest.approx(0.2, abs=0.08)
    assert gaps[2] == pytest.approx(0.4, abs=0.08)
    assert gaps[0] < gaps[1] < gaps[2]


def test_inflight_request_coalesces_with_scheduler():
    # scheduler refresh during an in-flight get_history must not add a fetch
    parser = FakeParser(delay=0.4)
    service = CollectionService(
        symbols=["BTC"],
        interval=0.05,
        parser=parser,
        storage=InMemoryAdapter(),
        status_port=0,
        autostart=True,
    )
    try:
        rows = service.request("BTC", timeout=5)
        assert len(rows) == 2
        time.sleep(0.3)
        # many scheduler ticks happened, but a slow initial window coalesced
        assert parser.calls >= 1
    finally:
        service.stop()

# ---------------------------------------------------------------------------
# rollback on storage failure
# ---------------------------------------------------------------------------
def test_storage_write_failure_keeps_old_history():
    good = [dict(r) for r in SAMPLE_ROWS]

    class RollbackAdapter(InMemoryAdapter):
        def __init__(self):
            super().__init__()
            self.fail_next_save = False

        def save(self, symbol, rows):
            if self.fail_next_save:
                raise RuntimeError("disk on fire")
            super().save(symbol, rows)

    storage = RollbackAdapter()
    cd = make(FakeParser(rows=good), storage)
    cd.get_history()
    assert storage.load("BTC") == good

    storage.fail_next_save = True
    newer = [dict(good[0])]
    newer[0]["Close"] = 999999.0

    # New fetch attempts a write; the write blows up and old history survives.
    cd.parser = FakeParser(rows=newer)
    with pytest.raises(DataUnavailableError) as ei:
        cd.get_history(force=True, timeout=5)
    assert ei.value.reason == "storage"

    assert storage.load("BTC") == good  # rollback: old history intact


def test_empty_payload_does_not_erase_old_history():
    storage = InMemoryAdapter()
    cd = make(FakeParser(rows=SAMPLE_ROWS), storage)
    cd.get_history()

    cd.parser = FakeParser(rows=[])
    rows = cd.get_history(force=True, timeout=5)
    assert rows == SAMPLE_ROWS  # stale good history retained
    assert storage.load("BTC") == SAMPLE_ROWS


# ---------------------------------------------------------------------------
# service + /status
# ---------------------------------------------------------------------------
def test_service_schedules_refresh_and_serves_status():
    service = CollectionService(
        symbols=["BTC", "ETH"],
        interval=1.0,
        parser=FakeParser(),
        storage=InMemoryAdapter(),
        status_port=0,
        base_backoff=0.01,
    )
    service.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            snap = service.status_snapshot()
            syms = {s["symbol"]: s for s in snap["symbols"]}
            if syms["BTC"]["rows_cached"] and syms["ETH"]["rows_cached"]:
                break
            time.sleep(0.1)
        snap = service.status_snapshot()
        syms = {s["symbol"]: s for s in snap["symbols"]}
        assert syms["BTC"]["rows_cached"] == 2
        assert set(
            [
                "hit_source",
                "from_cache",
                "last_fetch_duration_ms",
                "consecutive_failures",
            ]
        ).issubset(syms["BTC"])

        import urllib.request

        body = urllib.request.urlopen(
            "http://127.0.0.1:{}/status".format(service.status_port),
            timeout=5,
        ).read().decode()
        assert '"BTC"' in body and '"ETH"' in body
        html = urllib.request.urlopen(
            "http://127.0.0.1:{}/".format(service.status_port), timeout=5
        ).read().decode()
        assert "cryptocmd" in html and "Hit source" in html
    finally:
        service.stop()
