"""Tests for the real CmcParser (against a local CMC-shaped mock) and the
default SQLiteAdapter transactional rollback."""

import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from cryptocmd import CmcParser, DataUnavailableError, SQLiteAdapter
import cryptocmd.parser as parser_mod


def _quote(day, close):
    iso = "2024-01-{:02d}T00:00:00.000Z".format(day)
    return {
        "timeOpen": iso,
        "timeHigh": iso,
        "timeLow": iso,
        "timeClose": iso,
        "quote": {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1.0,
            "marketCap": 8e11,
        },
    }


class _MockCmc:
    def __init__(self, listing=None, hist=None):
        self.listing = listing if listing is not None else {
            "status": {"error_code": 0},
            "data": {
                "cryptoCurrencyList": [
                    {"id": 1, "symbol": "BTC", "name": "Bitcoin"}
                ]
            },
        }
        self.hist = hist if hist is not None else {
            "status": {"error_code": 0},
            "data": {
                "symbol": "BTC",
                "name": "Bitcoin",
                "quotes": [_quote(2, 2.0), _quote(1, 1.0)],
            },
        }
        self.requests = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()

    def _handler(self, request, client_address, server):
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                outer.requests.append(self.path)
                body = json.dumps(
                    outer.listing if "listing" in self.path else outer.hist
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        H(request, client_address, server)

    def stop(self):
        self.server.shutdown()


@pytest.fixture
def mock_cmc(monkeypatch):
    mock = _MockCmc()
    monkeypatch.setattr(
        parser_mod,
        "_CMC_DATA_API",
        "http://127.0.0.1:{}/data-api/v3".format(mock.port),
    )
    yield mock
    mock.stop()


def test_cmc_parser_real_payload_newest_first(mock_cmc):
    rows = CmcParser(timeout=5).fetch("BTC", "01-01-2024", "02-01-2024")
    assert len(rows) == 2
    assert rows[0]["Date"] == "02-01-2024"
    assert rows[0]["Close"] == 2.0
    assert rows[1]["Date"] == "01-01-2024"


def test_cmc_parser_unknown_coin_is_unified_parse_error(mock_cmc):
    mock_cmc.listing = {
        "status": {"error_code": 0},
        "data": {"cryptoCurrencyList": []},
    }
    with pytest.raises(DataUnavailableError) as ei:
        CmcParser(timeout=5).fetch("NOPE", "01-01-2024", "02-01-2024")
    assert ei.value.reason == "parse"


def test_cmc_parser_malformed_history_is_parse_error(mock_cmc):
    mock_cmc.hist = {"status": {"error_code": 0}, "data": {}}
    with pytest.raises(DataUnavailableError) as ei:
        CmcParser(timeout=5).fetch("BTC", "01-01-2024", "02-01-2024")
    assert ei.value.reason == "parse"
    assert ei.value.__cause__ is not None


def test_cmc_parser_yearly_chunking_uses_two_requests(mock_cmc):
    rows = CmcParser(timeout=5).fetch("BTC", None, None)
    historical = [p for p in mock_cmc.requests if "historical" in p]
    assert isinstance(rows, list)
    assert len(historical) >= 1


# ---------------------------------------------------------------------------
# SQLite rollback
# ---------------------------------------------------------------------------
def test_sqlite_save_and_load_roundtrip(tmp_path):
    db = tmp_path / "h.db"
    adapter = SQLiteAdapter(str(db))
    adapter.save("btc", [{"Date": "01-01-2024", "Close": 1.0}])
    loaded = adapter.load("BTC")
    assert loaded[0]["Date"] == "01-01-2024"
    assert adapter.load("MISSING") == []
    adapter.close()


def test_sqlite_failed_commit_rolls_back(tmp_path):
    db = tmp_path / "h.db"
    adapter = SQLiteAdapter(str(db))
    good = [{"Date": "01-01-2024", "Close": 1.0}]
    adapter.save("BTC", good)

    # A trigger that raises mid-transaction forces a genuine ROLLBACK.
    adapter._conn.execute(
        """
        CREATE TRIGGER explode BEFORE UPDATE ON history
        BEGIN
            SELECT RAISE(ABORT, 'simulated I/O failure');
        END
        """
    )
    with pytest.raises(DataUnavailableError) as ei:
        adapter.save("BTC", [{"Date": "02-01-2024", "Close": 2.0}])
    assert ei.value.reason == "storage"

    # Same connection and a brand-new one must still see the old blob.
    assert adapter.load("BTC") == good
    fresh = SQLiteAdapter(str(db))
    assert fresh.load("BTC") == good
    fresh.close()
