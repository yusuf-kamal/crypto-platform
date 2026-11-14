"""Unit tests for the one piece of src/streaming/binance_ws_producer.py that
is testable without a socket: the stream URL it subscribes to.

Getting this wrong is quiet rather than loud -- Binance accepts a connection
to a stream name that matches nothing and then simply sends no trades, so the
consumer would sit there with an empty topic and no error anywhere.
"""

from src.streaming.binance_ws_producer import WS_BASE, stream_url


def test_one_symbol():
    url = stream_url(["BTCUSDT"])
    assert url == WS_BASE + "btcusdt@trade"


def test_several_symbols_are_slash_separated():
    url = stream_url(["BTCUSDT", "ETHUSDT", "XRPUSDT"])
    assert url == WS_BASE + "btcusdt@trade/ethusdt@trade/xrpusdt@trade"


def test_symbols_are_lowercased():
    # Binance's combined stream rejects nothing and delivers nothing for an
    # uppercase stream name, so this is the detail that matters.
    assert "@trade" in stream_url(["BTCUSDT"])
    assert "BTCUSDT" not in stream_url(["BTCUSDT"])
    assert "btcusdt" in stream_url(["BTCUSDT"])


def test_it_uses_the_combined_stream_endpoint():
    # /stream?streams= is the combined endpoint; /ws/ would need one
    # connection per symbol.
    assert WS_BASE.startswith("wss://")
    assert "/stream?streams=" in WS_BASE
