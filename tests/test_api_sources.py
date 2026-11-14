"""Unit tests for src/ingestion/api_sources.py.

The retry logic is the only thing in this module worth testing, and it had no
coverage at all -- which matters because the whole point of the module is to
survive a rate limit without taking the DAG down with it. No network is
touched: requests.get and time.sleep are both replaced.
"""

import pytest
import requests

from src.ingestion import api_sources


class FakeResponse:
    def __init__(self, status_code=200, payload=None, bad_json=False):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._payload


@pytest.fixture
def no_sleep(monkeypatch):
    """Retries must not actually wait: the backoff is 5s, 10s, 15s."""
    slept = []
    monkeypatch.setattr(api_sources.time, "sleep", slept.append)
    return slept


def fake_get(monkeypatch, responses):
    """Serve `responses` in order; record the calls."""
    calls = []
    queue = list(responses)

    def _get(url, params=None, timeout=None):
        calls.append({"url": url, "params": params, "timeout": timeout})
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(api_sources.requests, "get", _get)
    return calls


def test_a_plain_200_is_returned_without_retrying(monkeypatch, no_sleep):
    calls = fake_get(monkeypatch, [FakeResponse(payload={"data": [1]})])
    assert api_sources.fetch_json("https://x/y") == {"data": [1]}
    assert len(calls) == 1
    assert no_sleep == []


def test_a_rate_limit_is_retried_and_then_succeeds(monkeypatch, no_sleep):
    calls = fake_get(monkeypatch, [
        FakeResponse(429),
        FakeResponse(payload={"data": "second try"}),
    ])
    assert api_sources.fetch_json("https://x/y") == {"data": "second try"}
    assert len(calls) == 2
    # Backoff grows with the attempt number rather than being flat.
    assert no_sleep == [5.0]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_every_retryable_status_is_retried(monkeypatch, no_sleep, status):
    fake_get(monkeypatch, [FakeResponse(status), FakeResponse()])
    assert api_sources.fetch_json("https://x/y") == {"ok": True}
    assert len(no_sleep) == 1


def test_it_gives_up_after_the_last_attempt(monkeypatch, no_sleep):
    calls = fake_get(monkeypatch, [FakeResponse(503)] * 3)
    with pytest.raises(RuntimeError, match="giving up"):
        api_sources.fetch_json("https://x/y")
    assert len(calls) == 3
    # Two pauses for three attempts: it does not sleep after the last one.
    assert no_sleep == [5.0, 10.0]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_a_permanent_error_is_not_retried(monkeypatch, no_sleep, status):
    # The point of the fix: retrying a 404 only delays the real error by the
    # whole backoff budget.
    calls = fake_get(monkeypatch, [FakeResponse(status)])
    with pytest.raises(RuntimeError, match="not retryable"):
        api_sources.fetch_json("https://x/y")
    assert len(calls) == 1
    assert no_sleep == []


def test_a_connection_error_is_retried(monkeypatch, no_sleep):
    fake_get(monkeypatch, [
        requests.ConnectionError("reset by peer"),
        FakeResponse(payload={"data": "recovered"}),
    ])
    assert api_sources.fetch_json("https://x/y") == {"data": "recovered"}


def test_unparseable_json_is_retried_then_reported(monkeypatch, no_sleep):
    # A truncated body raises on .json(), which is worth another attempt.
    calls = fake_get(monkeypatch, [FakeResponse(bad_json=True)] * 3)
    with pytest.raises(RuntimeError, match="giving up"):
        api_sources.fetch_json("https://x/y")
    assert len(calls) == 3


def test_the_two_endpoints_build_the_right_requests(monkeypatch, no_sleep):
    calls = fake_get(monkeypatch, [FakeResponse(), FakeResponse()])

    api_sources.fetch_market_chart("binancecoin", 365)
    api_sources.fetch_fear_greed(0)

    assert calls[0]["url"].endswith("/coins/binancecoin/market_chart")
    assert calls[0]["params"] == {"vs_currency": "usd", "days": 365}
    assert calls[1]["url"] == api_sources.FEAR_GREED_URL
    assert calls[1]["params"] == {"limit": 0, "format": "json"}
    # Every call is bounded: a hung API must not hold an Airflow slot forever.
    assert all(c["timeout"] == 60 for c in calls)
