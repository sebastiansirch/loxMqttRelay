import asyncio

import pytest

from loxwebsocket import const as loxwebsocket_const
from loxwebsocket.exceptions import LoxoneException
from loxwebsocket.lox_ws_api import LoxWs
from loxwebsocket.lxtoken import LxToken
from loxmqttrelay.config import global_config
from loxmqttrelay.loxwebsocket_compat import (
    apply_patches,
    reconnect_backoff_delay,
    run_reconnect_with_backoff,
)


# --- reconnect() waits with exponential backoff instead of a fixed delay ---

def test_reconnect_backoff_delay_first_attempt_is_always_immediate():
    """The very first reconnect attempt should never wait, regardless of config."""
    assert reconnect_backoff_delay(1) == 0.0


def test_reconnect_backoff_delay_follows_default_schedule_and_caps_at_connect_delay():
    assert reconnect_backoff_delay(1) == 0.0
    assert reconnect_backoff_delay(2) == 1.0
    assert reconnect_backoff_delay(3) == 2.0
    assert reconnect_backoff_delay(4) == 4.0
    assert reconnect_backoff_delay(5) == 8.0
    assert reconnect_backoff_delay(6) == loxwebsocket_const.CONNECT_DELAY
    assert reconnect_backoff_delay(7) == loxwebsocket_const.CONNECT_DELAY


def test_reconnect_backoff_delay_respects_custom_config(monkeypatch):
    monkeypatch.setattr(
        global_config.miniserver, "miniserver_websocket_reconnect_initial_delay_seconds", 0.1
    )
    monkeypatch.setattr(
        global_config.miniserver, "miniserver_websocket_reconnect_backoff_multiplier", 3.0
    )

    assert reconnect_backoff_delay(1) == 0.0
    assert reconnect_backoff_delay(2) == pytest.approx(0.1)
    assert reconnect_backoff_delay(3) == pytest.approx(0.3)
    assert reconnect_backoff_delay(4) == pytest.approx(0.9)


class _FakeReconnectingLoxWs:
    """
    Mimics just enough of LoxWs for run_reconnect_with_backoff() to drive:
    state/_max_reconnect_attempts/_token plus the async methods it calls
    (stop, _cancel_stale_background_tasks, http_ping, async_init, start,
    send_event) and the nested EventType enum it references.
    """

    class EventType:
        RECONNECTED = "RECONNECTED"

    def __init__(self, max_reconnect_attempts, ping_results=None, async_init_results=None):
        self.state = "CONNECTED"
        self._max_reconnect_attempts = max_reconnect_attempts
        self._token = LxToken(token="stale-token-value")
        self.stop_called = False
        self.start_called = False
        self.cancel_stale_background_tasks_called = False
        self.sent_events = []
        self._ping_results = iter(ping_results or [])
        self._async_init_results = iter(async_init_results or [])

    async def stop(self):
        self.stop_called = True

    def _cancel_stale_background_tasks(self):
        self.cancel_stale_background_tasks_called = True

    async def http_ping(self):
        return next(self._ping_results, True)

    async def async_init(self):
        return next(self._async_init_results, True)

    async def start(self):
        self.start_called = True

    async def send_event(self, event_type):
        self.sent_events.append(event_type)


@pytest.mark.asyncio
async def test_run_reconnect_with_backoff_succeeds_and_resets_token(monkeypatch):
    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    instance = _FakeReconnectingLoxWs(max_reconnect_attempts=0, async_init_results=[True])
    stale_token = instance._token

    await run_reconnect_with_backoff(instance)

    assert instance.stop_called is True
    assert instance.cancel_stale_background_tasks_called is True
    assert instance._token is not stale_token
    assert instance._token.token == ""
    assert instance.start_called is True
    assert instance.sent_events == [_FakeReconnectingLoxWs.EventType.RECONNECTED]
    assert delays == [0.0]


@pytest.mark.asyncio
async def test_run_reconnect_with_backoff_uses_increasing_delays_capped_at_connect_delay(monkeypatch):
    """
    First 3 attempts fail http_ping, next 2 fail async_init, 6th succeeds -
    confirms the first attempt is immediate, the wait then grows
    exponentially (1s, 2s, 4s, 8s), and caps at loxwebsocket's own
    CONNECT_DELAY (15s) on the 6th, matching what the fixed-delay behavior
    would have been from then on.
    """
    delays = []

    async def fake_sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    instance = _FakeReconnectingLoxWs(
        max_reconnect_attempts=0,
        ping_results=[False, False, False, True, True, True],
        async_init_results=[False, False, True],
    )

    await run_reconnect_with_backoff(instance)

    assert delays == [0.0, 1.0, 2.0, 4.0, 8.0, 15.0]
    assert instance.start_called is True


@pytest.mark.asyncio
async def test_run_reconnect_with_backoff_skips_when_already_reconnecting():
    """Mirrors reconnect()'s own re-entrancy guard."""
    instance = _FakeReconnectingLoxWs(max_reconnect_attempts=0)
    instance.state = "RECONNECTING"

    await run_reconnect_with_backoff(instance)

    assert instance.stop_called is False
    assert instance.cancel_stale_background_tasks_called is False


@pytest.mark.asyncio
async def test_run_reconnect_with_backoff_raises_after_exhausting_attempts(monkeypatch):
    async def fake_sleep(seconds):
        pass

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    instance = _FakeReconnectingLoxWs(
        max_reconnect_attempts=2,
        ping_results=[True, True],
        async_init_results=[False, False],
    )

    with pytest.raises(LoxoneException):
        await run_reconnect_with_backoff(instance)

    assert instance.start_called is False
    assert instance.sent_events == []


def test_patch_reconnect_uses_backoff_delay_is_installed_and_idempotent():
    apply_patches()
    patched_once = LoxWs.reconnect

    apply_patches()
    apply_patches()

    assert LoxWs.reconnect is patched_once
    assert getattr(LoxWs.reconnect, "_loxmqttrelay_backoff_patched", False) is True
