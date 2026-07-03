import asyncio
import logging

import pytest

from loxwebsocket.encryption import LxEncryptionHandler
from loxwebsocket.lox_ws_api import LoxWs
from loxwebsocket.lxtoken import LxToken
from loxmqttrelay.loxwebsocket_compat import (
    apply_patches,
    _close_stale_session_and_call,
    wrap_ws_send_str_with_shared_lock,
    log_if_normal_closure,
    reset_token_if_not_already_reconnecting,
)


def test_genarate_salt_returns_the_salt_it_set():
    """
    Unpatched, LxEncryptionHandler.genarate_salt() sets self._salt as a side
    effect but has no return statement (implicitly returns None). Both call
    sites in encrypt() do `self._salt = self.genarate_salt()`, which
    immediately clobbers the just-set salt with None. The patch must make
    genarate_salt() return the very value it set on self._salt.
    """
    apply_patches()

    handler = LxEncryptionHandler()
    returned = handler.genarate_salt()

    assert returned is not None
    assert returned == handler._salt
    assert returned != ""


def test_apply_patches_is_idempotent():
    """Calling apply_patches() repeatedly must not stack multiple wrappers."""
    apply_patches()
    apply_patches()
    apply_patches()

    handler = LxEncryptionHandler()
    returned = handler.genarate_salt()

    assert returned == handler._salt


@pytest.mark.asyncio
async def test_encrypt_no_longer_embeds_none_as_the_salt():
    """
    End-to-end check on the actual bug symptom: before the patch, the very
    first encrypt() call produces "salt/None/<command>...". After the patch
    it must contain a real salt instead of the literal string "None".
    """
    apply_patches()

    handler = LxEncryptionHandler()

    await handler.encrypt("jdev/sps/io/some-topic/1")

    assert handler._salt is not None
    assert handler._salt != "None"


# --- async_init() stale-session patch ---

class _FakeSession:
    def __init__(self, closed: bool = False, raise_on_close: bool = False):
        self.closed = closed
        self.close_called = False
        self._raise_on_close = raise_on_close

    async def close(self):
        self.close_called = True
        if self._raise_on_close:
            raise RuntimeError("boom")
        self.closed = True


@pytest.mark.asyncio
async def test_close_stale_session_and_call_closes_open_session_before_original():
    call_order = []
    session = _FakeSession(closed=False)

    class _FakeSelf:
        pass

    instance = _FakeSelf()
    instance._session = session

    async def fake_original(self):
        call_order.append("original")
        return "ok"

    async def tracking_close():
        call_order.append("close")
        session.closed = True

    session.close = tracking_close

    result = await _close_stale_session_and_call(instance, fake_original)

    assert result == "ok"
    assert call_order == ["close", "original"]


@pytest.mark.asyncio
async def test_close_stale_session_and_call_skips_close_when_already_closed():
    session = _FakeSession(closed=True)

    class _FakeSelf:
        pass

    instance = _FakeSelf()
    instance._session = session

    async def fake_original(self):
        return "ok"

    await _close_stale_session_and_call(instance, fake_original)

    assert session.close_called is False


@pytest.mark.asyncio
async def test_close_stale_session_and_call_handles_missing_session():
    """First-ever connect: there is no previous session at all yet."""
    class _FakeSelf:
        pass

    instance = _FakeSelf()  # no ._session attribute set
    called = []

    async def fake_original(self):
        called.append(True)
        return "ok"

    result = await _close_stale_session_and_call(instance, fake_original)

    assert result == "ok"
    assert called == [True]


@pytest.mark.asyncio
async def test_close_stale_session_and_call_still_runs_original_if_close_fails():
    session = _FakeSession(closed=False, raise_on_close=True)

    class _FakeSelf:
        pass

    instance = _FakeSelf()
    instance._session = session

    async def fake_original(self):
        return "ran anyway"

    result = await _close_stale_session_and_call(instance, fake_original)

    assert result == "ran anyway"
    assert session.close_called is True


def test_async_init_patch_is_installed_and_idempotent():
    """
    Note: LoxWs.async_init ends up wrapped by more than one patch (the
    stale-session-close patch and the write-serialization patch both wrap
    it), so the outermost function only carries the *last* patch's marker.
    Idempotency - no additional wrapping on repeated apply_patches() calls -
    is what's actually being verified here; see
    test_patch_websocket_write_serialization_is_installed_and_idempotent for
    that patch's own marker check.
    """
    apply_patches()
    patched_once = LoxWs.async_init

    apply_patches()
    apply_patches()

    assert LoxWs.async_init is patched_once


# --- shared write lock (keepalive vs. commands) ---

class _FakeWs:
    def __init__(self):
        self.sent = []

    async def send_str(self, data):
        self.sent.append(data)


class _FakeSelf:
    pass


@pytest.mark.asyncio
async def test_wrap_ws_send_str_serializes_concurrent_writes():
    """
    Load-style regression test for the keepalive/command race: fire many
    concurrent send_str() calls (as keep_alive() and command sends would
    under load) and confirm the wrapped version never lets two run at once.
    """
    instance = _FakeSelf()
    instance._ws = _FakeWs()

    concurrency = {"current": 0, "max": 0}
    original_send_str = instance._ws.send_str

    async def tracking_send_str(data):
        concurrency["current"] += 1
        concurrency["max"] = max(concurrency["max"], concurrency["current"])
        await asyncio.sleep(0.01)
        concurrency["current"] -= 1
        await original_send_str(data)

    instance._ws.send_str = tracking_send_str

    wrap_ws_send_str_with_shared_lock(instance)

    await asyncio.gather(*[instance._ws.send_str(f"msg{i}") for i in range(20)])

    assert concurrency["max"] == 1
    assert len(instance._ws.sent) == 20


def test_wrap_ws_send_str_reuses_same_lock_across_reconnects():
    instance = _FakeSelf()
    instance._ws = _FakeWs()

    wrap_ws_send_str_with_shared_lock(instance)
    lock_first = instance._loxmqttrelay_write_lock

    # simulate a reconnect: a brand new _ws object is assigned
    instance._ws = _FakeWs()
    wrap_ws_send_str_with_shared_lock(instance)

    assert instance._loxmqttrelay_write_lock is lock_first


def test_wrap_ws_send_str_is_idempotent_per_ws():
    instance = _FakeSelf()
    instance._ws = _FakeWs()

    wrap_ws_send_str_with_shared_lock(instance)
    wrapped_once = instance._ws.send_str
    wrap_ws_send_str_with_shared_lock(instance)

    assert instance._ws.send_str is wrapped_once


def test_wrap_ws_send_str_handles_missing_ws_gracefully():
    instance = _FakeSelf()  # no ._ws attribute set at all yet

    wrap_ws_send_str_with_shared_lock(instance)  # must not raise


def test_patch_websocket_write_serialization_is_installed_and_idempotent():
    apply_patches()
    patched_once = LoxWs.async_init

    apply_patches()
    apply_patches()

    assert LoxWs.async_init is patched_once
    assert getattr(LoxWs.async_init, "_loxmqttrelay_lock_patched", False) is True


# --- clearer logging for close code 1000 ---

def test_log_if_normal_closure_logs_on_code_1000(caplog):
    with caplog.at_level(logging.WARNING):
        log_if_normal_closure(1000)

    assert any("code 1000" in record.getMessage() for record in caplog.records)


def test_log_if_normal_closure_stays_silent_on_other_codes(caplog):
    with caplog.at_level(logging.WARNING):
        log_if_normal_closure(4004)
        log_if_normal_closure(None)

    assert not any("code 1000" in record.getMessage() for record in caplog.records)


def test_patch_normal_closure_logging_is_installed_and_idempotent():
    apply_patches()
    patched_once = LoxWs.handle_connection_interrupt

    apply_patches()
    apply_patches()

    assert LoxWs.handle_connection_interrupt is patched_once
    assert getattr(LoxWs.handle_connection_interrupt, "_loxmqttrelay_patched", False) is True


# --- reconnect() actually resets the token it reuses ---

class _FakeSelfWithToken:
    def __init__(self, state, token):
        self.state = state
        self._token = token


def test_reset_token_if_not_already_reconnecting_resets_when_idle():
    stale_token = LxToken(token="stale-token-value")
    instance = _FakeSelfWithToken(state="CLOSED", token=stale_token)

    did_reset = reset_token_if_not_already_reconnecting(instance, LxToken)

    assert did_reset is True
    assert instance._token is not stale_token
    assert instance._token.token == ""


def test_reset_token_if_not_already_reconnecting_skips_when_already_reconnecting():
    """
    Mirrors LoxWs.reconnect()'s own re-entrancy guard: if a reconnect is
    already under way, a concurrent call must not clobber the token it may
    still be relying on.
    """
    stale_token = LxToken(token="stale-token-value")
    instance = _FakeSelfWithToken(state="RECONNECTING", token=stale_token)

    did_reset = reset_token_if_not_already_reconnecting(instance, LxToken)

    assert did_reset is False
    assert instance._token is stale_token


def test_patch_reconnect_resets_token_is_installed_and_idempotent():
    apply_patches()
    patched_once = LoxWs.reconnect

    apply_patches()
    apply_patches()

    assert LoxWs.reconnect is patched_once
    assert getattr(LoxWs.reconnect, "_loxmqttrelay_patched", False) is True
