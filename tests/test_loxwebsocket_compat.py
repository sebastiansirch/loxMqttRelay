import pytest

from loxwebsocket.encryption import LxEncryptionHandler
from loxwebsocket.lox_ws_api import LoxWs
from loxmqttrelay.loxwebsocket_compat import apply_patches, _close_stale_session_and_call


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
    apply_patches()
    patched_once = LoxWs.async_init

    apply_patches()
    apply_patches()

    assert LoxWs.async_init is patched_once
    assert getattr(LoxWs.async_init, "_loxmqttrelay_patched", False) is True
