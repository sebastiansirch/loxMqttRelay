import pytest

from loxwebsocket.encryption import LxEncryptionHandler
from loxmqttrelay.loxwebsocket_compat import apply_patches


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
