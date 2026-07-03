"""
Runtime patches for known bugs in the third-party `loxwebsocket` dependency
that we can't fix upstream from here. Kept isolated in one module so it's
obvious what's being patched, why, and so the patch can be dropped cleanly
once a fixed `loxwebsocket` release is available.

Import `apply_patches()` once, before any websocket traffic is sent.
"""
from loxmqttrelay.logging_config import get_lazy_logger

logger = get_lazy_logger(__name__)

_patches_applied = False


def apply_patches() -> None:
    """Apply all known loxwebsocket patches. Safe to call more than once."""
    global _patches_applied
    if _patches_applied:
        return
    _patch_salt_generation_return_value()
    _patches_applied = True


def _patch_salt_generation_return_value() -> None:
    """
    LxEncryptionHandler.genarate_salt() (sic) sets self._salt as a side effect
    but has no `return` statement, so it implicitly returns None. Both call
    sites in LxEncryptionHandler.encrypt() do `self._salt = self.genarate_salt()`,
    which immediately overwrites the salt genarate_salt() just set correctly
    with None - on the very first encrypted command, and again every
    SALT_MAX_USE_COUNT (30) messages or SALT_MAX_AGE_SECONDS (1h) thereafter.
    Every encrypted websocket command after that point is built as
    "salt/None/<command>" instead of a real random salt.

    This wraps the original method so it still does exactly what it did
    before (including setting self._salt as a side effect), but additionally
    returns the salt it just set, fixing the call sites without having to
    duplicate or guess at the salt-generation logic itself.
    """
    try:
        from loxwebsocket.encryption import LxEncryptionHandler
    except ImportError:
        logger.warning(
            "loxwebsocket.encryption.LxEncryptionHandler not importable - "
            "skipping the genarate_salt() return-value patch"
        )
        return

    original_genarate_salt = LxEncryptionHandler.genarate_salt

    if getattr(original_genarate_salt, "_loxmqttrelay_patched", False):
        return

    def patched_genarate_salt(self):
        original_genarate_salt(self)
        return self._salt

    patched_genarate_salt._loxmqttrelay_patched = True
    LxEncryptionHandler.genarate_salt = patched_genarate_salt
    logger.info("Applied loxwebsocket compat patch: genarate_salt() return value")
