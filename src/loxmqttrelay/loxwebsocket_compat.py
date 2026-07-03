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
    _patch_async_init_closes_stale_session()
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


def _patch_async_init_closes_stale_session() -> None:
    """
    LoxWs.reconnect()'s retry loop calls async_init() on every attempt, but
    only calls stop() (which closes self._session/self._ws) once, before the
    loop starts - not between attempts. async_init() itself unconditionally
    does `self._session = aiohttp.ClientSession(...)`, silently overwriting
    (and leaking) the previous attempt's session on every failed reconnect
    attempt. Since reconnect() retries indefinitely by default
    (max_reconnect_attempts=0), a sustained outage leaks one ClientSession
    (and its underlying sockets) roughly every CONNECT_DELAY (15s) - visible
    as repeated "Unclosed client session" warnings - and can exhaust file
    descriptors on a long enough outage, making it even harder to ever
    reconnect.

    This wraps async_init() to close any still-open previous session first,
    so each attempt's resources are always released before the next one
    opens new ones - a bounded, self-cleaning retry loop instead of an
    accumulating one. It does not change the retry count, delay, or any
    other reconnect behavior - retries stay unlimited by design, since this
    is a long-running background service that should keep trying to recover
    from a Miniserver outage of unknown duration rather than give up.
    """
    try:
        from loxwebsocket.lox_ws_api import LoxWs
    except ImportError:
        logger.warning(
            "loxwebsocket.lox_ws_api.LoxWs not importable - "
            "skipping the stale-session-on-reconnect patch"
        )
        return

    original_async_init = LoxWs.async_init

    if getattr(original_async_init, "_loxmqttrelay_patched", False):
        return

    async def patched_async_init(self):
        return await _close_stale_session_and_call(self, original_async_init)

    patched_async_init._loxmqttrelay_patched = True
    LoxWs.async_init = patched_async_init
    logger.info("Applied loxwebsocket compat patch: async_init() closes stale session before reconnecting")


async def _close_stale_session_and_call(instance, original_async_init):
    """
    Closes `instance._session` first if it's still set and open, then calls
    `original_async_init(instance)`. Split out from the patch installer so
    the actual close-then-call behavior can be unit tested directly, without
    needing a real (or even patched) LoxWs instance or any network I/O.
    """
    old_session = getattr(instance, "_session", None)
    if old_session is not None and not old_session.closed:
        try:
            await old_session.close()
        except Exception:
            logger.warning(
                "Failed to close stale websocket session before reconnecting",
                exc_info=True,
            )
    return await original_async_init(instance)
