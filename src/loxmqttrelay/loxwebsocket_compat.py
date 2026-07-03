"""
Runtime patches for known bugs in the third-party `loxwebsocket` dependency
that we can't fix upstream from here. Kept isolated in one module so it's
obvious what's being patched, why, and so the patch can be dropped cleanly
once a fixed `loxwebsocket` release is available.

Import `apply_patches()` once, before any websocket traffic is sent.
"""
import asyncio

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
    _patch_websocket_writes_are_serialized()
    _patch_normal_closure_logging()
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


def _patch_websocket_writes_are_serialized() -> None:
    """
    Every write to the shared websocket connection - LoxWs's own keep_alive()
    (the sole heartbeat mechanism; both aiohttp's automatic heartbeat and
    autoping are explicitly disabled via heartbeat=None, autoping=False in
    async_init()'s ws_connect() call) and command sends (send_command(),
    send_websocket_command(), ...), plus loxMqttRelay's own
    send_to_minisever_via_websocket() - ultimately call self._ws.send_str().
    aiohttp does not guarantee concurrent writes on one websocket from
    different tasks stay un-interleaved, and unlike command sends,
    keep_alive() isn't actually protected against that at all:

        async with asyncio.Lock():
            await self._ws.send_str("keepalive")

    creates a FRESH, unshared Lock instance on every loop iteration, so it
    never actually excludes anything - not other keep_alive() iterations,
    and not loxMqttRelay's own writes either, since those are serialized by
    a *different* lock living in a different module
    (HttpMiniserverHandler._ws_write_lock) that loxwebsocket has no idea
    about.

    This is a plausible root cause for the "code 1000" (normal closure)
    disconnects some Miniservers perform when they judge the heartbeat to
    have failed: if the plaintext "keepalive" write interleaves with an
    encrypted jdev/sys/enc/... command write on the wire, the Miniserver can
    receive a malformed frame sequence and simply close the connection
    rather than diagnosing it further.

    Rather than patch every individual send call site (fragile if the
    library adds more), this wraps self._ws.send_str itself with one shared
    lock the moment the connection is established (right after async_init()
    assigns self._ws), so every future write through that connection -
    library-internal or loxMqttRelay's own - is serialized against every
    other one automatically. HttpMiniserverHandler._ws_write_lock is left in
    place as well (harmless, minor redundancy) rather than removed, since
    this patch covers a strictly larger set of writers.
    """
    try:
        from loxwebsocket.lox_ws_api import LoxWs
    except ImportError:
        logger.warning(
            "loxwebsocket.lox_ws_api.LoxWs not importable - "
            "skipping the websocket-write-serialization patch"
        )
        return

    original_async_init = LoxWs.async_init

    if getattr(original_async_init, "_loxmqttrelay_lock_patched", False):
        return

    async def patched_async_init(self):
        result = await original_async_init(self)
        wrap_ws_send_str_with_shared_lock(self)
        return result

    patched_async_init._loxmqttrelay_lock_patched = True
    LoxWs.async_init = patched_async_init
    logger.info(
        "Applied loxwebsocket compat patch: serialized websocket writes "
        "(keepalive vs. commands)"
    )


def wrap_ws_send_str_with_shared_lock(instance) -> None:
    """
    Wraps `instance._ws.send_str` so every call to it acquires
    `instance._loxmqttrelay_write_lock` (created once, reused across
    reconnects) first. Split out from the patch installer so it can be unit
    tested directly against a fake instance/websocket, without needing a
    real LoxWs or any network I/O.
    """
    lock = getattr(instance, "_loxmqttrelay_write_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        instance._loxmqttrelay_write_lock = lock

    ws = getattr(instance, "_ws", None)
    if ws is None:
        return
    if getattr(ws.send_str, "_loxmqttrelay_patched", False):
        return

    original_send_str = ws.send_str

    async def locked_send_str(data, *args, **kwargs):
        async with lock:
            return await original_send_str(data, *args, **kwargs)

    locked_send_str._loxmqttrelay_patched = True
    ws.send_str = locked_send_str


def _patch_normal_closure_logging() -> None:
    """
    handle_connection_interrupt()'s `match close_code:` has no case for 1000
    ("normal closure" per RFC 6455) - it falls into `case _: "...unknown
    code: {code}"`, which is misleading: 1000 means the Miniserver closed
    the connection deliberately, not that anything is unknown. In practice
    that happens on a Miniserver reboot/firmware update, or when it judges
    the client's heartbeat to have failed (see the write-serialization patch
    above for a plausible cause of the latter).

    Purely additive: logs one extra, clearer line when close_code == 1000,
    then still calls the original method unchanged - it does not alter
    reconnect behavior.
    """
    try:
        from loxwebsocket.lox_ws_api import LoxWs
    except ImportError:
        logger.warning(
            "loxwebsocket.lox_ws_api.LoxWs not importable - "
            "skipping the normal-closure logging patch"
        )
        return

    original_handle_connection_interrupt = LoxWs.handle_connection_interrupt

    if getattr(original_handle_connection_interrupt, "_loxmqttrelay_patched", False):
        return

    async def patched_handle_connection_interrupt(self, msg_type=None, exception=None):
        close_code = self._ws.close_code if self._ws else None
        log_if_normal_closure(close_code)
        return await original_handle_connection_interrupt(self, msg_type=msg_type, exception=exception)

    patched_handle_connection_interrupt._loxmqttrelay_patched = True
    LoxWs.handle_connection_interrupt = patched_handle_connection_interrupt
    logger.info("Applied loxwebsocket compat patch: clearer logging for close code 1000")


def log_if_normal_closure(close_code) -> None:
    """Split out from the patch installer so it can be unit tested directly."""
    if close_code == 1000:
        logger.warning(
            "WebSocket closed with code 1000 (normal closure) - the Miniserver closed the "
            "connection on purpose. Typically a Miniserver reboot/firmware update, or the "
            "Miniserver judging the heartbeat to have failed."
        )
