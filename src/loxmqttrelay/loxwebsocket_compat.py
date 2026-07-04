"""
Runtime patches for known bugs in the third-party `loxwebsocket` dependency
that we can't fix upstream from here. Kept isolated in one module so it's
obvious what's being patched, why, and so the patch can be dropped cleanly
once a fixed `loxwebsocket` release is available.

Import `apply_patches()` once, before any websocket traffic is sent.
"""
import asyncio

from loxmqttrelay.config import global_config
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
    _patch_encryption_handler_resets_salt_on_connect()
    _patch_normal_closure_logging()
    _patch_reconnect_uses_backoff_delay()
    _patch_key_salt_response_logs_raw_on_error()
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


def _patch_reconnect_uses_backoff_delay() -> None:
    """
    LoxWs.reconnect()'s retry loop waits a fixed c.CONNECT_DELAY (15s,
    loxwebsocket/const.py) before every single attempt, including the very
    first one right after a disconnect - so recovery from even a brief blip
    always takes at least 15s before the library so much as tries again.
    Confirmed against production logs (see docs/) that reconnects do
    otherwise succeed reliably - they're just always gated behind this fixed
    wait.

    Since the wait is inline inside reconnect()'s own loop body (not behind
    any separate, wrappable call), shortening it can't be done with a simple
    before/after wrap like the other patches in this module - this replaces
    reconnect() outright with a copy of the same orchestration (state guard,
    stop(), the retry loop, http_ping()/async_init()/start() calls, the
    give-up/raise branch) but:
      - the fixed sleep is replaced by reconnect_backoff_delay(attempt): the
        first attempt waits
        miniserver_websocket_reconnect_initial_delay_seconds (default 1s),
        each subsequent attempt's wait is multiplied by
        miniserver_websocket_reconnect_backoff_multiplier (default 2x),
        capped at loxwebsocket's own c.CONNECT_DELAY - so behavior converges
        back to identical-to-upstream once several attempts have failed
        (default: 1s, 2s, 4s, 8s, 15s, 15s, ...);
      - the token reset from the reconnect-token-reset fix (self._token, not
        the unused self.token) is folded in directly via
        reset_token_if_not_already_reconnecting(), superseding the
        standalone patch that used to wrap reconnect() for this alone;
      - the attempt-number log line's pre-existing off-by-one
        ("attempt {attempt + 1}" while attempt is already post-increment,
        so the very first attempt logs as "attempt 2") is fixed to log the
        actual attempt number - purely cosmetic, no behavior change.

    http_ping()/async_init()/start() are still called as plain instance
    method calls, so every other patch on this class (stale-session-close,
    write-serialization, salt-reset) keeps applying unchanged - only the
    orchestration/timing of the retry loop itself is duplicated here, not
    any protocol logic. If a future loxwebsocket release changes
    reconnect()'s own implementation, this patch needs to be revisited to
    match.
    """
    try:
        from loxwebsocket.lox_ws_api import LoxWs
    except ImportError:
        logger.warning(
            "loxwebsocket.lox_ws_api.LoxWs not importable - "
            "skipping the reconnect-backoff-delay patch"
        )
        return

    original_reconnect = LoxWs.reconnect

    if getattr(original_reconnect, "_loxmqttrelay_backoff_patched", False):
        return

    async def patched_reconnect(self):
        return await run_reconnect_with_backoff(self)

    patched_reconnect._loxmqttrelay_backoff_patched = True
    LoxWs.reconnect = patched_reconnect
    logger.info("Applied loxwebsocket compat patch: exponential backoff before reconnect attempts")


async def run_reconnect_with_backoff(instance) -> None:
    """
    Reimplementation of LoxWs.reconnect()'s orchestration loop, using
    reconnect_backoff_delay() instead of a fixed wait - see
    _patch_reconnect_uses_backoff_delay() for the full rationale. Split out
    from the patch installer so it can be unit tested directly against a
    fake instance exposing state/_max_reconnect_attempts/_token plus async
    stop()/http_ping()/async_init()/start() methods, without needing a real
    LoxWs or any network I/O.
    """
    from loxwebsocket.lxtoken import LxToken
    from loxwebsocket.exceptions import LoxoneException

    if instance.state == "RECONNECTING":
        return
    await instance.stop()
    reset_token_if_not_already_reconnecting(instance, LxToken)
    instance.state = "RECONNECTING"
    attempt = 0
    while instance._max_reconnect_attempts == 0 or instance._max_reconnect_attempts > attempt:
        attempt += 1
        delay = reconnect_backoff_delay(attempt)
        logger.info(f"Reconnect attempt {attempt} of {instance._max_reconnect_attempts}")
        logger.info(f"Waiting for {delay} seconds before retrying...")
        await asyncio.sleep(delay)
        if not await instance.http_ping():
            continue
        try:
            if await instance.async_init():
                logger.debug("Reconnection successful.")
                await instance.start()
                return
            else:
                logger.debug("Reconnection failed.")
        except Exception as e:
            logger.error("Reconnection failed: %s", e)
    logger.error("All reconnection attempts failed.")
    raise LoxoneException("All reconnection attempts failed.")


def reconnect_backoff_delay(attempt: int) -> float:
    """
    Returns the wait (seconds) before reconnect attempt number `attempt`
    (1-based): starts at
    miniserver.miniserver_websocket_reconnect_initial_delay_seconds,
    multiplies by miniserver.miniserver_websocket_reconnect_backoff_multiplier
    each further attempt, capped at loxwebsocket's own c.CONNECT_DELAY so
    behavior converges back to the library's original fixed delay once
    enough attempts have failed. Split out from the patch installer so it
    can be unit tested directly, without needing a real LoxWs.
    """
    from loxwebsocket import const as loxwebsocket_const

    initial = global_config.miniserver.miniserver_websocket_reconnect_initial_delay_seconds
    multiplier = global_config.miniserver.miniserver_websocket_reconnect_backoff_multiplier
    cap = loxwebsocket_const.CONNECT_DELAY
    return min(initial * (multiplier ** (attempt - 1)), cap)


def reset_token_if_not_already_reconnecting(instance, token_factory) -> bool:
    """
    If a reconnect isn't already in progress (mirrors LoxWs.reconnect()'s own
    re-entrancy guard), resets `instance._token` to a fresh instance via
    `token_factory()`. Returns True if the reset happened. Split out from the
    patch installer so it can be unit tested directly against a fake
    instance, without needing a real LoxWs or any network I/O.
    """
    if instance.state == "RECONNECTING":
        return False
    instance._token = token_factory()
    return True


def _patch_key_salt_response_logs_raw_on_error() -> None:
    """
    LxJsonKeySalt.read_user_salt_responce() has no try/except at all:

        def read_user_salt_responce(self, reponse):
            js = json.loads(reponse)
            value = js["LL"]["value"]     # <- crashes if js["LL"] isn't a dict
            self.key = value["key"]
            self.salt = value["salt"]

    Observed in production as `reconnect() failed: string indices must be
    integers, not 'str'` - meaning the Miniserver's response to
    `jdev/sys/getkey2/<user>` (sent from acquire_token(), reached once the
    reconnect-token-reset fix above routes a reconnect there directly) had
    `"LL"` as a plain string rather than the expected object, and nothing
    caught or logged what that string actually was before the exception
    propagated, uncaught, all the way up to reconnect()'s generic handler.

    A plain string LL is often how the Miniserver reports a rejected/failed
    request (e.g. rate-limiting after repeated failed auth attempts, an
    unrecognized user) rather than the request succeeding - but without the
    raw text, there's no way to tell which from the log.

    This wraps the method to log the raw response before re-raising the
    same exception unchanged, so a future occurrence shows what the
    Miniserver actually sent back instead of just the bare Python exception
    message.
    """
    try:
        from loxwebsocket.encryption import LxJsonKeySalt
    except ImportError:
        logger.warning(
            "loxwebsocket.encryption.LxJsonKeySalt not importable - "
            "skipping the key/salt-response logging patch"
        )
        return

    original_read_user_salt_responce = LxJsonKeySalt.read_user_salt_responce

    if getattr(original_read_user_salt_responce, "_loxmqttrelay_patched", False):
        return

    def patched_read_user_salt_responce(self, reponse):
        return log_raw_response_on_error(original_read_user_salt_responce, self, reponse)

    patched_read_user_salt_responce._loxmqttrelay_patched = True
    LxJsonKeySalt.read_user_salt_responce = patched_read_user_salt_responce
    logger.info("Applied loxwebsocket compat patch: log raw response on key/salt parse failure")


def log_raw_response_on_error(original_fn, instance, raw_response):
    """
    Calls `original_fn(instance, raw_response)`; on any exception, logs
    `raw_response` alongside it before re-raising the same exception
    unchanged. Split out from the patch installer so it can be unit tested
    directly against a fake original function, without needing a real
    LxJsonKeySalt or any network I/O.
    """
    try:
        return original_fn(instance, raw_response)
    except Exception:
        logger.error(
            f"Failed to parse Miniserver response - raw response: {raw_response!r}",
            exc_info=True,
        )
        raise


def _patch_encryption_handler_resets_salt_on_connect() -> None:
    """
    LxEncryptionHandler (self._encryption_handler) is created once in
    LoxWs.__init__() and never recreated or reset on reconnect - only
    self._ws/self._session get torn down and rebuilt (stop()/async_init()).
    Its salt-rotation state (self._salt, self._salt_used_count,
    self._salt_time_stamp) therefore survives across reconnects untouched,
    even though the Miniserver starts a completely fresh session on every
    reconnect (a new RSA-wrapped key exchange, "ENCRYPTION READY").

    encrypt()'s own logic:

        if self._salt != "" and self.new_salt_needed():
            prev_salt = self._salt
            self._salt = self.genarate_salt()
            s = "nextSalt/{}/{}/{}\0".format(prev_salt, self._salt, command)
        else:
            if self._salt == "":
                self._salt = self.genarate_salt()
            s = "salt/{}/{}\0".format(self._salt, command)

    only takes the "fresh session" branch (plain "salt/...") when
    self._salt == "". After any normal amount of prior traffic (more than
    SALT_MAX_USE_COUNT=30 messages, or SALT_MAX_AGE_SECONDS=1h - both
    virtually guaranteed to have already elapsed by the time a reconnect
    happens), self._salt is left over from the old session and
    new_salt_needed() is True, so the very FIRST encrypted command of a
    brand new session - acquire_token()'s initial getkey2 request - takes
    the "nextSalt/{prev_salt}/..." branch, referencing a salt value from a
    session the Miniserver has no memory of (it just did a fresh key
    exchange). A Miniserver enforcing salt-based replay protection has good
    reason to reject that as suspicious - observed in production as a
    `"Code": "401"` response to acquire_token()'s first request, immediately
    after "ENCRYPTION READY" (see docs/ for the raw response and the
    log_raw_response_on_error patch that surfaced it - credentials
    themselves were confirmed correct and not locked out, ruling out an
    actual auth problem).

    This resets the salt-rotation state on self._encryption_handler right
    before a fresh connection attempt, so the first command of every new
    session correctly takes the "salt/{salt}/{command}" (fresh) branch
    instead of an invalid "nextSalt" continuation from a session the
    Miniserver has already forgotten. It does not touch the AES key/IV
    (self._key/self._iv - also never regenerated across reconnects, but a
    separate concern not addressed here) or any other part of the
    encryption/auth flow.
    """
    try:
        from loxwebsocket.lox_ws_api import LoxWs
    except ImportError:
        logger.warning(
            "loxwebsocket.lox_ws_api.LoxWs not importable - "
            "skipping the salt-reset-on-reconnect patch"
        )
        return

    original_async_init = LoxWs.async_init

    if getattr(original_async_init, "_loxmqttrelay_salt_reset_patched", False):
        return

    async def patched_async_init(self):
        reset_salt_state(getattr(self, "_encryption_handler", None))
        return await original_async_init(self)

    patched_async_init._loxmqttrelay_salt_reset_patched = True
    LoxWs.async_init = patched_async_init
    logger.info("Applied loxwebsocket compat patch: reset salt state before a fresh connection")


def reset_salt_state(encryption_handler) -> bool:
    """
    Resets `encryption_handler`'s salt-rotation bookkeeping
    (_salt/_salt_used_count/_salt_time_stamp) to its just-constructed state,
    so the next encrypt() call takes the "fresh session" branch instead of
    an invalid "nextSalt" continuation. Returns True if a reset happened
    (False if encryption_handler is None). Split out from the patch
    installer so it can be unit tested directly against a fake or real
    LxEncryptionHandler, without needing a real LoxWs or any network I/O.
    """
    if encryption_handler is None:
        return False
    encryption_handler._salt = ""
    encryption_handler._salt_used_count = 0
    encryption_handler._salt_time_stamp = 0
    return True
