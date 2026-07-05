"""
Runtime patch for a single known gap in the third-party `loxwebsocket`
dependency: LoxWs.reconnect() waits a fixed 15 seconds (its own
CONNECT_DELAY constant) before every single reconnect attempt, including
the very first one right after a disconnect. Everything else we used to
patch here (ClientSession leak on reconnect, salt-state reset across
sessions, the reconnect()-token-reset bug, unsynchronized writes, stale
background tasks surviving a reconnect) is fixed natively in loxwebsocket
0.6.0 - see docs/ for the investigation. This module intentionally stays
minimal and mirrors upstream's own reconnect() as closely as possible, so
it's easy to drop entirely if a future loxwebsocket release adds backoff
itself.

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
    _patch_reconnect_uses_backoff_delay()
    _patches_applied = True


def _patch_reconnect_uses_backoff_delay() -> None:
    """
    LoxWs.reconnect()'s retry loop waits a fixed c.CONNECT_DELAY (15s,
    loxwebsocket/const.py) before every single attempt, including the very
    first one right after a disconnect - so recovery from even a brief blip
    always takes at least 15s before the library so much as tries again.

    Since the wait is inline inside reconnect()'s own loop body (not behind
    any separate, wrappable call), shortening it can't be done with a
    simple before/after wrap - this replaces reconnect() with a copy of
    loxwebsocket 0.6.0's own implementation, changed only to use
    reconnect_backoff_delay(attempt) instead of the fixed c.CONNECT_DELAY:
    the very first attempt is always immediate (0s), the second attempt
    waits miniserver_websocket_reconnect_initial_delay_seconds (default
    1s), each further attempt's wait is multiplied by
    miniserver_websocket_reconnect_backoff_multiplier (default 2x), capped
    at loxwebsocket's own c.CONNECT_DELAY - so behavior converges back to
    identical-to-upstream once several attempts have failed (default: 0s,
    1s, 2s, 4s, 8s, 15s, 15s, ...).

    Everything else - the state guard, stop(), the already-correct
    self._token reset, self._cancel_stale_background_tasks() (loxwebsocket
    0.6.0's own implementation, which - unlike an earlier version of this
    patch - correctly spares the currently-running task from cancelling
    itself), and the http_ping()/async_init()/start()/send_event() calls
    and give-up/raise branch - is copied verbatim from loxwebsocket 0.6.0's
    own reconnect(), so this patch changes only the one thing upstream
    doesn't do. If a future loxwebsocket release changes reconnect()'s own
    implementation, this patch needs to be revisited to match.
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
    Mirrors LoxWs.reconnect() (loxwebsocket 0.6.0) exactly, replacing only
    the fixed c.CONNECT_DELAY wait with reconnect_backoff_delay(attempt).
    Split out from the patch installer so it can be unit tested directly
    against a fake instance, without needing a real LoxWs.
    """
    from loxwebsocket.exceptions import LoxoneException
    from loxwebsocket.lxtoken import LxToken

    if instance.state == "RECONNECTING":
        return
    await instance.stop()
    instance._cancel_stale_background_tasks()
    instance._token = LxToken()
    instance.state = "RECONNECTING"
    attempt = 0
    limit = instance._max_reconnect_attempts if instance._max_reconnect_attempts else "unlimited"
    while instance._max_reconnect_attempts == 0 or instance._max_reconnect_attempts > attempt:
        attempt += 1
        delay = reconnect_backoff_delay(attempt)
        logger.info("Reconnect attempt %s of %s", attempt, limit)
        logger.info(f"Waiting for {delay} seconds before retrying...")
        await asyncio.sleep(delay)
        if not await instance.http_ping():
            continue
        try:
            if await instance.async_init():
                logger.debug("Reconnection successful.")
                await instance.start()
                await instance.send_event(instance.EventType.RECONNECTED)
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
    (1-based). The very first attempt is always immediate (0s) - right
    after a disconnect there's no reason to wait before even trying once.
    From the second attempt on, the backoff schedule kicks in: starts at
    miniserver.miniserver_websocket_reconnect_initial_delay_seconds,
    multiplies by miniserver.miniserver_websocket_reconnect_backoff_multiplier
    each further attempt, capped at loxwebsocket's own c.CONNECT_DELAY so
    behavior converges back to the library's original fixed delay once
    enough attempts have failed (default: 0s, 1s, 2s, 4s, 8s, 15s, 15s,
    ...). Split out from the patch installer so it can be unit tested
    directly, without needing a real LoxWs.
    """
    from loxwebsocket import const as loxwebsocket_const

    if attempt <= 1:
        return 0.0

    initial = global_config.miniserver.miniserver_websocket_reconnect_initial_delay_seconds
    multiplier = global_config.miniserver.miniserver_websocket_reconnect_backoff_multiplier
    cap = loxwebsocket_const.CONNECT_DELAY
    return min(initial * (multiplier ** (attempt - 2)), cap)
