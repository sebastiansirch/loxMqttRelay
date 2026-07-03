"""
Ensures that, for a given (normalized) topic, at most one send to the
Miniserver is in flight at a time - regardless of whether it's routed via
HTTP or WebSocket. Without this, two rapid messages on the same topic race
at several independent layers (gmqtt dispatches each incoming message as its
own asyncio task, the Rust side spawns each forward as its own task) with no
inherent ordering guarantee, and a retried, slow-to-complete send for an
OLDER value can physically land at the Miniserver after a NEWER value that
was already sent successfully in the meantime. It also means
WebSocketAckWaiter's per-topic response correlation (see websocket_ack.py)
is never asked to disambiguate between more than one in-flight command for
the same topic at once, which is the only case its FIFO-by-topic matching
can get wrong.

In coalescing mode (default), if newer values arrive for a topic while a
send for that topic is still queued (not yet started), only the LATEST one
is sent once its turn comes - a value superseded before it was ever sent is
dropped, not queued behind. This matches Loxone virtual inputs' last-write-
wins semantics and avoids spending a full send-and-retry cycle on a value
that's already stale by the time it would go out. With coalescing disabled,
every value is sent, strictly in arrival order, one at a time per topic - no
value is ever dropped, at the cost of added latency for that topic under a
sustained burst.
"""
import asyncio
from typing import Awaitable, Callable, Dict, List, Set

from loxmqttrelay.logging_config import get_lazy_logger

logger = get_lazy_logger(__name__)

SendFn = Callable[[], Awaitable[None]]


class TopicSequencer:
    def __init__(self) -> None:
        self._queues: Dict[str, List[SendFn]] = {}
        self._running: Set[str] = set()
        self._lock = asyncio.Lock()

    async def submit(self, topic: str, send_fn: SendFn, coalesce: bool, description: str = "") -> None:
        """
        Queue send_fn for `topic`. Returns as soon as it's queued (or, in
        coalescing mode, as soon as any now-superseded predecessor has been
        dropped) - it does not wait for send_fn to actually run. The actual
        send happens in a background per-topic worker, so submit() is itself
        fire-and-forget from the caller's point of view (matching how the
        Rust dispatcher already treats forwarding a message).
        """
        async with self._lock:
            queue = self._queues.setdefault(topic, [])
            if coalesce and queue:
                logger.debug(
                    f"Coalescing pending update for topic '{topic}': dropping a superseded "
                    f"value in favor of {description or 'a newer one'}"
                )
                queue[:] = [send_fn]
            else:
                queue.append(send_fn)

            if topic not in self._running:
                self._running.add(topic)
                asyncio.create_task(self._run(topic))

    async def _run(self, topic: str) -> None:
        while True:
            async with self._lock:
                queue = self._queues.get(topic, [])
                if not queue:
                    self._running.discard(topic)
                    self._queues.pop(topic, None)
                    return
                send_fn = queue.pop(0)
            try:
                await send_fn()
            except Exception:
                logger.exception(f"Unhandled error sending queued update for topic '{topic}'")
