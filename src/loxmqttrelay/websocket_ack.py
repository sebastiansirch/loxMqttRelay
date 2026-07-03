"""
Correlates outgoing websocket `jdev/sps/io/<topic>/<value>` commands with the
Miniserver's response, so the sender can know whether a command actually
succeeded instead of firing it blindly (which is what
send_websocket_command() alone does).
"""
import asyncio
from typing import Dict, List, Optional


class WebSocketAckWaiter:
    """
    Correlation is by topic only - the underlying loxwebsocket library keys
    its own response dispatch that way (event_dict keyed by the echoed
    control path's topic segment), it doesn't expose per-command request
    IDs. If the SAME topic has more than one write in flight at once, a
    response may resolve the wrong (but still pending) waiter for that
    topic - a narrow limitation that only affects near-simultaneous
    duplicate writes to the same virtual input, and even then produces a
    false positive/negative on that one topic, not a mix-up across topics.

    Errors the Miniserver responds to explicitly (e.g. the HTTP-equivalent
    of a 404 "control not found") are swallowed inside the underlying
    library before they reach us (see loxwebsocket_compat.py for what we do
    and don't patch there), so those currently only surface here as a
    timeout, not an immediate failure.
    """

    def __init__(self) -> None:
        self._pending: Dict[str, List["asyncio.Future"]] = {}
        self._registered_on = None

    def register(self, ws_client) -> None:
        """Idempotently register our dispatch callback on the given client."""
        if self._registered_on is ws_client:
            return
        ws_client.add_message_callback(self._on_message, message_types=[0])
        self._registered_on = ws_client

    async def _on_message(self, parsed_data, message_type) -> None:
        if not isinstance(parsed_data, dict):
            return
        for key, ll in parsed_data.items():
            if not isinstance(ll, dict) or "Code" not in ll:
                continue
            topic = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
            waiters = self._pending.get(topic)
            if not waiters:
                continue
            fut = waiters.pop(0)
            if not fut.done():
                fut.set_result(ll)
            if not waiters:
                self._pending.pop(topic, None)

    def start_wait(self, topic: str) -> "asyncio.Future":
        """
        Register a pending wait for `topic` and return its Future. Must be
        called BEFORE the corresponding command is sent, so a fast response
        can never arrive before we start listening for it.
        """
        fut = asyncio.get_running_loop().create_future()
        self._pending.setdefault(topic, []).append(fut)
        return fut

    async def await_ack(self, topic: str, fut: "asyncio.Future", timeout: float) -> Optional[dict]:
        """Wait up to `timeout` seconds for `fut` to resolve. None on timeout."""
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            waiters = self._pending.get(topic)
            if waiters and fut in waiters:
                waiters.remove(fut)
                if not waiters:
                    self._pending.pop(topic, None)
