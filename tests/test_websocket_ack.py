import asyncio

import pytest

from loxmqttrelay.websocket_ack import WebSocketAckWaiter


class FakeWsClient:
    def __init__(self):
        self.registered_callbacks = []

    def add_message_callback(self, callback, message_types=None):
        self.registered_callbacks.append((callback, message_types))


@pytest.mark.asyncio
async def test_register_is_idempotent_for_the_same_client():
    waiter = WebSocketAckWaiter()
    client = FakeWsClient()

    waiter.register(client)
    waiter.register(client)
    waiter.register(client)

    assert len(client.registered_callbacks) == 1
    assert client.registered_callbacks[0][1] == [0]


@pytest.mark.asyncio
async def test_wait_for_ack_resolves_on_matching_topic():
    waiter = WebSocketAckWaiter()

    fut = waiter.start_wait("mytopic")
    await waiter._on_message({b"mytopic": {"Code": "200", "control": "dev/sps/io/mytopic/1"}}, 0)

    result = await waiter.await_ack("mytopic", fut, timeout=1.0)

    assert result == {"Code": "200", "control": "dev/sps/io/mytopic/1"}


@pytest.mark.asyncio
async def test_wait_for_ack_ignores_unrelated_topics():
    waiter = WebSocketAckWaiter()

    fut = waiter.start_wait("mytopic")
    await waiter._on_message({b"othertopic": {"Code": "200"}}, 0)

    result = await waiter.await_ack("mytopic", fut, timeout=0.05)

    assert result is None


@pytest.mark.asyncio
async def test_wait_for_ack_times_out_with_no_response():
    waiter = WebSocketAckWaiter()

    fut = waiter.start_wait("mytopic")
    result = await waiter.await_ack("mytopic", fut, timeout=0.05)

    assert result is None
    # the pending entry must be cleaned up, not leaked
    assert "mytopic" not in waiter._pending


@pytest.mark.asyncio
async def test_multiple_waiters_for_same_topic_resolve_fifo():
    waiter = WebSocketAckWaiter()

    fut1 = waiter.start_wait("mytopic")
    fut2 = waiter.start_wait("mytopic")

    await waiter._on_message({b"mytopic": {"Code": "200", "seq": 1}}, 0)
    await waiter._on_message({b"mytopic": {"Code": "200", "seq": 2}}, 0)

    result1 = await waiter.await_ack("mytopic", fut1, timeout=1.0)
    result2 = await waiter.await_ack("mytopic", fut2, timeout=1.0)

    assert result1["seq"] == 1
    assert result2["seq"] == 2


@pytest.mark.asyncio
async def test_on_message_ignores_non_dict_and_malformed_entries():
    waiter = WebSocketAckWaiter()
    fut = waiter.start_wait("mytopic")

    # not a dict at all (e.g. the plain json_message shape without "control")
    await waiter._on_message("not a dict", 0)
    # dict values that aren't LL-shaped dicts with a Code
    await waiter._on_message({b"mytopic": "not a dict either"}, 0)
    await waiter._on_message({b"mytopic": {"no_code_here": True}}, 0)

    result = await waiter.await_ack("mytopic", fut, timeout=0.05)
    assert result is None
