import asyncio

import pytest

from loxmqttrelay.topic_sequencer import TopicSequencer


@pytest.mark.asyncio
async def test_same_topic_sends_never_overlap_without_coalescing():
    """Without coalescing, every submitted value must still be sent, one at a time, in order."""
    sequencer = TopicSequencer()
    order = []
    concurrency = {"current": 0, "max": 0}

    async def make_send(value):
        async def _send():
            concurrency["current"] += 1
            concurrency["max"] = max(concurrency["max"], concurrency["current"])
            await asyncio.sleep(0.02)
            concurrency["current"] -= 1
            order.append(value)
        return _send

    for value in (1, 2, 3):
        await sequencer.submit("topicA", await make_send(value), coalesce=False)

    await asyncio.sleep(0.15)

    assert order == [1, 2, 3]
    assert concurrency["max"] == 1


@pytest.mark.asyncio
async def test_coalescing_drops_superseded_values_still_queued():
    """
    The first submitted value starts running immediately (queue was empty).
    While it's running, two more are submitted for the same topic with
    coalescing on - only the LAST of those should ever run; the one in
    between must be dropped, never sent.
    """
    sequencer = TopicSequencer()
    ran = []

    async def make_send(value, delay=0.0):
        async def _send():
            if delay:
                await asyncio.sleep(delay)
            ran.append(value)
        return _send

    await sequencer.submit("topicA", await make_send(1, delay=0.05), coalesce=True)
    await asyncio.sleep(0.01)  # ensure value 1 has actually started running
    await sequencer.submit("topicA", await make_send(2), coalesce=True)
    await sequencer.submit("topicA", await make_send(3), coalesce=True)

    await asyncio.sleep(0.15)

    assert ran == [1, 3]  # 2 was superseded by 3 before it ever ran


@pytest.mark.asyncio
async def test_different_topics_run_independently_and_concurrently():
    sequencer = TopicSequencer()
    concurrency = {"current": 0, "max": 0}

    async def make_send():
        async def _send():
            concurrency["current"] += 1
            concurrency["max"] = max(concurrency["max"], concurrency["current"])
            await asyncio.sleep(0.03)
            concurrency["current"] -= 1
        return _send

    await sequencer.submit("topicA", await make_send(), coalesce=True)
    await sequencer.submit("topicB", await make_send(), coalesce=True)
    await sequencer.submit("topicC", await make_send(), coalesce=True)

    await asyncio.sleep(0.15)

    assert concurrency["max"] == 3  # all three topics ran at the same time


@pytest.mark.asyncio
async def test_bookkeeping_is_cleaned_up_after_draining():
    sequencer = TopicSequencer()

    async def _send():
        pass

    await sequencer.submit("topicA", _send, coalesce=True)
    await asyncio.sleep(0.05)

    assert "topicA" not in sequencer._queues
    assert "topicA" not in sequencer._running


@pytest.mark.asyncio
async def test_a_send_that_raises_does_not_stop_later_sends_for_the_topic():
    sequencer = TopicSequencer()
    ran = []

    async def failing_send():
        raise RuntimeError("boom")

    async def make_send(value):
        async def _send():
            ran.append(value)
        return _send

    await sequencer.submit("topicA", failing_send, coalesce=False)
    await sequencer.submit("topicA", await make_send("after failure"), coalesce=False)

    await asyncio.sleep(0.05)

    assert ran == ["after failure"]
