# Cancel Stale Background Tasks Immediately on Reconnect

Branch: `fix/cancel-stale-background-tasks-on-reconnect` (based on `fix/websocket-reliability`)

## Starting Question

Follow-up to `docs/reconnect-backoff-delay.md`: a 7-hour production log (after fixing a
separate MQTT-source issue that had been flooding the relay with redundant camera-topic
traffic) showed the reconnect fix working well overall - disconnect frequency dropped from
~37/hour to ~15.7/hour - but two disconnects took noticeably longer than the usual
~300ms, because the Miniserver returned `HTTP 503` on its public-key endpoint for the
first few reconnect attempts. While digging into one of those two incidents, a second,
independent issue surfaced in the log that warranted its own investigation.

## Analysis

Mid-backoff (attempt 4, sleeping for its 4s delay), the log showed a **second** "closed
with code 1000" / "Connection closed unexpectedly" pair - a few seconds after the first
one, and before the currently-running reconnect attempt had even finished waiting:

```
20:02:30,234  WebSocket closed with code 1000 ...
20:02:30,261  Reconnection failed: error generate_session_key... (503)
20:02:30,261  Reconnect attempt 2 of 0 - waiting 1.0s
20:02:31,311  Reconnection failed: error generate_session_key... (503)
20:02:31,311  Reconnect attempt 3 of 0 - waiting 2.0s
20:02:33,331  Reconnection failed: error generate_session_key... (503)
20:02:33,331  Reconnect attempt 4 of 0 - waiting 4.0s
20:02:36,076  WebSocket closed with code 1000 ...                    <- second one!
20:02:36,076  Connection error: Cannot write to closing transport of
              type ClientConnectionResetError with code: 1000
```

This second pair isn't a new Miniserver-initiated disconnect - the reconnect loop was
still mid-backoff, no new connection had been established yet to disconnect from. Tracing
it back to `loxwebsocket`'s own source:

```python
async def keep_alive(self, second: int) -> None:
    try:
        while self.state == "CONNECTED":
            await asyncio.sleep(second)          # KEEP_ALIVE_PERIOD = 60s
            async with asyncio.Lock():
                await self._ws.send_str("keepalive")
    except Exception as e:
        await self.handle_connection_interrupt(exception=e)

async def start(self) -> None:
    for task in self.background_tasks:
        task.cancel()
    self.background_tasks.clear()
    tasks = [
        asyncio.create_task(self.ws_listen(), ...),
        asyncio.create_task(self.keep_alive(c.KEEP_ALIVE_PERIOD), ...),
        asyncio.create_task(self.refresh_token(), ...),
    ]
    ...

async def handle_connection_interrupt(self, msg_type=None, exception=None):
    ...
    await self.reconnect()
```

`start()` only cancels the previous connection's background tasks (`ws_listen()`,
`keep_alive()`, `refresh_token()`) right before creating the *next* connection's tasks -
i.e. only after a **new** `async_init()` has already succeeded. Nothing cancels them the
moment the old connection dies. `keep_alive()` in particular checks `self.state ==
"CONNECTED"` only once per `KEEP_ALIVE_PERIOD` (60s) sleep cycle - if it happens to be
mid-sleep when the connection drops, it can wake up any time later, still holding a
reference to the now-closed `self._ws`, and try to write to it. That write fails
(`ClientConnectionResetError`), which is caught and forwarded to
`handle_connection_interrupt(exception=e)` - which logs a **second**, misleading "closed
with code 1000" line for the *same* earlier disconnect, and then calls `self.reconnect()`
itself.

In this production case that second `reconnect()` call was harmless: our own
re-entrancy guard (`state == "RECONNECTING"`, from the earlier token-reset fix, folded
into `run_reconnect_with_backoff()`) made it a no-op, and the real, already-in-progress
attempt 4 completed normally afterwards. But this confirms two things:

1. **Disconnect-frequency counts are inflated.** Some fraction of every "closed with code
   1000" seen in production logs isn't an independent new disconnect - it's a stale task
   rediscovering an already-handled one, sometimes several seconds and multiple failed
   attempts later.
2. **The race is real, not just theoretical.** A previous finding (`docs/`
   `logging-improvements.md`'s "string indices must be integers, not 'str'" incident) had
   circumstantially suggested a stale `refresh_token()` task's own `send_command()` could
   race the new connection's `acquire_token()` - since `send_command()` has no
   request/response correlation at all (raw, sequential `self._ws.receive()` calls - see
   `docs/websocket-reliability.md`'s ack-correlation note). This log is direct proof that
   old background tasks *do* survive multiple seconds and several failed reconnect
   attempts before failing - closing that window removes both the log noise and a real,
   if narrow, cross-talk risk during the handshake.

## Fix

`src/loxmqttrelay/loxwebsocket_compat.py`: `run_reconnect_with_backoff()` now cancels
`instance.background_tasks` immediately, right alongside `stop()`, instead of waiting for
the next successful `start()`:

```python
if instance.state == "RECONNECTING":
    return
await instance.stop()
cancel_background_tasks(instance)
reset_token_if_not_already_reconnecting(instance, LxToken)
instance.state = "RECONNECTING"
```

```python
def cancel_background_tasks(instance) -> int:
    tasks = list(instance.background_tasks)
    for task in tasks:
        task.cancel()
    instance.background_tasks.clear()
    return len(tasks)
```

This mirrors exactly what `start()` itself already does (`for task in
self.background_tasks: task.cancel()`) - it's just moved earlier, to the point where we
already know the old connection is dead, rather than deferred until a new one exists.
`ws_listen()`, `keep_alive()`, and `refresh_token()` for the dead connection are now
guaranteed gone before any reconnect attempt runs, closing the window entirely.

## Testing

- `cancel_background_tasks()`: cancels every task in the set and clears it, returning the
  count; no-ops cleanly on an empty set.
- `run_reconnect_with_backoff()`: a stale task (mimicking `keep_alive()`, sleeping far
  longer than the test needs) placed in `instance.background_tasks` before a reconnect run
  ends up cancelled and the set cleared - reproducing the production scenario directly.
  This test deliberately does *not* mock `asyncio.sleep` (unlike the other
  `run_reconnect_with_backoff` tests), since doing so would also resolve the stale task's
  own `asyncio.sleep(3600)` instantly instead of leaving it pending - defeating the point
  of the test; the real attempt-1 delay is `0.0s` here anyway; so it stays fast.

All 261 tests pass (257 existing + 4 new), run inside the Docker build image. The two
most timing-sensitive test files re-run 5x with no failures.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- `loxwebsocket`'s background tasks (`ws_listen()`, `keep_alive()`, `refresh_token()`)
  from a connection that just died were previously only cancelled inside `start()`, which
  doesn't run until a *new* `async_init()` has already succeeded - leaving a window,
  sometimes several seconds and multiple failed reconnect attempts wide, where a stale
  task can still be running. `run_reconnect_with_backoff()` now cancels them immediately,
  right alongside `stop()`.
- Confirmed in production: a stale `keep_alive()` task survived 3 failed reconnect
  attempts (~6s, while the Miniserver was returning `503` on its public-key endpoint),
  then failed writing to the already-closed transport and logged a second, misleading
  "closed with code 1000" for the *same* earlier disconnect - inflating apparent
  disconnect-frequency counts - and called `reconnect()` a second time (harmless only
  because of the existing re-entrancy guard).

### Why
Disconnect-frequency analysis across production logs depends on counting "closed with
code 1000" occurrences; if some fraction of those are stale-task echoes of an
already-handled disconnect rather than independent new ones, that count is unreliable.
Separately, this closes a real (previously only circumstantial) race window where a stale
task's own `send_command()` call could interleave with a new connection's handshake, since
`send_command()` has no request/response correlation at all.

### Changes
- `src/loxmqttrelay/loxwebsocket_compat.py`: `cancel_background_tasks()` (new) is called
  from `run_reconnect_with_backoff()` immediately after `stop()`.
- `tests/test_loxwebsocket_compat.py`: 4 new tests covering the helper directly and the
  end-to-end reconnect scenario with a stale task present.

### Test Plan
- [x] `pytest` (257 existing + 4 new tests, 261 total) passes, run inside the Docker build
      image; the two most timing-sensitive test files re-run 5x for flakiness with no
      failures.
- [x] `test_run_reconnect_with_backoff_cancels_stale_background_tasks`: reproduces the
      production scenario - a stale task present in `background_tasks` before a reconnect
      run ends up cancelled.

### Notes for Reviewers
- `cancel_background_tasks()` mirrors `start()`'s own cancellation code exactly (same
  loop, same `.clear()`) - it's purely moved earlier in the reconnect lifecycle, not new
  logic invented from scratch.
- This branch is based on `fix/websocket-reliability` (not `main`) since it directly
  extends `loxwebsocket_compat.py` as it exists there; it merges back into
  `fix/websocket-reliability` rather than being reviewed independently against `main`.
