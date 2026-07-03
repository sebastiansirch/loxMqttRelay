# WebSocket Reliability: Fixes for the Miniserver WebSocket Path

Branch: `fix/websocket-reliability`

## Starting Question

Follow-up to the load-test work on `fix/miniserver-http-session-reuse-and-basetopic-warning` (see
`docs/loadtest-http-session-fix.md`): does the same "message loss under load" problem apply if the
relay talks to the Miniserver over WebSocket instead of HTTP (`[miniserver] use_websocket = true`,
which is also the dataclass default)?

Short answer: the *specific* HTTP bug (a fresh `aiohttp.ClientSession`, and thus a fresh TCP
connection, per message) does not apply - WebSocket uses one persistent connection, so there is no
per-message connection churn to exhaust local ports. But the HTTP-side fixes (session reuse,
retry/backoff) only touch `send_to_miniserver_via_http()`; they have **zero effect** on
`send_to_minisever_via_websocket()`, which is a structurally different code path with its own,
partly more severe reliability gaps. This branch fixes those.

## Findings

### 1. `send_websocket_command()` is fire-and-forget - no result is ever checked

```python
async def send_websocket_command(self, device_uuid: str, value: str) -> None:
    command = f"jdev/sps/io/{device_uuid...}/{value}"
    enc_command = await self._encryption_handler.encrypt(command)
    await self._ws.send_str(enc_command)
```
(`loxwebsocket/lox_ws_api.py`, third-party dependency)

This writes the command to the socket and returns. The Miniserver *does* send a response for
`jdev/sps/io/...` commands (the library's own `extract_type_0_message` already special-cases a 404
"control not found" response with a warning log), but nothing in the send path waits for or
inspects it. Compare `send_command()` in the same file, used for the internal auth/token handshake
- it explicitly awaits a header + payload response. The relay's own
`send_to_minisever_via_websocket()` wrapper just called `send_websocket_command()` and, on no
exception, logged success and returned - regardless of whether the Miniserver ever actually
processed the write.

### 2. No concurrency control on the one shared, stateful connection

The HTTP path has `connection_semaphore` bounding concurrent requests. The WebSocket path had
nothing: every forwarded MQTT message immediately called `_ws.send_str()` on the single shared
connection, unsynchronized. `aiohttp` does not guarantee safety for concurrent writes on one
websocket from different tasks - under load (many MQTT messages arriving as concurrent tasks, per
the Rust dispatcher's `pyo3_async_runtimes::tokio::get_runtime().spawn(...)` pattern), sends could
interleave.

### 3. A likely bug in the `loxwebsocket` dependency's salt rotation - and it gets *worse* under load

`loxwebsocket/encryption.py`:

```python
def genarate_salt(self):                      # note: no `return`
    salt = get_random_bytes(c.SALT_BYTES)
    ...
    self._salt = salt

async def encrypt(self, command):
    ...
    self._salt = self.genarate_salt()          # overwrites self._salt with None!
```

`genarate_salt()` sets `self._salt` as a side effect but has no `return` statement (implicitly
returns `None`). Both call sites in `encrypt()` do `self._salt = self.genarate_salt()`, which
immediately clobbers the salt `genarate_salt()` just set with `None` - on the very first encrypted
command, and again every `SALT_MAX_USE_COUNT` (30) messages or `SALT_MAX_AGE_SECONDS` (1 hour),
whichever comes first. Every encrypted command sent after that point is built as
`"salt/None/<command>"` instead of using a real random salt.

This is a third-party bug (not in this repository's own code), and its real-world impact on the
Miniserver side (does it reject the command outright, or silently accept it with degraded replay
protection?) can't be confirmed without a real Miniserver. What *is* confirmed by reading the code:
it fires deterministically, and **more often at higher message throughput** (every 30 messages,
not every 30 minutes) - directly relevant to the "under load" framing of this whole investigation.

### 4. A reconnect race between the relay and the library's own reconnect loop

The original code:
```python
if "CONNECTED" not in ws_client.state:
    await ws_client.connect(user=..., password=..., loxone_url=self.ws_base_url, receive_updates=False)
```

`connect()` is guarded by an internal `_connect_lock`. But when the connection drops,
`loxwebsocket`'s own `handle_connection_interrupt()` starts a `reconnect()` loop that calls
`async_init()` **directly**, bypassing `connect()`/`_connect_lock` entirely. If a forwarded message
arrives while `state == "RECONNECTING"`, the relay's own check above still calls `connect()`,
which can run a second, concurrent handshake (`async_init()`) against the same `LoxWs` instance
while the library's own reconnect is already mid-flight. More load means more messages means a
higher chance one lands exactly in that window.

### 5. No ordering guarantee for rapid successive messages on the same topic

A follow-up question after the above fixes shipped: is it guaranteed that rapid successive
messages on the same topic arrive at the Miniserver in the order they were sent? No, at multiple
independent layers:

- **gmqtt** dispatches every incoming MQTT message as its own `asyncio.ensure_future(...)` task
  (`gmqtt/mqtt/utils.py`, `run_coroutine_or_function`) - it never awaits one message's handler
  before starting the next.
- **The Rust dispatcher** spawns each forward onto its own Tokio task
  (`pyo3_async_runtimes::tokio::get_runtime().spawn(...)`, `src/lib.rs`) rather than awaiting it -
  again, no serialization between consecutive messages on the same topic.
- **The HTTP path** allows up to `miniserver_max_parallel_connections` (default `5`) requests in
  flight at once, with no per-topic queue - two requests to the same topic can be in flight over
  different connections with no guarantee which one the Miniserver processes first.
- **The WebSocket path**, even with the write lock added above, only serializes the *send* (the
  `encrypt()+send_str()` step) - not the full send-plus-retry cycle. This was verified with a
  concrete repro: sending `"off"` (whose first attempt is dropped, forcing a retry) immediately
  followed by `"on"` for the same topic produced

  ```
  Wire send order (ms, value): [(0, 'off'), (1, 'on'), (152, 'on')]
  ```

  Because `WebSocketAckWaiter` correlates by topic only, the response meant for `"on"`'s first
  attempt resolved `"off"`'s pending wait instead (both were pending for the same topic at once) -
  `"off"` falsely reported success on attempt 1, while `"on"` timed out waiting for a response that
  had already been consumed, and had to retry ~150ms later. The final value happened to be correct
  in this run, but only by chance - with different timing the retried `"off"` could just as easily
  have landed *after* `"on"` and overwritten it.

  Critically, this isn't a rare, exotic race: it only requires ONE message to hit any transient
  failure (a dropped ack, a timeout, a brief Miniserver hiccup) for the danger window to open. With
  the retry defaults, that window is up to `~16.5s` for WebSocket
  (`3 × (ack_timeout + backoff) ≈ 3 × 5.5s`) or `~31.5s` for HTTP
  (`3 × (10s timeout + backoff)`) - any second message to the same topic sent within that window
  after the first one's failure is at risk. That covers very ordinary cases: a light switch
  double-tapped within a few seconds, a short "on, wait 2s, off" automation, or a dimmer/slider
  sending several updates in a row.

### 6. Whitelist-sync bug is unaffected either way

The whitelist-overwrite bug found and fixed on `feature/defensive-whitelist-sync` (see
`docs/defensive-whitelist-sync.md`) happens in Rust's `process_data()`, before the HTTP/WebSocket
choice is made - it applies identically regardless of which one is configured. No changes needed
here.

### 7. `ClientSession` leak in `loxwebsocket`'s reconnect loop (found from production logs)

A production log from a deployment running these fixes showed a long, sustained run of

```
ERROR [loxmqttrelay.http_miniserver_handler] Error sending weather/hfc1_uvi (as weather_hfc1_uvi)=0
to Miniserver via WebSocket: WebSocket not connected (state=RECONNECTING), giving up after 3 attempt(s)
```

for many different topics in a row - the retry/give-up behavior itself working exactly as designed
(the websocket genuinely wasn't connected, so there was nothing to send through), but persisting far
longer than a single blip. The revealing lines were:

```
ERROR [asyncio] Unclosed client session
client_session: <aiohttp.client.ClientSession object at 0x722d97fb7b60>
ERROR [loxwebsocket.lox_ws_api] Reconnection failed: Websocket closed while waiting for data.
INFO  [loxwebsocket.lox_ws_api] Reconnect attempt 16 of 0
INFO  [loxwebsocket.lox_ws_api] Waiting for 15 seconds before retrying...
```

"Reconnect attempt 16 of 0" means `_max_reconnect_attempts == 0` (unlimited - the relay never passes
a limit to `connect()`, so `loxwebsocket`'s own default applies) and the outage had already lasted
at least `16 × 15s ≈ 4 minutes` by this point. `loxwebsocket/lox_ws_api.py`'s `reconnect()`:

```python
async def reconnect(self) -> None:
    ...
    await self.stop()            # closes the old session/ws - but only ONCE, before the loop
    self.state = "RECONNECTING"
    while self._max_reconnect_attempts == 0 or ...:
        ...
        await asyncio.sleep(c.CONNECT_DELAY)      # 15s
        ...
        if await self.async_init():               # creates a NEW ClientSession every attempt
            ...
```

`async_init()` unconditionally does `self._session = aiohttp.ClientSession(...)`, overwriting the
previous attempt's session. `stop()` (which closes it) only runs once, before the retry loop starts
- not between attempts. Every failed reconnect attempt therefore leaks the `ClientSession` (and its
underlying sockets) it just opened - exactly the "Unclosed client session" warning in the log. Over
a long enough outage with unlimited retries, this can exhaust file descriptors on the host, making
it progressively *harder* to ever reconnect - a bug that can make its own root cause worse the
longer it runs.

What caused the *initial* disconnect isn't visible in the log (network blip, Miniserver reboot,
etc.) - this finding is specifically about the resource leak that compounds it, not the trigger.

Whether to also cap `_max_reconnect_attempts` (currently unlimited) was considered and explicitly
**rejected**: this is a long-running background service that should keep trying to recover from a
Miniserver outage of unknown duration (an hour, a day) rather than permanently give up and require
a manual restart. Now that the leak is fixed, unlimited retries are no longer resource-dangerous -
capping them would trade a real reliability property (self-healing after an outage) for a
theoretical concern that no longer applies. Only the leak was fixed; reconnect attempt count, delay,
and all other reconnect behavior are unchanged.

## Fixes

### Response correlation + retry (`src/loxmqttrelay/websocket_ack.py`, new)

`WebSocketAckWaiter` registers a message-type-0 callback on the shared `loxwebsocket` client and
lets `send_to_minisever_via_websocket()` wait for the response matching a topic before deciding
success/failure:

```python
ack_future = self._ws_ack_waiter.start_wait(normalized_topic)   # register BEFORE sending
async with self._ws_write_lock:
    await ws_client.send_websocket_command(normalized_topic, str(value))
ack = await self._ws_ack_waiter.await_ack(normalized_topic, ack_future, ack_timeout)
```

Correlation is by topic, matching how the underlying library's own dispatch is keyed
(`event_dict[control.split("/")[-2]] = LL`) - it doesn't expose per-command request IDs. If the
*same* topic has two writes in flight at once, a response could resolve the wrong (but still
pending) waiter for that one topic - a narrow, documented limitation, not a cross-topic mix-up.
Explicit Miniserver rejections (e.g. 404) are swallowed inside the library before reaching us (see
finding 1), so those currently surface only as a timeout, not an immediate failure - still correct,
just slower to detect than a real per-command error signal would be.

`send_to_minisever_via_websocket()` now retries a timeout or non-200 `Code` up to
`miniserver_websocket_retry_attempts` times (default `3`) with exponential backoff
(`miniserver_websocket_retry_backoff_seconds`, default `0.5s`), waiting up to
`miniserver_websocket_ack_timeout_seconds` (default `5.0s`) per attempt - mirroring
`send_to_miniserver_via_http()`'s retry shape on the other branch.

### Write serialization (`self._ws_write_lock`, an `asyncio.Lock`)

Wraps only the `encrypt()+send_str()` step (the part touching shared mutable state - the
connection and the encryption handler's salt), not the subsequent ack-wait, so concurrent senders
still pipeline (each awaits its own response independently) while writes themselves are never
interleaved.

### Reconnect-race fix (`_ensure_websocket_connected()`)

```python
if ws_client.state == "CONNECTED":
    return True
if ws_client.state == "RECONNECTING":
    # the library's own reconnect() task is already handling this - wait for it
    ...
    return ws_client.state == "CONNECTED"
await ws_client.connect(...)
```

Only calls `connect()` when the client isn't already reconnecting on its own, closing the race
described in finding 4.

### `loxwebsocket` salt patch (`src/loxmqttrelay/loxwebsocket_compat.py`, new)

Since `loxwebsocket` is an external PyPI dependency (not vendored in this repo), the salt-rotation
bug (finding 3) is fixed with a narrow runtime patch applied once at import time, before any
websocket traffic is sent:

```python
original_genarate_salt = LxEncryptionHandler.genarate_salt

def patched_genarate_salt(self):
    original_genarate_salt(self)   # still runs the real (buggy-return) implementation
    return self._salt              # ...but now returns what it just set, instead of None
LxEncryptionHandler.genarate_salt = patched_genarate_salt
```

This delegates to the original method for the actual salt-generation logic (so it stays in sync if
upstream changes that part) and only fixes the missing `return`. Isolated in its own module so it
can be deleted cleanly once a fixed `loxwebsocket` release exists upstream.

### `loxwebsocket` reconnect session-leak patch (`src/loxmqttrelay/loxwebsocket_compat.py`)

Fixes finding 7 the same way: a narrow runtime patch, not a change to reconnect behavior itself.
`async_init()` is wrapped to close any still-open previous `self._session` before calling the
original implementation (which opens the new one):

```python
async def _close_stale_session_and_call(instance, original_async_init):
    old_session = getattr(instance, "_session", None)
    if old_session is not None and not old_session.closed:
        try:
            await old_session.close()
        except Exception:
            logger.warning("Failed to close stale websocket session before reconnecting", exc_info=True)
    return await original_async_init(instance)
```

The close-then-call logic is split into its own function specifically so it can be unit tested
directly (with fake session/`self` objects), without needing a real or even patched `LoxWs`
instance and without touching the network. `_max_reconnect_attempts` (currently unlimited/`0`) is
deliberately left unchanged - see finding 7 for why a cap was considered and rejected.

### Per-topic sequencing, with coalescing (`src/loxmqttrelay/topic_sequencer.py`, new)

`TopicSequencer` ensures at most one send is in flight per (normalized) topic at a time - for
*both* HTTP and WebSocket, since it's applied at the shared `send_to_miniserver()` entry point
rather than inside either protocol-specific method:

```python
async def send_to_miniserver(self, topic, normalized_topic, value):
    async def _send():
        if global_config.miniserver.use_websocket:
            await self.send_to_minisever_via_websocket(topic, normalized_topic, value)
        else:
            await self.send_to_miniserver_via_http(topic, normalized_topic, value)

    await self._topic_sequencer.submit(
        normalized_topic, _send,
        coalesce=global_config.miniserver.miniserver_coalesce_topic_updates,
        description=f"{normalized_topic}={value}",
    )
```

For a given topic, `TopicSequencer` runs a background worker that processes one queued send at a
time. This closes finding 5 at its root: since only one send-and-retry cycle for a topic is ever
in flight, `WebSocketAckWaiter` never has more than one pending waiter for that topic either,
eliminating the cross-resolution bug demonstrated above as a side effect - not just delaying the
symptom.

`miniserver_coalesce_topic_updates` (default `true`) controls what happens when a newer value
arrives for a topic while the previous one is still queued (not yet started sending):
- `true`: the superseded, not-yet-sent value is dropped - only the latest one is sent once it's
  that topic's turn. Matches Loxone virtual inputs' last-write-wins semantics and avoids spending a
  full send-and-retry cycle on a value that would be immediately overwritten anyway.
- `false`: every value is still sent, strictly in arrival order, one at a time per topic - nothing
  is ever dropped, at the cost of added latency for that topic under a sustained burst.

Note what coalescing does and doesn't affect: it only ever drops a value that hasn't started
sending yet. A value that's already being sent (including through its retries) always runs to
completion before the next queued item for that topic is even considered.

### New `[miniserver]` config options

```toml
miniserver_websocket_retry_attempts = 3          # total attempts, incl. the first; 1 disables retrying
miniserver_websocket_retry_backoff_seconds = 0.5 # doubles after each retry
miniserver_websocket_ack_timeout_seconds = 5.0   # how long to wait for a response per attempt
miniserver_coalesce_topic_updates = true         # drop superseded, not-yet-sent values per topic
```

## Testing

Given `loxwebsocket` speaks Loxone's actual encrypted WebSocket protocol (RSA key exchange, AES
session encryption, JWT/token auth), a full protocol-level Docker infrastructure test (a fake
Miniserver that speaks that protocol, analogous to `loadtest/miniserver_mock` for HTTP) would mean
reimplementing a substantial part of the encrypted handshake just to mock it - out of scope for
this pass. Instead:

- **`tests/test_loxwebsocket_compat.py`** (8 tests): confirms `genarate_salt()` now returns the
  salt it set (not `None`), that `apply_patches()` is idempotent, and end-to-end that `encrypt()`
  no longer embeds the literal string `"None"` as the salt; plus 5 tests for the reconnect
  session-leak fix (finding 7) against `_close_stale_session_and_call()` directly with fake
  session/`self` objects - closes an open stale session before calling the original, skips closing
  an already-closed one, handles the very first connect (no session yet at all), still runs the
  original even if closing the stale session raises, and confirms the patch installs on `LoxWs`
  idempotently.
- **`tests/test_websocket_ack.py`** (6 tests): `WebSocketAckWaiter` registration idempotency,
  correct resolution on a matching topic, ignoring unrelated/malformed messages, timeout behavior
  and cleanup, and FIFO resolution for multiple pending waiters on the same topic.
- **`tests/test_http_miniserver_handler.py`** (7 new tests) against a `FakeWsClient` test double
  that mimics the real client's `state`/`connect`/`add_message_callback`/`send_websocket_command`
  shape and lets a test script exactly which response (or no response, i.e. a timeout) arrives for
  each successive send:
  - immediate success on the first ack
  - retry after a timeout, then succeed
  - retry after a non-200 `Code`, then succeed
  - gives up after the configured max attempts
  - waits for an in-progress reconnect instead of calling `connect()` again (finding 4's fix)
  - calls `connect()` when actually closed
  - **load-style concurrency test**: fires 50 concurrent `send_to_minisever_via_websocket()` calls
    against a client that tracks how many `send_websocket_command()` calls are in flight
    simultaneously, and asserts the observed maximum concurrency is exactly `1` - i.e. the write
    lock genuinely prevents overlapping writes under a realistic message burst, not just in
    isolation.
- **`tests/test_topic_sequencer.py`** (5 tests, new): same-topic sends never overlap and run in
  order without coalescing; a value superseded before it started is dropped with coalescing on;
  different topics run fully independently and concurrently; internal bookkeeping is cleaned up
  once a topic's queue drains; a raising send doesn't stop later sends for the same topic.
- **3 more tests in `tests/test_http_miniserver_handler.py`**, going through the actual
  `send_to_miniserver()` entry point (not the protocol-specific methods directly), reproducing
  finding 5's exact repro scenario and proving it's fixed:
  - `test_send_to_miniserver_prevents_reordering_across_a_retry`: the `"off"`/`"on"` scenario from
    finding 5, with coalescing off - asserts the wire order is `["off", "off", "on"]` (the retry
    completes before `"on"` is ever sent), never interleaved.
  - `test_send_to_miniserver_coalesces_by_default_under_rapid_updates`: three rapid values to one
    topic, the first still in flight when the second and third arrive - asserts only the first and
    last are ever sent.
  - `test_send_to_miniserver_without_coalescing_sends_every_value`: the same burst with
    `miniserver_coalesce_topic_updates = false` - asserts all three are sent, in order.

All 227 tests (198 existing + 29 new) pass, run inside the Docker build image (the project requires
Python 3.14 + the compiled Rust extension, unavailable in this environment outside Docker). The
timing-sensitive new tests were additionally run 5x in a row to check for flakiness - all passed
every time.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Investigated whether the message-loss-under-load problem fixed on
  `fix/miniserver-http-session-reuse-and-basetopic-warning` (a fresh `aiohttp.ClientSession` - and
  TCP connection - per HTTP request) also applies when `use_websocket = true` (the dataclass
  default). It doesn't - WebSocket uses one persistent connection, so there's no per-message
  connection churn - but the HTTP-side fixes don't transfer either, and the WebSocket path
  (`send_to_minisever_via_websocket()`) has its own set of reliability gaps, fixed here:
  1. `send_websocket_command()` is fire-and-forget; nothing ever checked whether the Miniserver
     actually processed a command.
  2. No concurrency control on the one shared, stateful connection - concurrent writes from
     different tasks were unsynchronized, which aiohttp does not guarantee is safe.
  3. A likely bug in the third-party `loxwebsocket` dependency: `LxEncryptionHandler.genarate_salt()`
     has no `return` statement, so `self._salt = self.genarate_salt()` in `encrypt()` clobbers the
     salt it just generated with `None` - on the first command and every 30 messages/1h
     thereafter, i.e. *more often* under higher message throughput.
  4. A race between the relay's own opportunistic `connect()` call and `loxwebsocket`'s internal
     `reconnect()` loop, which bypasses the same lock.
  5. No ordering guarantee for rapid successive messages on the same topic, at three independent
     layers (gmqtt's dispatch, the Rust forwarder's task spawn, and the send layer itself) - a
     retried older value could physically land at the Miniserver after a newer one that already
     succeeded. Verified with a concrete repro, and made worse by `WebSocketAckWaiter`'s per-topic
     (not per-command) correlation, which could resolve the wrong in-flight call for the same topic.
  6. (found via a production log) `loxwebsocket`'s `reconnect()` loop leaks one `ClientSession` per
     failed reconnect attempt - `async_init()` opens a new one on every attempt but `stop()` (which
     closes it) only runs once, before the loop starts. Under a sustained outage with unlimited
     retries (the default), this can exhaust file descriptors, making it progressively harder to
     ever reconnect.
- Added response correlation + retry with exponential backoff (`websocket_ack.py`,
  `send_to_minisever_via_websocket()`), a write lock serializing the encrypt+send step, a fix for
  the reconnect race, narrow runtime patches for the `genarate_salt()` bug and the reconnect
  session leak (`loxwebsocket_compat.py`, isolated so they can be dropped once fixed upstream), and
  a per-topic send sequencer (`topic_sequencer.py`) applied to both HTTP and WebSocket, with an
  optional coalescing mode that drops superseded, not-yet-sent values instead of queueing every one.
- New `[miniserver]` options: `miniserver_websocket_retry_attempts` (default `3`),
  `miniserver_websocket_retry_backoff_seconds` (default `0.5`),
  `miniserver_websocket_ack_timeout_seconds` (default `5.0`),
  `miniserver_coalesce_topic_updates` (default `true`).

### Why
`use_websocket = true` is the shipped default. Without these fixes, every websocket-forwarded
message was sent blind (no way to know if it worked), with no protection against interleaved
writes on the shared connection, a real chance of a corrupted session salt that gets *more* likely
under higher message volume, no guarantee that rapid messages to the same topic (a double-tapped
switch, a short automation, a dimmer slide) would even arrive in the right order, and - as seen in
production - a resource leak that made a real Miniserver outage harder to recover from the longer
it lasted.

### Changes
- `src/loxmqttrelay/websocket_ack.py` (new): `WebSocketAckWaiter` - correlates outgoing commands
  with the Miniserver's response by topic.
- `src/loxmqttrelay/loxwebsocket_compat.py` (new): runtime patches for the `genarate_salt()` bug
  and the reconnect-loop `ClientSession` leak.
- `src/loxmqttrelay/topic_sequencer.py` (new): `TopicSequencer` - per-topic send serialization with
  optional coalescing, applied to both HTTP and WebSocket at the `send_to_miniserver()` entry point.
- `src/loxmqttrelay/http_miniserver_handler.py`: `send_to_minisever_via_websocket()` rewritten to
  wait for and check the response, retry with backoff, serialize writes via `self._ws_write_lock`,
  and avoid the reconnect race via `_ensure_websocket_connected()`; `send_to_miniserver()` now
  routes through `TopicSequencer`.
- `src/loxmqttrelay/config.py`, `config/default_config.toml`: four new `[miniserver]` options.
- `README.md`: documents the new retry/ack-timeout/coalescing options.
- `tests/test_loxwebsocket_compat.py`, `tests/test_websocket_ack.py`, `tests/test_topic_sequencer.py`
  (all new), and 10 new tests in `tests/test_http_miniserver_handler.py`, including a
  concurrency/load-style test for the write lock and an end-to-end regression test reproducing the
  ordering bug's exact repro scenario.

### Test Plan
- [x] `pytest` (198 existing + 29 new tests, 227 total) passes, run inside the Docker build image;
      timing-sensitive new tests additionally run 5x in a row to check for flakiness.
- [x] `test_close_stale_session_and_call_*` (4 tests) / `test_async_init_patch_is_installed_and_idempotent`:
      confirms the reconnect session-leak fix closes a stale open session before reconnecting,
      leaves an already-closed one alone, handles the very first connect, still proceeds if closing
      fails, and installs on `LoxWs` idempotently.
- [x] `test_websocket_send_succeeds_on_first_ack` / `..._retries_after_timeout_then_succeeds` /
      `..._retries_on_non_200_code` / `..._gives_up_after_max_retries`: confirms the retry
      behavior end to end against a scripted fake client.
- [x] `test_websocket_waits_for_ongoing_reconnect_instead_of_reconnecting_again`: confirms the
      reconnect-race fix - `connect()` is never called while the client is already `RECONNECTING`.
- [x] `test_websocket_writes_are_serialized_under_concurrent_load`: 50 concurrent sends, asserts
      max observed concurrent `send_websocket_command()` executions is exactly 1.
- [x] `test_genarate_salt_returns_the_salt_it_set` / `test_encrypt_no_longer_embeds_none_as_the_salt`:
      confirms the salt patch fixes the actual bug symptom, not just the return value in isolation.
- [x] `test_send_to_miniserver_prevents_reordering_across_a_retry`: reproduces finding 5's exact
      scenario end to end and confirms it no longer reorders.
- [x] `test_send_to_miniserver_coalesces_by_default_under_rapid_updates` /
      `..._without_coalescing_sends_every_value`: confirms both coalescing modes behave as
      documented.

### Notes for Reviewers
- No full protocol-level infrastructure test (a fake Miniserver speaking the real encrypted
  WebSocket protocol) was built - doing so would mean reimplementing a substantial part of
  Loxone's RSA/AES/JWT handshake just to mock it. The concurrency test against `FakeWsClient`
  covers the property that mattered most (no interleaved writes under load) without that cost;
  flagging this explicitly as a coverage gap rather than a silent one.
- The `genarate_salt()` fix is a runtime patch, not a change to the vendored dependency (there is
  none - `loxwebsocket` is a normal PyPI dependency). Its real-world impact on actual Miniserver
  hardware (does the unpatched "None" salt get silently accepted or rejected?) is not independently
  confirmed - the patch is justified purely by the source-level bug being unambiguous, not by an
  observed failure against real hardware.
- `miniserver_coalesce_topic_updates` defaults to `true`, a behavior change from the previous
  (unserialized, unordered) send path. This is intentional - there is no scenario where sending a
  value that's already known to be stale before it even goes out is preferable to skipping it. Set
  it to `false` for the old "send everything" behavior if every intermediate value genuinely
  matters for some topic (e.g. accumulating counters rather than typical last-write-wins controls).
- `_max_reconnect_attempts` was deliberately left unlimited (finding 7) - considered capping it so
  a broken reconnect loop can't run "forever," but rejected: a long-running relay should keep trying
  to recover from an outage of unknown duration, and the leak that made unlimited retries risky is
  now fixed. Only raise this again if a *different* failure mode (not resource exhaustion) is found
  that unlimited retries make worse.
- This branch is independent of `fix/miniserver-http-session-reuse-and-basetopic-warning` and
  `feature/defensive-whitelist-sync` - all three are based on `main` and can be reviewed/merged in
  any order.
