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

### 5. Whitelist-sync bug is unaffected either way

The whitelist-overwrite bug found and fixed on `feature/defensive-whitelist-sync` (see
`docs/defensive-whitelist-sync.md`) happens in Rust's `process_data()`, before the HTTP/WebSocket
choice is made - it applies identically regardless of which one is configured. No changes needed
here.

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

### New `[miniserver]` config options

```toml
miniserver_websocket_retry_attempts = 3          # total attempts, incl. the first; 1 disables retrying
miniserver_websocket_retry_backoff_seconds = 0.5 # doubles after each retry
miniserver_websocket_ack_timeout_seconds = 5.0   # how long to wait for a response per attempt
```

## Testing

Given `loxwebsocket` speaks Loxone's actual encrypted WebSocket protocol (RSA key exchange, AES
session encryption, JWT/token auth), a full protocol-level Docker infrastructure test (a fake
Miniserver that speaks that protocol, analogous to `loadtest/miniserver_mock` for HTTP) would mean
reimplementing a substantial part of the encrypted handshake just to mock it - out of scope for
this pass. Instead:

- **`tests/test_loxwebsocket_compat.py`** (3 tests): confirms `genarate_salt()` now returns the
  salt it set (not `None`), that `apply_patches()` is idempotent, and end-to-end that `encrypt()`
  no longer embeds the literal string `"None"` as the salt.
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

All 214 tests (198 existing + 16 new) pass, run inside the Docker build image (the project requires
Python 3.14 + the compiled Rust extension, unavailable in this environment outside Docker).

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
- Added response correlation + retry with exponential backoff (`websocket_ack.py`,
  `send_to_minisever_via_websocket()`), a write lock serializing the encrypt+send step, a fix for
  the reconnect race, and a narrow runtime patch for the `genarate_salt()` bug
  (`loxwebsocket_compat.py`, isolated so it can be dropped once fixed upstream).
- New `[miniserver]` options: `miniserver_websocket_retry_attempts` (default `3`),
  `miniserver_websocket_retry_backoff_seconds` (default `0.5`),
  `miniserver_websocket_ack_timeout_seconds` (default `5.0`).

### Why
`use_websocket = true` is the shipped default. Without these fixes, every websocket-forwarded
message was sent blind (no way to know if it worked), with no protection against interleaved
writes on the shared connection, and a real chance of a corrupted session salt that gets *more*
likely, not less, under higher message volume.

### Changes
- `src/loxmqttrelay/websocket_ack.py` (new): `WebSocketAckWaiter` - correlates outgoing commands
  with the Miniserver's response by topic.
- `src/loxmqttrelay/loxwebsocket_compat.py` (new): runtime patch for the `genarate_salt()` bug.
- `src/loxmqttrelay/http_miniserver_handler.py`: `send_to_minisever_via_websocket()` rewritten to
  wait for and check the response, retry with backoff, serialize writes via `self._ws_write_lock`,
  and avoid the reconnect race via `_ensure_websocket_connected()`.
- `src/loxmqttrelay/config.py`, `config/default_config.toml`: three new `[miniserver]` options.
- `README.md`: documents the new retry/ack-timeout options for WebSocket communication.
- `tests/test_loxwebsocket_compat.py`, `tests/test_websocket_ack.py` (new), and 7 new tests in
  `tests/test_http_miniserver_handler.py`, including a concurrency/load-style test for the write
  lock.

### Test Plan
- [x] `pytest` (198 existing + 16 new tests, 214 total) passes, run inside the Docker build image.
- [x] `test_websocket_send_succeeds_on_first_ack` / `..._retries_after_timeout_then_succeeds` /
      `..._retries_on_non_200_code` / `..._gives_up_after_max_retries`: confirms the retry
      behavior end to end against a scripted fake client.
- [x] `test_websocket_waits_for_ongoing_reconnect_instead_of_reconnecting_again`: confirms the
      reconnect-race fix - `connect()` is never called while the client is already `RECONNECTING`.
- [x] `test_websocket_writes_are_serialized_under_concurrent_load`: 50 concurrent sends, asserts
      max observed concurrent `send_websocket_command()` executions is exactly 1.
- [x] `test_genarate_salt_returns_the_salt_it_set` / `test_encrypt_no_longer_embeds_none_as_the_salt`:
      confirms the salt patch fixes the actual bug symptom, not just the return value in isolation.

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
- This branch is independent of `fix/miniserver-http-session-reuse-and-basetopic-warning` and
  `feature/defensive-whitelist-sync` - all three are based on `main` and can be reviewed/merged in
  any order.
