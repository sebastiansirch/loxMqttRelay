# Logging Improvements: Per-Send Correlation IDs and Raw-Response Diagnostics

Branch: `feature/logging-improvements` (based on `fix/websocket-reliability`)

## Starting Question

Follow-up to `docs/websocket-reliability.md` findings 8 and 9: while reviewing a production log
containing the new `reconnect()` token-reset fix in action, two things stood out:

1. Log lines for retries of *different* topics interleave heavily under load (visible in every
   production log excerpt shared so far), making it tedious to trace one logical send's full
   attempt-1 → attempt-2 → give-up/success sequence by eye.
2. A new exception surfaced once the token-reset fix let reconnects progress further:
   `Reconnection failed: string indices must be integers, not 'str'` - a bare Python `TypeError`
   with zero context about what the Miniserver actually sent back.

## Analysis

### Correlation problem

Every `send_to_miniserver_via_http()` / `send_to_minisever_via_websocket()` log line already
includes topic and value, which is fairly identifying - but under a burst where the *same*
topic/value legitimately repeats (e.g. a retained sensor reading published again unchanged), or
when many different topics are all mid-retry at once (the common case in every production log seen
so far), there was no way to isolate "everything that happened for this one specific send" with a
single `grep`.

### The new crash: unguarded indexing in `loxwebsocket`

Traced the exception to `LxJsonKeySalt.read_user_salt_responce()` in `loxwebsocket/encryption.py`,
which has **no** `try`/`except` at all:

```python
def read_user_salt_responce(self, reponse):
    js = json.loads(reponse)
    value = js["LL"]["value"]     # <- "string indices must be integers, not 'str'" if js["LL"] is a str
    self.key = value["key"]
    self.salt = value["salt"]
```

called, also unguarded, from `LoxWs.acquire_token()`:

```python
async def acquire_token(self):
    message = await self.send_command(f"{c.CMD_GET_KEY_AND_SALT}{self._username}")
    key_and_salt = LxJsonKeySalt()
    key_and_salt.read_user_salt_responce(message)   # <- no try/except around this call either
    ...
```

`json.loads()` succeeds (the response *is* valid JSON), but `js["LL"]` turns out to be a plain
string rather than the expected object - meaning the Miniserver's response to
`jdev/sys/getkey2/<user>` wasn't shaped as a successful key/salt payload. This is very plausibly
how the Miniserver reports a *rejected* request (e.g. rate-limiting after repeated failed auth
attempts during an ongoing outage, an unrecognized/locked-out user) rather than the request
succeeding - but without the raw response text, there was no way to tell which, or to see the
actual rejection message.

Notably, this exact crash was **not reachable before** the `docs/websocket-reliability.md` finding
9 fix: previously, every reconnect attempt failed earlier (in the doomed `use_token()` detour)
before ever reaching `acquire_token()`'s network round-trip. The token-reset fix routing reconnects
straight to `acquire_token()` is why this second, previously-latent bug started surfacing.

## Fixes

### Request-ID logging (`src/loxmqttrelay/http_miniserver_handler.py`)

`send_to_miniserver()` generates a short correlation ID once per logical send and threads it
through to both protocol-specific methods:

```python
async def send_to_miniserver(self, topic, normalized_topic, value):
    request_id = uuid.uuid4().hex[:8]
    logger.debug(f"[{request_id}] Sending {topic} (as {normalized_topic})={value} to Miniserver")

    async def _send():
        if global_config.miniserver.use_websocket:
            await self.send_to_minisever_via_websocket(topic, normalized_topic, value, request_id)
        else:
            await self.send_to_miniserver_via_http(topic, normalized_topic, value, request_id)
    ...
```

Every log line in `send_to_miniserver_via_http()` and `send_to_minisever_via_websocket()` - across
all retry attempts - is now prefixed with `[{request_id}]`, so `grep '\[a3f9c1b2\]'` isolates one
logical send's complete lifecycle regardless of how many other topics are interleaved in the log
at the same time. `request_id` is an optional parameter defaulting to a freshly generated ID if not
supplied, so calling either protocol-specific method directly (as many existing tests already do)
requires no changes and never produces an untagged log line.

### Raw-response logging on parse failure (`src/loxmqttrelay/loxwebsocket_compat.py`)

`LxJsonKeySalt.read_user_salt_responce()` is wrapped to log the raw response text before
re-raising the same exception, unchanged:

```python
def patched_read_user_salt_responce(self, reponse):
    return log_raw_response_on_error(original_read_user_salt_responce, self, reponse)

def log_raw_response_on_error(original_fn, instance, raw_response):
    try:
        return original_fn(instance, raw_response)
    except Exception:
        logger.error(f"Failed to parse Miniserver response - raw response: {raw_response!r}", exc_info=True)
        raise
```

This does not change control flow or the exception type/message seen by callers
(`acquire_token()`, `async_init()`, `reconnect()`'s handling is all unaffected) - it only adds
visibility into *what* the Miniserver actually sent back, so a future occurrence of this crash (or
a related one) shows the rejection/error content instead of just the bare Python exception message.

Scope note: `hash_token()` and `acquire_token()`'s second response-parsing block already have their
own `try/except (KeyError, TypeError, json.JSONDecodeError)`, but likewise don't log the raw
content - only a fixed "Unexpected content in Loxone response" message. Extending the same
raw-logging treatment to those wasn't done here, since it would require re-implementing rather than
wrapping their internal logic (the raw response is a local variable inside a multi-step method, not
a single delegate call like `read_user_salt_responce()`). `read_user_salt_responce()` was both the
*confirmed* crash site and the one cleanly patchable without duplicating library logic.

## Testing

- **`tests/test_http_miniserver_handler.py`** (4 new tests): a retry sequence tags every log line
  (across attempts) with the same request ID; two separate sends get two different IDs; calling
  `send_to_minisever_via_websocket()` directly (no `request_id` passed) still tags its lines; an
  explicit `request_id` passed to `send_to_miniserver_via_http()` is honored verbatim.
- **`tests/test_loxwebsocket_compat.py`** (4 new tests): `log_raw_response_on_error()` returns the
  original result on success and is transparent; on failure it logs the raw response and re-raises
  the *same* exception (not swallowed, not a different type); an end-to-end check against the real
  (patched) `LxJsonKeySalt.read_user_salt_responce()` with a malformed `"LL"` string confirms the
  raw response appears in the log and a `TypeError` still propagates; the patch installs on
  `LxJsonKeySalt` idempotently.

All 246 tests (238 existing + 8 new) pass, run inside the Docker build image. Re-run 5x for
flakiness on the two most timing-sensitive files - no failures.

One test-infrastructure note surfaced while writing these: `TopicSequencer.submit()` (from the
per-topic sequencing work) returns as soon as a send is *queued*, not once it actually completes -
tests asserting on retry log lines need to explicitly drain the topic (poll
`handler._topic_sequencer._running`) rather than just `await`ing `send_to_miniserver()` directly,
and need one `await asyncio.sleep(0)` after `asyncio.create_task(...)` before draining so the new
task gets a chance to actually register itself as running first (otherwise the drain check races
ahead and returns immediately, before the task has started).

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Added a short correlation ID (`request_id`) to every log line produced by one logical
  `send_to_miniserver()` call, including across all of its retry attempts, so a specific send can be
  isolated with a single `grep` even when many other topics are concurrently retrying in the same
  log (the norm under load, per every production log excerpt reviewed so far).
- Found and fixed a second, previously-latent bug surfaced by the `reconnect()` token-reset fix
  (`docs/websocket-reliability.md` finding 9): `LxJsonKeySalt.read_user_salt_responce()` has no
  `try`/`except` at all, so when the Miniserver's response to `jdev/sys/getkey2/<user>` isn't
  shaped as expected (`"LL"` a plain string, not an object - plausibly a rejection/rate-limit
  message), a bare Python `TypeError` propagated all the way to `reconnect()`'s generic handler
  with zero information about what the Miniserver actually sent. Patched to log the raw response
  before re-raising the same exception unchanged.

### Why
Diagnosing production issues in this relay depends entirely on the log. Without a correlation ID,
tracing one message's outcome through a busy, heavily-interleaved log was tedious; without the raw
response, the newly-surfaced `read_user_salt_responce()` crash gave no way to tell whether it's a
Miniserver-side rejection (and if so, why) or something else.

### Changes
- `src/loxmqttrelay/http_miniserver_handler.py`: `send_to_miniserver()` generates a `request_id`
  and threads it through to `send_to_miniserver_via_http()` / `send_to_minisever_via_websocket()`
  (both now accept an optional `request_id` parameter, defaulting to a freshly generated one so
  direct calls - as in existing tests - are unaffected); every log line in both methods is prefixed
  with `[{request_id}]`.
- `src/loxmqttrelay/loxwebsocket_compat.py`: new patch wraps
  `LxJsonKeySalt.read_user_salt_responce()` to log the raw response on any parse/shape failure
  before re-raising unchanged.
- `tests/test_http_miniserver_handler.py`, `tests/test_loxwebsocket_compat.py`: 8 new tests
  covering both changes.

### Test Plan
- [x] `pytest` (238 existing + 8 new tests, 246 total) passes, run inside the Docker build image;
      the two most timing-sensitive test files re-run 5x for flakiness with no failures.
- [x] `test_send_to_miniserver_tags_every_retry_log_line_with_the_same_request_id`: a two-attempt
      retry sequence produces log lines that all share one request ID.
- [x] `test_send_to_miniserver_uses_different_request_ids_for_different_sends`: two separate sends
      get two different IDs.
- [x] `test_read_user_salt_responce_logs_raw_response_on_malformed_ll`: end-to-end against the real
      patched method with a malformed `"LL"` string - raw response appears in the log, `TypeError`
      still raised (behavior-preserving, purely additive).

### Notes for Reviewers
- Both `send_to_miniserver_via_http()` and `send_to_minisever_via_websocket()` keep `request_id`
  optional specifically so no existing call site or test needed to change.
- The raw-response-logging fix is scoped to the one confirmed, cleanly-patchable crash site
  (`read_user_salt_responce()`); `hash_token()`/`acquire_token()`'s own already-caught (but equally
  uninformative) parse failures were intentionally left alone rather than reimplementing their
  internals to add the same treatment - see the "Scope note" above.
- This branch is based on `fix/websocket-reliability` (not `main`) since it directly extends
  `loxwebsocket_compat.py` and `http_miniserver_handler.py` as they exist there; it merges back
  into `fix/websocket-reliability` rather than being reviewed independently against `main`.
