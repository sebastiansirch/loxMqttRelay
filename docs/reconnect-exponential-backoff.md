# Exponential Backoff Before Reconnect Attempts (loxwebsocket 0.6.0 baseline)

Branch: `feature/reconnect-exponential-backoff` (based on `main`, after the upstream
`loxwebsocket` 0.6.0 upgrade)

## Starting Question

An earlier, much larger investigation (`fix/websocket-reliability`, not merged into `main`)
had found and patched a whole series of `loxwebsocket` bugs: a `ClientSession` leak on
reconnect, salt state not reset across sessions, a reconnect()-token-reset no-op,
unsynchronized writes, and stale background tasks surviving a reconnect. Before continuing
that work, `main` was updated to the upstream project's latest state, which turned out to
include an upgrade to `loxwebsocket` 0.6.0.

## Analysis

Comparing the actual installed package source between 0.5.2 (what `fix/websocket-reliability`
was built against) and 0.6.0 (now on `main`) showed that upstream `loxwebsocket` had
independently found and fixed nearly every one of the same bugs:

- `genarate_salt()` now returns the salt (was a silent `None`-return bug).
- `async_init()` now closes any leftover session/socket at the start, and cleans up on a
  failed handshake too (`_close_connection_resources()`).
- `LxEncryptionHandler.reset_salt()` (new) is called at the start of every `async_init()` -
  the exact same three fields our own patch reset, for the exact same reason (stale
  `nextSalt` reference otherwise gets rejected with a spurious 401).
- `reconnect()` correctly resets `self._token` (not the unused `self.token`).
- `reconnect()`'s attempt-number log line no longer has the off-by-one we'd also fixed
  independently.
- Background tasks (`ws_listen()`, `keep_alive()`, `refresh_token()`) from a dead connection
  are now cancelled immediately via `_cancel_stale_background_tasks()`, rather than waiting
  for the next successful `start()` - the same race we found and fixed ourselves, but with a
  meaningful refinement: it spares the currently-running task (via `asyncio.current_task()`)
  from cancelling itself, which our own version didn't - a real self-cancellation risk our
  version had that this project's version does not.
- `send_command()` (used internally for the handshake: key exchange, `acquire_token`,
  `hash_token`, token refresh) now correlates responses via a proper `asyncio.Future` handed
  back from the listener task, instead of blindly reading the next two raw frames off the
  socket - eliminating the cross-talk risk a stale background task's own `send_command()`
  call could previously cause.
- Two additional protocol bugs neither project had noticed before: AES padding was PKCS#7
  instead of the zero-byte padding the protocol actually uses, and the base64 ciphertext
  wasn't URI-component-encoded correctly (missing `/` escaping).

The one thing upstream's `reconnect()` still does **not** do: it waits a fixed
`c.CONNECT_DELAY` (15s) before every single attempt, including the very first one right
after a disconnect - exactly the behavior our own backoff fix targeted.

## Decision

Given the above, only the exponential-backoff delay is re-implemented here, kept as close
to upstream as possible - not a full alternate reconnect strategy, not a reintroduction of
the other patches (all superseded natively by 0.6.0). `send_websocket_command()` (the path
`http_miniserver_handler.py` actually uses to send Miniserver values, as opposed to
`send_command()` used internally for the handshake) is unchanged in 0.6.0 and still
fire-and-forget - any retry/ack-correlation/message-ordering work for that path is a
separate concern, intentionally out of scope here.

## Fix

`src/loxmqttrelay/loxwebsocket_compat.py`: `_patch_reconnect_uses_backoff_delay()` replaces
`LoxWs.reconnect` with `run_reconnect_with_backoff()` - a line-for-line mirror of
loxwebsocket 0.6.0's own `reconnect()`, changed only to call
`reconnect_backoff_delay(attempt)` instead of the fixed `c.CONNECT_DELAY`:

```python
def reconnect_backoff_delay(attempt: int) -> float:
    if attempt <= 1:
        return 0.0
    initial = global_config.miniserver.miniserver_websocket_reconnect_initial_delay_seconds
    multiplier = global_config.miniserver.miniserver_websocket_reconnect_backoff_multiplier
    cap = loxwebsocket_const.CONNECT_DELAY
    return min(initial * (multiplier ** (attempt - 2)), cap)
```

Everything else - the state guard, `stop()`, `self._token = LxToken()` (already correct
upstream), `self._cancel_stale_background_tasks()` (upstream's own, already-correct
implementation - called directly rather than reimplemented), `http_ping()`/`async_init()`/
`start()`/`send_event(EventType.RECONNECTED)`, and the give-up/raise branch - is copied
verbatim from upstream's `reconnect()`. Calling upstream's own
`_cancel_stale_background_tasks()` (instead of reimplementing our own, as the now-superseded
patch on `fix/websocket-reliability` did) means the current-task-sparing behavior comes for
free and correctly, without us having to reason about it ourselves.

New config (`MiniserverConfig`, defaults shown):

```toml
[miniserver]
miniserver_websocket_reconnect_initial_delay_seconds = 1.0
miniserver_websocket_reconnect_backoff_multiplier = 2.0
```

With these defaults: the first attempt is immediate, then 1s, 2s, 4s, 8s, then 15s (capped
at loxwebsocket's own `CONNECT_DELAY`) from then on - converging back to
identical-to-upstream behavior once several attempts have failed, so a genuinely long
outage doesn't end up hammering the Miniserver every second indefinitely.

## Testing

- `reconnect_backoff_delay()`: first attempt always `0.0` regardless of config; follows the
  default schedule (0s, 1s, 2s, 4s, 8s) and caps at loxwebsocket's own `CONNECT_DELAY` from
  the 6th attempt on; respects custom config values.
- `run_reconnect_with_backoff()` (against a fake instance mirroring the pieces of `LoxWs`
  it touches, `asyncio.sleep` mocked to record delays instead of actually waiting):
  - succeeds on the first attempt, resets `_token`, calls `_cancel_stale_background_tasks()`,
    `start()`, and `send_event(EventType.RECONNECTED)`.
  - across a run that fails `http_ping()` three times then `async_init()` twice before
    succeeding on the 6th attempt, the recorded delays are exactly `[0.0, 1.0, 2.0, 4.0, 8.0,
    15.0]`.
  - already-`RECONNECTING` guard: returns immediately without calling `stop()` or
    `_cancel_stale_background_tasks()`.
  - exhausting `max_reconnect_attempts` raises `LoxoneException` without ever calling
    `start()` or sending the `RECONNECTED` event.
- `test_patch_reconnect_uses_backoff_delay_is_installed_and_idempotent`: the patch installs
  on `LoxWs.reconnect` and repeated `apply_patches()` calls don't wrap it again.

All 206 tests pass (this branch's full suite, matching `main`'s baseline plus 6 new tests),
run inside the Docker build image. The two relevant test files re-run 5x with no failures.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Adds a single, minimal patch to `loxwebsocket`: exponential backoff before reconnect
  attempts (first attempt immediate, then 1s/2s/4s/8s/15s-capped) instead of the library's
  fixed 15-second wait before every single attempt.
- No other changes: a comparison of `loxwebsocket` 0.5.2 vs. the 0.6.0 this repo now
  depends on (upstream commit `4e523cd`) showed upstream had independently fixed every
  other `loxwebsocket` bug previously patched on the (unmerged) `fix/websocket-reliability`
  branch - in some cases (stale background task cancellation, `send_command()` response
  correlation) more thoroughly than our own patches did. Those are intentionally not
  reintroduced here.

### Why
`loxwebsocket`'s reconnect loop waits the same fixed 15 seconds before its very first
attempt as before every subsequent one, so recovery from even a momentary Miniserver-side
disconnect always takes at least 15 seconds by design, independent of how quickly the
Miniserver actually becomes reachable again. This is the one remaining gap 0.6.0 doesn't
close itself.

### Changes
- `src/loxmqttrelay/loxwebsocket_compat.py` (new): `_patch_reconnect_uses_backoff_delay()` /
  `run_reconnect_with_backoff()` / `reconnect_backoff_delay()`, mirroring loxwebsocket
  0.6.0's own `reconnect()` except for the delay calculation.
- `src/loxmqttrelay/http_miniserver_handler.py`: imports and calls `apply_patches()` before
  any websocket traffic, otherwise unchanged.
- `src/loxmqttrelay/config.py`, `config/default_config.toml`, `README.md`: new
  `miniserver_websocket_reconnect_initial_delay_seconds` (default 1.0) and
  `miniserver_websocket_reconnect_backoff_multiplier` (default 2.0) config options.
- `tests/test_loxwebsocket_compat.py` (new): 6 tests covering the delay schedule and the
  full reconnect flow against a fake instance.

### Test Plan
- [x] `pytest` (206 total, including 6 new tests) passes, run inside the Docker build
      image; the relevant test files re-run 5x for flakiness with no failures.
- [x] `test_run_reconnect_with_backoff_uses_increasing_delays_capped_at_connect_delay`:
      confirms the exact schedule (0s, 1s, 2s, 4s, 8s, 15s) across a multi-attempt run.
- [x] `test_run_reconnect_with_backoff_raises_after_exhausting_attempts`: confirms
      give-up/raise behavior is unchanged when `max_reconnect_attempts` is set and
      exhausted.

### Notes for Reviewers
- This patch calls loxwebsocket's own `self._cancel_stale_background_tasks()` directly
  rather than reimplementing task cancellation - that method already correctly spares the
  currently-running task from self-cancellation, which an earlier, now-abandoned version of
  this fix (on `fix/websocket-reliability`, built against 0.5.2) did not.
- `send_websocket_command()` (the path actually used to send Miniserver values) remains
  fire-and-forget in 0.6.0 - unrelated to this fix and out of scope here.
- Kept deliberately minimal per the project's current direction: stay close to upstream,
  add only what upstream doesn't already provide.
