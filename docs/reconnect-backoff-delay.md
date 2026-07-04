# Exponential Backoff Before Reconnect Attempts (Instead of a Fixed 15s Wait)

Branch: `feature/reconnect-backoff-delay` (based on `fix/websocket-reliability`)

## Starting Question

Analysis of a production log (`docs/salt-reset-on-reconnect.md`'s follow-up investigation)
confirmed reconnects succeed reliably, but always only after a fixed 15-second wait -
even for the very first reconnect attempt right after a disconnect. Question: can that
fixed wait be overridden so the first attempt happens sooner (e.g. after 1 second)?

## Analysis

`loxwebsocket/const.py` defines `CONNECT_DELAY = 15`, read fresh via `c.CONNECT_DELAY`
inside `LoxWs.reconnect()`'s retry loop:

```python
async def reconnect(self) -> None:
    ...
    attempt = 0
    while self._max_reconnect_attempts == 0 or self._max_reconnect_attempts > attempt:
        attempt += 1
        _LOGGER.info(f"Reconnect attempt {attempt + 1} of {self._max_reconnect_attempts}")
        _LOGGER.info(f"Waiting for {c.CONNECT_DELAY} seconds before retrying...")
        await asyncio.sleep(c.CONNECT_DELAY)
        if not await self.http_ping():
            continue
        ...
```

The same fixed 15s applies before **every** attempt in this unbounded retry loop
(`max_reconnect_attempts=0`, kept unlimited by design - see `docs/websocket-reliability.md`
finding 7), not just the first one. Since the wait is inline inside the loop body - not
behind any separate, wrappable call - it can't be shortened with a simple before/after
wrap like the other patches in this module; the loop itself has to be reimplemented to
vary the delay per attempt.

Two smaller things fell out of reading this method closely enough to reimplement it:
- The log line `f"Reconnect attempt {attempt + 1} of ..."` is off by one: `attempt` is
  already post-increment at that point, so the very first attempt logs as "attempt 2" -
  exactly the confusing number seen in every production log analyzed so far. Cosmetic
  only, but worth fixing while rewriting this method anyway.
- The existing reconnect-token-reset fix (`docs/websocket-reliability.md` finding 9)
  wrapped this same method to reset `self._token` before the loop starts. Since this
  change replaces the method outright, that reset is folded in directly instead of
  chaining a second wrapper around a wrapper.

## Fix

`src/loxmqttrelay/loxwebsocket_compat.py`: `_patch_reconnect_uses_backoff_delay()`
replaces `LoxWs.reconnect` with `run_reconnect_with_backoff()` - the same orchestration
(state guard, `stop()`, the retry loop, `http_ping()`/`async_init()`/`start()` calls, the
give-up/raise branch), but the fixed sleep is replaced by `reconnect_backoff_delay(attempt)`:

```python
def reconnect_backoff_delay(attempt: int) -> float:
    if attempt <= 1:
        return 0.0
    initial = global_config.miniserver.miniserver_websocket_reconnect_initial_delay_seconds
    multiplier = global_config.miniserver.miniserver_websocket_reconnect_backoff_multiplier
    cap = loxwebsocket_const.CONNECT_DELAY
    return min(initial * (multiplier ** (attempt - 2)), cap)
```

The very first reconnect attempt is always immediate (0s, follow-up refinement - see
below) - right after a disconnect there's no reason to wait before even trying once. From
the second attempt on, the exponential schedule kicks in.

New config (`MiniserverConfig`, defaults shown):

```toml
[miniserver]
miniserver_websocket_reconnect_initial_delay_seconds = 1.0
miniserver_websocket_reconnect_backoff_multiplier = 2.0
```

With these defaults, the first attempt is immediate, then subsequent attempts wait 1s,
2s, 4s, 8s, then 15s (capped at loxwebsocket's own `CONNECT_DELAY`) from then on -
converging back to identical-to-upstream behavior once several attempts have failed, so a
genuinely long outage doesn't end up hammering the Miniserver every second indefinitely.

### Follow-up: immediate first attempt

Initial version of this fix still waited `miniserver_websocket_reconnect_initial_delay_seconds`
(1s by default) before the *first* attempt too. Follow-up request: the first attempt
should happen immediately, with the backoff schedule only kicking in from the second
attempt on. `reconnect_backoff_delay()` now special-cases `attempt <= 1` to return `0.0`
unconditionally (not configurable - there's no scenario where waiting before the very
first try after a disconnect is useful), and the exponential formula for later attempts
was shifted by one so the configured `initial_delay`/`multiplier` still describe the
*first backoff step* (now the second attempt) exactly as before.

`http_ping()`/`async_init()`/`start()` are still called as plain instance method calls, so
every other patch on this class (stale-session-close, write-serialization, salt-reset)
keeps applying unchanged - only the retry loop's own orchestration/timing is duplicated
here, not any protocol logic. This is a larger change than the other patches in this
module (a full reimplementation, not a wrap) because the fixed delay has no separate,
wrappable call site; if a future `loxwebsocket` release changes `reconnect()`'s own
implementation, this patch needs to be revisited to match.

## Testing

- `reconnect_backoff_delay()`: the first attempt is always `0.0` regardless of config;
  follows the default schedule (0s, 1s, 2s, 4s, 8s) and caps at `loxwebsocket`'s own
  `CONNECT_DELAY` from the 6th attempt on; respects custom config values.
- `run_reconnect_with_backoff()` (against a fake instance, `asyncio.sleep` mocked to
  record delays instead of actually waiting):
  - succeeds on the first attempt (delay `0.0`), resets `_token`, calls `start()`.
  - across a run that fails `http_ping()` three times then `async_init()` twice before
    succeeding on the 6th attempt, the recorded delays are exactly `[0.0, 1.0, 2.0, 4.0,
    8.0, 15.0]`.
  - already-`RECONNECTING` guard: returns immediately without calling `stop()`.
  - exhausting `max_reconnect_attempts` raises `LoxoneException` without ever calling
    `start()`.
- `test_patch_reconnect_uses_backoff_delay_is_installed_and_idempotent`: the patch
  installs on `LoxWs.reconnect` and repeated `apply_patches()` calls don't wrap it again.

All 258 tests pass (257 existing + 1 new), run inside the Docker build image. The two
most timing-sensitive test files (`test_http_miniserver_handler.py`,
`test_loxwebsocket_compat.py`) re-run 5x with no failures.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Replaced `loxwebsocket`'s fixed 15-second wait before every reconnect attempt with a
  configurable exponential backoff. The first attempt is now always immediate (0s);
  every attempt after that follows the configured schedule (default: 1s, 2s, 4s, 8s, then
  capped at 15s) - so recovery from a brief connection blip starts right away instead of
  always waiting the full 15 seconds, while a genuinely long outage still backs off to
  the same 15s cadence as before, rather than retrying every second indefinitely.
- Folded the existing reconnect-token-reset fix into this change (same effect, one fewer
  wrapper), and fixed a cosmetic off-by-one in the reconnect attempt-number log line
  (`"attempt 2"` on the very first attempt) noticed while rewriting the method.

### Why
The fixed 15s delay applied identically to the first reconnect attempt and every
subsequent one, so even momentary Miniserver-initiated disconnects (`docs/`
`websocket-reliability.md`'s close-code-1000 finding) took at least 15s to recover from
by design, not because reconnection itself was slow. An exponential backoff starts fast
for the common case (transient blip, back up within ~1-2s) without changing behavior for
a sustained outage (converges back to the original fixed cadence).

### Changes
- `src/loxmqttrelay/loxwebsocket_compat.py`: `_patch_reconnect_uses_backoff_delay()` /
  `run_reconnect_with_backoff()` / `reconnect_backoff_delay()` replace
  `_patch_reconnect_resets_token()`, which is now superseded (its token-reset behavior is
  folded directly into the new reimplementation).
- `src/loxmqttrelay/config.py`, `config/default_config.toml`, `README.md`: new
  `miniserver_websocket_reconnect_initial_delay_seconds` (default 1.0) and
  `miniserver_websocket_reconnect_backoff_multiplier` (default 2.0) config options.
- `tests/test_loxwebsocket_compat.py`: 6 new tests; the previous
  `test_patch_reconnect_resets_token_is_installed_and_idempotent` is replaced by
  `test_patch_reconnect_uses_backoff_delay_is_installed_and_idempotent` (same idempotency
  check, new patch/marker name) - the two direct tests of the still-used
  `reset_token_if_not_already_reconnecting()` helper are unchanged.

### Test Plan
- [x] `pytest` (257 existing + 1 new test, 258 total) passes, run inside the Docker build
      image; the two most timing-sensitive test files re-run 5x for flakiness with no
      failures.
- [x] `test_run_reconnect_with_backoff_uses_increasing_delays_capped_at_connect_delay`:
      confirms the exact schedule (0s, 1s, 2s, 4s, 8s, 15s) across a multi-attempt run.
- [x] `test_reconnect_backoff_delay_first_attempt_is_always_immediate`: confirms the first
      attempt is `0.0` regardless of config.
- [x] `test_run_reconnect_with_backoff_raises_after_exhausting_attempts`: confirms
      give-up/raise behavior is unchanged when `max_reconnect_attempts` is set and
      exhausted.

### Notes for Reviewers
- This is a full reimplementation of `reconnect()`'s orchestration loop, not a wrap
  (unlike every other patch in this module) - the fixed delay is inline inside the loop
  body with no separate call site to intercept. `http_ping()`/`async_init()`/`start()`
  are still plain instance method calls, so no protocol logic is duplicated, only the
  loop's own timing/orchestration. Flagged in the code as needing a revisit if
  `loxwebsocket` changes `reconnect()`'s upstream implementation.
- Reconnect attempts remain unlimited by design (unchanged from
  `docs/websocket-reliability.md` finding 7) - this only changes how long each attempt
  waits before trying, not how many are attempted.
- This branch is based on `fix/websocket-reliability` (not `main`) since it directly
  extends `loxwebsocket_compat.py` as it exists there; it merges back into
  `fix/websocket-reliability` rather than being reviewed independently against `main`.
