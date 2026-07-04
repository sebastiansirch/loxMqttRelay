# Salt State Not Reset Across Reconnects (Spurious 401 on Reconnect)

Branch: `fix/salt-reset-on-reconnect` (based on `fix/websocket-reliability`)

## Starting Question

A production log excerpt showed a reconnect that completed the RSA/AES handshake
successfully ("ENCRYPTION READY") but then immediately got rejected:

```json
{"LL": {"control": "jdev/sys/getkey2/<user>", "value": "...", "Code": "401"}}
```

raised via `docs/logging-improvements.md`'s raw-response logging patch. The obvious
suspicion - wrong or locked credentials - was explicitly ruled out: the same credentials
had just worked before the disconnect, and the user account was confirmed not locked out.
That meant the `401` had to have a purely technical cause independent of the actual
username/password.

## Analysis

`LoxWs.__init__()` creates `self._encryption_handler` (an `LxEncryptionHandler`) exactly
once. Every reconnect tears down and rebuilds `self._ws`/`self._session` (see the
stale-session-close and token-reset patches earlier on this branch), but the encryption
handler itself - and crucially its salt-rotation bookkeeping - is never recreated or
reset. The Miniserver, on the other hand, starts a completely fresh session on every
reconnect: a new RSA-wrapped AES key exchange, "ENCRYPTION READY" from scratch. It has no
memory of any salt used in the previous, now-dead session.

`LxEncryptionHandler.encrypt()` decides which framing to use based on leftover state:

```python
if self._salt != "" and self.new_salt_needed():
    prev_salt = self._salt
    self._salt = self.genarate_salt()
    s = "nextSalt/{}/{}/{}\0".format(prev_salt, self._salt, command)
else:
    if self._salt == "":
        self._salt = self.genarate_salt()
    s = "salt/{}/{}\0".format(self._salt, command)
```

The plain `"salt/{salt}/{command}"` ("fresh session") branch is only taken when
`self._salt == ""`. `new_salt_needed()` becomes `True` after `SALT_MAX_USE_COUNT` (30)
messages or `SALT_MAX_AGE_SECONDS` (1h) - both virtually guaranteed to have already
elapsed by the time any real-world reconnect happens. So by reconnect time, `self._salt`
is always some non-empty leftover value from the old session, and the very first
encrypted command of the brand-new session - `acquire_token()`'s initial
`jdev/sys/getkey2/<user>` request - takes the `"nextSalt/{prev_salt}/{new_salt}/..."`
continuation branch, referencing a salt from a session the Miniserver has never seen (it
just did a fresh key exchange seconds earlier). A Miniserver enforcing salt-based replay
protection has good reason to reject a `nextSalt` reference it can't recognize - which is
exactly the `401` observed, and fully explains it without any actual credentials problem.

This is independent of, and additional to, every other reconnect fix on this branch: the
stale-session-close, token-reset, and write-serialization patches all get the *connection*
and *token* into a correct state for a fresh reconnect attempt, but none of them touch the
encryption handler's salt bookkeeping, which is a separate piece of state living on the
same long-lived object across reconnects.

## Fix

`src/loxmqttrelay/loxwebsocket_compat.py`: new patch
`_patch_encryption_handler_resets_salt_on_connect()` wraps `LoxWs.async_init()` to reset
`self._encryption_handler`'s salt state right before a fresh connection attempt:

```python
async def patched_async_init(self):
    reset_salt_state(getattr(self, "_encryption_handler", None))
    return await original_async_init(self)
```

```python
def reset_salt_state(encryption_handler) -> bool:
    if encryption_handler is None:
        return False
    encryption_handler._salt = ""
    encryption_handler._salt_used_count = 0
    encryption_handler._salt_time_stamp = 0
    return True
```

With `_salt` reset to `""`, `encrypt()`'s very next call takes the `"salt/{salt}/..."`
(fresh session) branch instead of an invalid `nextSalt` continuation. This is registered
in `apply_patches()` alongside (but independent of) the stale-session-close and
write-serialization patches, all three of which now wrap `LoxWs.async_init()` in a chain.

Scope note: this only resets the salt-rotation bookkeeping. The AES key/IV
(`self._key`/`self._iv`) are likewise never regenerated across reconnects, but that's a
separate concern - not addressed here, since nothing observed so far points to it being a
problem (the RSA/AES handshake itself completes successfully on every reconnect; it's
specifically the first *encrypted command after* that handshake which fails).

## Testing

- `reset_salt_state()` unit tests: resets all three fields on a handler with stale state;
  returns `False` (no-op) when passed `None`.
- `test_patch_salt_reset_is_installed_and_idempotent`: the patch installs on
  `LoxWs.async_init` and repeated `apply_patches()` calls don't wrap it again.
- Two end-to-end tests against the real `LxEncryptionHandler`, decrypting the produced
  ciphertext via AES/CBC with the handler's own `_key`/`_iv` to check the actual plaintext
  command framing:
  - `test_reset_salt_state_makes_first_encrypt_use_fresh_salt_branch`: after
    `reset_salt_state()`, the first `encrypt()` call produces plaintext starting with
    `"salt/"`, not `"nextSalt"`.
  - `test_without_reset_a_reconnect_would_use_the_invalid_nextsalt_branch` (control): with
    the same stale state left untouched, the same call produces
    `"nextSalt/stale-salt-from-old-session/..."` - reproducing the original bug to confirm
    the fix actually addresses it, not just a symptom.
- One pre-existing test (`test_patch_websocket_write_serialization_is_installed_and_idempotent`)
  needed the same fix already applied earlier to
  `test_async_init_patch_is_installed_and_idempotent`: since this new patch chains yet
  another wrapper around `LoxWs.async_init`, only the outermost wrapper's marker attribute
  is visible on `LoxWs.async_init` directly, so the test now only checks identity/
  idempotency rather than a specific inner patch's marker.

All 251 tests pass (246 existing + 5 new), run inside the Docker build image. The two
most timing-sensitive test files (`test_http_miniserver_handler.py`,
`test_loxwebsocket_compat.py`) re-run 5x with no failures.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Found and fixed a bug where `LxEncryptionHandler`'s salt-rotation state
  (`_salt`/`_salt_used_count`/`_salt_time_stamp`) persists across reconnects even though
  the Miniserver starts a completely fresh session each time. This causes the first
  encrypted command of every new session (`acquire_token()`'s `getkey2` request) to use an
  invalid `"nextSalt/{old_salt}/..."` continuation referencing a salt from a session the
  Miniserver has already forgotten, which a Miniserver enforcing salt-based replay
  protection correctly rejects with `Code: "401"`.
- This fully explains a `401` observed in production immediately after a successful
  reconnect handshake, with credentials/lockout explicitly ruled out as the cause.

### Why
Reconnect logic elsewhere on this branch (stale-session-close, token-reset,
write-serialization) gets the connection and the auth token into a correct state for a
fresh attempt, but none of it touches `LxEncryptionHandler`'s own salt bookkeeping, which
lives on the same long-lived, never-recreated object across reconnects. Without this fix,
every reconnect's first command was doomed to a spurious rejection regardless of how
correct the credentials or how healthy the connection otherwise was.

### Changes
- `src/loxmqttrelay/loxwebsocket_compat.py`: new patch
  `_patch_encryption_handler_resets_salt_on_connect()` (plus the underlying
  `reset_salt_state()` helper) resets `self._encryption_handler`'s salt state right before
  every fresh `async_init()` connection attempt; registered in `apply_patches()`.
- `tests/test_loxwebsocket_compat.py`: 5 new tests, including two end-to-end tests that
  decrypt real AES ciphertext to directly verify the plaintext command framing
  (`"salt/..."` vs. the invalid `"nextSalt/..."`) before and after the fix; one existing
  test adjusted for the same chained-wrapper marker-visibility issue fixed previously for
  a different patch.

### Test Plan
- [x] `pytest` (246 existing + 5 new tests, 251 total) passes, run inside the Docker build
      image; the two most timing-sensitive test files re-run 5x for flakiness with no
      failures.
- [x] `test_reset_salt_state_makes_first_encrypt_use_fresh_salt_branch`: confirms the fix
      produces valid `"salt/..."` framing.
- [x] `test_without_reset_a_reconnect_would_use_the_invalid_nextsalt_branch`: control test
      reproducing the original bug's framing without the fix, confirming the fix targets
      the actual root cause.

### Notes for Reviewers
- Scoped narrowly to salt-rotation state; the AES key/IV are a separate, never-reset piece
  of state on the same handler, but nothing observed so far implicates it (the handshake
  itself always succeeds) - left alone rather than speculatively "fixed".
- This branch is based on `fix/websocket-reliability` (not `main`) since it directly
  extends `loxwebsocket_compat.py` as it exists there; it merges back into
  `fix/websocket-reliability` rather than being reviewed independently against `main`.
