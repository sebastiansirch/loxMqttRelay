# Bug Analysis & Fix: Topics Silently and Permanently Stop Forwarding

Branch: `feature/defensive-whitelist-sync`

## Starting Question

A user reported observing, after 1-2 days of uptime, that messages to one MQTT topic ("Topic A")
stopped arriving at the Loxone Miniserver entirely, while messages to other topics ("Topic B",
"Topic C") kept working normally. The question: is there a bug that can cause this - one specific
topic permanently going dark while others keep working - and if so, where?

This was analyzed independently of the load test work in `loadtest/` (see
`docs/loadtest-http-session-fix.md`) - the symptom (selective, permanent, silent, no error logged)
does not match either of the load-related bugs found there (both caused *random*, load-dependent
loss across all topics, not a deterministic, permanent loss of one specific topic).

## Root Cause

`src/loxmqttrelay/main.py`, `handle_miniserver_sync()` (called on relay startup, and whenever the
Miniserver publishes to `{base_topic}miniserverevent/startup` - typically configured to fire on
the Miniserver's own reboot):

```python
async def handle_miniserver_sync(self):
    if not global_config.miniserver.sync_with_miniserver:
        return
    initial_whitelist = global_config.topics.topic_whitelist.copy()
    try:
        inputs = await sync_miniserver_whitelist()
        global_config.update_config(ConfigSection.TOPICS, {'topic_whitelist': inputs})
        self.miniserver_data_processor.update_topic_whitelist(list(inputs))
        logger.info("Whitelist updated from miniserver configuration")
    except Exception as e:
        ...  # rolls back to initial_whitelist
```

`update_config(..., {'topic_whitelist': inputs})` **fully replaces** `topic_whitelist` with
whatever `sync_miniserver_whitelist()` returned (default `list_mode="set"` in
`Config.update_config`, `src/loxmqttrelay/config.py`) - it does not merge with the previous list.
`update_config()` also calls `self.save_config()` at the end, writing the new whitelist to
`config.toml` on disk.

The code only distinguishes "sync succeeded" (no exception) from "sync failed" (exception,
whitelist rolled back) - it has no notion of "sync succeeded but returned an incomplete list". If
a sync that completes without error returns a list missing Topic A's virtual input name (while
still containing B and C), the whitelist is fully overwritten with that incomplete list. Topic A
is now permanently excluded from forwarding:

- **Silently** - no error, no warning is logged; "sync succeeded" looks identical whether the
  list was complete or not.
- **Permanently** - the incomplete whitelist is persisted to `config.toml`, so it survives a relay
  restart. Only another (correct) sync can fix it.

This matches the reported symptom exactly: one topic (A) stops working, others (B, C) keep
working, and it looks "permanent" because it is - it's been written to disk.

### Why could a successful sync return an incomplete list?

`src/loxmqttrelay/miniserver_sync.py`, `load_miniserver_config()`:

```python
filelist = sorted(set(_CONFIG_FILE_PATTERN.findall(listing)))
filename = filelist[-1]   # "newest" by filename
```

The code comment itself flags the underlying assumption:

> There is no fixed-name pointer to the active config (confirmed by Loxone), so we list and pick
> the newest.

If the Miniserver's `/prog` directory contains more than one config archive - a stale backup, an
aborted/reverted upload, anything with a timestamp later than the currently active program - the
"newest by filename" heuristic can pick a config snapshot that is not actually the one currently
running, and that snapshot may be missing a virtual input that the active program has (or vice
versa). This is a plausible, real-world trigger requiring no XML corruption or encoding issue at
all - just more than one archive present on the Miniserver, which is common after any config
change.

A second, independent contributing factor: `extract_inputs()` (same file) is deliberately built to
tolerate malformed Loxone configs rather than fail on them - the lxml fallback uses
`etree.XMLParser(recover=True)`, and the pygixml fast path decodes with
`errors="replace"`. Both are correct choices for robustness against Loxone's known malformed-XML
quirks, but as a side effect a single malformed or oddly-encoded attribute on exactly one virtual
input can cause that one input's title to be silently skipped (`if title:` guards against `None`/
empty) while the rest of the document parses normally - again explaining why exactly one topic,
and not all of them, could go missing from an otherwise-successful sync.

### Preconditions

This bug only manifests if:
- `sync_with_miniserver = true` (the shipped `default_config.toml` sets this to `false`; the
  dataclass default, and the example in the README, is `true`)
- the resync is actually triggered again after the initial, correct startup sync - in practice,
  the Miniserver publishing to `miniserverevent/startup` on its own reboot, which is a very
  plausible event over 1-2 days of uptime (scheduled reboot, firmware update, power blip).

If `sync_with_miniserver = false`, the whitelist is static and this mechanism does not apply; a
topic going permanently dark in that configuration would point to something else (e.g. the
Miniserver rejecting that specific virtual input's URL because it was renamed/removed on the
Miniserver side - visible as `Miniserver returned <code>` in the relay log, not a relay bug).

## Fix

Two new `[miniserver]` options in `config.toml`, both implemented in
`src/loxmqttrelay/main.py`:

### `whitelist_sync_defensive` (default `true`)

```python
list_mode = "add" if defensive else "set"
global_config.update_config(ConfigSection.TOPICS, {'topic_whitelist': inputs}, list_mode=list_mode)
merged_whitelist = list(global_config.topics.topic_whitelist)
self.miniserver_data_processor.update_topic_whitelist(merged_whitelist)
```

When enabled, a sync only **adds** newly discovered topics to the whitelist (reusing
`Config.update_config`'s existing, already-tested `list_mode="add"` set-union logic) instead of
replacing it outright. The whitelist can then only grow over the relay's lifetime. This directly
matches the tradeoff requested when this fix was scoped: a few extra whitelisted topics are
harmless - the Miniserver simply rejects HTTP requests for virtual inputs it doesn't have -
whereas silently and permanently losing a topic is not acceptable. Setting
`whitelist_sync_defensive = false` restores the previous replace-on-sync behavior, e.g. for users
who rely on sync to also *remove* stale topics automatically.

### `whitelist_sync_interval_seconds` (default `0`, disabled)

```python
async def periodic_miniserver_sync(self):
    interval = global_config.miniserver.whitelist_sync_interval_seconds
    if interval <= 0:
        return
    if not global_config.miniserver.whitelist_sync_defensive:
        logger.warning(
            "Periodic miniserver sync is enabled with whitelist_sync_defensive=False: "
            "each periodic sync fully replaces the whitelist, so a transiently incomplete "
            "sync can silently drop topics again on the next interval. Consider enabling "
            "whitelist_sync_defensive."
        )
    logger.info(f"Periodic miniserver whitelist sync enabled every {interval}s")
    while True:
        await asyncio.sleep(interval)
        logger.info("Periodic miniserver sync triggered")
        await self.handle_miniserver_sync()
```

Started as a background task in `MQTTRelay.main()` alongside the UDP server. When set to a value
greater than `0`, the whitelist sync re-runs on that fixed interval, independent of whether the
Miniserver ever publishes `miniserverevent/startup`. This closes two gaps at once:

- A relay no longer strictly depends on the Miniserver-side startup event being configured at all,
  or being received (e.g. if the relay happened to be briefly disconnected from the broker at the
  exact moment the Miniserver published it).
- Combined with `whitelist_sync_defensive`, a later periodic sync will **self-heal** an earlier
  incomplete one: if a sync at some point picks the wrong/stale config file and misses Topic A,
  the next periodic sync (once the Miniserver's `/prog` directory reflects the correct state
  again) adds Topic A back - without ever having removed it in the meantime, since defensive mode
  never removes entries in the first place.

If a deployment enables periodic sync but explicitly disables defensive mode, a startup warning is
logged, since that specific combination reintroduces the original risk on every interval instead
of only on Miniserver-reboot events.

## Verification

Unit tests (`tests/test_mqtt_relay.py`), run in the Docker build image (the project requires
Python 3.14 + the compiled Rust extension):

- `test_whitelist_loading_sequence` (updated): with the new default
  (`whitelist_sync_defensive=true`), a sync starting from `{initial_topic1, initial_topic2}` that
  returns `[synced_topic1, synced_topic2]` results in the union of all four - none of the initial
  topics are lost.
- `test_whitelist_loading_sequence_non_defensive_replaces` (new): with
  `whitelist_sync_defensive=false`, the same sync results in exactly `{synced_topic1,
  synced_topic2}` - confirms the legacy replace behavior is intact and selectable.
- `test_whitelist_sync_on_miniserver_startup` (updated): a second sync triggered via the
  `miniserverevent/startup` MQTT message still contains the originally synced topics afterwards
  (defensive merge across repeated syncs).
- `test_periodic_sync_disabled_by_default` (new): `whitelist_sync_interval_seconds=0` means
  `periodic_miniserver_sync()` returns immediately without ever calling the sync.
- `test_periodic_sync_runs_repeatedly` (new): with a short interval, the sync is invoked more than
  once over time, not just once at startup.
- `test_periodic_sync_warns_on_risky_combo` (new): enabling periodic sync with
  `whitelist_sync_defensive=false` logs the expected warning.

All 202 tests (198 existing + 4 new) pass.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Root-caused a report of one specific MQTT topic permanently and silently no longer being
  forwarded to the Miniserver after 1-2 days of uptime, while other topics kept working: each
  miniserver whitelist sync (`handle_miniserver_sync()` in `main.py`) fully **replaces**
  `topic_whitelist` with whatever `sync_miniserver_whitelist()` returns and persists it to
  `config.toml` - a sync that succeeds but returns an incomplete list (e.g. because
  `load_miniserver_config()` picks the wrong config archive from the Miniserver's `/prog`
  directory, a risk the code's own comments already acknowledge, or a single malformed/oddly
  encoded virtual-input title is silently skipped by the deliberately tolerant XML extraction) then
  silently and permanently drops any topic missing from that snapshot.
- Added `whitelist_sync_defensive` (`[miniserver]`, default `true`): a sync now only **adds**
  newly discovered topics instead of replacing the whitelist, so it can only grow. A few extra
  whitelisted topics are harmless (the Miniserver just rejects requests for inputs it doesn't
  have); silently losing a topic forever is not. `false` restores the previous replace-on-sync
  behavior.
- Added `whitelist_sync_interval_seconds` (`[miniserver]`, default `0`/disabled): re-runs the
  whitelist sync on a fixed interval in addition to sync-on-startup and
  sync-on-`miniserverevent/startup`, so a missed or unconfigured Miniserver startup event no
  longer requires a relay restart to pick up Miniserver-side changes. Combined with defensive
  mode, a later periodic sync self-heals an earlier incomplete one. A startup warning is logged if
  periodic sync is enabled without defensive mode, since that combination can still drop topics on
  every interval.

### Why
A user reported exactly this symptom in production (one topic dark, others fine, after ~1-2 days)
and asked whether the codebase could produce it. Tracing the whitelist-sync code path confirms a
concrete, reproducible mechanism that matches every detail of the report (selective, silent,
permanent, appears after some runtime). This PR makes the whitelist sync fail safe in the
direction the reporter explicitly asked for: prefer a whitelist that's occasionally too broad over
one that can silently shrink.

### Changes
- `src/loxmqttrelay/config.py`: new `MiniserverConfig` fields `whitelist_sync_defensive` (default
  `true`) and `whitelist_sync_interval_seconds` (default `0`).
- `src/loxmqttrelay/main.py`: `handle_miniserver_sync()` uses `list_mode="add"` (merge) instead of
  `"set"` (replace) when defensive mode is on; new `periodic_miniserver_sync()` background task,
  started from `MQTTRelay.main()`.
- `config/default_config.toml`: documents and sets both new options.
- `README.md`: new "Defensive whitelist sync" and "Periodic Sync" subsections under Miniserver
  Integration.
- `tests/test_mqtt_relay.py`: 2 existing tests updated for the new default (merge instead of
  replace), 4 new tests for non-defensive mode, periodic sync enabled/disabled, and the risky-combo
  warning.

### Test Plan
- [x] `pytest` (198 existing + 4 new tests, 202 total) passes, run inside the Docker build image
      (the project requires Python 3.14 + the compiled Rust extension).
- [x] `test_whitelist_loading_sequence`: confirms the new default merges instead of replacing.
- [x] `test_whitelist_loading_sequence_non_defensive_replaces`: confirms the legacy replace
      behavior remains available and correct via `whitelist_sync_defensive=false`.
- [x] `test_periodic_sync_disabled_by_default` / `test_periodic_sync_runs_repeatedly`: confirms
      the interval gate and repeated execution.
- [x] `test_periodic_sync_warns_on_risky_combo`: confirms the startup warning for the
      periodic-sync-without-defensive-mode combination.

### Notes for Reviewers
- This is a behavior-**changing** default (`whitelist_sync_defensive` defaults to `true`, whereas
  the previous, only behavior was an unconditional replace). This is intentional: the previous
  behavior is the bug being fixed, and there is no legitimate use case for the whitelist silently
  shrinking due to a transient/incomplete sync. Anyone who relies on sync to prune stale topics can
  opt back into the old behavior via `whitelist_sync_defensive = false`.
- Periodic sync defaults to `0` (disabled/off) - purely additive, no behavior change unless a
  deployment opts in by setting an interval.
- This work is independent of, and unrelated to, the load-test-driven fixes in
  `docs/loadtest-http-session-fix.md` on the `fix/miniserver-http-session-reuse-and-basetopic-warning`
  branch - both branches are based on `main` and can be reviewed/merged independently.
