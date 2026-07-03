# Load Test & Fix: Message Loss Under Load (HTTP Forwarding to the Miniserver)

Branch: `fix/miniserver-http-session-reuse-and-basetopic-warning`

## Starting Question

Verify whether loxMqttRelay reliably forwards all messages from MQTT to the
Loxone Miniserver (and from UDP to MQTT) under higher load, without losing
any. To answer this, a Docker-based load test setup (mosquitto + Miniserver
mock + relay) was built and run against the actual relay code.

Result: **No** — messages were in fact lost under load, for one concrete,
reproducible reason (Bug 1). A follow-up question - "is the missing retry
behavior also fixed?" - surfaced a closely related second gap (Bug 2): even
after Bug 1 was fixed, a single failed HTTP attempt (timeout, connection
error, 5xx) was still permanently lost, with no retry at all. While building
the test harness, a third, independent issue was also discovered (Bug 3),
which can likewise cause silent message loss.

---

## Bug 1: New TCP connection per HTTP request → port exhaustion → loss

### Symptom

Under sustained load (reproducible from roughly 25,000–30,000 messages sent
in a short time window), messages that should have been forwarded to the
Miniserver via HTTP **never arrived** there — with no indication to the
sender (the MQTT publisher) that anything had gone wrong. Measured loss rate
at 30,000 messages: 5.9–17.4% (depending on the number of parallel topics /
timing).

The relay log showed errors like this at the same time:

```
ERROR [loxmqttrelay.http_miniserver_handler] Error 503: Connection error sending
loadtest/cmd/sensor140 (as loadtest_cmd_sensor140)=s3-29940 to Miniserver
(URL: http://miniserver-mock:8080/dev/sps/io/loadtest_cmd_sensor140/s3-29940):
Cannot assign requested address
```

`Cannot assign requested address` is `EADDRNOTAVAIL` — the operating system
has run out of free local (ephemeral) ports to open a new outbound TCP
connection.

### Root Cause

`src/loxmqttrelay/http_miniserver_handler.py`, method
`send_to_miniserver_via_http`, opened a **brand new** `aiohttp.ClientSession`
for **every single message**:

```python
async def send_to_miniserver_via_http(self, topic, normalized_topic, value):
    ...
    async with aiohttp.ClientSession(auth=self.auth, timeout=self.timeout) as session:
        ...
        async with self.connection_semaphore:
            async with session.get(url) as resp:
                ...
```

An `aiohttp.ClientSession` owns its own connection pool (`TCPConnector`). If
a new session is created per request and immediately closed again at the end
of the `async with` block, **no** connection is ever reused — every single
HTTP request opens a brand new TCP connection and tears it down again right
after. As is normal for TCP, the resulting sockets linger in the
`TIME_WAIT` state for a while (Linux default ~60s) and keep occupying a
local port during that time. At a high enough message rate, the available
ephemeral port range gets exhausted faster than ports free up via `TIME_WAIT`
expiry → `EADDRNOTAVAIL`.

The `miniserver_max_parallel_connections` semaphore limits how many requests
are *in flight at the same time*, but it does not prevent connection churn
over time — given enough total volume, the problem eventually occurs
regardless of the semaphore value.

Crucially, this is what turns the failure into actual data loss: none of the
`except` branches in `send_to_miniserver_via_http` retry. A failure is only
logged (`logger.error(...)`) and the function returns (implicitly `None`).
A failed request is therefore permanently lost, with neither the MQTT
publisher nor any other part of the system ever finding out.

### Fix

`HttpMiniserverHandler` now lazily creates **one** `aiohttp.ClientSession` on
the first request and keeps it open for the lifetime of the process
(`_get_session()`, guarded by an `asyncio.Lock` against duplicate creation
when several first requests arrive concurrently). All subsequent requests
reuse that same session and thus benefit from aiohttp's built-in keep-alive
connection pool — TCP connections are reused instead of being re-established
for every message. The existing semaphore that limits parallel requests is
left unchanged.

```python
async def _get_session(self) -> aiohttp.ClientSession:
    if self._session is None or self._session.closed:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(auth=self.auth, timeout=self.timeout)
    return self._session
```

### Verification

Using the same load test setup that originally uncovered the bug:

| Run | Messages | Loss before | Loss after |
|---|---|---|---|
| Scenario 2 (MQTT → HTTP) | 30,000 | 5.9% – 17.4% (reproduced twice) | 0% |
| Scenario 2 (MQTT → HTTP) | 50,000 | — | 0% |
| Scenario 2, run again right after (150,000 cumulative in the same session) | 50,000 | — | 0% |

Relay log after the fix: `grep -c "Cannot assign requested address"` → `0`,
`grep -c "ERROR"` → `0` across the whole test run.

---

## Bug 2: No retry on transient HTTP failures

### Symptom

After Bug 1 was fixed, a follow-up question was: does the relay retry a
failed request at all, or is a single failed attempt still permanently lost?
To answer it, `miniserver-mock` was configured to fail 10% of requests with
HTTP 503 (`MOCK_FAIL_RATE=0.1`, simulating a real Miniserver that is
momentarily overloaded or restarting), independent of Bug 1's port
exhaustion. Sending 5,000 messages through `mqtt_to_http` lost exactly
**10.220%** of them - matching the injected failure rate 1:1, i.e. every
single failed attempt was dropped for good.

### Root Cause

Every `except` branch in `send_to_miniserver_via_http` (`asyncio.TimeoutError`,
`OSError`, `aiohttp.ClientError`, generic `Exception`), and the non-200
response-status branch, only logged the failure and returned - there was no
retry logic anywhere on this path. A Miniserver that is briefly unreachable
(reboot, short network hiccup, momentary overload) causes real, permanent
message loss, not just under extreme load but under perfectly ordinary,
everyday conditions.

### Fix

`send_to_miniserver_via_http` now retries transient failures - request
timeouts, connection errors (`OSError`), `aiohttp.ClientError`, and HTTP 5xx
responses - up to `miniserver_http_retry_attempts` times (default `3`, i.e.
up to 2 retries), with exponential backoff between attempts
(`miniserver_http_retry_backoff_seconds`, default `0.5` → waits of 0.5s, 1s,
2s, ... between attempts). Both are configurable in `[miniserver]` in
`config.toml`; setting `miniserver_http_retry_attempts = 1` disables
retrying entirely. HTTP 4xx responses and unexpected (`Exception`) errors are
**not** retried, since retrying a client error or an unknown failure mode
would not help and could mask a real configuration problem.

```python
for attempt in range(1, max_attempts + 1):
    is_last_attempt = attempt == max_attempts
    try:
        async with self.connection_semaphore:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return { 'code': resp.status }
                if resp.status < 500 or is_last_attempt:
                    logger.warning(...)
                    return { 'code': resp.status }
                logger.warning(f"... retrying ({attempt}/{max_attempts})")
    except (asyncio.TimeoutError, OSError, aiohttp.ClientError) as e:
        if is_last_attempt:
            logger.error(...)
            return
        logger.warning(f"... retrying ({attempt}/{max_attempts}): {e}")
    except Exception as e:
        logger.error(...)
        return

    await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
```

The semaphore that limits parallel requests is only held per attempt, not
across the backoff sleep, so a retrying message does not block other
messages from being sent in the meantime.

### Verification

Same `MOCK_FAIL_RATE=0.1` setup as above, after the fix:

```
[FAIL] mqtt_to_http: sent=5000 received=4995 lost=5 (0.100%)
```

0.100% loss is exactly what the retry math predicts for 3 independent
attempts at a 10% per-attempt failure rate: `0.1³ = 0.001 = 0.1%` - a **100x**
reduction in loss compared to no retry, for a genuinely transient failure
mode. (This residual 0.1% is expected and correct: it is the probability of
*all three* attempts failing independently; a real Miniserver's failures are
rarely that persistent, but if higher reliability is needed,
`miniserver_http_retry_attempts` can be increased.) The relay log confirms
the retry behavior directly, e.g.:

```
WARNING [loxmqttrelay.http_miniserver_handler] Miniserver returned 503 for topic
loadtest/cmd/sensor0 (URL: http://miniserver-mock:8080/dev/sps/io/loadtest_cmd_sensor0/s2-0),
retrying (1/3)
```

---

## Bug 3: `base_topic` overlap causes silent message loss

### Symptom

When the load test was first set up with `base_topic = "loadtest/"` and
`subscriptions = ["loadtest/cmd/#"]`, **0 out of 1000** test messages arrived
at the Miniserver mock — with no load at all, and no error in the log.

### Root Cause

The Rust message dispatcher (`src/lib.rs`, `handle_mqtt_message`, line 461)
treats every received message whose topic starts with `base_topic` as a
reserved control message:

```rust
if topic.starts_with(&self.base_topic) {
    if topic == topics.miniserver_startup_topic { ... }
    else if topic == topics.config_get_topic { ... }
    else if topic == topics.config_set_topic || ... { ... }
    else if topic == topics.config_update_topic || topic == topics.config_restart_topic { ... }
    // no else branch - if none of the above match, nothing happens at all
}
else {
    let _ = self.process_data(py, &topic, &message);  // regular data message -> Miniserver
}
```

If `base_topic` is a prefix of a data subscription topic (e.g.
`base_topic="loadtest/"` with subscription `"loadtest/cmd/#"`), every message
on that topic falls into the `if` branch but matches none of the known
control topics — **nothing** happens: no forwarding, no log, no error. The
shipped `default_config.toml` avoids this by convention
(`base_topic="test/"` vs. `subscriptions=["topic3"]`), but nothing in the
code prevents or reports this kind of misconfiguration.

### Fix

`src/loxmqttrelay/main.py`: new function `warn_on_base_topic_overlap()`,
called on startup in `connect_and_subscribe_mqtt()` before subscriptions are
registered with the broker. For each subscription it checks whether its
fixed (non-wildcard) prefix falls into the `base_topic` namespace (or vice
versa), and if so logs a clear warning explaining the issue. The behavior
itself (the reserved namespace for control topics) was deliberately **left
unchanged**, to avoid a silent behavior change for existing deployments —
the warning only makes the misconfiguration visible.

---

## Load Test Harness (`loadtest/`)

### Architecture

```
UDP client → [loxmqttrelay] → MQTT publish → [mosquitto]
                                                   │
                                          MQTT subscribe
                                                   ▼
                                            [loxmqttrelay]
                                                   │
                                              HTTP GET
                                                   ▼
                                          [miniserver-mock]
```

Components (`loadtest/docker-compose.yml`):

- **mosquitto** — a real MQTT broker (`eclipse-mosquitto:2`)
- **loxmqttrelay** — built from the project's actual `Dockerfile`, no mocking inside the relay itself
- **miniserver-mock** — a custom aiohttp server (`loadtest/miniserver_mock/app.py`) that emulates
  the Loxone endpoints (`/dev/sps/io/{topic}/{value}`) and logs every incoming request (topic,
  value, timestamp); can simulate artificial latency (`MOCK_DELAY_MS`) and failure rate
  (`MOCK_FAIL_RATE`)
- **loadtest-runner** — generates load and compares sent vs. received messages (unique ID per
  message, set difference = loss)

### Scenarios

1. **`udp_to_mqtt`** — tests the relay's UDP intake in isolation (topic outside the relay's
   subscriptions, so only the UDP→MQTT path is measured)
2. **`mqtt_to_http`** — tests the forwarding path to the Miniserver in isolation (messages are
   published directly to the broker, bypassing UDP)
3. **`e2e_udp_to_http`** — the full chain, exactly like a real device: UDP → relay → MQTT → relay →
   HTTP → Miniserver

For each scenario, every message gets a unique ID, and at the end the set of sent IDs is compared
against the set of actually received IDs. The runner exits with code `0` if no message was lost,
`1` otherwise.

### Running it

```bash
cd loadtest
docker compose build
docker compose up -d mosquitto miniserver-mock loxmqttrelay
docker compose run --rm loadtest-runner
```

Configurable via environment variables: `NUM_MESSAGES` (default 5000), `NUM_TOPICS` (default 50),
`SCENARIOS` (default `1,2,3`). See `loadtest/README.md` for details, including how to simulate a
slow/unreliable Miniserver via `MOCK_DELAY_MS`/`MOCK_FAIL_RATE`.

### Test results at a glance

| Scenario | Messages | Loss (before fix) | Loss (after fix) |
|---|---|---|---|
| `udp_to_mqtt` | 1,000 – 50,000 | 0% (never affected) | 0% |
| `mqtt_to_http` | 1,000 – 20,000 | 0% | 0% |
| `mqtt_to_http` | 30,000 | 5.9% – 17.4% | 0% |
| `mqtt_to_http` | 50,000 (twice in a row, 150,000 total) | not tested | 0% |
| `e2e_udp_to_http` | 30,000 | 17.4% | 0% |
| `e2e_udp_to_http` | 50,000 | not tested | 0% |
| `mqtt_to_http` w/ `MOCK_FAIL_RATE=0.1` (Bug 2) | 5,000 | 10.220% (no retry) | 0.100% (3 attempts) |

The `udp_to_mqtt` path (plain UDP intake → MQTT publish) was never affected at any point — the
loss occurred exclusively on the HTTP forwarding path to the Miniserver, consistent with Bug 1 and
Bug 2. The 0.100% remaining after the retry fix is not a bug: it is the expected probability of
all 3 attempts failing independently at a 10% simulated failure rate (`0.1³`), reproducing exactly.

---

## Pull Request Description

> The section below can be used as-is as the PR description.

### Summary
- Added a load test setup (`loadtest/`): mosquitto + Miniserver HTTP mock + relay in Docker
  Compose, with three scenarios (UDP→MQTT, MQTT→HTTP, end-to-end) that diff sent vs. received
  messages by unique message ID.
- Found and fixed a bug: `send_to_miniserver_via_http` opened a new `aiohttp.ClientSession` (i.e.
  a new TCP connection) per message instead of reusing one; under load (~25k+ messages) this
  caused port exhaustion (`EADDRNOTAVAIL`), and since there is no retry, permanent, silent message
  loss (5.9–17.4% measured at 30k messages). Fix: one shared, lazily created `ClientSession` per
  handler instance.
- Found and fixed a second bug: a single failed HTTP attempt to the Miniserver (timeout,
  connection error, 5xx response) was never retried - permanent loss even under everyday
  conditions (Miniserver reboot, brief network hiccup), not just under extreme load. Reproduced
  with a 10% simulated failure rate → 10.220% message loss. Fix: retry transient failures up to
  `miniserver_http_retry_attempts` times (default 3) with exponential backoff
  (`miniserver_http_retry_backoff_seconds`, default 0.5s), both configurable in `config.toml`.
  HTTP 4xx and unexpected errors are not retried. Verified loss drops to 0.100% at the same 10%
  simulated failure rate - matching the expected `0.1³` probability of all 3 attempts failing.
- Found and fixed a third bug: messages on subscriptions that fall inside the `base_topic`
  namespace were silently dropped by the Rust dispatcher (treated as an unrecognized control
  topic), with no log or error. Fix: a startup warning when a subscription overlaps with
  `base_topic` (the behavior itself was deliberately left unchanged to avoid breaking existing
  deployments).

### Why
The question to answer was whether loxMqttRelay reliably forwards all requests to the Miniserver
under higher load. The answer before this fix: no — reproducible message loss starting at roughly
25,000–30,000 messages sent in a short time window, caused by the lack of HTTP connection reuse,
compounded by the complete absence of retries for any kind of transient HTTP failure.

### Changes
- `src/loxmqttrelay/http_miniserver_handler.py`: shared, reused `ClientSession` instead of a new
  session per request; retry with exponential backoff for transient failures (timeout, connection
  error, 5xx).
- `src/loxmqttrelay/config.py`, `config/default_config.toml`: new `[miniserver]` options
  `miniserver_http_retry_attempts` (default `3`) and `miniserver_http_retry_backoff_seconds`
  (default `0.5`).
- `src/loxmqttrelay/main.py`: `warn_on_base_topic_overlap()` at connection setup time.
- `tests/test_http_miniserver_handler.py`, `tests/test_mqtt_relay.py`: tests for all three fixes.
- `README.md`: documents the new retry configuration options.
- `loadtest/`: new, standalone Docker Compose load test setup (not part of the regular test
  suite/CI, run manually).

### Test Plan
- [x] `pytest` (204 original + 10 new tests, 214 total) passes, run inside the Docker build image
      (the project requires Python 3.14 + the compiled Rust extension).
- [x] Load test before the fix: `mqtt_to_http`/`e2e_udp_to_http` at 30,000 messages show
      5.9–17.4% loss, `Cannot assign requested address` errors in the relay log.
- [x] Load test after the connection-reuse fix: 50,000 messages, twice in a row with no restart
      in between (150,000 total), 0% loss across all three scenarios; no errors in the relay log.
- [x] `udp_to_mqtt` scenario as a control: no loss at any point (confirms the connection-reuse
      problem was specific to the HTTP forwarding path).
- [x] Load test with `MOCK_FAIL_RATE=0.1` before the retry fix: 10.220% loss at 5,000 messages
      (matches the injected failure rate 1:1 - confirms zero retries existed).
- [x] Load test with `MOCK_FAIL_RATE=0.1` after the retry fix: 0.100% loss at 5,000 messages
      (matches the expected `0.1³` probability for 3 independent attempts); relay log shows
      `retrying (n/3)` warnings for each recovered attempt.

### Notes for Reviewers
- The `base_topic` fix changes **no** runtime behavior, only visibility (a log warning) —
  deliberately minimally invasive, since actually changing behavior (e.g. forwarding overlapping
  topics anyway) could surprise existing deployments.
- Retries only cover transient failure modes (timeout, connection error, 5xx). HTTP 4xx responses
  and unexpected exceptions are intentionally not retried, since retrying them would not help and
  could mask a real configuration problem.
- `loadtest/` is a standalone, manually run setup (its own `docker-compose.yml`), not wired into
  the regular CI/test suite.
