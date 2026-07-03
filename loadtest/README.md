# Load Test Setup for loxMqttRelay

Builds an isolated Docker environment with three components:

- **mosquitto** – a real MQTT broker (eclipse-mosquitto:2)
- **loxmqttrelay** – the relay itself, built from the Dockerfile in the project root
- **miniserver-mock** – a minimal HTTP server that emulates the Loxone Miniserver endpoints
  (`/dev/sps/io/{topic}/{value}`) and logs every incoming request
- **loadtest-runner** – generates load and verifies that every message sent actually arrives

```
UDP client → [loxmqttrelay] → MQTT publish → [mosquitto] → MQTT subscribe → [loxmqttrelay] → HTTP GET → [miniserver-mock]
```

## Starting the setup

```bash
cd loadtest
docker compose build
docker compose up -d mosquitto miniserver-mock loxmqttrelay
```

## Running the load test

```bash
docker compose run --rm loadtest-runner
```

Configurable via environment variables (set before the command, or in `.env`):

| Variable       | Default | Meaning                                                  |
|----------------|---------|-----------------------------------------------------------|
| `NUM_MESSAGES` | 5000    | Number of messages per scenario                           |
| `NUM_TOPICS`   | 50      | Number of distinct topics to spread messages across       |
| `SCENARIOS`    | 1,2,3   | Which scenarios to run (comma-separated)                  |

Example for higher load:

```bash
NUM_MESSAGES=50000 NUM_TOPICS=200 docker compose run --rm loadtest-runner
```

## Scenarios

1. **`udp_to_mqtt`** – messages are sent to the relay via UDP (topic outside the whitelist range),
   arrival is checked directly at the MQTT broker. Tests only the relay's UDP intake.
2. **`mqtt_to_http`** – messages are published directly to the broker (relay subscription
   `loadtest/cmd/#`), arrival is checked at `miniserver-mock` via HTTP. Tests only the forwarding
   path towards the Miniserver.
3. **`e2e_udp_to_http`** – the full chain: UDP → relay → MQTT → relay → HTTP → miniserver-mock.
   Matches the real-world path of a device sending to the relay via UDP.

For each scenario, every message gets a unique ID, and at the end the set of sent IDs is compared
against the set of received IDs (set difference = loss). The runner exits with code `0` if no
message was lost in any scenario, `1` otherwise.

## Simulating more realistic load scenarios

`miniserver-mock` can simulate artificial latency and a failure rate, to check how the relay
behaves against a slow/unreliable real Miniserver:

```bash
MOCK_DELAY_MS=200 NUM_MESSAGES=20000 docker compose up -d --build miniserver-mock
docker compose run --rm loadtest-runner
```

`MOCK_FAIL_RATE=0.05`, for example, simulates 5% HTTP 5xx responses from the Miniserver.

`loadtest/relay-config/config.toml` → `miniserver_max_parallel_connections` controls how many HTTP
requests the relay sends to the Miniserver in parallel (default here: 20; production default in
the project: 5). With artificial latency enabled, this makes the relationship between
parallelism, throughput, and loss rate clearly visible.

## Known limits the load test makes visible

- **UDP is connectionless.** At very high burst rates, packets can be dropped at the
  operating-system/network level before the relay ever sees them — this is inherent UDP behavior,
  not a relay bug. Scenarios 1/3 make this visible; if loss occurs there, a lower send rate
  (instead of bursts) usually brings loss down to 0.
- **No retry on HTTP forwarding.** `http_miniserver_handler.py` only logs timeouts/errors when
  sending to the Miniserver, it never retries. Under sustained load against a slow Miniserver
  (high `MOCK_DELAY_MS` combined with a low `miniserver_max_parallel_connections` and the 10s
  timeout), messages can therefore be lost permanently. Scenarios 2/3 with `MOCK_DELAY_MS`/
  `MOCK_FAIL_RATE` set reproduce this deliberately.

## Cleaning up

```bash
docker compose down -v
```
