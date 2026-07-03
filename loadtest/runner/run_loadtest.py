#!/usr/bin/env python3
import os
import sys
import time

import common
import scenario1_udp_to_mqtt as s1
import scenario2_mqtt_to_http as s2
import scenario3_e2e_udp_to_http as s3

NUM_MESSAGES = int(os.environ.get("NUM_MESSAGES", "5000"))
NUM_TOPICS = int(os.environ.get("NUM_TOPICS", "50"))
SCENARIOS = os.environ.get("SCENARIOS", "1,2,3")


def wait_mock_ready(timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            common.mock_stats()
            return True
        except Exception:
            time.sleep(1)
    return False


def print_result(r: dict) -> None:
    status = "PASS" if r["lost"] == 0 else "FAIL"
    rate = f"{r['send_rate_msg_s']} msg/s" if r["send_rate_msg_s"] else "n/a"
    print(
        f"[{status}] {r['scenario']}: sent={r['sent']} received={r['received']} "
        f"lost={r['lost']} ({r['loss_pct']:.3f}%) send_rate={rate}"
    )
    if r["lost"]:
        print(f"        sample lost values: {r['sample_lost']}")


def main() -> None:
    print(f"Config: NUM_MESSAGES={NUM_MESSAGES} NUM_TOPICS={NUM_TOPICS} SCENARIOS={SCENARIOS}")

    print("Waiting for miniserver-mock to accept connections ...")
    if not wait_mock_ready():
        print("ERROR: miniserver-mock not reachable")
        sys.exit(2)

    print("Waiting for full relay pipeline (UDP -> MQTT -> subscription) to come up ...")
    if not common.wait_for_pipeline_ready():
        print("ERROR: relay pipeline did not become ready in time")
        sys.exit(2)
    print("Pipeline ready.\n")

    wanted = {s.strip() for s in SCENARIOS.split(",") if s.strip()}
    results = []

    if "1" in wanted:
        print("=== Scenario 1: UDP -> MQTT (relay ingestion) ===")
        r = s1.run(NUM_MESSAGES, NUM_TOPICS)
        print_result(r)
        results.append(r)

    if "2" in wanted:
        print("\n=== Scenario 2: MQTT -> HTTP (relay forwarding to Miniserver mock) ===")
        r = s2.run(NUM_MESSAGES, NUM_TOPICS)
        print_result(r)
        results.append(r)

    if "3" in wanted:
        print("\n=== Scenario 3: End-to-end UDP -> MQTT -> HTTP ===")
        r = s3.run(NUM_MESSAGES, NUM_TOPICS)
        print_result(r)
        results.append(r)

    print("\n=== Summary ===")
    for r in results:
        print_result(r)

    total_lost = sum(r["lost"] for r in results)
    sys.exit(0 if total_lost == 0 else 1)


if __name__ == "__main__":
    main()
