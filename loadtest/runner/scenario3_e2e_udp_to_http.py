"""
Scenario 3: full end-to-end path.

UDP-in -> relay publishes to MQTT -> relay (subscribed) receives it back ->
relay forwards via HTTP to the miniserver mock. This exercises the whole
relay exactly like a real device speaking UDP to it would.
"""
import time

import common

TOPIC_BASE = "loadtest/cmd"


def run(num_messages: int, num_topics: int) -> dict:
    common.mock_reset()

    sock = common.udp_socket()
    sent = set()
    t0 = time.time()
    for i in range(num_messages):
        val = f"s3-{i}"
        topic = f"{TOPIC_BASE}/sensor{i % num_topics}"
        sent.add(val)
        common.send_udp(sock, f"publish {topic} {val}")
    send_duration = time.time() - t0

    def current_count() -> int:
        return common.mock_stats()["count"]

    common.wait_for_drain(current_count, timeout=60, stable_for=2.0)
    export = common.mock_export()
    received = {e["value"] for e in export}

    lost = sent - received
    return {
        "scenario": "e2e_udp_to_http",
        "sent": len(sent),
        "received": len(received & sent),
        "lost": len(lost),
        "loss_pct": (100.0 * len(lost) / len(sent)) if sent else 0.0,
        "send_duration_s": round(send_duration, 3),
        "send_rate_msg_s": round(len(sent) / send_duration, 1) if send_duration > 0 else None,
        "sample_lost": sorted(lost)[:10],
    }
