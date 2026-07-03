"""
Scenario 1: UDP ingestion -> MQTT publish leg only.

Uses a topic the relay is NOT subscribed to, so this isolates the UDP-in ->
MQTT-out path without also triggering the MQTT -> HTTP forwarding leg.
"""
import time

import common

TOPIC_BASE = "loadtest/udpleg"


def run(num_messages: int, num_topics: int) -> dict:
    collector = common.MqttCollector(f"{TOPIC_BASE}/#", client_id="loadtest-udp-collector")
    collector.start()
    time.sleep(0.3)

    sock = common.udp_socket()
    sent = set()
    t0 = time.time()
    for i in range(num_messages):
        val = f"s1-{i}"
        topic = f"{TOPIC_BASE}/sensor{i % num_topics}"
        sent.add(val)
        common.send_udp(sock, f"publish {topic} {val}")
    send_duration = time.time() - t0

    common.wait_for_drain(collector.count, timeout=30, stable_for=1.5)
    received = set(collector.snapshot().keys())
    collector.stop()

    lost = sent - received
    return {
        "scenario": "udp_to_mqtt",
        "sent": len(sent),
        "received": len(received & sent),
        "lost": len(lost),
        "loss_pct": (100.0 * len(lost) / len(sent)) if sent else 0.0,
        "send_duration_s": round(send_duration, 3),
        "send_rate_msg_s": round(len(sent) / send_duration, 1) if send_duration > 0 else None,
        "sample_lost": sorted(lost)[:10],
    }
