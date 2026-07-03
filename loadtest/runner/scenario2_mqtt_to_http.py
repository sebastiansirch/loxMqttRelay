"""
Scenario 2: MQTT subscribe -> HTTP forward leg only.

Publishes directly to the broker (bypassing the relay's UDP intake) on a topic
the relay is subscribed to, then checks the miniserver mock received every
message via HTTP.
"""
import time

import paho.mqtt.client as mqtt

import common

TOPIC_BASE = "loadtest/cmd"


def run(num_messages: int, num_topics: int) -> dict:
    common.mock_reset()

    client = mqtt.Client(client_id="loadtest-mqtt-publisher", protocol=mqtt.MQTTv311)
    client.connect(common.MQTT_HOST, common.MQTT_PORT, keepalive=60)
    client.loop_start()

    sent = set()
    t0 = time.time()
    for i in range(num_messages):
        val = f"s2-{i}"
        topic = f"{TOPIC_BASE}/sensor{i % num_topics}"
        sent.add(val)
        client.publish(topic, val, qos=0)
    send_duration = time.time() - t0

    client.loop_stop()
    client.disconnect()

    def current_count() -> int:
        return common.mock_stats()["count"]

    common.wait_for_drain(current_count, timeout=60, stable_for=2.0)
    export = common.mock_export()
    received = {e["value"] for e in export}

    lost = sent - received
    return {
        "scenario": "mqtt_to_http",
        "sent": len(sent),
        "received": len(received & sent),
        "lost": len(lost),
        "loss_pct": (100.0 * len(lost) / len(sent)) if sent else 0.0,
        "send_duration_s": round(send_duration, 3),
        "send_rate_msg_s": round(len(sent) / send_duration, 1) if send_duration > 0 else None,
        "sample_lost": sorted(lost)[:10],
    }
