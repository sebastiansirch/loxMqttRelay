import json
import os
import socket
import threading
import time
import urllib.request

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
RELAY_UDP_HOST = os.environ.get("RELAY_UDP_HOST", "localhost")
RELAY_UDP_PORT = int(os.environ.get("RELAY_UDP_PORT", "11884"))
MOCK_HOST = os.environ.get("MOCK_HOST", "localhost")
MOCK_PORT = int(os.environ.get("MOCK_PORT", "8080"))


def udp_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def send_udp(sock: socket.socket, message: str) -> None:
    sock.sendto(message.encode("utf-8"), (RELAY_UDP_HOST, RELAY_UDP_PORT))


def mock_url(path: str) -> str:
    return f"http://{MOCK_HOST}:{MOCK_PORT}{path}"


def mock_reset() -> None:
    req = urllib.request.Request(mock_url("/_reset"), method="POST")
    urllib.request.urlopen(req, timeout=10).read()


def mock_stats() -> dict:
    with urllib.request.urlopen(mock_url("/_stats"), timeout=10) as r:
        return json.loads(r.read())


def mock_export() -> list:
    with urllib.request.urlopen(mock_url("/_export"), timeout=60) as r:
        return json.loads(r.read())


class MqttCollector:
    """Subscribes to a topic and thread-safely collects payloads of received messages."""

    def __init__(self, topic: str, client_id: str):
        self.topic = topic
        self.received: dict[str, float] = {}
        self._lock = threading.Lock()
        self._connected = threading.Event()
        self.client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        client.subscribe(self.topic, qos=0)
        self._connected.set()

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="ignore")
        with self._lock:
            self.received[payload] = time.time()

    def start(self, timeout: float = 15.0) -> None:
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.client.loop_start()
        if not self._connected.wait(timeout=timeout):
            raise RuntimeError(f"MQTT collector for '{self.topic}' failed to connect in time")

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()

    def count(self) -> int:
        with self._lock:
            return len(self.received)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self.received)


def wait_for_drain(get_count, timeout: float = 30.0, stable_for: float = 1.5, poll: float = 0.25) -> int:
    """Poll get_count() until it stops increasing for `stable_for` seconds, or timeout hits."""
    start = time.time()
    last = -1
    last_change = time.time()
    while time.time() - start < timeout:
        cur = get_count()
        if cur != last:
            last = cur
            last_change = time.time()
        elif time.time() - last_change >= stable_for:
            return cur
        time.sleep(poll)
    return last


def wait_for_pipeline_ready(timeout: float = 60.0) -> bool:
    """
    Confirms the full chain (UDP-in -> relay -> MQTT broker -> relay subscription)
    is actually up by round-tripping a warmup message through it, instead of
    guessing a fixed sleep.
    """
    collector = MqttCollector("loadtest/cmd/__warmup__", client_id="loadtest-warmup-collector")
    collector.start()
    sock = udp_socket()
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            send_udp(sock, "publish loadtest/cmd/__warmup__ ping")
            time.sleep(0.5)
            if collector.count() > 0:
                return True
        return False
    finally:
        collector.stop()
